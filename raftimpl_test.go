// Copyright 2021 Irfan Sharif.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package liveness

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/etcd-io/etcd/raft"
	"github.com/fortytw2/leaktest"
	"github.com/irfansharif/liveness/testutils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

// TestRaftImplClose tests to see that we're not leaking any goroutines on
// close.
func TestRaftImplClose(t *testing.T) {
	defer leaktest.Check(t)()

	New(
		WithID(ID(1)),
		WithImpl("raft"),
		WithBootstrap(),
	).Close()
}

// TestRaftImplIllegalID tests that we appropriately panic when instantiating
// liveness modules with illegal IDs.
func TestRaftImplIllegalID(t *testing.T) {
	defer leaktest.Check(t)()

	testutils.ShouldPanic(t, "invalid ID: 0", func() {
		New(
			WithID(ID(0)),
			WithImpl("raft"),
			WithBootstrap(),
		)
	})
}

// TestRaftImplMisroutedMessage asserts that misrouted liveness messages result
// in panics.
func TestRaftImplMisroutedMessage(t *testing.T) {
	defer leaktest.Check(t)()

	l := New(
		WithID(ID(1)),
		WithImpl("raft"),
		WithBootstrap(),
	)
	defer l.Close()

	testutils.ShouldPanic(t, "misrouted messaged, intended for 42 but sent to 1", func() {
		l.Inbox(Message{To: 42})
	})
}

type mockRaftNode struct {
	readyc  chan raft.Ready
	stepped []raftpb.Message

	raft.Node
}

func (m *mockRaftNode) Tick() {}

func (m *mockRaftNode) Ready() <-chan raft.Ready {
	return m.readyc
}

func (m *mockRaftNode) Stop() {}

func (m *mockRaftNode) Step(_ context.Context, msg raftpb.Message) error {
	m.stepped = append(m.stepped, msg)
	return nil
}

func (m *mockRaftNode) Advance() {}

// TestRaftImplTick tests the various responsibilities of liveness.Tick(). These
// include reading staged messages, generating outbound ones, and applying raft
// proposals.
func TestRaftImplTick(t *testing.T) {
	defer leaktest.Check(t)()

	mrn := &mockRaftNode{
		readyc: make(chan raft.Ready, 1),
	}
	l := New(
		WithID(ID(1)),
		WithImpl("raft"),
		WithBootstrap(),
		withMockRaftNode(mrn),
	)
	defer l.Close()

	impl := l.Liveness.(*raftImpl)

	// Ensure Tick steps through the inboxed message.
	l.Inbox(Message{
		To: l.ID(), From: l.ID(),
		Content: raftpb.Message{},
	})
	assert.Len(t, mrn.stepped, 0)
	assert.Len(t, impl.mu.inbox, 1)
	l.Tick()
	assert.Len(t, mrn.stepped, 1)
	assert.Len(t, impl.mu.inbox, 0)

	// Feed in a raft.Ready with an outbound message, and ensure Tick slates it
	// for delivery.
	mrn.readyc <- raft.Ready{
		Messages: []raftpb.Message{
			{To: uint64(l.ID()), From: uint64(l.ID())},
		},
	}
	testutils.SucceedsSoon(t, func() error {
		if len(impl.mu.raftReady) == 0 {
			return fmt.Errorf("raft ready message not unstaged")
		}
		return nil
	})
	assert.Len(t, l.Outbox(), 0)
	assert.Len(t, impl.mu.raftReady, 1)
	l.Tick()
	assert.Len(t, l.Outbox(), 1)
	assert.Len(t, impl.mu.raftReady, 0)

	// Feed in a raft.Ready with a committed entry, and ensure Tick applies it
	// to the in-memory state.
	payload, err := json.Marshal(proposal{Member: 42, Op: add})
	assert.NoError(t, err)

	mrn.readyc <- raft.Ready{
		CommittedEntries: []raftpb.Entry{
			{Type: raftpb.EntryNormal, Data: payload},
		},
	}
	testutils.SucceedsSoon(t, func() error {
		if len(impl.mu.raftReady) == 0 {
			return fmt.Errorf("raft ready message not unstaged")
		}
		return nil
	})
	assert.Len(t, l.Members(), 0) // the mock raft node means that we're not proposing our own membership
	l.Tick()
	assert.Len(t, l.Members(), 1)
	assert.Equal(t, l.Members()[0], ID(42))
}

// TestRaftImplSingleMembership tests basic invariants of a single-member
// liveness cluster (the module ID, and view of cluster membership).
func TestRaftImplSingleMembership(t *testing.T) {
	defer leaktest.Check(t)()

	n := newNetwork()
	id := ID(1)
	l := New(
		WithID(id),
		WithImpl("raft"),
		WithBootstrap(),
	)
	n.ls = append(n.ls, l)

	// Sanity check our ID.
	assert.Equal(t, id, l.ID())

	teardown := n.tick(t)
	defer teardown()

	// Check that we appear as live to ourself.
	testutils.SucceedsSoon(t, func() error {
		live, found := l.Live(id)
		if !found {
			return fmt.Errorf("expected to find self")
		}
		if !live {
			return fmt.Errorf("expected to find self as live")
		}
		return nil
	})

	// Check that the membership is what we expect.
	testutils.SucceedsSoon(t, func() error {
		members := l.Members()
		if numMembers := len(members); numMembers != 1 {
			return fmt.Errorf("expected # members == 1; got %d", numMembers)
		}
		if members[0] != id {
			return fmt.Errorf("expected membership == [%d]; got %s", id, members)
		}
		return nil
	})
}

// TestRaftImplMultipleAdd tests the construction of a multi-node cluster,
// verifying a stable view of cluster membership across all nodes.
func TestRaftImplMultipleAdd(t *testing.T) {
	defer leaktest.Check(t)()

	for _, tc := range []struct {
		bootstrap, members int
	}{
		{1, 1},
		{2, 2},
		{3, 3},
		{2, 5},
		{7, 10},
	} {
		t.Run(fmt.Sprintf("bootstrap=%d,members=%d", tc.bootstrap, tc.members), func(t *testing.T) {
			var cluster []Liveness
			var bootstrapped Liveness
			for i := 1; i <= tc.members; i++ {
				opts := []Option{WithID(ID(i)), WithImpl("raft")}
				if i == tc.bootstrap {
					opts = append(opts, WithBootstrap())
				}

				l := New(opts...)
				defer l.Close()

				cluster = append(cluster, l)
				if i == tc.bootstrap {
					bootstrapped = l
				}
			}

			teardown := tick(t, cluster...)
			defer teardown()

			for _, l := range cluster {
				if l.ID() == bootstrapped.ID() {
					continue
				}
				assert.NoError(t, bootstrapped.Add(context.Background(), l.ID()))
			}

			// Check that the membership is what we expect, and visible from
			// every node.
			testutils.SucceedsSoon(t, func() error {
				for _, l := range cluster {
					members := l.Members()
					if len(members) != tc.members {
						return fmt.Errorf("expected # members (from id=%s) == %d; got %d",
							l.ID(), tc.members, len(members))
					}

					for i, member := range members {
						if member != cluster[i].ID() {
							return fmt.Errorf("expected members[%d] == %s; got %s",
								i, cluster[i].ID(), member)
						}
					}
				}
				return nil
			})
		})
	}
}

// TestRaftImplMultipleReplive tests the deconstruction of a multi-node cluster,
// verifying a stable view of cluster membership across all nodes.
func TestRaftImplMultipleRemove(t *testing.T) {
	defer leaktest.Check(t)()

	for _, tc := range []struct {
		initial, remaining int
	}{
		{5, 2},
		{5, 1},
		{3, 2},
		{3, 3},
		{7, 4},
	} {
		t.Run(fmt.Sprintf("initial=%d,remaining=%d", tc.initial, tc.remaining), func(t *testing.T) {
			var cluster []Liveness
			for i := 1; i <= tc.initial; i++ {
				opts := []Option{WithID(ID(i)), WithImpl("raft")}
				if i == 1 {
					opts = append(opts, WithBootstrap())
				}

				l := New(opts...)
				defer l.Close()

				cluster = append(cluster, l)
			}

			l1 := cluster[0]
			teardown := tick(t, cluster...)
			defer teardown()

			// Add a bunch of nodes, and then size down again back to the remaining
			// members.
			for _, l := range cluster[1:] {
				assert.NoError(t, l1.Add(context.Background(), l.ID()))
			}

			for _, l := range cluster[tc.remaining:] {
				assert.NoError(t, l1.Remove(context.Background(), l.ID()))
			}

			// Check that the membership is what we expect (nothing but the remaining
			// members, visible only from the remaining ones).
			testutils.SucceedsSoon(t, func() error {
				for _, l := range cluster[:tc.remaining] {
					members := l.Members()
					if len(members) != tc.remaining {
						return fmt.Errorf("expected # members (from id=%s) == %d; got %d %s",
							l1.ID(), tc.remaining, len(members), members)
					}
				}

				for _, l := range cluster[tc.remaining:] {
					members := l.Members()
					if len(members) != 0 {
						return fmt.Errorf("expected # members (from id=%s) == 0; got %d %s", l.ID(), len(members), members)
					}
				}
				return nil
			})
		})
	}
}

func TestRaftImplPartitionedNode(t *testing.T) {
	defer leaktest.Check(t)()

	const numMembers = 10
	const bootstrapMember, downMember ID = 1, 1

	n := newNetwork()
	var bootstrapped Liveness
	for i := 1; i <= numMembers; i++ {
		opts := []Option{WithID(ID(i)), WithImpl("raft")}
		if ID(i) == bootstrapMember {
			opts = append(opts, WithBootstrap())
		}

		l := New(opts...) // closed by the network teardown below
		n.ls = append(n.ls, l)
		if ID(i) == bootstrapMember {
			bootstrapped = l
		}
	}

	teardown := n.tick(t)
	defer teardown()

	for _, l := range n.ls {
		if l.ID() == bootstrapped.ID() {
			continue
		}
		assert.NoError(t, bootstrapped.Add(context.Background(), l.ID()))
	}

	n.partition(downMember)
	testutils.SucceedsSoon(t, func() error {
		for _, l := range n.ls {
			live, found := l.Live(downMember)
			if !found {
				return fmt.Errorf("expected to find liveness status for id=%s", downMember)
			}
			if live {
				return fmt.Errorf("expected to find id=%s as non-live", downMember)
			}
		}
		return nil
	})

	n.heal(downMember)
	testutils.SucceedsSoon(t, func() error {
		for _, l := range n.ls {
			live, found := l.Live(downMember)
			if !found {
				return fmt.Errorf("expected to find liveness status for id=%s", downMember)
			}
			if !live {
				return fmt.Errorf("expected to find id=%s as live", downMember)
			}
		}
		return nil
	})
}

func TestRaftImplRestartedNode(t *testing.T) {
	defer leaktest.Check(t)()

	const numMembers = 10
	const bootstrapMember, restartedMember ID = 1, 3

	n := newNetwork()
	var bootstrapped Liveness
	for i := 1; i <= numMembers; i++ {
		opts := []Option{WithID(ID(i)), WithImpl("raft")}
		if ID(i) == bootstrapMember {
			opts = append(opts, WithBootstrap())
		}

		l := New(opts...) // closed by the network teardown below
		n.ls = append(n.ls, l)
		if ID(i) == bootstrapMember {
			bootstrapped = l
		}
	}

	teardown := n.tick(t)
	defer teardown()

	for _, l := range n.ls {
		if l.ID() == bootstrapped.ID() {
			continue
		}
		assert.NoError(t, bootstrapped.Add(context.Background(), l.ID()))
	}

	n.stop(restartedMember)
	n.restart(restartedMember)
	testutils.SucceedsSoon(t, func() error {
		for _, l := range n.ls {
			live, found := l.Live(restartedMember)
			if !found {
				return fmt.Errorf("expected to find liveness status for id=%s", restartedMember)
			}
			if live {
				return fmt.Errorf("expected to find id=%s as non-live", restartedMember)
			}
		}
		return nil
	})
}

func TestRaftImplRemovePartitionedNode(t *testing.T) {
	defer leaktest.Check(t)()

	const numMembers = 10
	const bootstrapMember, removedMember ID = 1, 3

	n := newNetwork()
	var bootstrapped Liveness
	for i := 1; i <= numMembers; i++ {
		opts := []Option{WithID(ID(i)), WithImpl("raft")}
		if ID(i) == bootstrapMember {
			opts = append(opts, WithBootstrap())
		}

		l := New(opts...) // closed by the network teardown below
		n.ls = append(n.ls, l)
		if ID(i) == bootstrapMember {
			bootstrapped = l
		}
	}

	teardown := n.tick(t)
	defer teardown()

	for _, l := range n.ls {
		if l.ID() == bootstrapped.ID() {
			continue
		}
		assert.NoError(t, bootstrapped.Add(context.Background(), l.ID()))
	}

	n.stop(removedMember)
	testutils.SucceedsSoon(t, func() error {
		for _, l := range n.ls {
			live, found := l.Live(removedMember)
			if !found {
				return fmt.Errorf("expected to find liveness status for id=%s", removedMember)
			}
			if live {
				return fmt.Errorf("expected to find id=%s as non-live", removedMember)
			}
		}
		return nil
	})

	assert.NoError(t, bootstrapped.Remove(context.Background(), removedMember))
	testutils.SucceedsSoon(t, func() error {
		for _, l := range n.ls {
			if l.ID() == removedMember {
				continue
			}

			_, found := l.Live(removedMember)
			if found {
				return fmt.Errorf("expected to not find liveness status for id=%s; membership (from %d): %s", removedMember, l.ID(), l.Members())
			}
		}
		return nil
	})

	n.restart(removedMember)

	testutils.SucceedsSoon(t, func() error {
		l := n.get(removedMember)
		if len(l.Members()) != 0 {
			return fmt.Errorf("expected to find empty membership, found %s", l.Members())
		}
		return nil
	})
}

// TestRaftImplMembersAppearNonLiveAfterQuickRestart checks to see that after a
// node-restart, all peer nodes appear non-live to the just-restarted node until
// they've individually refreshed once again.
func TestRaftImplMembersAppearNonLiveAfterQuickRestart(t *testing.T) {
	defer leaktest.Check(t)

	const numMembers = 2
	const bootstrapMember, stoppedMember ID = 1, 2

	n := newNetwork()
	var bootstrapped Liveness
	for i := 1; i <= numMembers; i++ {
		opts := []Option{WithID(ID(i)), WithImpl("raft")}
		if ID(i) == bootstrapMember {
			opts = append(opts, WithBootstrap())
		}

		l := New(opts...) // closed by the network teardown below
		n.ls = append(n.ls, l)
		if ID(i) == bootstrapMember {
			bootstrapped = l
		}
	}

	teardown := n.tick(t)
	defer teardown()

	for _, l := range n.ls {
		if l.ID() == bootstrapped.ID() {
			continue
		}
		assert.NoError(t, bootstrapped.Add(context.Background(), l.ID()))
	}

	testutils.SucceedsSoon(t, func() error {
		live, found := n.get(stoppedMember).Live(bootstrapMember)
		if !found {
			return fmt.Errorf("expected to find liveness status for id=%s", bootstrapMember)
		}
		if !live {
			return fmt.Errorf("expected to find id=%s as live", bootstrapMember)
		}
		return nil
	})

	n.stop(bootstrapMember)
	n.stop(stoppedMember)
	n.restart(stoppedMember)

	// TODO(irfansharif): This test wants to start off stoppedMember with a
	// heartbeat increment of inf, else it could be testing bootstrapMember's
	// heartbeat expiration.

	// live, found := n.get(stoppedMember).Live(bootstrapMember)
	// if !found {
	// 	t.Fatalf("expected to find liveness status for id=%s", bootstrapMember)
	// }
	// if live {
	// 	t.Fatalf("expected to find id=%s as non-live", bootstrapMember)
	// }

	testutils.SucceedsSoon(t, func() error {
		live, found := n.get(stoppedMember).Live(bootstrapMember)
		if !found {
			return fmt.Errorf("expected to find liveness status for id=%s", bootstrapMember)
		}
		if live {
			return fmt.Errorf("expected to find id=%s as non-live", bootstrapMember)
		}
		return nil
	})
}

func newNetwork() *network {
	return &network{
		ls:          make([]Liveness, 0),
		partitioned: make(map[ID]struct{}),
		stopped:     make(map[ID]struct{}),
	}
}

type network struct {
	sync.Mutex
	ls []Liveness

	partitioned map[ID]struct{}
	stopped     map[ID]struct{}
}

func (n *network) get(member ID) Liveness {
	return n.ls[int(member)-1]
}

func (n *network) set(member ID, l Liveness) {
	n.ls[int(member)-1] = l
}

func (n *network) stop(member ID) {
	n.Lock()
	defer n.Unlock()

	n.stopped[member] = struct{}{}
}

func (n *network) restart(member ID) {
	n.Lock()
	defer n.Unlock()

	old := n.get(member)
	old.Close()
	impl := old.(*liveness).Liveness.(*raftImpl)
	l := New(
		WithID(member),
		WithImpl("raft"),
		WithStorage(impl.raft.storage),
	)
	n.set(member, l)

	delete(n.stopped, member)
}

func (n *network) partition(member ID) {
	n.Lock()
	defer n.Unlock()

	n.partitioned[member] = struct{}{}
}

func (n *network) heal(member ID) {
	n.Lock()
	defer n.Unlock()

	delete(n.partitioned, member)
}

func (n *network) tick(t *testing.T) (teardown func() error) {
	for i, l := range n.ls {
		if got, exp := l.ID(), ID(i+1); got != exp {
			t.Fatalf("malformed cluster list; expected ls[%d]:%s got %s", i, exp, got)
		}
	}

	// Set up the liveness tickers + message delivery service in a separate
	// thread.
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			n.Lock()
			for _, l := range n.ls {
				if _, ok := n.partitioned[l.ID()]; ok {
					continue // don't bother processing the outbox
				}

				for _, msg := range l.Outbox() {
					if _, ok := n.partitioned[msg.To]; ok {
						continue // don't bother delivering to it
					}

					n.ls[msg.To-1].Inbox(msg)
				}
			}
			n.Unlock()

			for _, l := range n.ls {
				if _, ok := n.stopped[l.ID()]; ok {
					continue // don't bother ticking this node
				}
				l.Tick()
			}

			time.Sleep(10 * time.Millisecond)
		}
	})

	return func() error {
		cancel()
		if err := g.Wait(); err != nil && err != context.Canceled {
			return err
		}

		n.Lock()
		for _, l := range n.ls {
			l.Close()
		}
		n.Unlock()
		return nil
	}
}

func tick(t *testing.T, ls ...Liveness) (teardown func() error) {
	for i, l := range ls {
		if got, exp := l.ID(), ID(i+1); got != exp {
			t.Fatalf("malformed cluster list; expected ls[%d]:%s got %s", i, exp, got)
		}
	}

	// Set up the liveness tickers + message delivery service in a separate
	// thread.
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			for _, l := range ls {
				for _, msg := range l.Outbox() {
					ls[msg.To-1].Inbox(msg)
				}
			}
			for _, l := range ls {
				l.Tick()
			}

			time.Sleep(10 * time.Millisecond)
		}
	})

	return func() error {
		cancel()
		return g.Wait()
	}
}
