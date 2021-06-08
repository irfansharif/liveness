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
	"os"
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

	Start(
		WithID(ID(1)),
		WithImpl("raft"),
		WithBootstrap(),
	).Teardown()
}

// TestRaftImplIllegalID tests that we appropriately panic when instantiating
// liveness modules with illegal IDs.
func TestRaftImplIllegalID(t *testing.T) {
	defer leaktest.Check(t)()

	testutils.ShouldPanic(t, "invalid ID: 0", func() {
		Start(
			WithID(ID(0)),
			WithImpl("raft"),
			WithBootstrap(),
		)
	})
}

// TestRaftImplIllegalCentral tests that we appropriately panic when
// instantiating a liveness module to be bootstrapped, yet not including it in
// the central group.
func TestRaftImplIllegalCentral(t *testing.T) {
	defer leaktest.Check(t)()

	testutils.ShouldPanic(t, "bootstrap member 1 not found in central membership [liveness-id=2]", func() {
		Start(
			WithID(ID(1)),
			WithImpl("raft"),
			WithBootstrap(),
			WithCentral(2),
		)
	})
}

// TestRaftImplMisroutedMessage asserts that misrouted liveness messages result
// in panics.
func TestRaftImplMisroutedMessage(t *testing.T) {
	defer leaktest.Check(t)()

	l := Start(
		WithID(ID(1)),
		WithImpl("raft"),
		WithBootstrap(),
	)
	defer l.Teardown()

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
	l := Start(
		WithID(ID(1)),
		WithImpl("raft"),
		WithBootstrap(),
		withMockRaftNode(mrn),
	)
	defer l.Teardown()

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

func TestRaftImplID(t *testing.T) {
	defer leaktest.Check(t)()

	id := ID(42)
	l := Start(
		WithID(id),
		WithImpl("raft"),
		WithBootstrap(),
	)

	// Sanity check our ID.
	assert.Equal(t, id, l.ID())
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

		l := Start(opts...) // closed by the network teardown below
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

	n.partitionDeprecated(downMember)
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

	n.healDeprecated(downMember)
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
	t.Skipf("we're no longer detecting insta node restarts as failures; will need epochs")

	const numMembers = 10
	const bootstrapMember, restartedMember ID = 1, 3

	n := newNetwork()
	var bootstrapped Liveness
	for i := 1; i <= numMembers; i++ {
		opts := []Option{WithID(ID(i)), WithImpl("raft"), WithLoggingTo(os.Stderr)}
		if ID(i) == bootstrapMember {
			opts = append(opts, WithBootstrap())
		}

		l := Start(opts...) // closed by the network teardown below
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
				return fmt.Errorf("%d: expected to find liveness status for id=%s", l.ID(), restartedMember)
			}
			if live {
				return fmt.Errorf("%d: expected to find id=%s as non-live", l.ID(), restartedMember)
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

		l := Start(opts...) // closed by the network teardown below
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
			if l.ID() == removedMember {
				continue
			}

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

		l := Start(opts...) // closed by the network teardown below
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

func TestRaftImplCentralized(t *testing.T) {
	defer leaktest.Check(t)()

	for _, tc := range []struct {
		bootstrap, members int
		central            []int
	}{
		{1, 5, []int{1, 2, 3}},
		{2, 100, []int{2, 3, 10}},
	} {
		tname := fmt.Sprintf("bootstrap=%d,members=%d,central=%v", tc.bootstrap, tc.members, tc.central)
		t.Run(tname, func(t *testing.T) {
			var cluster []Liveness
			var bootstrapped Liveness
			for i := 1; i <= tc.members; i++ {
				var central []ID
				for _, m := range tc.central {
					central = append(central, ID(m))
				}
				opts := []Option{WithID(ID(i)), WithImpl("raft"), WithCentral(central...)}
				if i == tc.bootstrap {
					opts = append(opts, WithBootstrap())
				}

				l := Start(opts...)
				defer l.Teardown()

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
