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
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

func newNetwork() *network {
	return &network{
		ls:          make([]Liveness, 0),
		partitioned: make(map[ID]struct{}),
		severed:     make(map[link]struct{}),
		stopped:     make(map[ID]struct{}),
	}
}

type link struct {
	from, to ID
}

type network struct {
	sync.Mutex
	ls []Liveness

	partitioned map[ID]struct{}
	severed     map[link]struct{}
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
	old.Teardown()
	impl := old.(*L).Liveness.(*raftImpl)
	l := Start(
		WithID(member),
		WithImpl("raft"),
		WithStorage(impl.raft.storage),
	)
	n.set(member, l)

	delete(n.stopped, member)
}

func (n *network) partitionDeprecated(member ID) {
	n.Lock()
	defer n.Unlock()

	n.partitioned[member] = struct{}{}
}

func (n *network) healDeprecated(member ID) {
	n.Lock()
	defer n.Unlock()

	delete(n.partitioned, member)
}

// partition reconfigures the network to abide by the provided partitions. Each
// partition in the list captures the set of IDs contained within that
// partition. The set of all nodes across all partitions must be the same as the
// set of nodes the cluster. Each partition must be disjoint, i.e. no same node
// can exist in two partitions.
func (n *network) partition(t *testing.T, partitions [][]ID) error {
	n.Lock()
	defer n.Unlock()

	// Ensure that partitions (a) encompass all nodes in the cluster, and (b)
	// are completely disjoint.
	set := make(map[ID]struct{})
	for _, partition := range partitions {
		for _, id := range partition {
			if _, ok := set[id]; ok {
				return fmt.Errorf("expected partitions %s to be disjoint, found %s in multiple",
					partitions, id)
			}
			set[id] = struct{}{}
		}
	}
	if len(n.ls) != len(set) {
		return fmt.Errorf("expected partitions %s to cover %d nodes, found only %d",
			partitions, len(n.ls), len(set))
	}
	for _, l := range n.ls {
		if _, ok := set[l.ID()]; !ok {
			return fmt.Errorf("expected partitions %s to cover all nodes, %s not found",
				partitions, l.ID())
		}
	}

	// Heal existing partitions (we're going to re-partition below).
	for _, a := range n.ls {
		for _, b := range n.ls {
			n.healLocked(a.ID(), b.ID())
		}
	}

	// For every node in a given partition, sever its link to every other node.
	for i, partitionA := range partitions {
		for j, partitionB := range partitions {
			if i == j {
				continue
			}

			for _, a := range partitionA {
				for _, b := range partitionB {
					n.severLocked(a, b)
				}
			}
		}
	}

	return nil
}

func (n *network) sever(from, to ID) {
	n.Lock()
	defer n.Unlock()

	n.severLocked(from, to)
}

func (n *network) severLocked(from, to ID) {
	n.severed[link{from, to}] = struct{}{}
}

func (n *network) heal(from, to ID) {
	n.Lock()
	defer n.Unlock()

	n.healLocked(from, to)
}

func (n *network) healLocked(from, to ID) {
	delete(n.severed, link{from, to})
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

					if _, ok := n.severed[link{msg.From, msg.To}]; ok {
						continue // don't bother delivering msg, the network link is severed
					}
					n.ls[msg.To-1].Inbox(msg)
				}
			}
			n.Unlock()

			for _, l := range n.ls {
				if _, ok := n.stopped[l.ID()]; ok {
					continue // don't bother ticking this node, it's stopped
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
			l.Teardown()
		}
		n.Unlock()
		return nil
	}
}
