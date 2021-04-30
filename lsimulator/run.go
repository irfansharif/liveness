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

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/irfansharif/liveness"
	"golang.org/x/sync/errgroup"
)

// TODO(irfansharif): It'd be cool to generate a gif-like form of the
// simulation, similar to Spencer's gossipsim.

type network struct {
	time  int64
	log   *log.Logger
	nodes []*node
}

func (n *network) tick() {
	n.time += 1
	if n.time%100 == 0 {
		n.log.Printf("t=%d: ticked!", n.time)
	}

	// Process the inbox for all nodes.
	for _, n := range n.nodes {
		n.Inbox(n.inbox...)
		n.inbox = n.inbox[:0]
	}

	// Tick each node.
	for _, n := range n.nodes {
		n.Tick()
	}

	// Process the outbox for each node.
	for _, n := range n.nodes {
		n.network.deliver(n.Outbox()...)
	}
}

func (n *network) deliver(msgs ...liveness.Message) {
	for _, m := range msgs {
		n.nodes[m.To-1].inbox = append(n.nodes[m.To-1].inbox, m) // IDs are 1-indexed
	}
}

type node struct {
	log *log.Logger
	*network

	liveness.Liveness
	inbox []liveness.Message
}

func (s *simulator) run(nodes int, fdetector string, duration time.Duration) error {
	s.log.Printf("running with %d nodes, using %s; simulating %s", nodes, fdetector, duration)

	n := &network{
		log: log.New(s.log.Writer(), "NETW: ", 0),
	}

	for i := 1; i <= nodes; i++ {
		var opts []liveness.Option
		opts = append(opts, liveness.WithID(liveness.ID(i)))
		opts = append(opts, liveness.WithImpl(fdetector))
		if i == 1 {
			opts = append(opts, liveness.WithBootstrap())
		}

		n := &node{
			log:      log.New(s.log.Writer(), fmt.Sprintf("N%03d: ", i), 0),
			network:  n,
			Liveness: liveness.Start(opts...),
		}
		n.nodes = append(n.nodes, n)
	}

	// Use an errgroup with a cancellable context to simulate until either
	// (a) the "program" has finished executing, or
	// (b) we've run out of time to simulate.
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { // the "program"
		defer cancel()

		n1 := n.nodes[0] // add all nodes to n1 (works as long as we're adding to an already initialized node)
		for _, nX := range n.nodes[1:] {
			if err := n1.Add(ctx, nX.ID()); err != nil {
				return err
			}
		}

		var members []liveness.ID
		for {
			if ms := n1.Members(); len(ms) == nodes {
				members = ms
				break
			}
		}
		n.log.Printf("N%03d: members: %s", n1.ID(), members)

		// Loop until the membership view has settled down across all nodes.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if err := func() error {
				for _, nX := range n.nodes[1:] {
					membersFromElsewhere := nX.Members()
					if len(membersFromElsewhere) != len(members) {
						return fmt.Errorf("mismatched view of membership between N%03d and N%03d; expected %d members, got %d",
							n1.ID(), nX.ID(), len(members), len(membersFromElsewhere))
					}
					for i := range membersFromElsewhere {
						if membersFromElsewhere[i] != members[i] {
							return fmt.Errorf("mismatched view of membership between N%03d and N%03d; expected %s got %s",
								n1.ID(), nX.ID(), members, membersFromElsewhere)
						}
					}
				}
				return nil
			}(); err != nil {
				n.log.Printf("waiting: %s", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			break
		}

		for _, member := range members {
			live, _ := n1.Live(member)
			n.log.Printf("    N%03d: member=%s: live=%t", n1.ID(), member, live)
		}
		n.log.Printf("t=%d: done!", n.time)
		return nil
	})

	g.Go(func() error { // the simulation runner
		defer cancel()

		const dilation int64 = 1
		numTicks := duration.Milliseconds() / dilation
		ticker := time.NewTicker(time.Duration(dilation) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				n.tick()
				if n.time == numTicks {
					return nil
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	if err := g.Wait(); err != nil && err != context.Canceled {
		return err
	}
	return nil
}
