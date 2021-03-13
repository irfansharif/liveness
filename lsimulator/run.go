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
	"fmt"
	"log"
	"time"

	"github.com/irfansharif/liveness"
)

type cluster struct {
	log   *log.Logger
	nodes []*node
}

func (c *cluster) tick() {
	c.log.Print("ticked")
	// Transmit the set of outbound messages from each node to the intended
	// destination.
	for _, n := range c.nodes {
		n.tick()
	}
}

func (c *cluster) deliver(m msg) {
	c.nodes[m.to].inbox = append(c.nodes[m.to].inbox, m)
}

type node struct {
	c   *cluster
	log *log.Logger

	*liveness.Liveness
	inbox []msg
}

type msg struct {
	to, from int
	content  string
}

func (n *node) tick() {
	// Process the set of inbound messages.
	for _, m := range n.inbox {
		n.log.Printf("received %q from %d", m.content, m.from)

		lmsg := liveness.Message{
			To: liveness.ID(m.to), From: liveness.ID(m.from),
			Content: m.content,
		}
		n.Inbox(lmsg)
	}
	n.inbox = n.inbox[:0]

	n.Tick()

	// Deliver the set of outbound messages.
	for _, lmsg := range n.Outbox() {
		m := msg{
			to:      int(lmsg.To),
			from:    int(lmsg.From),
			content: lmsg.Content,
		}
		n.c.deliver(m)
	}
}

func (s *simulator) run(nodes int, fdetector string, duration time.Duration) error {
	s.log.Printf("running with %d nodes, using %s; simulating %s", nodes, fdetector, duration)

	c := &cluster{}
	c.log = log.New(s.log.Writer(), "CLUS: ", 0)
	for i := 0; i < nodes; i++ {
		n := &node{
			c:        c,
			Liveness: liveness.New(liveness.WithSelf(liveness.ID(i))),
		}
		n.log = log.New(s.log.Writer(), fmt.Sprintf("N%03d: ", i), 0)
		c.nodes = append(c.nodes, n)
	}

	// Create a fully connected network of nodes.
	for _, n := range c.nodes {
		for _, m := range c.nodes {
			if n.ID() == m.ID() {
				continue
			}

			n.Add(m.ID())
		}
	}

	// Seed the cluster with the first message.
	c.deliver(msg{to: 0, from: 0, content: "pong"})
	for tick := 0; tick < int(duration.Seconds()); tick++ {
		c.tick()
	}
	return nil
}
