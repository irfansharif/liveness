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
	"sort"
)

// Liveness is a decentralized failure detector. It's the stand-alone module
// that's to be connected with other instances of the same type. It's typically
// be embedded into into something resembling a "member" of a cluster, where
// individual members are identified using unique IDs.
//
// Members communicate with one another using liveness messages. The
// transmission of the messages is left up to the caller, which lets this
// package be portable to any distributed system.
//
// Liveness also does not use any internal timers. It only understands the
// passage of time through successive Tick()s. How frequently that is called is
// also left to the caller.
type Liveness struct {
	self    ID
	members map[ID]bool

	inbox  []Message
	outbox []Message
}

// ID is used to identify an individual member in the cluster.
type ID int

// Message is the unit of data transmitted between members in the cluster. These
// could be messages that capture heartbeats, membership changes, etc.
type Message struct {
	To, From ID
	Content  string
}

// Live returns whether or not the given member is considered live. It can also
// be used to determine a member's own view of its liveness.
func (l *Liveness) Live(member ID) (live, found bool) {
	live, found = l.members[member]
	return live, found
}

// Members retrieves the current known membership of the cluster. The members
// may or may not be live.
func (l *Liveness) Members() []ID {
	members := make([]ID, 0, len(l.members))
	for member := range l.members {
		members = append(members, member)
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i] < members[j]
	})
	return members
}

// Tick moves forward the failure detectors internal notion of time. The caller
// is responsible for calling it periodically.
func (l *Liveness) Tick() {
	l.outbox = l.outbox[:0] // Clear out our outbox.
	if len(l.inbox) == 0 {
		return
	}

	// Forward the entire set of messages we received to the next member over.
	for _, msg := range l.inbox {
		msg := msg
		msg.To = ID(int(l.self+1) % len(l.Members()))
		msg.From = l.self
		if msg.Content == "ping" {
			msg.Content = "pong"
		} else {
			msg.Content = "ping"
		}

		l.outbox = append(l.outbox, msg)
	}
	l.inbox = l.inbox[:0] // Clear out our inbox.
}

// Outbox returns a list of outbound messages.
func (l *Liveness) Outbox() []Message {
	outbox := make([]Message, len(l.outbox))
	copy(outbox, l.outbox)
	return outbox
}

// Inbox returns a channel through which a liveness module receives incoming
// messages.
func (l *Liveness) Inbox(msgs ...Message) {
	for _, msg := range msgs {
		l.inbox = append(l.inbox, msg)
	}
}

// Add adds the identified member to the cluster.
func (l *Liveness) Add(member ID) {
	l.members[member] = true
}

// Remove removes the identified member from the cluster.
func (l *Liveness) Remove(member ID) {
	delete(l.members, member)
}

// ID returns the module's own identifier.
func (l *Liveness) ID() ID {
	return l.self
}

// Option is used to configure a new liveness module.
type Option func(*Liveness)

// New instantiates a liveness modules using the provided configuration options.
func New(opts ...Option) *Liveness {
	l := &Liveness{
		inbox:   make([]Message, 0),
		outbox:  make([]Message, 0),
		members: make(map[ID]bool),
	}
	for _, opt := range opts {
		opt(l)
	}
	l.members[l.ID()] = true

	return l
}

// WithSelf is used to configure a liveness module with the given ID.
func WithSelf(id ID) Option {
	return func(l *Liveness) {
		l.self = id
	}
}
