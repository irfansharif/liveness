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
	"log"
)

// ID is used to identify an individual member in the cluster. The caller is
// responsible for guaranteeing that these are unique.
type ID int

func (id ID) String() string {
	return fmt.Sprintf("%d", id)
}

// Message is the unit of data transmitted between members in the cluster. These
// could be messages that capture heartbeats, membership changes, etc.
type Message struct {
	To, From ID
	Content  interface{}
}

// Liveness is a decentralized failure detector. It's the stand-alone module
// that's to be connected with other instances of the same type. It's typically
// be embedded into into something resembling a 'member' of a cluster, where
// individual members are identified using unique IDs.
//
// Members communicate with one another using liveness messages. The
// transmission of the messages is left up to the caller, which lets this
// package be portable to any distributed system.
//
// Liveness also does not use any internal timers. It understands the passage of
// time through successive Tick()s. How frequently that is called is also left
// to the caller.
//
// XXX: What's the story around restarting a node? It should probably want to
// hold onto the same ID. What about the peers? How does it know who to join?
// Does it rediscover everything from scratch? What about it's own view of
// membership? It can join if it wasn't kicked out, but can't if it was?
type Liveness interface {
	// ID returns the module's own identifier.
	ID() ID

	// Add adds the identified member to the cluster. This is a transitive
	// operation. Adding a member will also transitively propose other members it knows
	// about.
	Add(ctx context.Context, member ID) error

	// Remove removes the identified member from the cluster.
	Remove(ctx context.Context, member ID) error

	// Inbox accepts the incoming message.
	Inbox(msgs ...Message)

	// Outbox returns a list of outbound messages.
	Outbox() []Message

	// Tick moves forward the failure detectors internal notion of time. The caller
	// is responsible for calling it periodically.
	Tick()

	// Live returns whether or not the given member is considered live. It can
	// also be used to determine a member's own view of its liveness.
	Live(member ID) (live, found bool)

	// Members retrieves the current known membership of the cluster. The
	// members may or may not be live.
	Members() []ID

	// Close XXX:
	Close()
}

type liveness struct {
	Liveness

	id, seed ID
	logger   *log.Logger
}

// Option is used to configure a new liveness module.
type Option func(l *liveness)

// New instantiates a liveness modules using the provided configuration options.
func New(opts ...Option) *liveness {
	l := &liveness{}
	for _, opt := range opts {
		opt(l)
	}

	return l
}

// WithID is used to configure a liveness module with the given ID.
func WithID(id ID) Option {
	return func(l *liveness) {
		l.id = id
	}
}

// WithLogger is used to configure a liveness module with the given logger.
func WithLogger(logger *log.Logger) Option {
	return func(l *liveness) {
		l.logger = logger
	}
}

func WithSeed(id ID) Option {
	return func(l *liveness) {
		l.seed = id
	}
}

// WithImpl is used to swap out various liveness implementations.
// XXX: This is a bit weird, where the other options configure the liveness
// module, some of which is needed to configure the implementation, and so the
// ordering matters.
func WithImpl(impl string) Option {
	switch impl {
	case "raft":
		return func(l *liveness) {
			l.Liveness = newRaftImpl(l)
		}
	default:
		panic(fmt.Sprintf("unrecognized impl: %s", impl))
	}
}
