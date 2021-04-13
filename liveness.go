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

	"github.com/etcd-io/etcd/raft"
)

// ID is used to identify an individual member in the cluster. The caller is
// responsible for guaranteeing that these are unique.
type ID uint64

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
// individual members are identified using unique IDs (guaranteed by the caller).
//
// Members communicate with one another using Messages. The transmission of
// these messages is left up to the caller, which lets this package be portable
// to any existing system. Liveness does not use any internal timers. It
// understands the passage of time through successive Tick()s. How frequently
// that is called is also left to the caller.
//
// XXX: We'll want to implement node restarts. It should probably want to hold
// onto the same ID (so defer to caller to guarantee that, something to be
// persisted to stable storage?). What about the peers? How does it know who to
// join? Does it rediscover everything from scratch? What about it's own view of
// membership? Actually, all of that happens implicitly by reading from stable
// raft storage.
//
// What about if it was kicked out in the interim? I think we'll want some kind
// of hand-shaking, right? It shouldn't be able to join if it was kicked out.
type Liveness interface {
	// ID returns the module's own identifier.
	ID() ID

	// Add adds the identified member to the cluster. This is a transitive
	// operation. Adding a member will also transitively propose other members
	// it knows about.
	Add(ctx context.Context, member ID) error

	// Remove removes the identified member from the cluster.
	Remove(ctx context.Context, member ID) error

	// Tick moves forward the failure detectors internal notion of time. The
	// caller is responsible for calling it periodically.
	Tick()

	// Live returns whether or not the given member is considered live. It can
	// also be used to determine a member's own view of its liveness.
	Live(member ID) (live, found bool)

	// Members retrieves the current known membership of the cluster. The
	// members may or may not be live.
	Members() []ID

	// Inbox informs the module of incoming incoming messages from elsewhere.
	Inbox(msgs ...Message)

	// Outbox returns a list of outbound messages to send to other modules.
	Outbox() []Message

	// Close tears down the liveness module and frees up any internal resources
	// (typically goroutines).
	Close()
}

type liveness struct {
	Liveness

	id        ID
	impl      string
	logger    *log.Logger
	bootstrap bool

	mockRaftNode    raft.Node
	fromRaftStorage raft.Storage
}

// Option is used to configure a new liveness module.
type Option func(l *liveness)

// New instantiates a liveness modules using the provided configuration options.
//
// TODO(irfansharif): This also "starts" the module by firing off the goroutines
// underneath. Rename?
func New(opts ...Option) *liveness {
	l := &liveness{}
	for _, opt := range opts {
		opt(l)
	}

	switch l.impl {
	case "raft":
		l.Liveness = startRaftImpl(l)
	default:
		panic(fmt.Sprintf("unrecognized impl: %s", l.impl))
	}

	return l
}

// WithID configures the liveness module with the given ID.
func WithID(id ID) Option {
	return func(l *liveness) {
		l.id = id
	}
}

// WithLogger instantiates the liveness module with the given logger.
func WithLogger(logger *log.Logger) Option {
	return func(l *liveness) {
		l.logger = logger
	}
}

// WithBootstrap instructs the liveness module to bootstrap the cluster. Only
// one liveness instance per cluster is to be bootstrapped, and subsequent
// members are added to it or to other members added to it.
//
// TODO(irfansharif): This API is making up for the deficiency in the raft impl,
// where we have to start off either with a consensus group of one node, or in
// that waiting peer state. Ideally we'd be able to simply l.Bootstrap() a
// brother. Also, does this API generalize to other implementations?
func WithBootstrap() Option {
	return func(l *liveness) {
		l.bootstrap = true
	}
}

// WithImpl configures the specific liveness implementation to use.
func WithImpl(impl string) Option {
	switch impl {
	case "raft":
		return func(l *liveness) {
			l.impl = impl
		}
	default:
		panic(fmt.Sprintf("unrecognized impl: %s", impl))
	}
}

func withMockRaftNode(mock raft.Node) Option {
	return func(l *liveness) {
		l.mockRaftNode = mock
	}
}

func withExistingRaftStorage(existing raft.Storage) Option {
	return func(l *liveness) {
		l.fromRaftStorage = existing
	}
}
