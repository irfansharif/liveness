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
	"io"

	"github.com/etcd-io/etcd/raft"
)

// Message is the unit of data transmitted between members in the cluster. These
// could be messages that capture heartbeats, membership changes, etc.
type Message struct {
	To, From ID
	Content  interface{}
}

// Liveness is a decentralized failure detector. It's the stand-alone module
// that's to be connected with other instances of the same type. It's typically
// embedded into something resembling a member of a cluster, where individual
// members are identified using unique IDs (as guaranteed by the caller).
//
// Members communicate with one another using Messages. The transmission of
// these messages is left up to the caller, which lets this package be portable
// to any existing system. Liveness does not use any internal timers. Instead it
// understands the passage of time through successive Tick()s. How frequently
// that is called is also left to the caller (typically every 10ms), but comes
// with the benefit that it's a more deterministic and testable.
//
// Liveness modules depend on stable storage for continuity across member
// restarts. It's used to persist the assigned ID, last known
// peer/membership-list, etc.
type Liveness interface {
	// ID returns the module's own identifier.
	ID() ID

	// Add adds the identified member to the cluster.
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

	// Teardown tears down the liveness module and frees up any internal
	// resources (typically goroutines).
	Teardown()
}

// ID is used to identify an individual member in the cluster. The caller is
// responsible for guaranteeing that these are unique.
type ID uint64

func (id ID) String() string {
	return fmt.Sprintf("%d", id)
}

// Storage is the medium through which a given liveness module interfaces with
// stable storage. The data stored within is used to re-initialize liveness
// modules after service restarts. This is where the assigned ID and last known
// peer/membership-list is stored.
//
// TODO(irfansharif): Generalize this beyond just raft storage, and write
// adaptors for test-only in-memory instances that wrap around
// raft.MemoryStorage?
type Storage interface {
	raft.Storage
}

// L is a concrete implementation of the Liveness interface.
type L struct {
	Liveness

	// User provided options.
	id           ID
	impl         string
	central      map[ID]struct{}
	loggingTo    io.Writer
	bootstrap    bool
	storage      Storage
	mockRaftNode raft.Node
}

// Option is used to configure a new liveness module.
type Option func(l *L)

// Start starts a liveness modules using the provided configuration options.
func Start(opts ...Option) *L {
	l := &L{}
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
	return func(l *L) {
		l.id = id
	}
}

// WithLoggingTo instructs the liveness module to log to the given io.Writer.
func WithLoggingTo(w io.Writer) Option {
	return func(l *L) {
		l.loggingTo = w
	}
}

// WithBootstrap instructs the liveness module to bootstrap the cluster. Only
// one liveness module per cluster is to be bootstrapped, just once, and
// subsequent members are added to it (also transitively).
//
// TODO(irfansharif): This API is making up for the deficiency in the raft impl,
// where we have to start off either with a consensus group of one node, or in
// that waiting peer state. Ideally we'd be able to simply l.Bootstrap() a
// brother. Also, does this API generalize to other implementations?
func WithBootstrap() Option {
	return func(l *L) {
		l.bootstrap = true
	}
}

// WithImpl configures the specific liveness implementation to use.
func WithImpl(impl string) Option {
	switch impl {
	case "raft":
		return func(l *L) {
			l.impl = impl
		}
	default:
		panic(fmt.Sprintf("unrecognized impl: %s", impl))
	}
}

// WithStorage configures the liveness module to use the provided stable storage.
func WithStorage(storage Storage) Option {
	return func(l *L) {
		l.storage = storage
	}
}

// WithCentral configures the liveness module to maintain a consensus group only
// among the specified members.
//
// TODO(irfansharif): When used as a library, it'd be more usable if we could
// specify a group size here instead. We'd add the first N members as part of
// the consensus group, and the remaining as learners. When one of the N members
// is removed, we could transparently promote a learner if one was available.
func WithCentral(members ...ID) Option {
	return func(l *L) {
		central := make(map[ID]struct{})
		for _, m := range members {
			central[m] = struct{}{}
		}
		l.central = central
	}
}

// withMockRaftNode is a testing helper to mock out raft.Node.
func withMockRaftNode(mock raft.Node) Option {
	return func(l *L) {
		l.mockRaftNode = mock
	}
}
