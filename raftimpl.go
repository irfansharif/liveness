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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/etcd-io/etcd/raft"
)

type raftImpl struct {
	l *liveness

	mu struct {
		sync.Mutex
		members map[ID]bool // member: liveness
		inbox   []Message
		outbox  []Message
		closed  bool

		raftReady []raft.Ready
	}

	raft struct {
		node    raft.Node
		storage *raft.MemoryStorage
	}

	closeCh chan struct{}
}

func (r *raftImpl) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.closed {
		return
	}

	close(r.closeCh)
	r.mu.closed = true
}

func (r *raftImpl) Live(member ID) (live, found bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	live, found = r.mu.members[member]
	return live, found
}

func (r *raftImpl) Members() []ID {
	r.mu.Lock()
	defer r.mu.Unlock()

	members := make([]ID, 0, len(r.mu.members))
	for member := range r.mu.members {
		members = append(members, member)
	}

	sort.Slice(members, func(i, j int) bool {
		return members[i] < members[j]
	})
	return members
}

// Tick moves forward the failure detectors internal notion of time. The caller
// is responsible for calling it periodically.
//
// TODO(irfansharif): Implement raft snapshots.
func (r *raftImpl) Tick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Step through all received messages.
	for _, msg := range r.mu.inbox {
		raftmsg := msg.Content.(raftpb.Message)
		if err := r.raft.node.Step(context.Background(), raftmsg); err != nil {
			r.l.logger.Fatal(err)
		}
	}

	r.mu.inbox = r.mu.inbox[:0]   // clear out the inbox
	r.mu.outbox = r.mu.outbox[:0] // clear out the outbox

	// Tick the raft node.
	r.raft.node.Tick()

	// Step through all received raft ready messages.
	for _, rd := range r.mu.raftReady {
		if err := r.saveLocked(rd.Entries, rd.HardState, rd.Snapshot); err != nil {
			r.l.logger.Fatal(err)
		}

		// Slate all outgoing raft messages for delivery.
		for _, raftM := range rd.Messages {
			msg := Message{
				To:      ID(raftM.To),
				From:    ID(raftM.From),
				Content: raftM,
			}
			r.mu.outbox = append(r.mu.outbox, msg)
		}

		// Process all committed entries.
		for _, entry := range rd.CommittedEntries {
			switch entry.Type {
			case raftpb.EntryNormal:
				if err := r.processEntryLocked(entry); err != nil {
					r.l.logger.Fatal(err)
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(entry.Data); err != nil {
					r.l.logger.Fatal(err)
				}
				r.raft.node.ApplyConfChange(cc)

				if id := ID(cc.NodeID); id == r.ID() && cc.Type == raftpb.ConfChangeRemoveNode {
					r.mu.members = make(map[ID]bool) // Drop all members, including ourselves.
					// TODO(irfansharif): How do we signal to the caller that
					// we've been removed?
				}
			}
		}
		r.raft.node.Advance()
	}

	r.mu.raftReady = r.mu.raftReady[:0] // clear out the raft ready buffer
}

// Outbox returns a list of outbound messages.
func (r *raftImpl) Outbox() []Message {
	r.mu.Lock()
	defer r.mu.Unlock()

	outbox := make([]Message, len(r.mu.outbox))
	copy(outbox, r.mu.outbox)
	return outbox
}

// Inbox accepts the incoming message.
func (r *raftImpl) Inbox(msgs ...Message) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, msg := range msgs {
		if msg.To != r.ID() {
			panic(fmt.Sprintf("misrouted messaged, intended for %d but sent to %d", msg.To, r.ID()))
		}
	}
	r.mu.inbox = append(r.mu.inbox, msgs...)
}

// ID returns the module's own identifier.
func (r *raftImpl) ID() ID {
	return r.l.id
}

// Add adds the identified member to the cluster. It retries/reproposes
// internally, returning only once the member was successfully added.
func (r *raftImpl) Add(ctx context.Context, member ID) error {
	// XXX: first check to see that we can propose. if we can, call into
	// ProposeConfChange, to add it to the raft group then send a proposal
	// through raft to have each member add this new node to the member.
	//
	// does the ordering matter? what if we add a new node, and it's still
	// unhealthy? maybe we should add as unhealthy, getting it through the
	// existing consensus group, and only then adding it as a member?
	// Should we vary this to have the nodes propose themselves? We could have
	// the node being joined through propose the actual raft membership change.
	// And once the joining node finds out about it (because it's own raft
	// module tells it so), it can propose itself. But I don't really understand
	// the point of that, I don't think there would be much. If we want control
	// over 'initialization', we could have the liveness modules wait until it
	// hears about it's own addition. When we eventually introduce
	// timers/heartbeats, we could add the raft member to the raft group, and
	// then propose a membership at the liveness level except marking it as
	// unhealthy; counting on the newly added member to heartbeat itself. Does
	// this let us decouple the addition of the node to the raft group from the
	// addition of the node at the liveness layer? Maybe not? Wouldn't raft
	// require heartbeats from that newly-added raft node.
	// What's a better protocol for it? Send a membership proposal first, which
	// if accepted, can be taken as signal to add the extra member to the raft
	// group. Actually, I'm not sure. At some level if the conf-change proposal
	// goes through, it's the same as the membership proposal going through? The
	// proposing node needs to find a majority of existing nodes (i.e. excluding
	// the node being added) accessible. Anyway, I'm not sure if the ordering
	// matters.
	//
	// What happens if only one of these things goes through and then we fail?
	// Because we're registering nodes based on proposal entries, rather than
	// conf change entries, it might be the case that the newly added node never
	// gets to add itself as a liveness-level member? Should we just skip
	// proposal based membership altogether in favor of piggy-backing on top of
	// the conf-change mechanism? It's somewhat of a simplification: adding a
	// member would only need a single raft proposal round.
	//
	// We could also hollow out Add, and let it only send a raft proposal, which
	// if accepted, prompts the caller to then start the other node, and propose
	// a conf-change? Kind of cockroach does with the join rpc.
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeAddNode, NodeID: uint64(member)}
	if err := r.raft.node.ProposeConfChange(ctx, cc); err != nil {
		return err
	}
	return r.proposeWithRetry(ctx, member, add)
}

// Remove removes the identified member from the cluster. It retries/reproposes
// internally, returning only once the member was successfully removed.
func (r *raftImpl) Remove(ctx context.Context, member ID) error {
	// XXX: Same questions here around the ordering of these two things.
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: uint64(member)}
	if err := r.raft.node.ProposeConfChange(ctx, cc); err != nil {
		return err
	}
	return r.proposeWithRetry(ctx, member, remove)
}

func (r *raftImpl) saveLocked(entries []raftpb.Entry, hs raftpb.HardState, snapshot raftpb.Snapshot) error {
	if err := r.raft.storage.Append(entries); err != nil {
		r.l.logger.Fatal(err)
	}

	if !raft.IsEmptyHardState(hs) {
		if err := r.raft.storage.SetHardState(hs); err != nil {
			return err
		}
	}
	if !raft.IsEmptySnap(snapshot) {
		if err := r.raft.storage.ApplySnapshot(snapshot); err != nil {
			return err
		}
	}
	return nil
}

type operation string

const add operation = "add"
const remove operation = "remove"

func (r *raftImpl) processEntryLocked(entry raftpb.Entry) error {
	if entry.Data == nil {
		return nil
	}

	membership := bytes.SplitN(entry.Data, []byte(":"), 2)
	id, err := strconv.Atoi(string(membership[0]))
	if err != nil {
		return err
	}

	op := operation(membership[1])
	if op == add {
		r.mu.members[ID(id)] = true
	} else {
		delete(r.mu.members, ID(id))
	}
	return nil
}

// runRaftLoop sources from raft.Node.Ready() and collects it in the embedded
// slice. It's to be run in a separate goroutine in order to process raft ready
// messages in Tick(). This lets us hide raft's internal multi-threadedness and
// heavy use of channels behind the single-threaded interface for Tick().
//
// When tearing down, it also closes the underlying raft node.
func (r *raftImpl) runRaftLoop() {
	for {
		select {
		case rd := <-r.raft.node.Ready():
			r.mu.Lock()
			r.mu.raftReady = append(r.mu.raftReady, rd)
			r.mu.Unlock()
		case <-r.closeCh:
			r.raft.node.Stop()
			return
		}
	}
}

// proposeWithRetry submits a raft proposal performing the specified operation
// on the given member, and re-proposes internally until it's committed.
func (r *raftImpl) proposeWithRetry(ctx context.Context, member ID, op operation) error {
	for {
		childCtx, _ := context.WithTimeout(ctx, 100*time.Millisecond)
		err := r.propose(childCtx, member, op)
		if err != nil && err != context.DeadlineExceeded {
			return err
		}
		if err == context.DeadlineExceeded {
			select {
			case <-ctx.Done(): // the top-level context has expired
				return ctx.Err()
			default:
				continue // re-propose
			}
		}

		return nil
	}
}

// propose submits a raft proposal to perform the specified operation on the
// given member, and returns only when it's both (a) committed, and (b) applied
// to this module.
func (r *raftImpl) propose(ctx context.Context, member ID, op operation) error {
	if err := r.raft.node.Propose(ctx, []byte(fmt.Sprintf("%d:%s", member, op))); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, found := r.Live(member)
		if op == add && found || op == remove && !found {
			return nil
		}
	}
}

func startRaftImpl(l *liveness) *raftImpl {
	if l.id == 0 {
		panic(fmt.Sprintf("invalid ID: %d", l.id))
	}

	r := &raftImpl{
		l:       l,
		closeCh: make(chan struct{}),
	}
	r.mu.members = make(map[ID]bool)
	r.mu.outbox = make([]Message, 0)
	var logger *log.Logger
	if l.logger != nil {
		logger = l.logger
		logger.SetPrefix("[raft] ")
		logger.SetFlags(0)
	} else {
		logger = log.New(ioutil.Discard, "[raft] ", 0)
	}

	r.raft.storage = raft.NewMemoryStorage()
	if l.mockRaftNode != nil {
		r.raft.node = l.mockRaftNode
		go r.runRaftLoop()
		return r
	}

	c := &raft.Config{
		ID:              uint64(l.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         r.raft.storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	raft.SetLogger(&raft.DefaultLogger{Logger: logger})

	// XXX: We have two possible implementations.
	// 1. An implementation where every node is a member
	// 2. An implementation where there's a tiny set of members that use raft
	//   among them, and other nodes find them to do anything.
	//
	// Let's focus on (1) first. We can start off with just node 1 being in the
	// raft group, and each additional node seeding with node 1, proposing
	// changes to it to get themselves added.

	// XXX: So there are two aspects: membership, and health. Will need to
	// implement timers and heartbeats for node health.
	// XXX: We'll also want to be able to switch out members of the raft group
	// itself, I think. Though that's a whole another level of complexity that
	// might not be all that worthwhile? It'll entail simulating movement of the
	// raft group. The same applies for where the initial raft group gets
	// formed. It has to start somewhere, and I don't think we should be too
	// interested in having raft groups form independently and join together,
	// cause we'd have to do that wiring ourselves. Maybe that's reasonable to
	// do with rapid?
	// So we'll start with a seed node, and have all nodes try to add themselves
	// to the raft group.

	// XXX: This basic raft backed implementation is essentially doing the same
	// thing raft is internally doing with its heartbeats. On the one hand,
	// that's kind of silly. Seems like the thing we'd otherwise be using paxos
	// for. On the other hand, I'm not familiar with it.
	var peers []raft.Peer
	if l.bootstrap {
		peers = append(peers, raft.Peer{ID: uint64(l.id)})
		// There's no good time to propose with self to let future members learn
		// about it. So we initialize the raft storage with raft state
		// constructed by hand.
		if err := r.raft.storage.SetHardState(raftpb.HardState{
			Term: 1, Commit: 1, Vote: uint64(l.id),
		}); err != nil {
			logger.Fatal(err)
		}
		if err := r.raft.storage.Append([]raftpb.Entry{
			{
				Term: 1, Index: 1,
				Type: raftpb.EntryNormal,
				Data: []byte(fmt.Sprintf("%d:%s", l.id, add)),
			},
		}); err != nil {
			logger.Fatal(err)
		}
	}

	r.raft.node = raft.StartNode(c, peers)
	go r.runRaftLoop()
	return r
}
