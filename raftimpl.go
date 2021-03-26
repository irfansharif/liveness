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

	sync.Mutex // protects everything below

	members map[ID]bool // member: liveness
	inbox   []Message
	outbox  []Message
	close   struct {
		ch   chan struct{}
		done bool
	}

	raft struct {
		ready   []raft.Ready
		node    raft.Node
		storage *raft.MemoryStorage
	}
}

func (r *raftImpl) Close() {
	r.Lock()
	defer r.Unlock()

	if r.close.done {
		return
	}

	close(r.close.ch)
	r.close.done = true
}

func (r *raftImpl) Live(member ID) (live, found bool) {
	if member == r.ID() {
		return true, true
	}

	r.Lock()
	defer r.Unlock()
	live, found = r.members[member]
	return live, found
}

func (r *raftImpl) Members() []ID {
	r.Lock()
	defer r.Unlock()

	members := make([]ID, 0, len(r.members))
	for member := range r.members {
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
// XXX: Implement raft snapshots.
func (r *raftImpl) Tick() {
	r.Lock()
	defer r.Unlock()

	// Step through all received messages.
	for _, msg := range r.inbox {
		raftmsg := msg.Content.(raftpb.Message)
		if err := r.raft.node.Step(context.Background(), raftmsg); err != nil {
			_ = err
		}
	}

	r.inbox = r.inbox[:0]   // clear out the inbox
	r.outbox = r.outbox[:0] // clear out the outbox

	// Tick the raft node.
	r.raft.node.Tick()

	// Step through all received raft ready messages.
	for _, rd := range r.raft.ready {
		if err := r.saveLocked(rd.Entries, rd.HardState, rd.Snapshot); err != nil {
			log.Fatal(err)
		}

		// Slate all outgoing raft messages for delivery.
		for _, raftM := range rd.Messages {
			msg := Message{
				To:      ID(raftM.To),
				From:    ID(raftM.From),
				Content: raftM,
			}
			r.outbox = append(r.outbox, msg)
		}

		// Process all committed entries.
		for _, entry := range rd.CommittedEntries {
			switch entry.Type {
			case raftpb.EntryNormal:
				if err := r.processEntryLocked(entry); err != nil {
					log.Fatal(err)
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(entry.Data); err != nil {
					log.Fatal(err)
				}
				r.raft.node.ApplyConfChange(cc)
			}
		}
		r.raft.node.Advance()
	}

	r.raft.ready = r.raft.ready[:0] // clear out the raft ready buffer
}

// Outbox returns a list of outbound messages.
func (r *raftImpl) Outbox() []Message {
	r.Lock()
	defer r.Unlock()

	outbox := make([]Message, len(r.outbox))
	copy(outbox, r.outbox)
	return outbox
}

// Inbox accepts the incoming message.
func (r *raftImpl) Inbox(msgs ...Message) {
	r.Lock()
	defer r.Unlock()

	r.inbox = append(r.inbox, msgs...)
}

// ID returns the module's own identifier.
func (r *raftImpl) ID() ID {
	return r.l.id
}

// Add adds the identified member to the cluster. This is a transitive operation
// (transitively adding other members it knows about). It retries/reproposes
// internally, returning only once the member was successfully added.
func (r *raftImpl) Add(ctx context.Context, member ID) error {
	// XXX: first check to see that we can propose.
	// if we can, call into ProposeConfChange, to add it to the raft group
	// then send a proposal through raft to have each member add this new node
	// to the member.
	// does the ordering matter? what if we add a new node, and it's still
	// unhealthy? maybe we should add as unhealthy, getting it through the
	// existing consensus group, and only then adding it as a member?
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: uint64(member),
	}
	if err := r.raft.node.ProposeConfChange(ctx, cc); err != nil {
		return err
	}
	return r.proposeWithRetry(ctx, member, add)
}

// Remove removes the identified member from the cluster. It retries/reproposes
// internally, returning only once the member was successfully removed.
func (r *raftImpl) Remove(ctx context.Context, member ID) error {
	return r.proposeWithRetry(ctx, member, remove)
}

func (r *raftImpl) saveLocked(entries []raftpb.Entry, hs raftpb.HardState, snapshot raftpb.Snapshot) error {
	if err := r.raft.storage.Append(entries); err != nil {
		log.Fatal(err)
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
		r.members[ID(id)] = true
	} else {
		delete(r.members, ID(id))
	}
	return nil
}

// runRaftLoop source from raft.Node.Ready() and collects it in the embedded
// map. It's to be run in a separate goroutine in order to process raft ready
// messages in Tick(). This lets us hide raft's internal multi-threadedness and
// heavy use of channels behind the single-threaded interface for Tick().
func (r *raftImpl) runRaftLoop() {
	for {
		select {
		case rd := <-r.raft.node.Ready():
			r.Lock()
			r.raft.ready = append(r.raft.ready, rd)
			r.Unlock()
		case <-r.close.ch:
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
			case <-ctx.Done(): // the top-level context has expired, we don't get to repropose
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

func newRaftImpl(l *liveness) *raftImpl {
	r := &raftImpl{
		l:       l,
		members: make(map[ID]bool),
		outbox:  make([]Message, 0),
	}
	if r.ID() == 0 {
		panic(fmt.Sprintf("invalid ID: %d", r.ID()))
	}

	r.raft.storage = raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              uint64(l.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         r.raft.storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	var logger *log.Logger
	if l.logger != nil {
		logger = l.logger
		logger.SetPrefix("[raft] ")
		logger.SetFlags(0)
	} else {
		logger = log.New(ioutil.Discard, "[raft] ", 0)
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

	// XXX: Will need to implement timers and heartbeats for node health.
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
	var peers []raft.Peer
	if l.id == l.seed {
		peers = append(peers, raft.Peer{ID: uint64(l.id)})
		// There's no good time to propose with self to let future members learn
		// about it. So we initialize the raft storage with raft state
		// constructed by hand.
		_ = r.raft.storage.SetHardState(raftpb.HardState{
			Term: 1, Commit: 1, Vote: uint64(l.id),
		})
		_ = r.raft.storage.Append([]raftpb.Entry{
			{
				Term: 1, Index: 1,
				Type: raftpb.EntryNormal,
				Data: []byte(fmt.Sprintf("%d:%s", l.id, add)),
			},
		})
	}
	r.raft.node = raft.StartNode(c, peers)

	go r.runRaftLoop()
	return r
}
