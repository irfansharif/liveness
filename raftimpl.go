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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/etcd-io/etcd/raft"
)

// TODO(irfansharif): When we implement the centralized version of this, we
// could "fake" replica removal/recovery through the network itself, promoting
// stand-by nodes to assume raft-membership, and dropping old ones. Does that
// simulate anything useful though? Maybe just movement of the leader node over
// time, as failures happen? Feels far-fetched; there are more useful things to
// simulate.

// TODO(irfansharif): It's unclear what this constant should be. It's coupled to
// the ticker duration the caller decides to use, so should probably be
// configurable? If ticker duration is 10ms, heartbeatIncrement=100 is a
// heartbeat extension of 1s, with heartbeats happening every 0.5s.
const heartbeatIncrement = 20

// raftImpl implements the Liveness interface. It adds all members to the
// consensus group, and health is maintained by means of periodic proposals.
type raftImpl struct {
	l   *L
	log *log.Logger

	mu struct {
		sync.Mutex
		members       map[ID]uint64 // member: ticker expiration
		inbox, outbox []Message

		removedFromGroup        bool
		readyToAcceptHeartbeats bool
		inflight                map[uint64]chan struct{}
		raftReady               []raft.Ready
		ticker                  uint64
		closed                  bool
	}

	raft struct {
		node    raft.Node
		storage *raft.MemoryStorage
	}

	heartbeatCh chan struct{}
	closeCh     chan struct{}
}

// Teardown is part of the Liveness interface.
func (r *raftImpl) Teardown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.closed {
		return
	}

	close(r.closeCh)
	r.raft.node.Stop()
	r.mu.closed = true
}

// Live is part of the Liveness interface.
//
// TODO(irfansharif): In doing Live() this way (by reading the applied state
// based on raft commands, without real timers), a lagging follower will always
// have a stale view of liveness. The up-to-date view of liveness will only
// appear at the leader node (and up-to-date) followers. In raft, no single
// follower is aware that it's lagging behind. So it can't distinguish between a
// heartbeat request made "at the same time" and one made in the past, that it's
// only now finding out about. We'd have to go to the leader to retrieve an
// up-to-date view of liveness. We can do this through the linearizable switch,
// by exporting livenessWithRetry, accepting the cost that it's inaccessible for
// members that can't contact the leader.
func (r *raftImpl) Live(member ID) (live, found bool) {
	return r.liveInternal(member)
}

func (r *raftImpl) liveInternal(member ID) (live, found bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	exp, found := r.mu.members[member]
	return r.mu.ticker < exp, found
}

// Members is part of the Liveness interface.
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

// Tick is part of the Liveness interface.
//
// TODO(irfansharif): Implement raft snapshots.
func (r *raftImpl) Tick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.removedFromGroup {
		// TODO(irfansharif): What's a better API to signal to the ticker thread
		// that we're done, and should no longer be ticked?
		return
	}

	// We trigger a heartbeat around the halfway point from when we last
	// heartbeat, to when the heartbeat will no longer be valid.
	if r.mu.ticker%(heartbeatIncrement/2) == 0 {
		// Only signal the worker/heartbeat thread if it's waiting to be
		// signalled.
		select {
		case r.heartbeatCh <- struct{}{}:
		default:
		}
	}
	r.mu.ticker += 1

	// Step through all received messages.
	for _, msg := range r.mu.inbox {
		raftmsg := msg.Content.(raftpb.Message)
		if err := r.raft.node.Step(context.Background(), raftmsg); err != nil {
			r.log.Fatal(err)
		}
	}

	r.mu.inbox = r.mu.inbox[:0]   // clear out the inbox
	r.mu.outbox = r.mu.outbox[:0] // clear out the outbox

	// Tick the raft node.
	r.raft.node.Tick()

	// Step through all received raft ready messages.
	for _, rd := range r.mu.raftReady {
		if err := r.saveLocked(rd.Entries, rd.HardState, rd.Snapshot); err != nil {
			r.log.Fatal(err)
		}

		// Look for any read request results, and if found, signal the waiting
		// thread.
		if len(rd.ReadStates) != 0 {
			for _, rs := range rd.ReadStates {
				req := binary.BigEndian.Uint64(rs.RequestCtx)
				if ch, ok := r.mu.inflight[req]; ok {
					delete(r.mu.inflight, req)
					close(ch)
				}
			}
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
				if err := r.applyEntryLocked(entry); err != nil {
					log.Fatal(err, string(entry.Data))
					r.log.Fatal(err)
				}
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(entry.Data); err != nil {
					r.log.Fatal(err)
				}
				r.raft.node.ApplyConfChange(cc)

				if id := ID(cc.NodeID); id == r.ID() && cc.Type == raftpb.ConfChangeRemoveNode {
					r.mu.members = make(map[ID]uint64) // drop all members, including ourselves
					// TODO(irfansharif): How do we signal to the caller that
					// we've been removed?
				}
			}
		}
		r.raft.node.Advance()
	}

	r.mu.raftReady = r.mu.raftReady[:0] // clear out the raft ready buffer
}

// Outbox is part of the Liveness interface.
func (r *raftImpl) Outbox() []Message {
	r.mu.Lock()
	defer r.mu.Unlock()

	outbox := make([]Message, len(r.mu.outbox))
	copy(outbox, r.mu.outbox)
	return outbox
}

// Inbox is part of the Liveness interface.
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

// ID is part of the Liveness interface.
func (r *raftImpl) ID() ID {
	return r.l.id
}

// Add is part of the Liveness interface. It retries/reproposes internally,
// returning only once the member was successfully added.
func (r *raftImpl) Add(ctx context.Context, member ID) error {
	// TODO(irfansharif): What happens if the same member is added concurrently?
	if _, found := r.liveInternal(member); found {
		return fmt.Errorf("can't add %d, already exists", member)
	}

	ctype, learner := raftpb.ConfChangeAddLearnerNode, true
	if len(r.l.central) == 0 {
		ctype, learner = raftpb.ConfChangeAddNode, false
	} else {
		if _, ok := r.l.central[member]; ok {
			ctype, learner = raftpb.ConfChangeAddNode, false
		}
	}
	cc := raftpb.ConfChange{Type: ctype, NodeID: uint64(member)}
	if err := r.raft.node.ProposeConfChange(ctx, cc); err != nil {
		return err
	}
	r.log.Printf("proposing to add l=%d as learner=%t", member, learner)
	return r.proposeWithRetry(ctx, proposal{Member: member, Op: add})
}

// Remove is part of the Liveness interface. It retries/reproposes internally,
// returning only once the member was successfully removed.
func (r *raftImpl) Remove(ctx context.Context, member ID) error {
	cc := raftpb.ConfChange{Type: raftpb.ConfChangeRemoveNode, NodeID: uint64(member)}
	if err := r.raft.node.ProposeConfChange(ctx, cc); err != nil {
		return err
	}

	r.log.Printf("proposing to remove l=%d", member)
	return r.proposeWithRetry(ctx, proposal{Member: member, Op: remove})
}

func (r *raftImpl) saveLocked(entries []raftpb.Entry, hs raftpb.HardState, snapshot raftpb.Snapshot) error {
	if err := r.raft.storage.Append(entries); err != nil {
		r.log.Fatal(err)
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
const heartbeat operation = "heartbeat"

type proposal struct {
	Op     operation
	Member ID
}

func (r *raftImpl) applyEntryLocked(entry raftpb.Entry) error {
	if entry.Data == nil {
		return nil
	}

	var proposal proposal
	if err := json.Unmarshal(entry.Data, &proposal); err != nil {
		return err
	}

	switch proposal.Op {
	case add:
		r.mu.members[proposal.Member] = r.mu.ticker - 1 // add the member as non-live, expecting a heartbeat
		r.log.Printf("added l=%d", proposal.Member)
	case heartbeat:
		_, ok := r.mu.members[proposal.Member]
		if !ok {
			// We've received a heartbeat for a member that's no longer part of
			// peer-list. This can happen if the proposals removing the member
			// happen concurrently with a heartbeat (raft might re-route
			// proposals internally). We'll just ignore the heartbeat.
			r.log.Printf("received heartbeat for l=%d (no longer a member)", proposal.Member)
			return nil
		}

		// We're applying a heartbeat request. Compute at this node, starting
		// now, when the heartbeat is set to expire.
		//
		// TODO(irfansharif): Do we need the notion of epochs? Do we want a node
		// to be able to quickly restart and insta-heartbeat without other nodes
		// finding about it? Probably not, there's no benefit to it, and it's
		// not what CRDB does. It also opens up the possibility for false
		// negatives. So what should be done? Heartbeats could also include
		// epochs; that way all other nodes would learn about the epoch bump.

		// When applying heartbeat requests it happens from one of two ways:
		//
		// 	a. through regular raft replication, while we're live and healthy
		// 	b. when restarting a node, and reapplying previous entries
		//
		// Computing the heartbeat extension ticker count during (a) is fine, but
		// during (b) is nonsensical. What's to be done? Ideally we'd be able to
		// distinguish and not apply any heartbeat requests from before we were
		// started up. We want to only rely on the subsequent heartbeat from all
		// peers to ascertain health status. Unfortunately etcd doesn't expose
		// an API for us to ascertain which log records are being read off
		// stable storage, and which ones are being received live.
		//
		// To work around this, we submit a linearizable read-only request
		// through raft during startup, and wait for it to be received back here
		// before applying any heartbeat proposals. This has the effect of
		// filtering out all earlier heartbeat proposals.

		if r.mu.readyToAcceptHeartbeats {
			r.mu.members[proposal.Member] = r.mu.ticker + heartbeatIncrement
			r.log.Printf("received heartbeat for l=%d (at t=%d, valid until t=%d)",
				proposal.Member, r.mu.ticker, r.mu.ticker+heartbeatIncrement)
		}
	case remove:
		delete(r.mu.members, proposal.Member)
		if proposal.Member == r.ID() {
			r.mu.removedFromGroup = true
			r.log.Printf("removed l=%d (self)", proposal.Member)
		} else {
			r.log.Printf("removed l=%d", proposal.Member)
		}
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
			return
		}
	}
}

// runHeartbeatLoop periodically sends a heartbeat proposal for itself. It's to
// be run in a separate goroutine.
func (r *raftImpl) runHeartbeatLoop() {
	for {
		select {
		case <-r.heartbeatCh:
			if _, found := r.liveInternal(r.ID()); !found {
				continue
			}

			ctx := context.Background()
			err := r.proposeWithRetry(ctx, proposal{Op: heartbeat, Member: r.ID()})
			if err != nil && err != raft.ErrStopped { // we could've been closed in the interim
				r.log.Fatal(err)
			}
		case <-r.closeCh:
			return
		}
	}
}

// prepareToAcceptHeartbeats is run during start-up, in a separate goroutine, to
// catch this member up to the current raft state. It does so by submitting a
// linearizable read-only request through raft, and waits for it to be accepted.
// Once that happens, we can start accepting subsequent heartbeat proposals
// through raft (while having filtered our heartbeat proposals from before we
// had been started).
func (r *raftImpl) prepareToAcceptHeartbeats() {
	linearizeCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := r.linearizeWithRetry(ctx); err != nil {
			r.log.Printf("error during linearization: %v", err)
			return
		}

		r.mu.Lock()
		r.mu.readyToAcceptHeartbeats = true
		r.mu.Unlock()
		close(linearizeCh)
	}()

	select {
	case <-r.closeCh:
		return
	case <-linearizeCh:
		return
	}
}

// linearizeWithRetry submits a linearizable read-only raft proposal and waits
// for it to come out of raft. This has the effect of catching up this liveness
// with the rest of the consensus group. Future reads of the membership state
// are guaranteed to be at least as up-to-date as the submitted read request.
//
// TODO(irfansharif): This seems useful enough to export? Name it Synchronize?
// It'd be useful in conjunction with Live() and Members().
func (r *raftImpl) linearizeWithRetry(ctx context.Context) error {
	req, ch := rand.Uint64(), make(chan struct{})
	r.mu.Lock()
	r.mu.inflight[req] = ch
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		delete(r.mu.inflight, req) // usually cleaned up downstream of raft, but we could error out
		r.mu.Unlock()
	}()

	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, req)
	for {
		childCtx, _ := context.WithTimeout(ctx, 100*time.Millisecond)
		if err := r.raft.node.ReadIndex(ctx, bs); err != nil {
			return err
		}

		select {
		case <-ctx.Done(): // the top-level context has expired
			return ctx.Err()
		case <-childCtx.Done():
			continue // re-propose
		case <-ch:
		}
		return nil
	}
}

// proposeWithRetry submits a raft proposal performing the specified operation
// on the given member, and re-proposes internally until it's committed.
func (r *raftImpl) proposeWithRetry(ctx context.Context, pr proposal) error {
	for {
		childCtx, _ := context.WithTimeout(ctx, 100*time.Millisecond)
		err := r.propose(childCtx, pr)
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
func (r *raftImpl) propose(ctx context.Context, pr proposal) error {
	payload, err := json.Marshal(pr)
	if err != nil {
		return err
	}

	if err := r.raft.node.Propose(ctx, payload); err != nil {
		return err
	}

	if pr.Op == heartbeat {
		return nil // for heartbeats, we just fire and forget
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, found := r.liveInternal(pr.Member)
		if pr.Op == add && found || pr.Op == remove && !found {
			return nil
		}
	}
}

func startRaftImpl(l *L) *raftImpl {
	if l.id == 0 {
		panic(fmt.Sprintf("invalid ID: %d", l.id))
	}

	r := &raftImpl{
		l:           l,
		closeCh:     make(chan struct{}),
		heartbeatCh: make(chan struct{}),
	}
	r.mu.members = make(map[ID]uint64)
	r.mu.inflight = make(map[uint64]chan struct{})
	r.mu.outbox = make([]Message, 0)

	logger := log.New(ioutil.Discard, fmt.Sprintf("[raft,l=%d] ", l.id), log.Ltime|log.Lmicroseconds|log.Lshortfile|log.Lmsgprefix)
	if l.loggingTo != nil {
		logger.SetOutput(l.loggingTo)
	}
	r.log = logger

	r.raft.storage = raft.NewMemoryStorage()
	if l.storage != nil {
		// Recover the in-memory storage from persistent snapshot, state and
		// entries.
		hs, _, err := l.storage.InitialState()
		if err != nil {
			logger.Fatal(err)
		}
		if err := r.raft.storage.SetHardState(hs); err != nil {
			logger.Fatal(err)
		}

		fi, err := l.storage.FirstIndex()
		if err != nil {
			logger.Fatal(err)
		}
		li, err := l.storage.LastIndex()
		if err != nil {
			logger.Fatal(err)
		}
		entries, err := l.storage.Entries(fi, li+1, math.MaxUint64)
		if err != nil && err != raft.ErrUnavailable { // it's possible that there were never any entries
			logger.Fatal(err)
		}
		if err := r.raft.storage.Append(entries); err != nil {
			logger.Fatal(err)
		}
	}

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

	// TODO(irfansharif): Pass in an option for raft-internal logging?
	discard := log.New(ioutil.Discard, "[raft-internal] ", 0)
	raft.SetLogger(&raft.DefaultLogger{Logger: discard})

	var peers []raft.Peer
	if l.bootstrap {
		if len(l.central) != 0 {
			if _, ok := l.central[l.id]; !ok {
				cmembers := make([]ID, 0, len(l.central))
				for cmember := range l.central {
					cmembers = append(cmembers, cmember)
				}
				panic(fmt.Sprintf("bootstrap member %d not found in central membership %s", l.id, cmembers))
			}
		}

		peers = append(peers, raft.Peer{ID: uint64(l.id)})
		// There's no good time to propose with self to let future members learn
		// about it. So we initialize the raft storage with raft state
		// constructed by hand.
		if err := r.raft.storage.SetHardState(raftpb.HardState{
			Term: 1, Commit: 1, Vote: uint64(l.id),
		}); err != nil {
			logger.Fatal(err)
		}
		payload, err := json.Marshal(proposal{Op: add, Member: l.id})
		if err != nil {
			logger.Fatal(err)
		}
		if err := r.raft.storage.Append([]raftpb.Entry{
			{
				Term: 1, Index: 1,
				Type: raftpb.EntryNormal,
				Data: payload,
			},
		}); err != nil {
			logger.Fatal(err)
		}
	}

	r.raft.node = raft.StartNode(c, peers)
	go r.runRaftLoop()
	go r.runHeartbeatLoop()
	go r.prepareToAcceptHeartbeats()

	return r
}
