package raftest

// OrderingStorage wraps any pkg/storage.Storage and records every
// SaveHardState call alongside RecordSend(Message) calls in a single
// monotonic event log. It is the SC5 layer-3 enforcement (see
// pkg/raft/ready.go package doc) — the assertion-driven proof that the
// driver persisted (Term, VotedFor) BEFORE shipping any vote-granted
// response under that pair.
//
// Usage by Cluster.tickOnce (Phase 5 driver, plan 05-05) and Phase 7's
// public driver:
//
//	msgs, hs := node.Ready()
//	if hs != nil {
//	    if err := storage.SaveHardState(*hs); err != nil { ... }
//	}
//	for _, m := range msgs {
//	    storage.RecordSend(m)   // event log: marks "about to Send"
//	    transport.Send(ctx, m)
//	}
//
// At test teardown:
//
//	storage.AssertHardStatePrecedesVoteGrantedResponse(t)
//
// flags any vote-granted response that was emitted without an earlier
// matching SaveHardState in the same log.

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage"
)

// orderingEventKind discriminates entries in the monotonic event log.
type orderingEventKind int

const (
	eventSaveHS orderingEventKind = iota + 1
	eventSend
)

// orderingEvent is one record in the monotonic event log. Only the
// kind-relevant field (hs or msg) is populated for any given event.
type orderingEvent struct {
	seq  uint64
	kind orderingEventKind
	hs   raft.HardState
	msg  raft.Message
}

// OrderingStorage wraps a storage.Storage and records SaveHardState +
// RecordSend events for the SC5 precedence assertion. All other
// Storage methods are forwarded to inner unchanged.
//
// Construction: NewOrderingStorage(inner).
//
// Concurrency: safe for concurrent SaveHardState / RecordSend calls
// from multiple goroutines via an internal Mutex. The event log is
// strictly monotonic in seq across all callers.
type OrderingStorage struct {
	inner storage.Storage

	mu     sync.Mutex
	seq    uint64
	events []orderingEvent
}

// Compile-time interface assertion. Catches drift if storage.Storage
// grows a method that OrderingStorage forgot to forward.
var _ storage.Storage = (*OrderingStorage)(nil)

// NewOrderingStorage wraps inner. The wrapper does not initialise inner;
// the caller is responsible for that (e.g. memory.New()).
func NewOrderingStorage(inner storage.Storage) *OrderingStorage {
	return &OrderingStorage{inner: inner}
}

// SaveHardState records the call sequence then delegates. The recording
// happens BEFORE the delegate so a slow / blocking inner SaveHardState
// (e.g. a real fsync under file storage) cannot be reordered past a
// concurrent RecordSend at the test-harness level — the event log
// reflects the sequence of CALLS, which is exactly what SC5 asserts on.
func (o *OrderingStorage) SaveHardState(hs raft.HardState) error {
	o.mu.Lock()
	o.seq++
	o.events = append(o.events, orderingEvent{seq: o.seq, kind: eventSaveHS, hs: hs})
	o.mu.Unlock()
	return o.inner.SaveHardState(hs)
}

// LoadHardState delegates without recording — load does not participate
// in the persist-before-send ordering invariant.
func (o *OrderingStorage) LoadHardState() (raft.HardState, error) {
	return o.inner.LoadHardState()
}

// Append delegates.
func (o *OrderingStorage) Append(entries []raft.Entry) error {
	return o.inner.Append(entries)
}

// TruncateSuffix delegates.
func (o *OrderingStorage) TruncateSuffix(from raft.Index) error {
	return o.inner.TruncateSuffix(from)
}

// Entries delegates.
func (o *OrderingStorage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	return o.inner.Entries(lo, hi)
}

// Term delegates.
func (o *OrderingStorage) Term(index raft.Index) (raft.Term, error) {
	return o.inner.Term(index)
}

// FirstIndex delegates.
func (o *OrderingStorage) FirstIndex() (raft.Index, error) {
	return o.inner.FirstIndex()
}

// LastIndex delegates.
func (o *OrderingStorage) LastIndex() (raft.Index, error) {
	return o.inner.LastIndex()
}

// Snapshot delegates.
func (o *OrderingStorage) Snapshot() ([]byte, raft.Index, error) {
	return o.inner.Snapshot()
}

// Restore delegates.
func (o *OrderingStorage) Restore(data []byte) error {
	return o.inner.Restore(data)
}

// RecordSend is called by the test driver immediately BEFORE handing
// the message to a transport. The recording goes into the same event
// log as SaveHardState so the precedence assertion can compare seq
// numbers directly.
//
// Driver discipline: every msg returned by Ready() MUST flow through
// RecordSend before reaching the transport. The Phase-5 Cluster and
// the Phase-7 public driver are the only callers in production; tests
// invoke it directly.
func (o *OrderingStorage) RecordSend(m raft.Message) {
	o.mu.Lock()
	o.seq++
	o.events = append(o.events, orderingEvent{seq: o.seq, kind: eventSend, msg: m})
	o.mu.Unlock()
}

// Events returns a defensive copy of the recorded event log. Useful
// for tests that want to inspect ordering beyond the canned precedence
// assertion below.
func (o *OrderingStorage) Events() []orderingEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.events)
}

// AssertHardStatePrecedesVoteGrantedResponse — SC5 layer-3 invariant.
//
// For every recorded Send(MsgRequestVoteResponse{VoteGranted=true,
// Term=T, To=C}) there MUST exist an earlier (strictly lower seq)
// recorded SaveHardState{CurrentTerm=T, VotedFor=C}. If none is found
// the test fails via t.Fatalf with the offending message and seq.
//
// Rationale: Raft §5.2 requires a vote to be on durable storage BEFORE
// the granting node responds. Persist-after-send would corrupt the
// "one vote per term" rule on crash recovery — a peer that observed
// the grant could elect the candidate, while the granter restarts
// believing it never voted. The driver discipline (Ready -> Save -> Send)
// makes this impossible; OrderingStorage proves it.
//
// "C" (the candidate) is read from the Message.To field of the granted
// response, which by construction in handleRequestVoteLocked equals
// m.From of the inbound RequestVote — i.e. the candidate the vote was
// granted to. OrderingStorage is per-node; tests construct one per
// granter.
func (o *OrderingStorage) AssertHardStatePrecedesVoteGrantedResponse(t *testing.T) {
	t.Helper()
	if err := o.CheckHardStatePrecedesVoteGrantedResponse(); err != nil {
		t.Fatalf("%v", err)
	}
}

// CheckHardStatePrecedesVoteGrantedResponse is the testing.T-free form
// of the SC5 precedence check. Returns nil on a clean log, or an error
// describing the first offending Send event. Useful for callers that
// want to assert the negative case (the assertion itself catches a
// violation) without failing the surrounding test.
func (o *OrderingStorage) CheckHardStatePrecedesVoteGrantedResponse() error {
	events := o.Events()

	for i, e := range events {
		if e.kind != eventSend {
			continue
		}
		m := e.msg
		if m.Type != raft.MsgRequestVoteResponse {
			continue
		}
		if !m.VoteGranted {
			continue
		}
		// Search backwards for the matching SaveHardState.
		found := false
		for j := i - 1; j >= 0; j-- {
			p := events[j]
			if p.kind != eventSaveHS {
				continue
			}
			if p.hs.CurrentTerm == m.Term && p.hs.VotedFor == m.To {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("SC5 violation: Send(MsgRequestVoteResponse VoteGranted=true Term=%d To=%q) "+
				"at seq=%d without prior SaveHardState{CurrentTerm=%d, VotedFor=%q}",
				m.Term, m.To, e.seq, m.Term, m.To)
		}
	}
	return nil
}
