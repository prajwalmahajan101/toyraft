package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// wireMessage is the JSON projection of raft.Message per docs/WIRE.md §2.
//
// It exists so the frozen raft.Message stays tag-free (ADR-0015): all
// snake_case field naming and base64 encoding live here, not on the core type.
// omitempty is applied to every optional field so a zero-valued frame marshals
// to the minimal key set shown in the WIRE.md §2.1-2.4 examples; parsers MUST
// treat missing fields as zero values (WIRE §2).
//
// Entry.Data is a []byte and marshals as STANDARD base64 via encoding/json's
// default — deliberately NOT customized (WIRE §2: "Go encoding/json default").
type wireMessage struct {
	Type raft.MessageType `json:"type"`
	Term raft.Term        `json:"term"`
	From raft.NodeID      `json:"from"`
	To   raft.NodeID      `json:"to"`

	// RequestVote / RequestVoteResponse
	LastLogIndex raft.Index `json:"last_log_index,omitempty"`
	LastLogTerm  raft.Term  `json:"last_log_term,omitempty"`
	VoteGranted  bool       `json:"vote_granted,omitempty"`

	// AppendEntries / AppendEntriesResponse
	PrevLogIndex raft.Index  `json:"prev_log_index,omitempty"`
	PrevLogTerm  raft.Term   `json:"prev_log_term,omitempty"`
	Entries      []wireEntry `json:"entries,omitempty"`
	LeaderCommit raft.Index  `json:"leader_commit,omitempty"`
	Success      bool        `json:"success,omitempty"`
	MatchIndex   raft.Index  `json:"match_index,omitempty"`

	// Fast-rollback hints (Raft §5.3 optimisation)
	ConflictTerm  raft.Term  `json:"conflict_term,omitempty"`
	ConflictIndex raft.Index `json:"conflict_index,omitempty"`
}

// wireEntry is the JSON projection of raft.Entry (WIRE §2, entries[]).
// Data uses the encoding/json default (standard base64) — do NOT customize.
type wireEntry struct {
	Term  raft.Term  `json:"term"`
	Index raft.Index `json:"index"`
	Data  []byte     `json:"data"`
}

// fromMessage projects a raft.Message onto its wire DTO. It is the exact
// inverse of toMessage for the wire-visible field set.
func fromMessage(m raft.Message) wireMessage {
	w := wireMessage{
		Type:          m.Type,
		Term:          m.Term,
		From:          m.From,
		To:            m.To,
		LastLogIndex:  m.LastLogIndex,
		LastLogTerm:   m.LastLogTerm,
		VoteGranted:   m.VoteGranted,
		PrevLogIndex:  m.PrevLogIndex,
		PrevLogTerm:   m.PrevLogTerm,
		LeaderCommit:  m.LeaderCommit,
		Success:       m.Success,
		MatchIndex:    m.MatchIndex,
		ConflictTerm:  m.ConflictTerm,
		ConflictIndex: m.ConflictIndex,
	}
	if len(m.Entries) > 0 {
		w.Entries = make([]wireEntry, len(m.Entries))
		for i, e := range m.Entries {
			w.Entries[i] = wireEntry{Term: e.Term, Index: e.Index, Data: e.Data}
		}
	}
	return w
}

// toMessage projects a wire DTO back onto a raft.Message. It performs no
// validation beyond the structural projection; semantic validation (rejecting
// MsgTick / unknown types) lives in validateWireType, called by decodeMessage.
func toMessage(w wireMessage) (raft.Message, error) {
	m := raft.Message{
		Type:          w.Type,
		Term:          w.Term,
		From:          w.From,
		To:            w.To,
		LastLogIndex:  w.LastLogIndex,
		LastLogTerm:   w.LastLogTerm,
		VoteGranted:   w.VoteGranted,
		PrevLogIndex:  w.PrevLogIndex,
		PrevLogTerm:   w.PrevLogTerm,
		LeaderCommit:  w.LeaderCommit,
		Success:       w.Success,
		MatchIndex:    w.MatchIndex,
		ConflictTerm:  w.ConflictTerm,
		ConflictIndex: w.ConflictIndex,
	}
	if len(w.Entries) > 0 {
		m.Entries = make([]raft.Entry, len(w.Entries))
		for i, e := range w.Entries {
			m.Entries[i] = raft.Entry{Term: e.Term, Index: e.Index, Data: e.Data}
		}
	}
	return m, nil
}

// decodeMessage is the PURE wire decoder shared by the HTTP handler (09-03)
// and the fuzz target (SC5). It unmarshals a JSON body into the wireMessage
// DTO in the lenient default mode (WIRE §6.1 forward-compat — unknown fields
// are ignored, never an error), projects it to a raft.Message, and validates
// the MessageType.
//
// It NEVER panics on arbitrary input: any parse or validation failure is
// returned as a *wireError the handler maps to the WIRE §3 sentinel table.
//
// Note: json.Unmarshal is used directly (no strict-decode option) precisely so
// unknown JSON fields are ignored rather than rejected (WIRE §6.1).
func decodeMessage(b []byte) (raft.Message, error) {
	var w wireMessage
	if err := json.Unmarshal(b, &w); err != nil {
		return raft.Message{}, &wireError{kind: errBadRequest, err: fmt.Errorf("decode: %w", err)}
	}
	if err := validateWireType(w.Type); err != nil {
		return raft.Message{}, err
	}
	return toMessage(w)
}

// validateWireType enforces WIRE §2.5 + §6.2: the wire-visible MessageType
// enum is 0..3. MsgTick (255) is internal-only and MUST NOT appear on the
// wire; unknown types (>3) are a v2 RPC a v1 receiver does not support. Both
// are rejected as bad_request (400).
func validateWireType(t raft.MessageType) error {
	switch t {
	case raft.MsgRequestVote, raft.MsgRequestVoteResponse,
		raft.MsgAppendEntries, raft.MsgAppendEntriesResp:
		return nil
	case raft.MsgTick:
		return &wireError{kind: errBadRequest, err: fmt.Errorf("type=255 (MsgTick) is internal-only and not wire-visible")}
	default:
		return &wireError{kind: errBadRequest, err: fmt.Errorf("unknown MessageType %d", uint8(t))}
	}
}

// errKind classifies a wire/step failure onto the WIRE §3 sentinel table.
type errKind int

const (
	errBadRequest errKind = iota
	errNotLeader
	errStopped
	errProposalDropped
	errPayloadTooLarge
	errUnsupportedMedia
)

// wireError is the typed error the handler maps to an (HTTP status, sentinel)
// pair via statusForError. leaderHint is carried through for the not_leader
// case (WIRE §3 leader_hint + §4 X-Raft-Leader-Hint header).
type wireError struct {
	kind       errKind
	err        error
	leaderHint raft.NodeID
}

func (e *wireError) Error() string {
	sentinel := sentinelForKind(e.kind)
	if e.err != nil {
		return sentinel + ": " + e.err.Error()
	}
	return sentinel
}

func (e *wireError) Unwrap() error { return e.err }

// sentinelForKind maps an errKind to its stable lowercase WIRE §3 sentinel.
func sentinelForKind(k errKind) string {
	switch k {
	case errNotLeader:
		return "not_leader"
	case errStopped:
		return "stopped"
	case errProposalDropped:
		return "proposal_dropped"
	case errPayloadTooLarge:
		return "payload_too_large"
	case errUnsupportedMedia:
		return "unsupported_media"
	case errBadRequest:
		fallthrough
	default:
		return "bad_request"
	}
}

// httpStatusForKind maps an errKind to its WIRE §3 HTTP status code.
func httpStatusForKind(k errKind) int {
	switch k {
	case errNotLeader:
		return http.StatusConflict // 409
	case errStopped, errProposalDropped:
		return http.StatusServiceUnavailable // 503
	case errPayloadTooLarge:
		return http.StatusRequestEntityTooLarge // 413
	case errUnsupportedMedia:
		return http.StatusUnsupportedMediaType // 415
	case errBadRequest:
		fallthrough
	default:
		return http.StatusBadRequest // 400
	}
}

// statusForError maps any error the wire layer or Node.Step produced onto the
// WIRE §3 (httpStatus, sentinel, leaderHint) triple the handler (09-03) writes.
//
// A typed *wireError carries its own kind. A raw core sentinel is classified
// here: *raft.ErrNotLeader -> 409 (carrying its LeaderHint), raft.ErrStopped
// and raft.ErrProposalDropped -> 503. Anything else falls through to
// bad_request/400 (WIRE §3 default for parse/validation failures).
func statusForError(err error) (status int, sentinel string, leaderHint raft.NodeID) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	var we *wireError
	if errors.As(err, &we) {
		return httpStatusForKind(we.kind), sentinelForKind(we.kind), we.leaderHint
	}
	var nl *raft.ErrNotLeader
	if errors.As(err, &nl) {
		return http.StatusConflict, "not_leader", nl.LeaderHint
	}
	if errors.Is(err, raft.ErrStopped) {
		return http.StatusServiceUnavailable, "stopped", ""
	}
	if errors.Is(err, raft.ErrProposalDropped) {
		return http.StatusServiceUnavailable, "proposal_dropped", ""
	}
	return http.StatusBadRequest, "bad_request", ""
}
