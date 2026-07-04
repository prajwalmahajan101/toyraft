package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/prajwalmahajan101/toyraft/pkg/kvsm"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// maxValueBytes bounds a PUT /kv/{key} body. The KV demo values are small;
// this cap keeps a rogue client from buffering an unbounded proposal.
const maxValueBytes = 1 << 20 // 1 MiB

// raftNode is the NARROW slice of raft.Node the KV handler depends on, so a
// test can stub it (no real cluster). The concrete *nodeImpl returned by
// raft.New satisfies this superset. The handler NEVER depends on the concrete
// type — only on this interface.
type raftNode interface {
	Status() raft.Status
	LeaderHint() raft.NodeID
	Propose(ctx context.Context, data []byte) (raft.Index, raft.Term, error)
}

// roleName maps the frozen raft.Role uint8 to the LOCKED lowercase wire string.
// This is the SINGLE source of the /status role contract: toyraftctl `status`
// and smoke.sh `role_of`/`find_leader` (10-03/10-04) parse these EXACT strings.
// raft.Role is a bare uint8 with NO String()/MarshalJSON, so json.Marshal of a
// raw Status would emit `"Role":2` and silently break both consumers — the
// handler MUST route every role through here instead.
func roleName(r raft.Role) string {
	switch r {
	case raft.Follower:
		return "follower"
	case raft.Candidate:
		return "candidate"
	case raft.Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// statusResp is the EXPLICIT wire shape for GET /status. role is a lowercase
// STRING (via roleName), never the raw uint8 — see roleName's contract note.
type statusResp struct {
	Role        string            `json:"role"`
	Term        uint64            `json:"term"`
	CommitIndex uint64            `json:"commit_index"`
	ApplyIndex  uint64            `json:"apply_index"`
	LeaderHint  string            `json:"leader_hint"`
	Members     map[string]uint64 `json:"members"`
}

// kvHandler serves the client-facing KV API (/kv/{key} + /status). It gates
// EVERY route on leadership: a non-leader 307-redirects to the leader's CLIENT
// url (WIRE §5.1/§5.2 — GET is leader-only-read, so it redirects too), or
// 503s when no leader is known.
type kvHandler struct {
	node       raftNode
	sm         *kvsm.KV
	clientURLs map[raft.NodeID]string // ALL nodes, CLIENT urls — redirect targets
}

// newKVHandler builds the /kv + /status mux backed by node and sm, using
// clientURLs (the leader's CLIENT url, distinct from the peer transport's
// PeerURLs) as the redirect Location base.
func newKVHandler(node raftNode, sm *kvsm.KV, clientURLs map[raft.NodeID]string) http.Handler {
	h := &kvHandler{node: node, sm: sm, clientURLs: clientURLs}
	mux := http.NewServeMux()
	// Go 1.22 method-pattern routes; {key} extracted via r.PathValue("key").
	mux.HandleFunc("PUT /kv/{key}", h.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", h.handleDelete)
	mux.HandleFunc("GET /kv/{key}", h.handleGet)
	mux.HandleFunc("GET /status", h.handleStatus)
	return mux
}

// leaderGate returns true when this node is the leader and the request may be
// served locally. Otherwise it writes the full non-leader response (307 to the
// leader's CLIENT url + X-Raft-Leader-Hint, or 503 no_leader_known when the
// hint is empty) and returns false — the caller MUST return immediately.
//
// This gate applies to ALL four routes including GET: v1 GET is leader-only-read
// (WIRE §5.2), so followers redirect reads exactly as they redirect writes.
func (h *kvHandler) leaderGate(w http.ResponseWriter, r *http.Request) bool {
	if h.node.Status().Role == raft.Leader {
		return true
	}
	hint := h.node.LeaderHint()
	if hint == "" { // WIRE §5.1 — no leader known
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"no_leader_known","leader_hint":""}`))
		return false
	}
	// Location is the leader's CLIENT url + the SAME resource path (WIRE §5.1).
	w.Header().Set("Location", h.clientURLs[hint]+r.URL.Path)
	w.Header().Set("X-Raft-Leader-Hint", string(hint))
	w.WriteHeader(http.StatusTemporaryRedirect) // 307 preserves method + body
	return false
}

// propose marshals op and blocks on node.Propose. On success it writes 200; a
// dropped/lost proposal is a 503 the client SHOULD retry. A mid-flight
// step-down (ErrNotLeader) is redirected like any other non-leader response.
func (h *kvHandler) propose(w http.ResponseWriter, r *http.Request, op kvsm.Op) {
	data, err := json.Marshal(op)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"marshal_op"}`))
		return
	}
	if _, _, err := h.node.Propose(r.Context(), data); err != nil {
		var notLeader *raft.ErrNotLeader
		if errors.As(err, &notLeader) {
			hint := notLeader.LeaderHint
			if hint == "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"no_leader_known","leader_hint":""}`))
				return
			}
			w.Header().Set("Location", h.clientURLs[hint]+r.URL.Path)
			w.Header().Set("X-Raft-Leader-Hint", string(hint))
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"proposal_dropped"}`))
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *kvHandler) handlePut(w http.ResponseWriter, r *http.Request) {
	if !h.leaderGate(w, r) {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = w.Write([]byte(`{"error":"payload_too_large"}`))
		return
	}
	h.propose(w, r, kvsm.Op{Kind: "set", Key: r.PathValue("key"), Value: body})
}

func (h *kvHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if !h.leaderGate(w, r) {
		return
	}
	h.propose(w, r, kvsm.Op{Kind: "del", Key: r.PathValue("key")})
}

func (h *kvHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	if !h.leaderGate(w, r) {
		return
	}
	// Leader-only-read: Propose discards the Apply result (node_public.go), so
	// the leader reads the applied map directly via kvsm.Get (WIRE §5.2).
	v, ok := h.sm.Get(r.PathValue("key"))
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(v)
}

func (h *kvHandler) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !h.leaderGate(w, r) {
		return
	}
	s := h.node.Status()
	members := make(map[string]uint64, len(s.MatchIndex))
	for id, idx := range s.MatchIndex {
		members[string(id)] = uint64(idx)
	}
	resp := statusResp{
		Role:        roleName(s.Role), // lowercase STRING — never the raw uint8
		Term:        uint64(s.Term),
		CommitIndex: uint64(s.CommitIndex),
		ApplyIndex:  uint64(s.ApplyIndex),
		LeaderHint:  string(s.LeaderHint),
		Members:     members,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
