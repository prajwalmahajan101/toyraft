package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/kvsm"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// The stubNode type implements the narrow raftNode interface with a constant
// role/hint so the KV handler can be driven via httptest with NO real cluster
// (SC4).
type stubNode struct {
	role       raft.Role
	hint       raft.NodeID
	proposeErr error // returned by Propose; nil = success
}

func (s stubNode) Status() raft.Status {
	return raft.Status{Role: s.role, LeaderHint: s.hint}
}

func (s stubNode) LeaderHint() raft.NodeID { return s.hint }

func (s stubNode) Propose(_ context.Context, _ []byte) (raft.Index, raft.Term, any, error) {
	if s.proposeErr != nil {
		return 0, 0, nil, s.proposeErr
	}
	return 1, 1, nil, nil
}

// clientBook is the redirect address book used across the tests.
func clientBook() map[raft.NodeID]string {
	return map[raft.NodeID]string{"n2": "http://127.0.0.1:9002"}
}

// TestFollowerRedirect proves SC4: a follower PUT returns 307 + Location
// (leader CLIENT url + path) + X-Raft-Leader-Hint.
func TestFollowerRedirect(t *testing.T) {
	h := newKVHandler(stubNode{role: raft.Follower, hint: "n2"}, kvsm.New(), clientBook())
	req := httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader("bar"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("PUT follower: got %d, want 307", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "http://127.0.0.1:9002/kv/foo" {
		t.Fatalf("Location = %q, want http://127.0.0.1:9002/kv/foo", loc)
	}
	if hint := rec.Header().Get("X-Raft-Leader-Hint"); hint != "n2" {
		t.Fatalf("X-Raft-Leader-Hint = %q, want n2", hint)
	}
}

// TestFollowerRedirectGET proves GET is leader-only-read: a follower redirects
// reads exactly as writes (WIRE §5.2).
func TestFollowerRedirectGET(t *testing.T) {
	h := newKVHandler(stubNode{role: raft.Follower, hint: "n2"}, kvsm.New(), clientBook())
	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTemporaryRedirect {
		t.Fatalf("GET follower: got %d, want 307", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != "http://127.0.0.1:9002/kv/foo" {
		t.Fatalf("Location = %q, want http://127.0.0.1:9002/kv/foo", loc)
	}
	if hint := rec.Header().Get("X-Raft-Leader-Hint"); hint != "n2" {
		t.Fatalf("X-Raft-Leader-Hint = %q, want n2", hint)
	}
}

// TestNoLeader503: a follower with an empty hint returns 503 no_leader_known.
func TestNoLeader503(t *testing.T) {
	h := newKVHandler(stubNode{role: raft.Follower, hint: ""}, kvsm.New(), clientBook())
	req := httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader("bar"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("PUT no-leader: got %d, want 503", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "no_leader_known") {
		t.Fatalf("body = %q, want it to contain no_leader_known", body)
	}
}

// TestLeaderServesGet: on the leader, GET reads the applied kvsm map directly;
// a present key -> 200 + value, a missing key -> 404.
func TestLeaderServesGet(t *testing.T) {
	sm := kvsm.New()
	setOp, err := json.Marshal(kvsm.Op{Kind: "set", Key: "k", Value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Apply(raft.Entry{Index: 1, Term: 1, Data: setOp}); err != nil {
		t.Fatal(err)
	}

	h := newKVHandler(stubNode{role: raft.Leader}, sm, clientBook())

	req := httptest.NewRequest(http.MethodGet, "/kv/k", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET k: got %d, want 200", rec.Code)
	}
	if body, _ := io.ReadAll(rec.Body); string(body) != "v" {
		t.Fatalf("GET k body = %q, want v", body)
	}

	req = httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET missing: got %d, want 404", rec.Code)
	}
}

// TestLeaderServesPut: on the leader, PUT proposes then returns 200.
func TestLeaderServesPut(t *testing.T) {
	h := newKVHandler(stubNode{role: raft.Leader}, kvsm.New(), clientBook())
	req := httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader("bar"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT leader: got %d, want 200", rec.Code)
	}
}

// TestStatusRoleString is the wire-contract guard: GET /status MUST emit the
// role as a lowercase STRING ("leader"/"follower"), NOT the raw raft.Role
// uint8. This fails loudly if anyone reverts to json.Marshal(node.Status()),
// which would emit `"role":2`. toyraftctl status + smoke.sh find_leader depend
// on this exact string.
func TestStatusRoleString(t *testing.T) {
	for _, tc := range []struct {
		role raft.Role
		want string
	}{
		{raft.Leader, "leader"},
		{raft.Follower, "follower"},
	} {
		var h http.Handler
		if tc.role == raft.Leader {
			h = newKVHandler(stubNode{role: raft.Leader}, kvsm.New(), clientBook())
		} else {
			// A follower would redirect /status; to observe the leader-path
			// encoding for the follower ROLE we must still hit the leader gate.
			// Instead assert roleName directly for the follower case AND the
			// leader-served JSON for the leader case.
			if got := roleName(tc.role); got != tc.want {
				t.Fatalf("roleName(%d) = %q, want %q", tc.role, got, tc.want)
			}
			continue
		}
		req := httptest.NewRequest(http.MethodGet, "/status", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /status: got %d, want 200", rec.Code)
		}
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode /status: %v", err)
		}
		if resp["role"] != tc.want {
			t.Fatalf("role = %v (%T), want %q (a string)", resp["role"], resp["role"], tc.want)
		}
	}
}
