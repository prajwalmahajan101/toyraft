package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// newTestClient builds the SAME kind of client main() uses (default redirect
// policy, so 307 is followed) with a short timeout for the tests.
func newTestClient() *http.Client { return &http.Client{Timeout: 5 * time.Second} }

// kvStore is a trivial in-memory KV backing an httptest.Server: PUT stores the
// body, GET serves it (404 when absent), DELETE removes it.
type kvStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newKVStore() *kvStore { return &kvStore{m: map[string][]byte{}} }

func (s *kvStore) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.m[r.PathValue("key")] = body
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		v, ok := s.m[r.PathValue("key")]
		s.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write(v)
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		delete(s.m, r.PathValue("key"))
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// TestSetGetRoundTrip: doSet then doGet returns the written value (SC1 client
// half, single-node happy path).
func TestSetGetRoundTrip(t *testing.T) {
	srv := httptest.NewServer(newKVStore().handler())
	defer srv.Close()

	if err := doSet(newTestClient(), srv.URL, "foo", "bar"); err != nil {
		t.Fatalf("doSet: %v", err)
	}
	got, err := doGet(newTestClient(), srv.URL, "foo")
	if err != nil {
		t.Fatalf("doGet: %v", err)
	}
	if got != "bar" {
		t.Fatalf("doGet = %q, want %q", got, "bar")
	}
}

// TestPutFollows307: a "follower" server replies 307 with Location pointing at a
// "leader" server; the default http.Client MUST re-issue the PUT to the leader
// WITH the body intact. This proves the bytes.NewReader/GetBody path (Pitfall 6):
// a 307 preserves the method, and GetBody replays the body across the redirect.
func TestPutFollows307(t *testing.T) {
	leaderStore := newKVStore()
	leader := httptest.NewServer(leaderStore.handler())
	defer leader.Close()

	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", leader.URL+r.URL.Path)
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	// Aim the PUT at the FOLLOWER; the client must follow the 307 to the leader.
	if err := doSet(newTestClient(), follower.URL, "foo", "bar"); err != nil {
		t.Fatalf("doSet through follower: %v", err)
	}

	leaderStore.mu.Lock()
	got, ok := leaderStore.m["foo"]
	leaderStore.mu.Unlock()
	if !ok {
		t.Fatal("leader never received the PUT (307 not followed or body dropped)")
	}
	if string(got) != "bar" {
		t.Fatalf("leader got body %q, want %q (body not replayed across 307)", got, "bar")
	}
}

// TestGetNotFound: a 404 from the server surfaces as errNotFound (non-nil err),
// so main can exit non-zero rather than printing an empty value.
func TestGetNotFound(t *testing.T) {
	srv := httptest.NewServer(newKVStore().handler())
	defer srv.Close()

	_, err := doGet(newTestClient(), srv.URL, "missing")
	if err == nil {
		t.Fatal("doGet on absent key: want error, got nil")
	}
	if err != errNotFound {
		t.Fatalf("doGet err = %v, want errNotFound", err)
	}
}

// TestStatusDecodesRoleString guards the 10-02 lowercase-role contract from the
// CONSUMER side: /status serves {"role":"leader",...} and the decode yields
// Role == "leader" (a string). A regression to an int role field would fail to
// compile or decode here.
func TestStatusDecodesRoleString(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"role":"leader","term":3,"commit_index":7,"apply_index":7,"leader_hint":"node-1","members":{"node-1":7,"node-2":6}}`)
	}))
	defer srv.Close()

	s, err := doStatus(newTestClient(), srv.URL)
	if err != nil {
		t.Fatalf("doStatus: %v", err)
	}
	if s.Role != "leader" {
		t.Fatalf("Role = %q, want lowercase string %q", s.Role, "leader")
	}
	if s.Term != 3 || s.CommitIndex != 7 || s.LeaderHint != "node-1" {
		t.Fatalf("status decode mismatch: %+v", s)
	}
	if s.Members["node-1"] != 7 || s.Members["node-2"] != 6 {
		t.Fatalf("members decode mismatch: %+v", s.Members)
	}
}
