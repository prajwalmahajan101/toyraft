package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// testConfig returns a minimal, valid http.Config for a server under test.
// maxBody caps the inbound body so the 413 case is reachable with a small
// payload; pass 0 to fall back to the package default. The clock is a Real
// clock — the handler reads it only on Close, never per request.
func testConfig(maxBody int64) Config {
	return Config{
		NodeID:       "node-1",
		ListenAddr:   "127.0.0.1:0",
		PeerURLs:     map[raft.NodeID]string{"node-2": "http://127.0.0.1:1/"},
		Clock:        clock.NewReal(),
		MaxBodyBytes: maxBody,
	}
}

// newTestServer builds a Server with step registered (step may be nil) and
// returns it. It does NOT call Start — tests drive the mux directly via
// httptest.NewRecorder, so no listener goroutine is spawned.
func newTestServer(t *testing.T, maxBody int64, step func(ctx context.Context, msg raft.Message) error) *Server {
	t.Helper()
	s := NewServer(testConfig(maxBody))
	s.Register(step)
	return s
}

// validAppendEntriesBody is a WIRE §2.3-valid AppendEntries frame the handler
// decodes without error, so the response status is governed entirely by what
// step returns.
func validAppendEntriesBody(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(wireMessage{
		Type:         raft.MsgAppendEntries,
		Term:         7,
		From:         "node-2",
		To:           "node-1",
		PrevLogIndex: 41,
		PrevLogTerm:  6,
		LeaderCommit: 40,
	})
	if err != nil {
		t.Fatalf("marshal AppendEntries body: %v", err)
	}
	return b
}

// doJSON POSTs body to path with the given Content-Type against s.mux and
// returns the recorded response.
func doJSON(s *Server, method, path, contentType string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	return rec
}

// decodeEnvelope decodes the JSON error envelope from a recorded response.
func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) errorEnvelope {
	t.Helper()
	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode error envelope from %q: %v", rec.Body.String(), err)
	}
	return env
}

// TestLeaderHintHeader proves SC4: when step returns *raft.ErrNotLeader the
// response is 409 carrying X-Raft-Leader-Hint AND {"error":"not_leader",
// "leader_hint":...}; when the hint is empty the header is ABSENT (not "").
func TestLeaderHintHeader(t *testing.T) {
	t.Run("hint present", func(t *testing.T) {
		step := func(_ context.Context, _ raft.Message) error {
			return &raft.ErrNotLeader{LeaderHint: "node-2"}
		}
		s := newTestServer(t, 0, step)

		rec := doJSON(s, http.MethodPost, "/raft/message", "application/json", validAppendEntriesBody(t))

		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409", rec.Code)
		}
		if got := rec.Header().Get("X-Raft-Leader-Hint"); got != "node-2" {
			t.Fatalf("X-Raft-Leader-Hint = %q, want %q", got, "node-2")
		}
		env := decodeEnvelope(t, rec)
		if env.Error != "not_leader" {
			t.Fatalf("error = %q, want not_leader", env.Error)
		}
		if env.LeaderHint != "node-2" {
			t.Fatalf("leader_hint = %q, want node-2", env.LeaderHint)
		}
	})

	t.Run("empty hint omits header", func(t *testing.T) {
		step := func(_ context.Context, _ raft.Message) error {
			return &raft.ErrNotLeader{LeaderHint: ""}
		}
		s := newTestServer(t, 0, step)

		rec := doJSON(s, http.MethodPost, "/raft/message", "application/json", validAppendEntriesBody(t))

		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409", rec.Code)
		}
		// The header MUST be ABSENT, not present-but-empty.
		if _, present := rec.Header()["X-Raft-Leader-Hint"]; present {
			t.Fatalf("X-Raft-Leader-Hint present with empty hint; want absent")
		}
		env := decodeEnvelope(t, rec)
		if env.Error != "not_leader" {
			t.Fatalf("error = %q, want not_leader", env.Error)
		}
		if env.LeaderHint != "" {
			t.Fatalf("leader_hint = %q, want empty", env.LeaderHint)
		}
	})
}

// TestStatusTable asserts the full WIRE §1/§3 status contract row-by-row,
// including EMPTY 204/404/405 bodies (Pitfall 2).
func TestStatusTable(t *testing.T) {
	valid := validAppendEntriesBody(t)

	// A body one byte past a 4-byte cap forces the 413 path.
	const smallCap = 4
	overCap := bytes.Repeat([]byte("x"), smallCap+1)

	tests := []struct {
		name        string
		maxBody     int64
		step        func(ctx context.Context, msg raft.Message) error
		method      string
		path        string
		contentType string
		body        []byte

		wantStatus   int
		wantSentinel string // "" => assert body is EMPTY
	}{
		{
			name:         "valid message, step ok -> 204 empty",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         valid,
			wantStatus:   http.StatusNoContent,
			wantSentinel: "",
		},
		{
			name:         "non-JSON content-type -> 415",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "text/plain",
			body:         valid,
			wantStatus:   http.StatusUnsupportedMediaType,
			wantSentinel: "unsupported_media",
		},
		{
			name:         "missing content-type -> 415",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "",
			body:         valid,
			wantStatus:   http.StatusUnsupportedMediaType,
			wantSentinel: "unsupported_media",
		},
		{
			name:         "body over MaxBodyBytes -> 413",
			maxBody:      smallCap,
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         overCap,
			wantStatus:   http.StatusRequestEntityTooLarge,
			wantSentinel: "payload_too_large",
		},
		{
			name:         "malformed JSON -> 400",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         []byte("{not json"),
			wantStatus:   http.StatusBadRequest,
			wantSentinel: "bad_request",
		},
		{
			name:         "type=255 (MsgTick) -> 400",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         []byte(`{"type":255,"term":1,"from":"a","to":"b"}`),
			wantStatus:   http.StatusBadRequest,
			wantSentinel: "bad_request",
		},
		{
			name:         "unknown type=99 -> 400",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         []byte(`{"type":99,"term":1,"from":"a","to":"b"}`),
			wantStatus:   http.StatusBadRequest,
			wantSentinel: "bad_request",
		},
		{
			name:         "step returns ErrStopped -> 503",
			step:         func(context.Context, raft.Message) error { return raft.ErrStopped },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         valid,
			wantStatus:   http.StatusServiceUnavailable,
			wantSentinel: "stopped",
		},
		{
			name:         "step returns ErrProposalDropped -> 503",
			step:         func(context.Context, raft.Message) error { return raft.ErrProposalDropped },
			method:       http.MethodPost,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         valid,
			wantStatus:   http.StatusServiceUnavailable,
			wantSentinel: "proposal_dropped",
		},
		{
			name:         "GET /raft/message -> 405 empty",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodGet,
			path:         "/raft/message",
			contentType:  "application/json",
			body:         nil,
			wantStatus:   http.StatusMethodNotAllowed,
			wantSentinel: "",
		},
		{
			name:         "POST /other/path -> 404 empty",
			step:         func(context.Context, raft.Message) error { return nil },
			method:       http.MethodPost,
			path:         "/other/path",
			contentType:  "application/json",
			body:         valid,
			wantStatus:   http.StatusNotFound,
			wantSentinel: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t, tc.maxBody, tc.step)
			rec := doJSON(s, tc.method, tc.path, tc.contentType, tc.body)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %q)", rec.Code, tc.wantStatus, rec.Body.String())
			}

			if tc.wantSentinel == "" {
				// 204 / 404 / 405 MUST carry an EMPTY body (Pitfall 2).
				if rec.Body.Len() != 0 {
					t.Fatalf("body = %q, want empty", rec.Body.String())
				}
				return
			}

			env := decodeEnvelope(t, rec)
			if env.Error != tc.wantSentinel {
				t.Fatalf("error = %q, want %q", env.Error, tc.wantSentinel)
			}
		})
	}
}

// TestNilStepTolerated proves the handler treats a never-registered / nil step
// as a no-op success (Register(nil) is tolerated), returning 204 empty.
func TestNilStepTolerated(t *testing.T) {
	s := newTestServer(t, 0, nil)
	rec := doJSON(s, http.MethodPost, "/raft/message", "application/json", validAppendEntriesBody(t))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body = %q, want empty", rec.Body.String())
	}
}

// TestCloseIdempotentAndJoins proves Close over a live listener is idempotent
// and joins the listener goroutine (goleak in main_test.go catches any leak).
func TestCloseIdempotentAndJoins(t *testing.T) {
	s := NewServer(testConfig(0))
	s.Register(func(context.Context, raft.Message) error { return nil })
	s.Start()

	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second Close is a no-op returning nil (sync.Once).
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestServeAgainstLiveServer round-trips one RPC through a real listener to
// prove the wire path end-to-end (not just the mux), then Closes cleanly.
func TestServeAgainstLiveServer(t *testing.T) {
	got := make(chan raft.Message, 1)
	s := NewServer(testConfig(0))
	s.Register(func(_ context.Context, m raft.Message) error {
		got <- m
		return nil
	})

	ts := httptest.NewServer(s.mux)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/raft/message", "application/json", bytes.NewReader(validAppendEntriesBody(t)))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		t.Fatalf("drain body: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}

	select {
	case m := <-got:
		if m.Type != raft.MsgAppendEntries || m.From != "node-2" {
			t.Fatalf("received msg = %+v, want AppendEntries from node-2", m)
		}
	default:
		t.Fatalf("step was not invoked")
	}

	// Close the server's Shutdown path (the httptest server owns the listener,
	// but Close must still be idempotent + return nil).
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
