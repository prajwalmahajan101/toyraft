package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// defaultMaxBodyBytes caps an inbound POST /raft/message body when Config
// leaves MaxBodyBytes unset (WIRE §1 recommends ~8 MiB).
const defaultMaxBodyBytes int64 = 8 << 20 // 8 MiB

// defaultShutdownTimeout bounds graceful drain on Close when Config leaves
// ShutdownTimeout unset.
const defaultShutdownTimeout = 5 * time.Second

// errorEnvelope is the JSON body written for any non-204 application error
// (WIRE §3). leader_hint is emitted only for the not_leader case; omitempty
// keeps it out of every other envelope.
type errorEnvelope struct {
	Error      string      `json:"error"`
	LeaderHint raft.NodeID `json:"leader_hint,omitempty"`
}

// Server is the inbound half of the HTTP transport: it decodes peer RPCs on
// POST /raft/message, drives the registered step callback, and maps whatever
// step returns onto the WIRE §1/§3 status table (including the SC4
// X-Raft-Leader-Hint header).
//
// It owns only the inbound path; outbound Send lives on the client (09-04) and
// the two are assembled into a single raft.Transport in 09-05. The handler is
// testable with a fake step and needs no live Raft core: it classifies errors
// purely via statusForError (wire.go), so a fake step returning
// *raft.ErrNotLeader exercises the full leader-hint path.
//
// All timing routes through cfg.Clock; this file never reads the wall clock
// directly (the ShutdownTimeout deadline is enforced via cfg.Clock.After).
type Server struct {
	cfg Config
	srv *http.Server
	mux *http.ServeMux

	// mu guards step so Register may be called before Start races the handler.
	mu   sync.RWMutex
	step func(ctx context.Context, msg raft.Message) error

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// NewServer builds a Server bound to cfg.ListenAddr with the POST /raft/message
// route and a catch-all that yields empty 404/405 bodies. It does NOT start
// listening; call Start for that. cfg.Clock MUST be non-nil (Config.Validate).
func NewServer(cfg Config) *Server {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaultMaxBodyBytes
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	s := &Server{cfg: cfg, mux: http.NewServeMux()}

	// Go 1.22+ method-pattern routing: only POST on the exact path reaches the
	// message handler.
	s.mux.HandleFunc("POST /raft/message", s.handleMessage)
	// Catch-all: any other method/path. ServeMux's default 404 writes the body
	// "404 page not found"; WIRE §1 requires EMPTY 404/405 bodies, so we force
	// the status with no body here (Pitfall 2).
	s.mux.HandleFunc("/", s.handleFallback)

	s.srv = &http.Server{Addr: cfg.ListenAddr, Handler: s.mux}
	return s
}

// Register installs the inbound step callback. It is safe to call before Start
// and tolerates a nil callback (no-op until a real one is registered).
func (s *Server) Register(step func(ctx context.Context, msg raft.Message) error) {
	s.mu.Lock()
	s.step = step
	s.mu.Unlock()
}

// Start runs the listener in a tracked goroutine so Close can join it
// (goleak-clean, Pitfall 7). It returns immediately; a bind failure surfaces
// out of Close (http.Server.Shutdown races the returned ListenAndServe error).
func (s *Server) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// ListenAndServe blocks until Shutdown/Close, returning ErrServerClosed
		// on graceful stop (not a real error).
		_ = s.srv.ListenAndServe()
	}()
}

// Close gracefully drains the server (http.Server.Shutdown) within
// cfg.ShutdownTimeout, then joins the listener goroutine. It is idempotent: the
// first call performs the shutdown and returns its error; subsequent calls
// return nil (sync.Once). The shutdown deadline is enforced via cfg.Clock.After
// rather than context.WithTimeout, so the wall clock is never read directly.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Bound Shutdown by ShutdownTimeout using the injected clock as the sole
		// time source. When the clock fires we cancel ctx, which makes Shutdown
		// return ctx.Err() and stop waiting for in-flight requests.
		done := make(chan struct{})
		go func() {
			select {
			case <-s.cfg.Clock.After(s.cfg.ShutdownTimeout):
				cancel()
			case <-done:
			}
		}()

		s.closeErr = s.srv.Shutdown(ctx)
		close(done)
		s.wg.Wait()
	})
	return s.closeErr
}

// handleFallback serves every request not matched by "POST /raft/message":
//   - the path is /raft/message but the method is not POST -> 405, empty body.
//   - any other path -> 404, empty body.
//
// Both bodies are EMPTY per WIRE §1 (Pitfall 2: never write ServeMux's default
// "404 page not found").
func (s *Server) handleFallback(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/raft/message" {
		w.WriteHeader(http.StatusMethodNotAllowed) // 405, empty body
		return
	}
	w.WriteHeader(http.StatusNotFound) // 404, empty body
}

// handleMessage decodes a peer RPC, drives the registered step, and maps the
// outcome onto the WIRE §1/§3 status table.
func (s *Server) handleMessage(w http.ResponseWriter, r *http.Request) {
	// 1. Content-Type gate (WIRE §1: non-JSON -> 415).
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		writeError(w, &wireError{kind: errUnsupportedMedia})
		return
	}

	// 2. Bound the body; past cfg.MaxBodyBytes MaxBytesReader makes Read fail
	//    with *http.MaxBytesError, which we map to 413 (WIRE §1).
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, &wireError{kind: errPayloadTooLarge})
			return
		}
		// Any other read failure is a malformed request body.
		writeError(w, &wireError{kind: errBadRequest, err: err})
		return
	}

	// 3. Decode via the shared wire decoder (09-02). Parse error / type=255 /
	//    unknown type all surface as a *wireError{errBadRequest}.
	msg, err := decodeMessage(body)
	if err != nil {
		writeError(w, err)
		return
	}

	// 4. Drive the registered step. A nil step (never Register'd, or Register(nil))
	//    is a no-op success — the message is accepted but not processed.
	step := s.loadStep()
	if step == nil {
		w.WriteHeader(http.StatusNoContent) // 204, empty body
		return
	}
	if err := step(r.Context(), msg); err != nil {
		writeError(w, err)
		return
	}

	// 5. Success: 204 No Content, empty body (WIRE §1).
	w.WriteHeader(http.StatusNoContent)
}

// loadStep returns the currently-registered step under the read lock.
func (s *Server) loadStep() func(ctx context.Context, msg raft.Message) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.step
}

// writeError maps err onto the WIRE §3 (status, sentinel, leaderHint) triple
// via statusForError (wire.go — the single error table shared with the client),
// sets X-Raft-Leader-Hint when a non-empty hint is present, and writes the JSON
// error envelope.
func writeError(w http.ResponseWriter, err error) {
	status, sentinel, hint := statusForError(err)

	// SC4 / WIRE §4: emit the header verbatim, OMITTED (not empty) when the hint
	// is empty.
	if hint != "" {
		w.Header().Set("X-Raft-Leader-Hint", string(hint))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	// leader_hint appears in the envelope only for the not_leader case with a
	// known leader; omitempty keeps it out otherwise.
	_ = json.NewEncoder(w).Encode(errorEnvelope{Error: sentinel, LeaderHint: hint})
}

// isJSONContentType reports whether the Content-Type header names
// application/json, tolerating parameters (e.g. "application/json; charset=utf-8")
// and case per RFC 2045. A missing/invalid header is not JSON (WIRE §1 -> 415).
func isJSONContentType(ct string) bool {
	if ct == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return strings.EqualFold(mediaType, "application/json")
}
