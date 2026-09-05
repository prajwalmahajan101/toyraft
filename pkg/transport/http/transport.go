package http

import (
	"context"
	"sync"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// Transport is the unified HTTP implementation of raft.Transport. It is the
// thin composition seam over the two focused halves built in prior plans:
//
//   - the Server (09-03) owns the INBOUND path: it binds cfg.ListenAddr, decodes
//     POST /raft/message, and drives the registered step through the WIRE §1/§3
//     status table.
//   - the client (09-04) owns the OUTBOUND path: Send resolves PeerURLs[msg.To],
//     marshals via the 09-02 wire layer, and POSTs with bounded backoff.
//
// This file adds no handler or backoff logic — it only wires both to the SAME
// Config and delegates the three raft.Transport methods:
//
//	Send     -> client.Send
//	Register -> server.Register
//	Close    -> server.Close (Shutdown + listener join) then client idle-conn cleanup
//
// All timing routes through cfg.Clock (the server's ShutdownTimeout deadline and
// the client's backoff); this file reads the wall clock only via cfg.Clock and
// so is check-no-time-now clean.
type Transport struct {
	server *Server
	client *client

	closeOnce sync.Once
	closeErr  error
}

// Compile-time proof that Transport satisfies the frozen raft.Transport
// contract (Send/Register/Close). SC1's conformance suite exercises the
// behaviour; this line guards the shape.
var _ raft.Transport = (*Transport)(nil)

// New validates cfg and assembles the unified transport from one Config: a
// shared pooled client (outbound) and a listening server (inbound). The server's
// listener starts immediately in a tracked goroutine that Close joins, so a
// caller can Send/receive as soon as New returns. Register may be called before
// or after New; the inbound handler reads the latest registered step under a
// lock.
//
// A nil cfg.Clock defaults to the real clock (parity with raft.Config), so an
// external caller who cannot construct internal/clock may leave it unset.
// PeerURLs must be non-empty (Config.Validate); a self-URL or empty NodeID is
// rejected. New returns the validation error verbatim.
func New(cfg Config) (*Transport, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	t := &Transport{
		server: NewServer(cfg),
		client: newClient(cfg),
	}
	// Start the inbound listener now; Close joins its goroutine (goleak-clean).
	t.server.Start()
	return t, nil
}

// Send routes an outbound Raft RPC to PeerURLs[msg.To] via the shared client.
// Best-effort per the raft.Transport contract: an error (unknown peer, exhausted
// backoff, ctx cancel) is returned %w-wrapped for the driver to log-and-drop; it
// never causes the Raft core to retry (the heartbeat is the retry signal).
func (t *Transport) Send(ctx context.Context, msg raft.Message) error {
	return t.client.Send(ctx, msg)
}

// Register installs the inbound step callback on the server. Safe to call before
// or after Start; a nil callback is tolerated (accepted-but-not-processed).
func (t *Transport) Register(step func(ctx context.Context, msg raft.Message) error) {
	t.server.Register(step)
}

// Close shuts the inbound server down gracefully (Shutdown within
// cfg.ShutdownTimeout, then joins the listener goroutine) and releases the
// client's pooled idle connections. It is idempotent: the first call performs
// the shutdown and returns the server's first non-nil error; subsequent calls
// return nil (sync.Once), matching the raft.Transport error contract.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		// Server.Close is itself sync.Once-guarded and joins the listener; the
		// client only needs its idle connections dropped (no goroutines to join).
		t.closeErr = t.server.Close()
		t.client.http.CloseIdleConnections()
	})
	return t.closeErr
}
