// Command toyraftd is the reference ToyRaft daemon (DEMO-02/03/04). It composes
// Phase 8 file storage + Phase 10 kvsm + Phase 9 HTTP transport + Phase 7
// raft.Node, and stands up a SECOND client-facing http.Server for the KV API
// with follower 307 redirects.
//
// TWO-LISTENER model (the single load-bearing fact): each node runs TWO HTTP
// listeners. The frozen Phase 9 transport server serves ONLY POST /raft/message
// on the PEER port (consensus plane). The KV API (/kv, /status, /debug/*) is a
// SEPARATE http.Server this daemon owns on the CLIENT port. A follower's
// redirect Location points at the leader's CLIENT url — never its peer url — so
// the daemon builds a SECOND address book (clientURLs) alongside the transport's
// PeerURLs.
//
// Port scheme (LOCK): -peers carries the raft PEER addresses ("id=host:peerport").
// A node's CLIENT port is derived by the fixed convention clientPort =
// peerPort + 2000 (so the demo's peer 7001 -> client 9001, 7002 -> 9002, ...).
// This daemon's OWN client bind addr comes from -listen; every OTHER node's
// client url is derived from its peer host:port via the +2000 offset. smoke.sh
// and the README MUST agree with this scheme.
package main

import (
	"context"
	"errors"
	_ "expvar" // side-effect: registers /debug/vars on DefaultServeMux
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof" // side-effect: registers /debug/pprof/* on DefaultServeMux
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/kvsm"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	filestorage "github.com/prajwalmahajan101/toyraft/pkg/storage/file"
	httptransport "github.com/prajwalmahajan101/toyraft/pkg/transport/http"
)

// clientPortOffset is the fixed peer->client port delta (LOCK): a node's client
// port is its peer port + this offset. Demo peer 7001 -> client 9001.
const clientPortOffset = 2000

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "toyraftd:", err)
		os.Exit(1)
	}
}

// run parses flags, wires the node + two servers, and blocks until a signal
// triggers graceful shutdown. It returns a non-nil error on any startup or
// shutdown failure so main can exit non-zero — no silent fallbacks.
func run() error {
	var (
		id       = flag.String("id", "", "this node's stable NodeID; MUST appear in -peers")
		peers    = flag.String("peers", "", "cluster membership as 'id=host:peerport,...' (raft consensus plane; includes self)")
		listen   = flag.String("listen", "", "this node's CLIENT API bind address host:port (serves /kv, /status, /debug/*)")
		dataDir  = flag.String("data-dir", "", "directory for the durable append-only log + hard state")
		seed     = flag.Int64("seed", 0, "deterministic RNG seed for election-timeout draws (0 = clock-derived entropy)")
		logLevel = flag.String("log-level", "info", "log verbosity: debug|info|warn|error")
	)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(),
			"toyraftd — reference ToyRaft daemon (peer transport + client KV API)\n\n"+
				"Usage: toyraftd -id <NodeID> -peers <id=host:peerport,...> -listen <host:clientport> -data-dir <dir> [-seed N] [-log-level lvl]\n\n"+
				"Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(),
			"\nPort scheme: -peers gives PEER (consensus) addresses; each node's CLIENT\n"+
				"port is its peer port + %d (demo peer 7001 -> client 9001). -listen is\n"+
				"THIS node's client bind addr; other nodes' client urls are derived from\n"+
				"their -peers host:port via the +%d offset.\n", clientPortOffset, clientPortOffset)
	}
	flag.Parse()

	if *id == "" || *peers == "" || *listen == "" || *dataDir == "" {
		flag.Usage()
		return errors.New("-id, -peers, -listen and -data-dir are all required")
	}

	selfID := raft.NodeID(*id)

	// Parse -peers into the peer address book, then derive the three collections.
	peerAddrs, err := parsePeers(*peers)
	if err != nil {
		return err
	}
	if _, ok := peerAddrs[selfID]; !ok {
		return fmt.Errorf("-id %q does not appear in -peers", *id)
	}

	allIDs, peerURLs, clientURLs, err := deriveAddressBooks(selfID, peerAddrs)
	if err != nil {
		return err
	}

	logger, err := newLogger(*logLevel)
	if err != nil {
		return err
	}

	store, err := filestorage.New(*dataDir)
	if err != nil {
		return fmt.Errorf("open storage at %q: %w", *dataDir, err)
	}

	sm := kvsm.New()

	tr, err := httptransport.New(httptransport.Config{
		NodeID:      selfID,
		ListenAddr:  peerAddrs[selfID], // self's PEER addr from -peers, NOT -listen
		PeerURLs:    peerURLs,          // EXCLUDES self
		Clock:       clock.NewReal(),
		SendTimeout: 1 * time.Second,
		Backoff: httptransport.BackoffConfig{
			Base:        50 * time.Millisecond,
			Factor:      2,
			MaxAttempts: 3,
		},
		MaxBodyBytes:    0, // server default (8 MiB)
		ShutdownTimeout: 5 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("build transport: %w", err)
	}

	node, err := raft.New(raft.Config{
		NodeID:       selfID,
		Peers:        allIDs, // INCLUDES self; odd N enforced by Validate
		Storage:      store,
		Transport:    tr,
		StateMachine: sm,
		Seed:         *seed,
		Logger:       logger,
	})
	if err != nil {
		_ = tr.Close() // release the just-started listener before bailing
		return fmt.Errorf("build node: %w", err)
	}

	// DECISION (RESEARCH Open-Q 2): the client API is served on
	// http.DefaultServeMux so the blank imports of net/http/pprof and expvar
	// light up /debug/pprof/* and /debug/vars with zero extra wiring. We
	// register the KV routes onto the SAME default mux by delegating them to
	// newKVHandler's mux via a catch-all under the specific /kv/ + /status
	// paths (leaving /debug/* to the blank-import handlers).
	kvMux := newKVHandler(node, sm, clientURLs)
	http.Handle("/kv/", kvMux)
	http.Handle("/status", kvMux)

	kvSrv := &http.Server{Addr: *listen, Handler: http.DefaultServeMux}
	kvErrCh := make(chan error, 1)
	go func() {
		if serveErr := kvSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			kvErrCh <- serveErr
			return
		}
		kvErrCh <- nil
	}()

	// Start raft under a fresh background context — Start's ctx bounds bring-up
	// only, never node lifetime.
	if err := node.Start(context.Background()); err != nil {
		_ = kvSrv.Close()
		_ = node.Stop()
		return fmt.Errorf("start node: %w", err)
	}

	logger.Info("toyraftd up",
		"id", *id, "peer_addr", peerAddrs[selfID], "client_addr", *listen,
		"peers", len(allIDs))

	// Block until SIGINT/SIGTERM, then shut the CLIENT server down (Shutdown)
	// BEFORE node.Stop() — node.Stop closes the transport internally, so we must
	// not double-close it. No goroutine/port leak.
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-sigCtx.Done():
		logger.Info("shutdown signal received")
	case serveErr := <-kvErrCh:
		if serveErr != nil {
			_ = node.Stop()
			return fmt.Errorf("client server: %w", serveErr)
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutErr := kvSrv.Shutdown(shutCtx)
	stopErr := node.Stop() // closes the transport internally (no double-close)

	if shutErr != nil {
		return fmt.Errorf("client server shutdown: %w", shutErr)
	}
	if stopErr != nil {
		return fmt.Errorf("node stop: %w", stopErr)
	}
	logger.Info("toyraftd stopped cleanly")
	return nil
}

// parsePeers parses "id=host:peerport,id=host:peerport,..." into an address
// book. It rejects empty entries, missing '=', and duplicate IDs.
func parsePeers(spec string) (map[raft.NodeID]string, error) {
	out := make(map[raft.NodeID]string)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("malformed -peers entry %q (want id=host:peerport)", part)
		}
		if _, dup := out[raft.NodeID(id)]; dup {
			return nil, fmt.Errorf("duplicate peer id %q in -peers", id)
		}
		out[raft.NodeID(id)] = addr
	}
	if len(out) == 0 {
		return nil, errors.New("-peers is empty")
	}
	return out, nil
}

// deriveAddressBooks turns the single -peers map into the THREE collections the
// wiring needs:
//   - allIDs: ALL member ids (includes self) for raft.Config.Peers; odd N.
//   - peerURLs: "http://"+peerAddr for every id EXCEPT self (self in PeerURLs is
//     a hard http.Config.Validate error).
//   - clientURLs: the derived CLIENT url for ALL ids (redirect Location targets),
//     using clientPort = peerPort + clientPortOffset.
func deriveAddressBooks(self raft.NodeID, peerAddrs map[raft.NodeID]string) (
	allIDs []raft.NodeID, peerURLs, clientURLs map[raft.NodeID]string, err error,
) {
	peerURLs = make(map[raft.NodeID]string)
	clientURLs = make(map[raft.NodeID]string)
	for id, addr := range peerAddrs {
		allIDs = append(allIDs, id)
		if id != self {
			peerURLs[id] = "http://" + addr
		}
		curl, derr := clientURLFor(addr)
		if derr != nil {
			return nil, nil, nil, fmt.Errorf("derive client url for %q: %w", id, derr)
		}
		clientURLs[id] = curl
	}
	if len(allIDs)%2 == 0 {
		return nil, nil, nil, fmt.Errorf("cluster size must be odd for a clean majority, got %d", len(allIDs))
	}
	return allIDs, peerURLs, clientURLs, nil
}

// clientURLFor derives a node's CLIENT base url from its PEER host:port by
// adding clientPortOffset to the port (7001 -> http://host:9001).
func clientURLFor(peerAddr string) (string, error) {
	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return "", fmt.Errorf("split host:port: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", fmt.Errorf("parse port %q: %w", portStr, err)
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(port+clientPortOffset)), nil
}

// newLogger builds a slog text logger at the requested level (debug|info|warn|
// error), rejecting an unknown level with a clear error.
func newLogger(level string) (*slog.Logger, error) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown -log-level %q (want debug|info|warn|error)", level)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})), nil
}
