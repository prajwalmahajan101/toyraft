package http_test

import (
	"net"
	"os/exec"
	"regexp"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	nethttp "github.com/prajwalmahajan101/toyraft/pkg/transport/http"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/transporttest"
)

// freeAddr reserves an ephemeral loopback port by binding and immediately
// releasing a listener, returning "127.0.0.1:PORT". The tiny bind/close race
// window is acceptable for a local test: the port is handed straight to the
// server which re-binds it. Using a discovered port (rather than :0 + late
// lookup) lets both nodes cross-reference each other's PeerURLs before either
// listener is up.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve ephemeral port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release ephemeral port: %v", err)
	}
	return addr
}

// waitListening blocks until the server at addr accepts a TCP connection or the
// deadline passes. Start() returns before ListenAndServe has bound, so the
// conformance suite's first Send could otherwise race the listener. This is a
// real-clock readiness gate (loopback comes up in single-digit ms), not a
// logical timer — it mirrors the suite's own bounded-wait discipline.
func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("server at %s never came up", addr)
}

// newHTTPPair builds two connected http.Transports (node A, node B) each on its
// own real loopback listener, cross-referencing the other's URL in PeerURLs. It
// is the factory transporttest.RunConformance drives for the HTTP side of SC1 —
// the SAME suite pkg/transport/inproc satisfies. The pair is lossless (loopback
// HTTP, healthy config: single attempt, no injected failure), as the suite
// requires. cleanup closes both transports (joining their listener goroutines).
func newHTTPPair(t *testing.T) (a, b raft.Transport, cleanup func()) {
	t.Helper()

	addrA := freeAddr(t)
	addrB := freeAddr(t)
	urlA := "http://" + addrA
	urlB := "http://" + addrB

	cfgA := nethttp.Config{
		NodeID:      "A",
		ListenAddr:  addrA,
		PeerURLs:    map[raft.NodeID]string{"B": urlB},
		Clock:       clock.NewReal(),
		SendTimeout: 2 * time.Second,
		Backoff:     nethttp.BackoffConfig{Base: time.Millisecond, Factor: 2, MaxAttempts: 1},
	}
	cfgB := nethttp.Config{
		NodeID:      "B",
		ListenAddr:  addrB,
		PeerURLs:    map[raft.NodeID]string{"A": urlA},
		Clock:       clock.NewReal(),
		SendTimeout: 2 * time.Second,
		Backoff:     nethttp.BackoffConfig{Base: time.Millisecond, Factor: 2, MaxAttempts: 1},
	}

	ta, err := nethttp.New(cfgA)
	if err != nil {
		t.Fatalf("New(A): %v", err)
	}
	tb, err := nethttp.New(cfgB)
	if err != nil {
		_ = ta.Close()
		t.Fatalf("New(B): %v", err)
	}

	// Ensure both listeners are accepting before the suite Sends (Start returns
	// before ListenAndServe binds).
	waitListening(t, addrA)
	waitListening(t, addrB)

	return ta, tb, func() {
		_ = ta.Close()
		_ = tb.Close()
	}
}

// TestHTTPConformance is the HTTP side of SC1: the unified http.Transport passes
// the impl-agnostic C1-C9 behavioural suite (transporttest.RunConformance) over
// real httptest-grade loopback pairs — the exact suite pkg/transport/inproc runs
// in 09-01.
func TestHTTPConformance(t *testing.T) {
	transporttest.RunConformance(t, func() (raft.Transport, raft.Transport, func()) {
		return newHTTPPair(t)
	})
}

// externalHTTPLibs matches any third-party HTTP framework whose presence in the
// dependency graph would violate SC2's stdlib-only mandate.
var externalHTTPLibs = regexp.MustCompile(`(?i)chi|gorilla|gin|echo|fasthttp`)

// TestNoExternalHTTPDeps encodes SC2 as a test: `go list -deps
// ./pkg/transport/http` must name NO external HTTP library (chi/gorilla/gin/
// echo/fasthttp). It shells out to the toolchain so the assertion tracks the
// REAL resolved dependency graph, not just the import list of these files.
func TestNoExternalHTTPDeps(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "./").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps failed: %v\n%s", err, out)
	}
	for _, line := range splitLines(string(out)) {
		if externalHTTPLibs.MatchString(line) {
			t.Fatalf("SC2 violated: external HTTP lib in dependency graph: %q", line)
		}
	}
}

// splitLines splits go list output on newlines, dropping the trailing empty
// element, without pulling in strings just for a Split.
func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
