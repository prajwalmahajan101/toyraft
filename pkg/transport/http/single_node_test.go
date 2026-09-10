package http_test

import (
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	transporthttp "github.com/prajwalmahajan101/toyraft/pkg/transport/http"
)

// TestSelfOnlySingleNodeConfig proves friction-5: a single-node self-only
// cluster (raft.Config.Peers == [self], no peers to dial) has an EMPTY PeerURLs
// and must Validate and build. Previously Validate hard-rejected empty PeerURLs,
// so neither shipped transport could back a single-node embed.
func TestSelfOnlySingleNodeConfig(t *testing.T) {
	// Build via New: it applies the nil-Clock default (ADR-0023) then validates,
	// which is the path an external embedder uses (Validate alone still requires
	// a non-nil Clock for direct in-module callers).
	tr, err := transporthttp.New(transporthttp.Config{
		NodeID:     "n0",
		ListenAddr: freeAddr(t),
		PeerURLs:   nil, // self-only: no peer to resolve
	})
	if err != nil {
		t.Fatalf("New(self-only): %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	// Compile-time + runtime proof it is a usable raft.Transport.
	var _ raft.Transport = tr
}

// TestSelfURLStillRejected proves the self-URL guard survives the relaxation: a
// non-empty PeerURLs that contains the node's own ID is still a config error.
func TestSelfURLStillRejected(t *testing.T) {
	_, err := transporthttp.New(transporthttp.Config{
		NodeID:     "n0",
		ListenAddr: "127.0.0.1:0",
		PeerURLs:   map[raft.NodeID]string{"n0": "http://127.0.0.1:9000"},
	})
	if err == nil {
		t.Fatal("New: self-URL in PeerURLs accepted; want error")
	}
}
