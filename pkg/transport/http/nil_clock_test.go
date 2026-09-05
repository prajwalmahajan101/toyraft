package http_test

import (
	"context"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	nethttp "github.com/prajwalmahajan101/toyraft/pkg/transport/http"
)

// TestNewDefaultsNilClock locks in the v1.0.0-rc.2 external-constructibility fix
// (ADR-0023): an external module cannot supply a Config.Clock (internal/clock has
// no public constructor and its interface returns un-nameable types), so New must
// default a nil Clock to the real clock — parity with pkg/raft.Config. This test
// builds a two-node pair with Clock LEFT UNSET on both Configs and drives a real
// A->B round-trip, proving the defaulted clock yields a working transport. It runs
// under the package goleak TestMain, so a leaked listener/connection also fails it.
func TestNewDefaultsNilClock(t *testing.T) {
	addrA := freeAddr(t)
	addrB := freeAddr(t)
	urlA := "http://" + addrA
	urlB := "http://" + addrB

	// Clock deliberately omitted from both Configs — the external-caller scenario.
	cfgA := nethttp.Config{
		NodeID:      "A",
		ListenAddr:  addrA,
		PeerURLs:    map[raft.NodeID]string{"B": urlB},
		SendTimeout: 2 * time.Second,
		Backoff:     nethttp.BackoffConfig{Base: time.Millisecond, Factor: 2, MaxAttempts: 1},
	}
	cfgB := nethttp.Config{
		NodeID:      "B",
		ListenAddr:  addrB,
		PeerURLs:    map[raft.NodeID]string{"A": urlA},
		SendTimeout: 2 * time.Second,
		Backoff:     nethttp.BackoffConfig{Base: time.Millisecond, Factor: 2, MaxAttempts: 1},
	}

	ta, err := nethttp.New(cfgA)
	if err != nil {
		t.Fatalf("New(A) with nil Clock: %v", err)
	}
	defer func() { _ = ta.Close() }()

	tb, err := nethttp.New(cfgB)
	if err != nil {
		t.Fatalf("New(B) with nil Clock: %v", err)
	}
	defer func() { _ = tb.Close() }()

	// Register a receiver on B and confirm A's Send actually reaches it, exercising
	// the defaulted clock on both the outbound (client backoff) and inbound paths.
	got := make(chan raft.Message, 1)
	tb.Register(func(_ context.Context, m raft.Message) error {
		got <- m
		return nil
	})

	waitListening(t, addrA)
	waitListening(t, addrB)

	msg := raft.Message{Type: raft.MsgAppendEntries, Term: 1, From: "A", To: "B"}
	if err := ta.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send A->B with nil-clock transports: %v", err)
	}

	select {
	case rx := <-got:
		if rx.From != "A" || rx.To != "B" || rx.Type != raft.MsgAppendEntries {
			t.Fatalf("B received wrong message: %+v", rx)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B never received the message from A")
	}
}
