package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestIsNotLeader covers the FRICTION-05 redirect predicate: the three
// "cannot serve as leader" errors classify true (including when wrapped), and
// unrelated errors classify false.
func TestIsNotLeader(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"ErrNotLeader", &ErrNotLeader{LeaderHint: "n2"}, true},
		{"ErrNotLeader empty hint", &ErrNotLeader{}, true},
		{"ErrProposalDropped", ErrProposalDropped, true},
		{"ErrStopped", ErrStopped, true},
		{"wrapped ErrNotLeader", fmt.Errorf("propose: %w", &ErrNotLeader{LeaderHint: "n3"}), true},
		{"wrapped ErrProposalDropped", fmt.Errorf("propose: %w", ErrProposalDropped), true},
		{"wrapped ErrStopped", fmt.Errorf("step: %w", ErrStopped), true},
		{"nil", nil, false},
		{"context.Canceled", context.Canceled, false},
		{"ErrInvalidConfig", ErrInvalidConfig, false},
		{"ErrSnapshotUnsupported", ErrSnapshotUnsupported, false},
		{"plain error", errors.New("boom"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsNotLeader(tc.err); got != tc.want {
				t.Fatalf("IsNotLeader(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
