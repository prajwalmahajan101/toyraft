package raft

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain installs goleak so any future goroutine leak in pkg/raft is
// caught at test-suite teardown. Phase 2 has no goroutines, but installing
// the shim now prevents per-phase churn (TESTING.md §8; RESEARCH.md Q5).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
