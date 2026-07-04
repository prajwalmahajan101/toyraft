package kvsm_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain installs goleak so any goroutine escaping a kvsm test (e.g. the
// concurrent Apply/Get workers) fails the suite (TESTING.md §8, Phase 9
// pattern).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
