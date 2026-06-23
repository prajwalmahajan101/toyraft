package inproc_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain installs the package-level goleak baseline (TESTING.md §8).
// Combined with per-test goleak.VerifyNone in hub_test.go, this nails
// down SC4: Hub.Close releases blocked senders and leaves no residual
// goroutines.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
