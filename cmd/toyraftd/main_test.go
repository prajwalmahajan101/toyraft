package main

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain guards the whole cmd/toyraftd test binary against goroutine leaks:
// the handler tests use httptest + a stub Node (no real cluster, no live
// goroutines), so any residual goroutine at exit is a regression.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
