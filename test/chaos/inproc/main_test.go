package inproc_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces a clean goroutine baseline for the in-process chaos
// matrix package (mirrors internal/raftest/main_test.go). Any Hub dispatcher
// or Fake clock consumer that leaks past a scenario's Cleanup surfaces here
// as a goleak failure — the chaos suite drives many NewCluster/Close cycles,
// so this guards against per-scenario dispatcher/pump leaks.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
