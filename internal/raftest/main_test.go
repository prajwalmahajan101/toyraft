package raftest_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain enforces a clean goroutine baseline for the raftest package.
// Any Hub dispatcher or Fake clock consumer that leaks past a test's
// Cleanup will surface here as a goleak failure.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
