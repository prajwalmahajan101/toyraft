package clock_test

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain installs goleak.VerifyTestMain as the package's testing baseline
// (TESTING.md §8). Any goroutine that escapes a test in this package will
// fail the suite, so Real/Fake helpers added by later plans must clean up
// after themselves.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
