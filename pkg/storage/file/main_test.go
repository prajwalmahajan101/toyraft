package file

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain installs goleak.VerifyTestMain as this directory's testing baseline
// (TESTING.md §8): any goroutine that escapes a test fails the suite.
//
// Package placement (08-04/08-05 coexistence): a directory may contain BOTH an
// internal test package (`file`) and an external one (`file_test`), but their
// tests compile into a SINGLE test binary that can define TestMain only ONCE.
// This TestMain lives in the INTERNAL package `file` so 08-05's fault tests
// (package file) share it directly, while it also governs the external
// conformance tests (package file_test) in the same binary — both packages
// coexist under one goleak baseline. See segment_test.go (package file) for the
// other internal test in this directory.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
