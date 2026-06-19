//go:build raftdebug
// +build raftdebug

package raft

import (
	"fmt"
	"strings"
	"testing"
)

// requirePanic asserts that fn panics with a message containing substr.
// Helper for the §4 invariant tests (RESEARCH.md §7.2).
func requirePanic(t *testing.T, substr string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected panic containing %q, got no panic", substr)
		}
		var msg string
		switch v := r.(type) {
		case string:
			msg = v
		case error:
			msg = v.Error()
		default:
			msg = fmt.Sprintf("%v", v)
		}
		if !strings.Contains(msg, substr) {
			t.Fatalf("panic %q does not contain %q", msg, substr)
		}
	}()
	fn()
}

func TestLogInvariant_IndexGap(t *testing.T) {
	var l Log
	requirePanic(t, "index gap", func() {
		l.Append(Entry{Term: 1, Index: 2}) // expects Index==1 (LastIndex+1)
	})
}

func TestLogInvariant_NonContiguousBatch(t *testing.T) {
	var l Log
	requirePanic(t, "non-contiguous batch", func() {
		l.Append(
			Entry{Term: 1, Index: 1},
			Entry{Term: 1, Index: 3}, // gap: should be Index 2
		)
	})
}

func TestLogInvariant_NonMonotonicTermBatch(t *testing.T) {
	var l Log
	requirePanic(t, "non-monotonic term", func() {
		l.Append(
			Entry{Term: 2, Index: 1},
			Entry{Term: 1, Index: 2}, // term regressed within batch
		)
	})
}

func TestLogInvariant_TermRegressionAcrossAppend(t *testing.T) {
	var l Log
	l.Append(Entry{Term: 5, Index: 1}) // prevTerm becomes 5
	requirePanic(t, "non-monotonic term", func() {
		l.Append(Entry{Term: 4, Index: 2}) // 4 < prevTerm 5
	})
}

func TestLogInvariant_ZeroTermEntry(t *testing.T) {
	var l Log
	requirePanic(t, "zero-term entry", func() {
		l.Append(Entry{Term: 0, Index: 1}) // Term 0 reserved for sentinel
	})
}
