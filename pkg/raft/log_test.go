package raft

import (
	"errors"
	"io"
	"testing"
)

// TestLog_ZeroValue asserts the FOUND-05 / SC5 zero-value contract: a
// freshly-declared Log{} is a valid empty log without construction.
func TestLog_ZeroValue(t *testing.T) {
	var l Log
	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0", got)
	}
	if got := l.LastTerm(); got != 0 {
		t.Fatalf("LastTerm() = %d, want 0", got)
	}
	if !l.Match(0, 0) {
		t.Fatalf("Match(0, 0) = false, want true (pre-log sentinel)")
	}
	term, err := l.Term(0)
	if err != nil {
		t.Fatalf("Term(0) err = %v, want nil", err)
	}
	if term != 0 {
		t.Fatalf("Term(0) = %d, want 0", term)
	}
}

func TestLog_AppendSingle(t *testing.T) {
	var l Log
	l.Append(Entry{Term: 1, Index: 1, Data: []byte("a")})
	if got := l.LastIndex(); got != 1 {
		t.Fatalf("LastIndex() = %d, want 1", got)
	}
	if got := l.LastTerm(); got != 1 {
		t.Fatalf("LastTerm() = %d, want 1", got)
	}
	term, err := l.Term(1)
	if err != nil || term != 1 {
		t.Fatalf("Term(1) = (%d, %v), want (1, nil)", term, err)
	}
	if !l.Match(1, 1) {
		t.Fatalf("Match(1,1) = false, want true")
	}
	if l.Match(1, 2) {
		t.Fatalf("Match(1,2) = true, want false")
	}
}

func TestLog_AppendBatch(t *testing.T) {
	var l Log
	l.Append(
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
		Entry{Term: 2, Index: 3},
	)
	if got := l.LastIndex(); got != 3 {
		t.Fatalf("LastIndex() = %d, want 3", got)
	}
	if got := l.LastTerm(); got != 2 {
		t.Fatalf("LastTerm() = %d, want 2", got)
	}
	term, err := l.Term(2)
	if err != nil || term != 1 {
		t.Fatalf("Term(2) = (%d, %v), want (1, nil)", term, err)
	}
}

func TestLog_AppendThenAppend(t *testing.T) {
	var l Log
	l.Append(
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
	)
	l.Append(
		Entry{Term: 1, Index: 3},
		Entry{Term: 2, Index: 4},
	)
	if got := l.LastIndex(); got != 4 {
		t.Fatalf("LastIndex() = %d, want 4", got)
	}
	if got := l.LastTerm(); got != 2 {
		t.Fatalf("LastTerm() = %d, want 2", got)
	}
}

func TestLog_AppendEmpty(t *testing.T) {
	var l Log
	// Empty variadic call must be a no-op (FOUND-05 symmetry).
	l.Append()
	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() after empty Append = %d, want 0", got)
	}
	// Same after a real append.
	l.Append(Entry{Term: 1, Index: 1})
	l.Append()
	if got := l.LastIndex(); got != 1 {
		t.Fatalf("LastIndex() after empty Append on non-empty log = %d, want 1", got)
	}
}

func appendN(l *Log, n int, term Term) {
	for i := 1; i <= n; i++ {
		l.Append(Entry{Term: term, Index: Index(int(l.LastIndex()) + 1)})
		_ = i
	}
}

func TestLog_TruncateSuffix_Mid(t *testing.T) {
	var l Log
	appendN(&l, 5, 1)
	l.TruncateSuffix(3)
	if got := l.LastIndex(); got != 2 {
		t.Fatalf("LastIndex() after TruncateSuffix(3) = %d, want 2", got)
	}
	term, err := l.Term(2)
	if err != nil || term != 1 {
		t.Fatalf("Term(2) = (%d, %v), want (1, nil)", term, err)
	}
	term, err = l.Term(1)
	if err != nil || term != 1 {
		t.Fatalf("Term(1) = (%d, %v), want (1, nil)", term, err)
	}
}

func TestLog_TruncateSuffix_NoOp_PastTail(t *testing.T) {
	var l Log
	appendN(&l, 5, 1)
	l.TruncateSuffix(99)
	if got := l.LastIndex(); got != 5 {
		t.Fatalf("LastIndex() = %d, want 5 (no-op)", got)
	}
}

func TestLog_TruncateSuffix_All(t *testing.T) {
	var l Log
	appendN(&l, 5, 1)
	l.TruncateSuffix(1)
	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0", got)
	}
}

func TestLog_TruncateSuffix_Zero(t *testing.T) {
	var l Log
	appendN(&l, 5, 1)
	l.TruncateSuffix(0)
	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() after TruncateSuffix(0) = %d, want 0", got)
	}
}

func TestLog_TruncateSuffix_AtLastPlusOne(t *testing.T) {
	var l Log
	appendN(&l, 5, 1)
	l.TruncateSuffix(6)
	if got := l.LastIndex(); got != 5 {
		t.Fatalf("LastIndex() = %d, want 5 (idempotent no-op)", got)
	}
}

func TestLog_Term_Sentinel(t *testing.T) {
	var l Log
	term, err := l.Term(0)
	if err != nil || term != 0 {
		t.Fatalf("Term(0) on empty = (%d, %v), want (0, nil)", term, err)
	}
	l.Append(Entry{Term: 3, Index: 1}, Entry{Term: 3, Index: 2})
	term, err = l.Term(0)
	if err != nil || term != 0 {
		t.Fatalf("Term(0) on non-empty = (%d, %v), want (0, nil)", term, err)
	}
}

func TestLog_Term_OutOfRange(t *testing.T) {
	var l Log
	l.Append(Entry{Term: 1, Index: 1})
	term, err := l.Term(2)
	if err == nil {
		t.Fatalf("Term(2) err = nil, want non-nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Term(2) err = %v, want errors.Is(err, io.ErrUnexpectedEOF)", err)
	}
	if term != 0 {
		t.Fatalf("Term(2) term = %d, want 0 on error", term)
	}
}

func TestLog_Match_PreLog(t *testing.T) {
	var l Log
	if !l.Match(0, 0) {
		t.Fatalf("Match(0,0) on empty = false, want true")
	}
	if l.Match(0, 1) {
		t.Fatalf("Match(0,1) on empty = true, want false (prevTerm must be 0 when prevIdx=0)")
	}
	l.Append(
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
		Entry{Term: 2, Index: 3},
	)
	if !l.Match(0, 0) {
		t.Fatalf("Match(0,0) on 3-entry log = false, want true")
	}
	if l.Match(0, 1) {
		t.Fatalf("Match(0,1) on 3-entry log = true, want false")
	}
}

func TestLog_Match_HitMiss(t *testing.T) {
	var l Log
	l.Append(
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
		Entry{Term: 2, Index: 3},
	)
	if !l.Match(2, 1) {
		t.Fatalf("Match(2, 1) = false, want true")
	}
	if l.Match(2, 99) {
		t.Fatalf("Match(2, 99) = true, want false")
	}
	if !l.Match(3, 2) {
		t.Fatalf("Match(3, 2) = false, want true")
	}
}

func TestLog_Match_OutOfRange(t *testing.T) {
	var l Log
	l.Append(Entry{Term: 1, Index: 1})
	if l.Match(2, 1) {
		t.Fatalf("Match(2, 1) on 1-entry log = true, want false")
	}
	if l.Match(99, 1) {
		t.Fatalf("Match(99, 1) = true, want false")
	}
}

func TestLog_LastIndexLastTerm_Empty(t *testing.T) {
	var l Log
	if got := l.LastIndex(); got != 0 {
		t.Fatalf("LastIndex() = %d, want 0", got)
	}
	if got := l.LastTerm(); got != 0 {
		t.Fatalf("LastTerm() = %d, want 0", got)
	}
}

func TestLog_AppendAfterTruncate(t *testing.T) {
	var l Log
	appendN(&l, 5, 1)
	l.TruncateSuffix(3)
	l.Append(Entry{Term: 2, Index: 3})
	if got := l.LastIndex(); got != 3 {
		t.Fatalf("LastIndex() = %d, want 3", got)
	}
	if got := l.LastTerm(); got != 2 {
		t.Fatalf("LastTerm() = %d, want 2", got)
	}
	term, err := l.Term(3)
	if err != nil || term != 2 {
		t.Fatalf("Term(3) = (%d, %v), want (2, nil)", term, err)
	}
}
