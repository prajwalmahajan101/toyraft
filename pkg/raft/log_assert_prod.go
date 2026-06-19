//go:build !raftdebug
// +build !raftdebug

package raft

// assertAppendInvariants is the production no-op. The compiler eliminates
// the call site via dead-code elimination — zero runtime cost.
// Counterpart: log_assert_debug.go (//go:build raftdebug).
func assertAppendInvariants(_ Index, _ Term, _ []Entry) {}
