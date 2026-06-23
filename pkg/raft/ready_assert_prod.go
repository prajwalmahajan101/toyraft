//go:build !raftdebug
// +build !raftdebug

package raft

// assertReadyInvariantsLocked is the production no-op. The compiler
// eliminates the call site via dead-code elimination — zero runtime
// cost. Counterpart: ready_assert_debug.go (//go:build raftdebug).
func assertReadyInvariantsLocked(_ *node) {}
