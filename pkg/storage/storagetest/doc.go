// Package storagetest provides a reusable conformance harness that
// verifies a storage.Storage implementation satisfies the LLD §3
// contract.
//
// Usage from an impl's _test.go file:
//
//	func TestConformance(t *testing.T) {
//	    storagetest.RunConformance(t, func(t *testing.T) storage.Storage {
//	        return mystorage.New()
//	    })
//	}
//
// Each conformance sub-test calls the factory to obtain a FRESH
// storage.Storage — sub-tests share no state. A single failing
// invariant fails one sub-test, not the whole suite.
//
// This package is intentionally public (not under internal/) per
// ROADMAP Phase 3 SC3: third-party Storage implementors are expected
// to run this harness against their own impls.
//
// Adding a new invariant: append a t.Run block inside RunConformance
// and document the LLD §3 contract line it covers. Removing an
// invariant requires an RFC — the public API includes the set of
// invariants the harness enforces.
package storagetest
