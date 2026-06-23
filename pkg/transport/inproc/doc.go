// Package inproc implements an in-process Hub for transport-layer tests.
// The PUBLIC surface (NewHub, Connect, Partition, Heal, DropRate, Delay,
// Reorder, Duplicate, Close) is FROZEN by docs/LLD.md §6 — drift requires
// an ADR and an LLD amendment in the same PR (Working Agreement 4).
//
// This package is the SOLE in-process Transport implementation in v1; a
// shared transport conformance harness will be considered only when a
// second impl arrives (Phase 9 HTTP transport).
//
// The Hub delivers messages through a single dispatcher goroutine
// ordered by a (deliverAt, seq) min-heap. Plan 04-04 layered the chaos
// knobs (Partition/Heal, DropRate, Delay, Reorder, Duplicate) on top:
// every knob is driven by ONE int64 Seed via SHA-256-tagged per-knob
// sub-RNGs (see ADR-0007). Same Seed plus the same canned message
// sequence yields a byte-identical delivery trace.
//
// Concurrency: ADR-0004 (single mutex). Hub.Close shutdown ordering
// follows CONCURRENCY.md §6 (cancel → WaitGroup join → sync.Once-guarded
// cleanup). All channel sends escape via h.ctx.Done() per CONCURRENCY.md
// §5.
package inproc
