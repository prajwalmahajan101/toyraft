// Package inproc implements an in-process Hub for transport-layer tests.
// The PUBLIC surface (NewHub, Connect, Partition, Heal, DropRate, Delay,
// Reorder, Duplicate, Close) is FROZEN by docs/LLD.md §6 — drift requires
// an ADR and an LLD amendment in the same PR (Working Agreement 4).
//
// This package is the SOLE in-process Transport implementation in v1; a
// shared transport conformance harness will be considered only when a
// second impl arrives (Phase 9 HTTP transport).
//
// In plan 04-03 (this plan) the Hub delivers messages FIFO with no
// chaos. Per-link Partition/Drop/Delay/Reorder/Duplicate land in plan
// 04-04 layered on top of the single-dispatcher heap. See ADR-0007.
//
// Concurrency: ADR-0004 (single mutex). Hub.Close shutdown ordering
// follows CONCURRENCY.md §6 (cancel → WaitGroup join → sync.Once-guarded
// cleanup). All channel sends escape via h.ctx.Done() per CONCURRENCY.md
// §5.
package inproc
