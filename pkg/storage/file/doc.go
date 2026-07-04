// Package file provides an append-only, fsynced, crash-recoverable
// implementation of storage.Storage (LLD §3). It is the durable
// counterpart to pkg/storage/memory: a byte-for-byte behavioral drop-in
// that passes the identical pkg/storage/storagetest conformance suite
// while adding on-disk durability semantics UNDER the frozen 10-method
// contract (ADR-0005) — it does not change the contract.
//
// # On-disk format (STOR-03, STOR-06)
//
// The log is a sequence of segment files, each a versioned header
// (magic ‖ version) followed by length+CRC-framed records, one record
// per raft.Entry:
//
//	[uint32 len][uint32 crc][payload]
//	payload = Term(uint64 LE) ‖ Index(uint64 LE) ‖ Data(len-16 bytes)
//	crc     = CRC32-Castagnoli over payload (NOT over the len field)
//
// All multi-byte integers are little-endian. Recovery scans each segment
// and truncates at the first short-read or bad-CRC record (the torn-tail
// boundary), so a crash loses at most the single in-flight record. See
// segment.go for the framing/scan primitives and ADR-0013 for the ratified
// format.
//
// # Durability (STOR-04, STOR-05, REPL-09)
//
// Append and TruncateSuffix fsync before returning. SaveHardState is
// atomic via tmp-file + os.Rename + parent-directory fsync. Segment
// creation fsyncs the parent directory before the next Append returns.
// The durability surface (open/create/rename/dir-fsync/readdir/truncate)
// lives behind the unexported vfs seam (vfs.go) so an in-process fault
// fake can be swapped in for crash-injection tests.
//
// # Platform assumption
//
// Durability assumes a POSIX/Linux filesystem: os.Rename is atomic only
// on the same Unix filesystem (rename(2)), and directory fsync is the
// established Linux idiom for making a create/rename durable. ToyRaft is
// localhost/Linux-scoped (see docs/SECURITY.md); the assumption is
// recorded in ADR-0013.
//
// # Zero value
//
// Unlike pkg/storage/memory, the zero value of Storage is NOT usable:
// construction goes through New(dir) because opening the directory and
// running crash recovery can fail. Use a constructed *Storage only.
package file
