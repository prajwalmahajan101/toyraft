package raft

import (
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// Storage is the subset of pkg/storage.Storage that the internal *node
// consumes. Declared here (rather than imported) to defeat the import
// cycle pkg/raft -> pkg/storage -> pkg/raft. pkg/storage.Storage
// structurally satisfies this interface, so callers may pass any
// concrete storage backend (memory, file) without an explicit cast.
//
// Method signatures MUST stay byte-identical to pkg/storage.Storage so
// the structural-typing assertion holds; the LLD-drift gate
// (scripts/check-lld-drift) flags divergence.
type Storage interface {
	// LogStorage methods
	Append(entries []Entry) error
	TruncateSuffix(from Index) error
	Entries(lo, hi Index) ([]Entry, error)
	Term(index Index) (Term, error)
	FirstIndex() (Index, error)
	LastIndex() (Index, error)

	// StateStorage methods
	SaveHardState(hs HardState) error
	LoadHardState() (HardState, error)
	Snapshot() (data []byte, lastIndex Index, err error)
	Restore(data []byte) error
}

// Config carries the construction-time parameters of a Raft node.
//
// Field names mirror docs/LLD.md §2-§3 and MUST NOT drift; pkg/storage and
// future drivers reference the durations directly. A zero-value Config is
// NOT valid — it lacks Storage, Transport, StateMachine and NodeID; callers
// should populate at least NodeID, Peers, Storage, Transport, and
// StateMachine. Defaults for the timeouts are applied by applyDefaults
// during newNode construction (per RESEARCH §Pattern 4).
//
// The three timer fields are explicit time.Duration values (never raw
// ints, never tick counts) so callers reading the struct can reason in
// wall-clock units. The internal *node converts them to tick counts at
// construction time — the state machine itself only ever sees tick
// counters (CONCURRENCY §6 / Pitfall C-6).
type Config struct {
	// NodeID is this node's stable identifier. MUST be non-empty and MUST
	// appear in Peers (Pitfall 8 — peers includes self). LLD §3 (R-2).
	NodeID NodeID

	// Peers is the full cluster membership including ID. Order is not
	// significant; newNode sorts a clone via slices.Sort to defeat
	// platform-dependent map-iteration order drift (C-8).
	Peers []NodeID

	// ElectionTimeoutMin / ElectionTimeoutMax bracket the randomised
	// election timeout draw (Raft §5.2). Min < Max strictly. Defaults:
	// 150ms / 300ms (LLD §2; RATIFIED decision 1 — see applyDefaults).
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is the leader's heartbeat cadence. P1-5 requires
	// HeartbeatInterval*3 <= ElectionTimeoutMin so a single dropped
	// heartbeat does not provoke an election. Default: 50ms (REPL-01 /
	// LLD §2; RATIFIED decision 1 — see applyDefaults).
	HeartbeatInterval time.Duration

	// Seed deterministically seeds the per-node math/rand/v2 RNG that
	// draws election timeouts. Zero degrades to Clock-driven entropy
	// (clk.Now().UnixNano() XOR FNV(nodeID)) so the unset case still
	// produces divergent per-node draws. Callers wanting determinism
	// must supply a nonzero value.
	Seed int64

	// Clock is the time source. nil is replaced by clock.NewReal()
	// during applyDefaults. Tests supply clock.NewFake() for determinism
	// (Phase 4 ADR-0006). Wall-clock access in pkg/raft routes through
	// this Clock (gated by scripts/check-no-time-now.sh).
	Clock clock.Clock

	// Storage is the durable backing store. MUST be non-nil; v1 always
	// uses pkg/storage/memory in tests and pkg/storage/file in production.
	// The Storage interface is declared locally in this package (see
	// type Storage below) to avoid the pkg/raft <-> pkg/storage import
	// cycle; pkg/storage.Storage structurally satisfies it.
	Storage Storage

	// Transport ships Raft Messages to peers (best-effort, lossy-tolerant;
	// LLD §3). MUST be non-nil. The driver (07-03) calls Register before the
	// tick loop starts and routes outbound Ready messages through Send.
	Transport Transport

	// StateMachine is the consumer-owned replicated state; the Apply target
	// for committed entries (LLD §3). MUST be non-nil. Apply is invoked
	// exactly once per committed entry, in index order, from one goroutine.
	StateMachine StateMachine

	// StopTimeout bounds Stop()'s drain of the apply channel and tick-loop
	// join. Default 5s (API-08/SC5; applied by applyDefaults).
	StopTimeout time.Duration

	// Logger is the structured logger. nil is replaced by a silent logger
	// (slog.New(slog.DiscardHandler)) during applyDefaults — a library MUST
	// NOT write to stderr unless the consumer opts in by setting Logger.
	Logger *slog.Logger
}

// applyDefaults fills zero-valued timer fields with their LLD §2 defaults.
// Idempotent: re-running over a populated Config is a no-op for each
// already-set field.
func (c *Config) applyDefaults() {
	// RATIFIED decision 1 (locked): defaults are the LLD §2 timings
	// 50/150/300 ms, NOT the prior 100/300/600. REPL-01 / SC1 lock the
	// 50 ms heartbeat default, and the LLD §2 pairing gives a comfortable
	// heartbeat:election ratio of 3x at min and 6x at max. This is a
	// deliberate spec-alignment fix, not drift. Validate's
	// HeartbeatInterval*3 <= ElectionTimeoutMin invariant (a frozen
	// formula) now reads 150 <= 150 at the default — valid at the
	// boundary; the formula is unchanged.
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.Clock == nil {
		c.Clock = clock.NewReal()
	}
	if c.StopTimeout == 0 {
		c.StopTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		// R-3: a library defaults to SILENT — no stderr output unless the
		// consumer sets Config.Logger. slog.Default() would leak to stderr.
		c.Logger = slog.New(slog.DiscardHandler)
	}
}

// Validate checks the configured invariants. All failures wrap
// ErrInvalidConfig so callers can use errors.Is for a coarse classifier
// while %w preserves the specific field cause for logs.
//
// Invariants enforced:
//   - NodeID is non-empty.
//   - Storage is non-nil.
//   - Peers contains NodeID (Pitfall 8 — self must be in peers).
//   - Peers has odd N (R-1 — even N can split quorum; SC1 / API-01).
//   - Transport is non-nil (R-2).
//   - StateMachine is non-nil (R-2).
//   - ElectionTimeoutMin < ElectionTimeoutMax (strictly).
//   - HeartbeatInterval*3 <= ElectionTimeoutMin (P1-5 — one dropped
//     heartbeat must not trigger an election under the configured budget).
//
// Validate is called by newNode AFTER applyDefaults; callers may invoke
// it directly to sanity-check a populated Config before construction.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("%w: NodeID is empty", ErrInvalidConfig)
	}
	if c.Storage == nil {
		return fmt.Errorf("%w: Storage is nil", ErrInvalidConfig)
	}
	if !slices.Contains(c.Peers, c.NodeID) {
		return fmt.Errorf("%w: Peers does not contain NodeID %q (Pitfall 8 — self must appear in Peers)", ErrInvalidConfig, c.NodeID)
	}
	if len(c.Peers)%2 == 0 {
		return fmt.Errorf("%w: Peers must be odd for a clean majority, got %d (even N can split quorum)", ErrInvalidConfig, len(c.Peers))
	}
	if c.Transport == nil {
		return fmt.Errorf("%w: Transport is nil", ErrInvalidConfig)
	}
	if c.StateMachine == nil {
		return fmt.Errorf("%w: StateMachine is nil", ErrInvalidConfig)
	}
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return fmt.Errorf("%w: ElectionTimeoutMin (%s) must be < ElectionTimeoutMax (%s)", ErrInvalidConfig, c.ElectionTimeoutMin, c.ElectionTimeoutMax)
	}
	if c.HeartbeatInterval*3 > c.ElectionTimeoutMin {
		return fmt.Errorf("%w: HeartbeatInterval*3 (%s) > ElectionTimeoutMin (%s) — P1-5 margin violated", ErrInvalidConfig, c.HeartbeatInterval*3, c.ElectionTimeoutMin)
	}
	return nil
}

// tickInterval returns the wall-clock duration of one tick. The driver
// (Phase 7) calls node.Step(Message{Type: MsgTick}) at this cadence; the
// internal state machine converts every other duration into a count of
// these ticks at construction time (RESEARCH §Pattern 4).
func (c *Config) tickInterval() time.Duration {
	return c.HeartbeatInterval
}
