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
// Field names mirror docs/LLD.md §2 and MUST NOT drift; pkg/storage and
// future drivers reference the durations directly. A zero-value Config is
// NOT valid — it lacks Storage and ID; callers should populate at least
// ID, Peers, and Storage. Defaults for the three timeouts are applied by
// applyDefaults during newNode construction (per RESEARCH §Pattern 4).
//
// The three timer fields are explicit time.Duration values (never raw
// ints, never tick counts) so callers reading the struct can reason in
// wall-clock units. The internal *node converts them to tick counts at
// construction time — the state machine itself only ever sees tick
// counters (CONCURRENCY §6 / Pitfall C-6).
type Config struct {
	// ID is this node's stable identifier. MUST be non-empty and MUST
	// appear in Peers (Pitfall 8 — peers includes self).
	ID NodeID

	// Peers is the full cluster membership including ID. Order is not
	// significant; newNode sorts a clone via slices.Sort to defeat
	// platform-dependent map-iteration order drift (C-8).
	Peers []NodeID

	// ElectionTimeoutMin / ElectionTimeoutMax bracket the randomised
	// election timeout draw (Raft §5.2). Min < Max strictly. Defaults:
	// 300ms / 600ms.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// HeartbeatInterval is the leader's heartbeat cadence. P1-5 requires
	// HeartbeatInterval*3 <= ElectionTimeoutMin so a single dropped
	// heartbeat does not provoke an election. Default: 100ms.
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

	// Logger is the structured logger. nil is replaced by slog.Default()
	// during applyDefaults.
	Logger *slog.Logger
}

// applyDefaults fills zero-valued timer fields with their LLD §2 defaults.
// Idempotent: re-running over a populated Config is a no-op for each
// already-set field.
func (c *Config) applyDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 600 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.Clock == nil {
		c.Clock = clock.NewReal()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Validate checks the configured invariants. All failures wrap
// ErrInvalidConfig so callers can use errors.Is for a coarse classifier
// while %w preserves the specific field cause for logs.
//
// Invariants enforced:
//   - ID is non-empty.
//   - Storage is non-nil.
//   - Peers contains ID (Pitfall 8 — self must be in peers).
//   - ElectionTimeoutMin < ElectionTimeoutMax (strictly).
//   - HeartbeatInterval*3 <= ElectionTimeoutMin (P1-5 — one dropped
//     heartbeat must not trigger an election under the configured budget).
//
// Validate is called by newNode AFTER applyDefaults; callers may invoke
// it directly to sanity-check a populated Config before construction.
func (c *Config) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("%w: ID is empty", ErrInvalidConfig)
	}
	if c.Storage == nil {
		return fmt.Errorf("%w: Storage is nil", ErrInvalidConfig)
	}
	if !slices.Contains(c.Peers, c.ID) {
		return fmt.Errorf("%w: Peers does not contain ID %q (Pitfall 8 — self must appear in Peers)", ErrInvalidConfig, c.ID)
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
