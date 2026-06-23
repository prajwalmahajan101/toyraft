package raft

import (
	"log/slog"
	mrand "math/rand/v2"
	"slices"
	"sync"
)

// node is the internal Raft state-machine struct. It is NOT exported in
// Phase 5; Phase 7 wraps it behind the public raft.Node interface in
// node_public.go (LLD §3). Holding the type unexported here keeps the
// step()/Ready() event-loop surface internal — handlers operate on
// *node directly without going through an interface boundary that would
// invite alternative drivers (ADR-0008).
//
// Concurrency: every field is guarded by mu (ADR-0004). All *Locked
// methods MUST be called with mu held. Step is the only public mutator;
// it acquires mu exactly once and delegates to stepLocked, which
// dispatches by MessageType + role.
type node struct {
	mu sync.Mutex

	// Configured identity.
	id    NodeID
	peers []NodeID // sorted at construction (slices.Sort); INCLUDES self (Pitfall 8)

	// Persistent Raft state (mirrored to Storage on every transition).
	currentTerm Term
	votedFor    NodeID

	// Volatile per-node state.
	role          Role
	log           *Log
	commitIndex   Index
	lastApplied   Index
	leaderHint    NodeID
	lastHeartbeat int // tick counter, never time.Time (C-6 / C-7)

	// Volatile leader-only state. Maps are populated on becomeLeader (05-03)
	// and cleared by maybeStepDownLocked. Nil during follower/candidate
	// phases — leader handlers in Phase 6 lazily allocate on entry.
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// Volatile candidate-only state. Populated on becomeCandidate (05-03)
	// and cleared by maybeStepDownLocked.
	votesReceived map[NodeID]bool

	// Election timing (tick-domain). electionTimeout is the per-term
	// randomised draw from [ElectionTimeoutMin, ElectionTimeoutMax)
	// converted to ticks via cfg.tickInterval(); electionElapsed is the
	// running tick counter the follower / candidate compare against.
	rng             *mrand.Rand
	electionTimeout int
	electionElapsed int

	// stepDownEpoch is the TOCTOU-prevention token (ELEC-08 / P0-5). It
	// monotonically increments every time maybeStepDownLocked promotes a
	// higher term, allowing Ready() to discard outbound messages queued
	// under a prior role. Pending messages tag themselves with the epoch
	// at enqueue time; Ready() compares against the current epoch on
	// drain.
	stepDownEpoch uint64

	// Outbound buffers consumed by Ready() (plan 05-04 lands Ready).
	// pendingMsgs is appended to by queueMsgLocked; pendingHS is set by
	// queueHardStateLocked when (currentTerm, votedFor) changes and the
	// driver must persist before shipping any message (SC5).
	pendingMsgs []pendingMsg
	pendingHS   *HardState

	// Construction-time captured references.
	storage Storage
	cfg     *Config
	log2    *slog.Logger // alias to cfg.Logger captured at construction; named log2 to avoid clashing with the replicated log field

	// started flips true once LoadHardState completes (plan 05-02's first
	// responsibility — the start sequence). Step returns ErrStopped until
	// then; this keeps a stray MsgTick from operating on a zero
	// currentTerm before the persistent state is loaded (Pitfall 6).
	started bool
}

// pendingMsg pairs an outbound Message with the stepDownEpoch at enqueue
// time. Ready() (plan 05-04) drops any pendingMsg whose epoch is older
// than the current stepDownEpoch — that's the TOCTOU-prevention bite of
// the epoch token pattern (ADR-0008).
type pendingMsg struct {
	epoch uint64
	msg   Message
}

// newNode constructs a *node from cfg. It applies defaults, validates,
// clones + sorts the peer set (slices.Sort — C-8 prevention), and
// captures the slog logger. It does NOT call LoadHardState — that runs
// in the start sequence landed by plan 05-02 next to the RNG seed draw.
// started remains false until then; Step returns ErrStopped in the
// interim (Pitfall 6).
//
// The internal Log is constructed via &Log{} (zero-value safe per
// FOUND-05). Phase 2 deliberately exposes no NewLog constructor.
func newNode(cfg *Config) (*node, error) {
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	n := &node{
		id:         cfg.ID,
		peers:      peers,
		role:       Follower,
		log:        &Log{},
		nextIndex:  make(map[NodeID]Index),
		matchIndex: make(map[NodeID]Index),
		storage:    cfg.Storage,
		cfg:        cfg,
		log2:       cfg.Logger,
	}
	return n, nil
}

// Step is the single inbound event point for the state machine. It is
// safe for concurrent callers (Driver, tests) — serialisation is done
// by acquiring n.mu exactly once and delegating to stepLocked.
//
// Returns ErrStopped if the node has not finished its start sequence
// (LoadHardState pending). This makes a stray MsgTick during driver
// start-up a deterministic no-op error rather than a crash on zero
// state (Pitfall 6).
func (n *node) Step(m Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		return ErrStopped
	}
	return n.stepLocked(m)
}
