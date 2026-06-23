package raft

import (
	"fmt"
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

	// started flips true once LoadHardState completes (plan 05-02's
	// start sequence). Step returns ErrStopped until then; this keeps
	// a stray MsgTick from operating on a zero currentTerm before the
	// persistent state is loaded (Pitfall 6).
	started bool

	// onElectionTrigger is the W2-parallel hook (05-02 <-> 05-03) for
	// the follower's election-timeout transition. When nil (default),
	// the trigger is a deterministic no-op so 05-02 lands without
	// touching 05-03's becomeCandidateLocked. 05-03 wires this to
	// becomeCandidateLocked at construction time so the post-merge
	// path is the real Raft §5.2 promotion. Tests in 05-02 install
	// recorders here to assert the trigger fires at the right tick.
	onElectionTrigger func()
}

// pendingMsg pairs an outbound Message with the stepDownEpoch at enqueue
// time. Ready() (plan 05-04) drops any pendingMsg whose epoch is older
// than the current stepDownEpoch — that's the TOCTOU-prevention bite of
// the epoch token pattern (ADR-0008).
type pendingMsg struct {
	epoch uint64
	msg   Message
}

// newNode constructs a *node from cfg and runs the full start sequence:
// apply defaults -> validate -> clone+sort peers -> LoadHardState ->
// construct per-node RNG -> draw the initial randomised election timeout
// -> flip started=true. Step returns ErrStopped before started flips
// (Pitfall 6 — pre-start state machine guard).
//
// The HardState load is the FIRST persistent-state action; a stray
// MsgTick that races construction will hit the ErrStopped guard rather
// than operate on zero currentTerm. The RNG is per-node (P1-4 — no
// shared math/rand global) and seeded via Config.Seed XOR FNV(nodeID)
// per ADR-0009. resetElectionTimeoutLocked draws the first timeout
// before started=true so a Follower's very first tick has a non-zero
// electionTimeout to compare against.
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
	// Load persisted HardState BEFORE accepting any Step events. A
	// stray MsgTick before this completes would race on zero
	// currentTerm (Pitfall 6).
	hs, err := cfg.Storage.LoadHardState()
	if err != nil {
		return nil, fmt.Errorf("raft: load hard state: %w", err)
	}
	n.currentTerm = hs.CurrentTerm
	n.votedFor = hs.VotedFor
	n.commitIndex = hs.Commit
	n.rng = newNodeRNG(cfg.Seed, cfg.ID)
	// resetElectionTimeoutLocked lives in follower.go; Go allows
	// forward references within a package.
	n.resetElectionTimeoutLocked()
	// 05-03 wires the election-trigger hook so the follower's
	// election timeout calls becomeCandidateLocked. The helper lives
	// in pkg/raft/candidate.go; 05-02 ships the hook as nil-safe so
	// its own tests pass without depending on the candidate file.
	n.wireElectionTriggerLocked()
	n.started = true
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
