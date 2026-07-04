package inproc_test

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/raftest"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// seedFlag is the master seed for the whole matrix. `go test` forwards flags
// after `-args`, so
//
//	go test ./test/chaos/inproc -run TestSeededMatrix -args -seed=<n>
//
// sets it (SC1). ONE seed threads into raftest.NewCluster(t, N, seed), which
// seeds BOTH the per-node election RNG and all five inproc.Hub chaos sub-RNGs
// (ADR-0007 split-seed). Default 42 so a bare `go test ./...` (CI) still runs
// deterministically without any flag.
var seedFlag = flag.Int64("seed", 42, "master seed for the chaos matrix (byte-deterministic)")

// knobs is one table row: an odd node count, a tick budget, the five inproc
// Hub chaos knobs, and the POSITIVE oracles (wantCommits / wantLeaderChanges)
// asserted against the REAL committed-index delta and observed leader changes.
type knobs struct {
	name               string
	n                  int           // odd node count; RESEARCH Open-Q1 recommends 5
	ticks              int           // number of Cluster.Tick iterations
	drop               float64       // DropRate on n00
	delayMin, delayMax time.Duration // global Send delay range
	reorder            bool          // enable per-receiver reorder
	qd                 int           // reorder queueDepth; MUST be >=2 to actually shuffle (Pitfall 6)
	dup                float64       // Duplicate probability
	partition          bool          // Partition n00<->n01, Heal at healAt committed entries
	healAt             int           // committed-delta at which to Heal the partition
	wantCommits        int           // POSITIVE oracle: committed-index DELTA >= this many
	wantLeaderChanges  int           // POSITIVE oracle: >= this many leader transitions
}

// table is the seeded chaos matrix. Thresholds are VISIBLY parameterised and
// tuned against the COMMIT-INDEX signal (not append count). The last row
// ("all-five") is the SC1 combo row: it exercises DropRate + Delay +
// Reorder(qd>=2) + Duplicate + Partition/Heal SIMULTANEOUSLY.
var table = []knobs{
	// Clean baseline: proves the harness commits with no chaos.
	{name: "steady", n: 5, ticks: 60, wantCommits: 10, wantLeaderChanges: 0},
	// Loss + latency: drops from n00 + a global 5-20ms delay range.
	{name: "drop-delay", n: 5, ticks: 80, drop: 0.2, delayMin: 5 * time.Millisecond, delayMax: 20 * time.Millisecond, wantCommits: 5, wantLeaderChanges: 0},
	// Reorder (qd=5, a real shuffle) + 30% duplication.
	{name: "reorder-dup", n: 5, ticks: 80, reorder: true, qd: 5, dup: 0.3, wantCommits: 5, wantLeaderChanges: 0},
	// Partition n00 to force re-election; heal mid-run.
	{name: "partition-churn", n: 5, ticks: 120, partition: true, healAt: 8, wantCommits: 3, wantLeaderChanges: 1},
	// THE SC1 COMBO ROW: all five knobs at once.
	{name: "all-five", n: 5, ticks: 140, drop: 0.15, delayMin: 5 * time.Millisecond, delayMax: 15 * time.Millisecond, reorder: true, qd: 5, dup: 0.2, partition: true, healAt: 10, wantCommits: 3, wantLeaderChanges: 1},
}

// comboRow is the "all-five" row, reused by the byte-determinism gate.
var comboRow = table[len(table)-1]

// nodeIDs returns the harness's zero-padded node IDs n00..n0{n-1} (cluster.go
// builds IDs via fmt.Sprintf("n%02d", i)).
func nodeIDs(n int) []raft.NodeID {
	ids := make([]raft.NodeID, n)
	for i := range ids {
		ids[i] = raft.NodeID(fmt.Sprintf("n%02d", i))
	}
	return ids
}

// maxCommit returns the MAX c.CommitIndex(id) across all node ids. Commit is a
// cluster-wide monotone quantity; the max across nodes is the committed
// frontier (the REAL SC3 signal — read from the commit index, NOT from
// ProposeToLeader's leader-local append ok).
func maxCommit(c *raftest.Cluster, ids []raft.NodeID) raft.Index {
	var mx raft.Index
	for _, id := range ids {
		if ci := c.CommitIndex(id); ci > mx {
			mx = ci
		}
	}
	return mx
}

// runScenario builds a seeded cluster, applies the row's five knobs to the
// (already-seeded) Hub, drives k.ticks synchronous ticks — asserting the three
// continuous safety invariants EVERY tick — and returns the COMMITTED-index
// delta over the run, the observed leader-change count, and the recorded
// history snapshot for the Phase-12 dump.
//
// Determinism: all state is read through deterministic seams (c.Leader,
// c.CommitIndex, c.Recorder, the Assert* methods). No time.Now, no map-range
// for ordered output, no fresh un-seeded rand.
func runScenario(t *testing.T, seed int64, k knobs) (commits, leaderChanges int, snap []raftest.HistoryEvent) {
	t.Helper()

	c := raftest.NewCluster(t, k.n, seed) // seeds election RNG + Hub chaos RNGs from ONE seed
	ids := nodeIDs(k.n)

	if k.reorder && k.qd < 2 {
		t.Fatalf("reorder row %q needs qd>=2 (Pitfall 6); got %d", k.name, k.qd)
	}
	if k.drop > 0 {
		c.Hub.DropRate("n00", k.drop)
	}
	if k.delayMax > 0 {
		c.Hub.Delay(k.delayMin, k.delayMax)
	}
	if k.reorder {
		c.Hub.Reorder(true, k.qd)
	}
	if k.dup > 0 {
		c.Hub.Duplicate(k.dup)
	}
	if k.partition {
		c.Hub.Partition("n00", "n01")
	}

	startCommit := maxCommit(c, ids)
	lastLeader := raft.NodeID("")
	healed := false

	for i := range k.ticks {
		c.Tick(20 * time.Millisecond) // d <= ElectionTimeoutMin (cluster Pitfall-7)

		// Continuous correctness invariants — run EVERY tick (CHAOS-03).
		c.AssertAtMostOneLeaderPerTerm()
		c.AssertLogMatching()
		c.AssertNoCommittedEntryLost()

		if id, _ := c.Leader(); id != "" && id != lastLeader {
			leaderChanges++
			lastLeader = id
		}

		// Fire a proposal at the current leader for LIVENESS. We deliberately do
		// NOT record this call here: ProposeToLeader returns on the leader-local
		// APPEND (not commit), and its append-poll is bounded by real wall-clock
		// against a concurrent goroutine, so both its ok-bit AND the per-tick
		// commit-index TRAJECTORY are NOT per-seed byte-deterministic (only the
		// final CONVERGED committed prefix is — cf. the delivery quiesce in
		// cluster.go). The Phase-12 history is instead recorded from the settled
		// committed log after the run (recordCommittedHistory), which IS
		// byte-identical per seed. See [Rule 1] in the SUMMARY.
		payload := fmt.Appendf(nil, `{"op":"set","key":"k%d","value":"v%d"}`, i, i)
		c.ProposeToLeader(payload)

		if k.partition && !healed {
			committed := int(maxCommit(c, ids) - startCommit)
			if committed >= k.healAt {
				c.Hub.Heal("n00", "n01")
				healed = true
			}
		}
	}

	commits = int(maxCommit(c, ids) - startCommit) // COMMITTED delta — the real SC3 signal

	// Record the DETERMINISTIC history: one client op per COMMITTED entry, in
	// commit order, read from the settled committed log. This converged prefix
	// is byte-identical per seed (proven by TestSeededMatrix_SameSeedIdenticalTrace),
	// unlike the racy in-flight proposal timing above — and it is exactly the
	// linearizable operation sequence Phase 12's Porcupine consumer needs.
	recordCommittedHistory(c, ids)
	return commits, leaderChanges, c.Recorder.Snapshot()
}

// committedLog returns the settled committed prefix (entries at indices
// 1..committedFrontier) read from the node holding the longest log. Under the
// safety invariants (LogMatching + NoCommittedEntryLost, asserted every tick)
// every node's committed prefix agrees, so this prefix is the cluster's
// committed history — and it is byte-identical per seed.
func committedLog(c *raftest.Cluster, ids []raft.NodeID) []raft.Entry {
	frontier := int(maxCommit(c, ids))
	// Pick the node with the most entries (it holds the full committed prefix).
	var best []raft.Entry
	for _, id := range ids {
		if l := c.LogOf(id); len(l) > len(best) {
			best = l
		}
	}
	if frontier > len(best) {
		frontier = len(best)
	}
	return best[:frontier]
}

// recordCommittedHistory records one deterministic HistoryEvent per committed
// entry, in commit order, into c.Recorder. Input is the entry's Data (the kvsm
// Op JSON envelope Phase 12 interprets); Output is the entry's 1-based commit
// index. The FakeClock is advanced 1ns between records so each Call/Return pair
// is distinct per the Recorder contract (BeginCall doc) while staying fully
// deterministic (no wall-clock).
func recordCommittedHistory(c *raftest.Cluster, ids []raft.NodeID) {
	for i, e := range committedLog(c, ids) {
		callID := c.Recorder.BeginCall(0, string(e.Data))
		c.Recorder.EndCall(0, callID, i+1) // Output = 1-based commit index
		c.Clock.Advance(1)                 // keep Call timestamps distinct + deterministic
	}
}

// TestSeededMatrix sweeps the five inproc Hub knobs in combination from a
// single -seed flag and asserts POSITIVE oracles on the COMMITTED-index delta
// (SC3) plus leader-change counts. Each row's history is dumped to
// test/artifacts/chaos/<seed>/ for the Phase-12 Porcupine consumer (SC4).
func TestSeededMatrix(t *testing.T) {
	for _, k := range table {
		t.Run(k.name, func(t *testing.T) {
			commits, leaderChanges, snap := runScenario(t, *seedFlag, k)

			// POSITIVE oracle on the COMMITTED signal (SC3, CHAOS-05).
			if commits < k.wantCommits {
				t.Fatalf("oracle %q: committed-index advanced %d, want >= %d (seed=%d)",
					k.name, commits, k.wantCommits, *seedFlag)
			}
			if leaderChanges < k.wantLeaderChanges {
				t.Fatalf("oracle %q: %d leader changes, want >= %d (seed=%d)",
					k.name, leaderChanges, k.wantLeaderChanges, *seedFlag)
			}

			// SC4: dump the per-scenario history for Phase 12.
			dumpHistory(t, *seedFlag, k.name, snap)
			// Also write the canonical test/artifacts/chaos/<seed>/history.json
			// (name="") for the SC1 combo row so SC4's exact path exists.
			if k.name == comboRow.name {
				dumpHistory(t, *seedFlag, "", snap)
			}
		})
	}
}

// repoRoot walks up from the test's working directory to the module root (the
// dir holding go.mod). History artifacts are anchored there — at the REPO-ROOT
// test/artifacts/ that .gitignore ignores and that the sibling process-kill
// suite (test/artifacts/chaos/11/) also writes to — NOT the package-relative
// path (go test runs with CWD = the package dir, so a bare "test/artifacts"
// would land under test/chaos/inproc/test/ and escape the gitignore).
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repoRoot: no go.mod found walking up from working dir")
		}
		dir = parent
	}
}

// dumpHistory writes events as indented JSON under the REPO-ROOT
// test/artifacts/chaos/<seed>/. A non-empty name yields "<name>-history.json";
// the empty name yields the canonical "history.json". Timestamps come from the
// FakeClock (recorder.go), so the bytes are identical per seed.
func dumpHistory(t *testing.T, seed int64, name string, events []raftest.HistoryEvent) {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "test", "artifacts", "chaos", strconv.FormatInt(seed, 10))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	b, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fn := "history.json"
	if name != "" {
		fn = name + "-history.json"
	}
	if err := os.WriteFile(filepath.Join(dir, fn), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestSeededMatrix_SameSeedIdenticalTrace is the BINDING SC1 determinism gate
// (mirrors chaos_test.go's TestHub_SameSeedIdenticalTrace): it runs the
// "all-five" combo scenario TWICE at the same seed and asserts the recorded
// histories are byte-identical. This — NOT check-no-time-now (which does not
// scan test/chaos/ and exempts *_test.go) — is what actually guards
// determinism for this package.
func TestSeededMatrix_SameSeedIdenticalTrace(t *testing.T) {
	_, _, a := runScenario(t, *seedFlag, comboRow)
	_, _, b := runScenario(t, *seedFlag, comboRow)

	if !reflect.DeepEqual(a, b) {
		aj, _ := json.Marshal(a)
		bj, _ := json.Marshal(b)
		t.Fatalf("same seed %d produced divergent history (determinism broken):\nrun A: %s\nrun B: %s",
			*seedFlag, aj, bj)
	}
}
