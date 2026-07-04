package linearizability

import "github.com/anishathalye/porcupine"

// scenario is a named, hand-authored concurrent history. Option A is LOCKED: the
// three SC2 histories are scripted []porcupine.Operation, NOT recorded from the
// chaos harness (Option B is explicitly NOT implemented). The Phase-11 dumps are
// single-client and non-overlapping, so pointing Porcupine at them would ship a
// green gate that stays green even if Raft were broken (RESEARCH Pitfall 1). The
// scripted histories below carry >=2 clients with OVERLAPPING reads so each one is
// NON-trivially linearizable.
//
// Timestamp convention (RESEARCH Pitfall 3): small integers where overlap =>
// intersecting [Call,Return] intervals and sequential => disjoint intervals.
// No `set` uses an empty value (model forbid-empty constraint, 12-01). Any `del`
// treats the post-del observable state as absent (kvOutput{Found:false}), matching
// kvsm.go:60-77.
type scenario struct {
	name string
	ops  []porcupine.Operation
}

// setIn/getIn/delIn build the typed Input halves; obs builds a get Output. Ops are
// written out longhand (one literal per line) so each carries an explicit ClientId,
// keeping the corpus auditable op-by-op.
func setIn(key, value string) kvInput { return kvInput{Op: "set", Key: key, Value: value} }
func getIn(key string) kvInput        { return kvInput{Op: "get", Key: key} }
func delIn(key string) kvInput        { return kvInput{Op: "del", Key: key} }

// obs builds a get Output that observed value (Found=false when value is "").
func obs(value string) kvOutput { return kvOutput{Value: value, Found: value != ""} }

// scenarios is the SC2 corpus: three named concurrent multi-client histories, each
// genuinely linearizable under some interleaving. LIN-04 names the flavor; the
// histories themselves are scripted.
var scenarios = []scenario{
	// steady-state: two clients interleave set/get on two keys under normal
	// operation. The load-bearing overlap is on key "x": client 1's set(x,"v1")
	// runs [10,40] while client 2's get(x) runs [20,30] entirely INSIDE that
	// write's interval. A get is linearizable either observing the pre-write
	// absent state ("") or the post-write value ("v1"); this history observes
	// "v1", legal under the linearization set(x,"v1") < get(x)->"v1". Key "y" is
	// a disjoint sequential set-then-read that anchors the second client.
	{
		name: "steady-state",
		ops: []porcupine.Operation{
			{ClientId: 1, Input: setIn("x", "v1"), Output: kvOutput{}, Call: 10, Return: 40}, // client 1 writes x, wide interval
			{ClientId: 2, Input: getIn("x"), Output: obs("v1"), Call: 20, Return: 30},        // client 2 reads x mid-write -> v1
			{ClientId: 2, Input: setIn("y", "w1"), Output: kvOutput{}, Call: 50, Return: 60}, // client 2 writes y (disjoint)
			{ClientId: 1, Input: getIn("y"), Output: obs("w1"), Call: 65, Return: 75},        // client 1 reads y after -> w1
		},
	},

	// leader-churn: a value is committed, then (modeling a leader change) two later
	// reads by DIFFERENT clients must still observe the committed value — churn does
	// not reorder committed client effects (RESEARCH Figure-7 framing). client 1
	// commits set(k,"leader1") in [10,20]. After the (implicit) churn, client 2's
	// get(k) [30,50] OVERLAPS client 3's get(k) [40,60]; both observe "leader1".
	// The linearization set(k,"leader1") < get(k)->"leader1" < get(k)->"leader1"
	// certifies it: the committed effect survives the leadership change.
	{
		name: "leader-churn",
		ops: []porcupine.Operation{
			{ClientId: 1, Input: setIn("k", "leader1"), Output: kvOutput{}, Call: 10, Return: 20}, // committed before churn
			{ClientId: 2, Input: getIn("k"), Output: obs("leader1"), Call: 30, Return: 50},        // post-churn read A
			{ClientId: 3, Input: getIn("k"), Output: obs("leader1"), Call: 40, Return: 60},        // post-churn read B (overlaps A)
		},
	},

	// packet-loss: wider concurrency where a RETRIED write appears as two overlapping
	// set ops (a drop-delay makes the client resend the same value). client 1's
	// set(p,"retry") [10,45] and client 2's set(p,"retry") [20,50] OVERLAP each other
	// AND overlap client 3's get(p) [30,40], which observes "retry". Because both
	// writes carry the SAME value, any interleaving that orders a set before the get
	// yields "retry" — the history is linearizable despite the concurrency. A final
	// del(p) [60,70] then a get(p)->absent [75,85] confirms the del/absent collapse.
	{
		name: "packet-loss",
		ops: []porcupine.Operation{
			{ClientId: 1, Input: setIn("p", "retry"), Output: kvOutput{}, Call: 10, Return: 45},    // original write
			{ClientId: 2, Input: setIn("p", "retry"), Output: kvOutput{}, Call: 20, Return: 50},    // retried write (overlaps original)
			{ClientId: 3, Input: getIn("p"), Output: obs("retry"), Call: 30, Return: 40},           // concurrent read -> retry
			{ClientId: 1, Input: delIn("p"), Output: kvOutput{Found: false}, Call: 60, Return: 70}, // delete after writes settle
			{ClientId: 2, Input: getIn("p"), Output: obs(""), Call: 75, Return: 85},                // read after del -> absent
		},
	},
}
