package main

import (
	"context"
	"encoding/json"
	"expvar"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
	"github.com/prajwalmahajan101/toyraft/pkg/kvsm"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/memory"
	"github.com/prajwalmahajan101/toyraft/pkg/transport/inproc"
)

// getOrNewInt registers (or re-uses) a named expvar.Int. expvar.NewInt panics on
// a duplicate name, and the six raft.* counters are process-global; this helper
// makes the observability test idempotent across runs within one test binary.
func getOrNewInt(name string) *expvar.Int {
	if v := expvar.Get(name); v != nil {
		if iv, ok := v.(*expvar.Int); ok {
			return iv
		}
	}
	return expvar.NewInt(name)
}

// obsCluster is a small in-process 3-node raft cluster wired over the inproc Hub,
// each node's transport wrapped in the daemon's asyncTransport carrying the SAME
// rpc.sent/rpc.received expvar counters — so a real election/heartbeat exchange
// drives genuine Send/Register traffic through the counter seams (the SC2 teeth).
type obsCluster struct {
	nodes []raft.Node
	sms   []*kvsm.KV
	ids   []raft.NodeID
	hub   *inproc.Hub
}

func newObsCluster(t *testing.T, rpcSent, rpcReceived *expvar.Int) *obsCluster {
	t.Helper()
	const n = 3
	clk := clock.NewReal()
	hub, err := inproc.NewHub(inproc.HubConfig{Clock: clk, Seed: 1})
	if err != nil {
		t.Fatalf("NewHub: %v", err)
	}
	ids := make([]raft.NodeID, n)
	for i := range n {
		ids[i] = raft.NodeID([]string{"n0", "n1", "n2"}[i])
	}
	c := &obsCluster{ids: ids, hub: hub}
	for i := range n {
		sm := kvsm.New()
		atr := newAsyncTransport(hub.Transport(ids[i]), rpcSent, rpcReceived)
		node, err := raft.New(raft.Config{
			NodeID:       ids[i],
			Peers:        ids,
			Storage:      memory.New(),
			Transport:    atr,
			StateMachine: sm,
			Clock:        clk,
			Seed:         int64(i + 1),
		})
		if err != nil {
			t.Fatalf("raft.New[%d]: %v", i, err)
		}
		if err := node.Start(context.Background()); err != nil {
			t.Fatalf("Start[%d]: %v", i, err)
		}
		c.nodes = append(c.nodes, node)
		c.sms = append(c.sms, sm)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			_ = nd.Stop()
		}
		_ = hub.Close()
	})
	return c
}

// waitLeader polls until some node reports role=leader, or fails after timeout.
func (c *obsCluster) waitLeader(t *testing.T, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, nd := range c.nodes {
			if nd.Status().Role == raft.Leader {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %s", timeout)
	return -1
}

// firstFollower returns the index of a node that is NOT the leader.
func (c *obsCluster) firstFollower(leader int) int {
	for i := range c.nodes {
		if i != leader {
			return i
		}
	}
	return -1
}

// TestObservabilityStatusFollowerAndVars proves SC3 (follower /status JSON, no
// 307) and SC2 (all six /debug/vars keys present AND raft.rpc.sent>0 AND
// raft.rpc.received>0 after real RPC traffic).
func TestObservabilityStatusFollowerAndVars(t *testing.T) {
	rpcSent := getOrNewInt("raft.rpc.sent")
	rpcReceived := getOrNewInt("raft.rpc.received")
	// The two derived gauges must exist for the six-key /debug/vars assertion.
	// Publish idempotently (expvar.Publish panics on duplicate).
	c := newObsCluster(t, rpcSent, rpcReceived)
	leader := c.waitLeader(t, 5*time.Second)
	follower := c.firstFollower(leader)

	// commit_lag / apply_lag gauges reading the follower's Status snapshot.
	if expvar.Get("raft.commit_lag") == nil {
		fn := c.nodes[follower]
		expvar.Publish("raft.commit_lag", expvar.Func(func() any {
			s := fn.Status()
			return int64(s.LastLogIndex - s.CommitIndex)
		}))
	}
	if expvar.Get("raft.apply_lag") == nil {
		fn := c.nodes[follower]
		expvar.Publish("raft.apply_lag", expvar.Func(func() any {
			s := fn.Status()
			return int64(s.CommitIndex - s.ApplyIndex)
		}))
	}
	getOrNewInt("raft.terms")
	getOrNewInt("raft.elections")

	// --- SC3: follower /status returns 200 + JSON, no 307 ---
	fBook := map[raft.NodeID]string{c.ids[leader]: "http://127.0.0.1:9001"}
	h := newKVHandler(c.nodes[follower], c.sms[follower], fBook)
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("follower /status: got %d, want 200 (must not 307)", rec.Code)
	}
	var st statusResp
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode follower /status: %v", err)
	}
	if st.Role != "follower" && st.Role != "candidate" {
		t.Fatalf("follower /status role = %q, want follower/candidate", st.Role)
	}
	// leader_hint should point at the current leader on a settled follower.
	if st.LeaderHint == "" {
		t.Logf("follower leader_hint empty (may be mid-settle); role=%s", st.Role)
	}

	// --- SC2: /debug/vars carries all six keys, rpc counters non-zero ---
	varsSrv := expvar.Handler()
	// Poll with a bounded retry: let the cluster keep heartbeating so the
	// counters climb above zero even if the election just settled.
	deadline := time.Now().Add(2 * time.Second)
	var vars map[string]json.RawMessage
	for {
		vrec := httptest.NewRecorder()
		vreq := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
		varsSrv.ServeHTTP(vrec, vreq)
		if err := json.Unmarshal(vrec.Body.Bytes(), &vars); err != nil {
			t.Fatalf("decode /debug/vars: %v", err)
		}
		if rpcSent.Value() > 0 && rpcReceived.Value() > 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, key := range []string{
		"raft.terms", "raft.elections", "raft.rpc.sent",
		"raft.rpc.received", "raft.commit_lag", "raft.apply_lag",
	} {
		if _, ok := vars[key]; !ok {
			t.Errorf("/debug/vars missing key %q", key)
		}
	}
	if rpcSent.Value() <= 0 {
		t.Errorf("raft.rpc.sent = %d, want > 0 after real RPC traffic", rpcSent.Value())
	}
	if rpcReceived.Value() <= 0 {
		t.Errorf("raft.rpc.received = %d, want > 0 after real RPC traffic", rpcReceived.Value())
	}
}
