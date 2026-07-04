package kvsm_test

import (
	"bytes"
	"encoding/json"
	"sync"
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/kvsm"
	"github.com/prajwalmahajan101/toyraft/pkg/raft"
)

// setOp marshals a "set" Op into a raft.Entry the apply loop would deliver.
func setOp(t *testing.T, key string, value []byte) raft.Entry {
	t.Helper()
	data, err := json.Marshal(kvsm.Op{Kind: "set", Key: key, Value: value})
	if err != nil {
		t.Fatalf("marshal set op: %v", err)
	}
	return raft.Entry{Data: data}
}

// delOp marshals a "del" Op into a raft.Entry.
func delOp(t *testing.T, key string) raft.Entry {
	t.Helper()
	data, err := json.Marshal(kvsm.Op{Kind: "del", Key: key})
	if err != nil {
		t.Fatalf("marshal del op: %v", err)
	}
	return raft.Entry{Data: data}
}

func TestApplySetGetDelete(t *testing.T) {
	k := kvsm.New()

	// SET returns the stored value and no error.
	res, err := k.Apply(setOp(t, "k", []byte("v")))
	if err != nil {
		t.Fatalf("Apply set: unexpected error: %v", err)
	}
	got, ok := res.([]byte)
	if !ok || !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Apply set result = %v (%T); want []byte(\"v\")", res, res)
	}

	// GET observes the SET.
	v, ok := k.Get("k")
	if !ok || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("Get after set = (%q, %v); want (\"v\", true)", v, ok)
	}

	// DELETE returns (nil, nil).
	res, err = k.Apply(delOp(t, "k"))
	if err != nil {
		t.Fatalf("Apply del: unexpected error: %v", err)
	}
	if res != nil {
		t.Fatalf("Apply del result = %v; want nil", res)
	}

	// GET no longer sees the key.
	if v, ok := k.Get("k"); ok {
		t.Fatalf("Get after del = (%q, true); want (nil, false)", v)
	}
}

func TestApplyUnknownOp(t *testing.T) {
	k := kvsm.New()
	data, err := json.Marshal(kvsm.Op{Kind: "frob", Key: "k"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := k.Apply(raft.Entry{Data: data}); err == nil {
		t.Fatal("Apply unknown op: want non-nil error, got nil")
	}
}

func TestApplyBadJSON(t *testing.T) {
	k := kvsm.New()
	// Seed one key so we can assert the map is unchanged after a bad decode.
	if _, err := k.Apply(setOp(t, "keep", []byte("me"))); err != nil {
		t.Fatalf("seed set: %v", err)
	}

	if _, err := k.Apply(raft.Entry{Data: []byte("{not json")}); err == nil {
		t.Fatal("Apply bad JSON: want non-nil error, got nil")
	}

	// Map is unchanged: the seeded key survives, no phantom key appeared.
	if v, ok := k.Get("keep"); !ok || !bytes.Equal(v, []byte("me")) {
		t.Fatalf("Get after bad json = (%q, %v); want (\"me\", true)", v, ok)
	}
}

func TestApplyDeterministic(t *testing.T) {
	ops := []raft.Entry{
		setOp(t, "a", []byte("1")),
		setOp(t, "b", []byte("2")),
		setOp(t, "a", []byte("3")), // overwrite
		delOp(t, "b"),
		setOp(t, "c", []byte("4")),
	}

	k1, k2 := kvsm.New(), kvsm.New()
	for _, e := range ops {
		if _, err := k1.Apply(e); err != nil {
			t.Fatalf("k1 apply: %v", err)
		}
		if _, err := k2.Apply(e); err != nil {
			t.Fatalf("k2 apply: %v", err)
		}
	}

	for _, key := range []string{"a", "b", "c"} {
		v1, ok1 := k1.Get(key)
		v2, ok2 := k2.Get(key)
		if ok1 != ok2 || !bytes.Equal(v1, v2) {
			t.Fatalf("determinism broke at %q: k1=(%q,%v) k2=(%q,%v)",
				key, v1, ok1, v2, ok2)
		}
	}
}

func TestConcurrentApplyAndGet(t *testing.T) {
	k := kvsm.New()
	const iters = 1000

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer: hammer Apply(setOp) from the single-apply-goroutine role.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if _, err := k.Apply(setOp(t, "k", []byte("v"))); err != nil {
				t.Errorf("concurrent Apply: %v", err)
				return
			}
		}
	}()

	// Reader: hammer Get from the client-API-goroutine role.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			k.Get("k")
		}
	}()

	wg.Wait() // join before return: goleak-clean.
}
