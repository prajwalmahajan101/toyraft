package file_test

import (
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/storage"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/file"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/storagetest"
)

// TestConformance runs the full pkg/storage/storagetest suite against
// file.New, mirroring pkg/storage/memory's memory_test.go. Each sub-test gets
// a FRESH on-disk store rooted at its own t.TempDir(), so sub-tests share no
// state and the empty-directory path exercises the fresh-store contract.
//
// Using the EXTERNAL package file_test proves the PUBLIC surface of
// pkg/storage/file (New, the 10 interface methods, Close) is sufficient — no
// impl-internal seam is needed to satisfy the shared suite. Passing all 14
// sub-tests with NO carve-outs is SC1: the file impl is a behavioral drop-in
// for memory (including the HardStateRoundtrip trap that Commit is persisted).
func TestConformance(t *testing.T) {
	storagetest.RunConformance(t, func(t *testing.T) storage.Storage {
		s, err := file.New(t.TempDir())
		if err != nil {
			t.Fatalf("file.New: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	})
}
