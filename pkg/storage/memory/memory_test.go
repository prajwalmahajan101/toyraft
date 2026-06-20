package memory_test

import (
	"testing"

	"github.com/prajwalmahajan101/toyraft/pkg/storage"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/memory"
	"github.com/prajwalmahajan101/toyraft/pkg/storage/storagetest"
)

// TestConformance runs the full pkg/storage/storagetest suite against
// memory.New. Sub-tests get a fresh *memory.Storage each via the
// factory. Using package memory_test (external test package) proves
// the public surface of pkg/storage/memory is sufficient.
func TestConformance(t *testing.T) {
	storagetest.RunConformance(t, func(t *testing.T) storage.Storage {
		return memory.New()
	})
}
