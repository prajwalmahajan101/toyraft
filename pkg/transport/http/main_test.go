package http

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain installs the package-level goleak baseline for pkg/transport/http
// (TESTING.md §8), matching the pattern in pkg/transport/inproc and
// pkg/storage/file. It governs BOTH the internal `package http` tests
// (server/client/wire/fuzz) and the external `package http_test` conformance
// suite — Go links them into a single test binary sharing this TestMain.
//
// Its job here is to prove Transport.Close is goleak-clean: New spawns a
// listener goroutine per node and Send may spawn keep-alive connection
// goroutines; if Close failed to join the listener or drop idle connections,
// the conformance pairs would leak and this baseline would fail the whole suite
// under -race.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
