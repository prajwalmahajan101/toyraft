package raftest

import (
	"sync"

	"github.com/prajwalmahajan101/toyraft/internal/clock"
)

// Recorder captures client operations as HistoryEvent values with
// FakeClock-derived timestamps. Concurrency-safe via a single mutex per
// ADR-0004 spirit. Call/Return are int64 nanoseconds from clk.Now() —
// never wall-clock — so two runs at the same seed produce a byte-identical
// snapshot (SC5).
type Recorder struct {
	clk    clock.Clock
	mu     sync.Mutex
	events []HistoryEvent
}

// NewRecorder builds a Recorder bound to clk. Panics if clk is nil — a
// missing clock is a programmer error, not a runtime condition.
func NewRecorder(clk clock.Clock) *Recorder {
	if clk == nil {
		panic("raftest: Recorder requires a non-nil Clock")
	}
	return &Recorder{clk: clk}
}

// BeginCall records an invocation and returns a callID (the invocation
// timestamp) that the caller passes back to EndCall to match the
// invocation→response pair. The Return field is set to -1 as an
// "in-flight" sentinel until EndCall fills it in.
//
// The FakeClock model (ADR-0006) is synchronous, so two BeginCalls
// cannot land at the same instant within a single goroutine; callers
// that issue concurrent BeginCalls should Advance the clock between
// them to keep callIDs unique per ClientID.
func (r *Recorder) BeginCall(clientID int, op any) (callID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	callID = r.clk.Now().UnixNano()
	r.events = append(r.events, HistoryEvent{
		ClientID: clientID,
		Input:    op,
		Call:     callID,
		Return:   -1,
	})
	return callID
}

// EndCall matches by (clientID, callID) and fills in Output/Return.
// If no matching open call exists, EndCall is a no-op — a double-EndCall
// is a test bug surfaced via missing data, not a panic-worthy condition.
func (r *Recorder) EndCall(clientID int, callID int64, result any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.events {
		e := &r.events[i]
		if e.ClientID == clientID && e.Call == callID && e.Return == -1 {
			e.Output = result
			e.Return = r.clk.Now().UnixNano()
			return
		}
	}
}

// Snapshot returns a defensive copy of the recorded events. Callers may
// freely mutate the returned slice without affecting the Recorder.
func (r *Recorder) Snapshot() []HistoryEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]HistoryEvent, len(r.events))
	copy(out, r.events)
	return out
}
