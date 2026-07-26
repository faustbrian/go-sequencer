package sequencer

import (
	"errors"
	"fmt"
)

// ErrInvalidTransition reports a transition outside the explicit state graph.
var ErrInvalidTransition = errors.New("sequencer: invalid state transition")

// State is the durable current projection for one operation version.
type State uint8

const (
	// Pending is registered but not yet eligible.
	Pending State = iota + 1
	// Eligible is ready to be claimed.
	Eligible
	// Claimed has an active durable ownership lease.
	Claimed
	// Running is executing under a valid lease.
	Running
	// Succeeded completed successfully.
	Succeeded
	// Skipped completed without handler effects.
	Skipped
	// Failed exhausted its failure policy.
	Failed
	// Retryable may run again after its eligibility time.
	Retryable
	// Deferred is intentionally delayed.
	Deferred
	// Canceled was administratively stopped.
	Canceled
	// RolledBack had its effects compensated.
	RolledBack
	// Blocked cannot proceed until an explicit reset.
	Blocked
)

var stateNames = map[State]string{
	Pending: "pending", Eligible: "eligible", Claimed: "claimed",
	Running: "running", Succeeded: "succeeded", Skipped: "skipped",
	Failed: "failed", Retryable: "retryable", Deferred: "deferred",
	Canceled: "canceled", RolledBack: "rolled_back", Blocked: "blocked",
}

// String returns stable ledger text.
func (state State) String() string {
	if name, ok := stateNames[state]; ok {
		return name
	}
	return "unknown"
}

var transitions = map[State]map[State]struct{}{
	Pending:   set(Eligible, Deferred, Skipped, Blocked, Canceled),
	Eligible:  set(Claimed, Deferred, Skipped, Blocked, Canceled),
	Claimed:   set(Running, Eligible, Failed, Canceled),
	Running:   set(Succeeded, Skipped, Failed, Retryable, Deferred, Blocked, Canceled),
	Retryable: set(Eligible, Failed, Canceled),
	Deferred:  set(Eligible, Canceled),
	Failed:    set(Eligible, RolledBack),
	Succeeded: set(RolledBack),
	Blocked:   set(Eligible, Canceled),
}

func set(states ...State) map[State]struct{} {
	result := make(map[State]struct{}, len(states))
	for _, state := range states {
		result[state] = struct{}{}
	}
	return result
}

// ValidateTransition verifies an explicit durable state transition.
func ValidateTransition(from, to State) error {
	if _, ok := transitions[from][to]; !ok {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}
