package sequencer_test

import (
	"errors"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestStateTransitionsAreExplicit(t *testing.T) {
	t.Parallel()

	states := []sequencer.State{
		sequencer.Pending, sequencer.Eligible, sequencer.Claimed,
		sequencer.Running, sequencer.Succeeded, sequencer.Skipped,
		sequencer.Failed, sequencer.Retryable, sequencer.Deferred,
		sequencer.Canceled, sequencer.RolledBack, sequencer.Blocked,
	}
	allowed := map[sequencer.State]map[sequencer.State]bool{
		sequencer.Pending:   {sequencer.Eligible: true, sequencer.Deferred: true, sequencer.Skipped: true, sequencer.Blocked: true, sequencer.Canceled: true},
		sequencer.Eligible:  {sequencer.Claimed: true, sequencer.Deferred: true, sequencer.Skipped: true, sequencer.Blocked: true, sequencer.Canceled: true},
		sequencer.Claimed:   {sequencer.Running: true, sequencer.Eligible: true, sequencer.Retryable: true, sequencer.Failed: true, sequencer.Canceled: true},
		sequencer.Running:   {sequencer.Succeeded: true, sequencer.Skipped: true, sequencer.Failed: true, sequencer.Retryable: true, sequencer.Deferred: true, sequencer.Blocked: true, sequencer.Canceled: true},
		sequencer.Retryable: {sequencer.Eligible: true, sequencer.Failed: true, sequencer.Canceled: true},
		sequencer.Deferred:  {sequencer.Eligible: true, sequencer.Canceled: true},
		sequencer.Failed:    {sequencer.Eligible: true, sequencer.RolledBack: true},
		sequencer.Succeeded: {sequencer.Eligible: true, sequencer.RolledBack: true},
		sequencer.Blocked:   {sequencer.Eligible: true, sequencer.Canceled: true},
	}
	for _, from := range states {
		for _, to := range states {
			err := sequencer.ValidateTransition(from, to)
			if allowed[from][to] && err != nil {
				t.Errorf("%s -> %s error = %v, want allowed", from, to, err)
			}
			if !allowed[from][to] && !errors.Is(err, sequencer.ErrInvalidTransition) {
				t.Errorf("%s -> %s error = %v, want ErrInvalidTransition", from, to, err)
			}
		}
	}

	for _, unknown := range []sequencer.State{0, 255} {
		if err := sequencer.ValidateTransition(unknown, sequencer.Eligible); !errors.Is(err, sequencer.ErrInvalidTransition) {
			t.Errorf("%d -> eligible error = %v, want ErrInvalidTransition", unknown, err)
		}
		if err := sequencer.ValidateTransition(sequencer.Eligible, unknown); !errors.Is(err, sequencer.ErrInvalidTransition) {
			t.Errorf("eligible -> %d error = %v, want ErrInvalidTransition", unknown, err)
		}
	}
}

func TestEveryStateHasStableText(t *testing.T) {
	t.Parallel()

	states := []sequencer.State{
		sequencer.Pending, sequencer.Eligible, sequencer.Claimed,
		sequencer.Running, sequencer.Succeeded, sequencer.Skipped,
		sequencer.Failed, sequencer.Retryable, sequencer.Deferred,
		sequencer.Canceled, sequencer.RolledBack, sequencer.Blocked,
	}
	for _, state := range states {
		if state.String() == "unknown" {
			t.Fatalf("state %d has no stable text", state)
		}
	}
	if got := sequencer.State(255).String(); got != "unknown" {
		t.Fatalf("unknown state string = %q", got)
	}
}
