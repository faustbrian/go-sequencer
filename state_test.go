package sequencer_test

import (
	"errors"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestStateTransitionsAreExplicit(t *testing.T) {
	t.Parallel()

	valid := []struct{ from, to sequencer.State }{
		{sequencer.Pending, sequencer.Eligible},
		{sequencer.Eligible, sequencer.Claimed},
		{sequencer.Claimed, sequencer.Running},
		{sequencer.Running, sequencer.Succeeded},
		{sequencer.Running, sequencer.Retryable},
		{sequencer.Retryable, sequencer.Eligible},
		{sequencer.Succeeded, sequencer.RolledBack},
	}
	for _, transition := range valid {
		if err := sequencer.ValidateTransition(transition.from, transition.to); err != nil {
			t.Errorf("%s -> %s: %v", transition.from, transition.to, err)
		}
	}

	if err := sequencer.ValidateTransition(sequencer.Succeeded, sequencer.Running); !errors.Is(err, sequencer.ErrInvalidTransition) {
		t.Fatalf("error = %v, want ErrInvalidTransition", err)
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
