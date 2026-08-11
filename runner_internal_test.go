package sequencer

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestDurableStateErrorPreservesTerminalClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state State
		want  error
	}{
		{Blocked, ErrBlocked},
		{Canceled, ErrCanceled},
		{Indeterminate, ErrUnknownResult},
		{DeadLettered, ErrPermanent},
		{State(255), ErrInvalidOperation},
	}
	for _, test := range tests {
		if got := durableStateError(test.state); !errors.Is(got, test.want) {
			t.Errorf("durableStateError(%s) = %v", test.state, got)
		}
	}
}

func TestRunnerChannelAndDurableRecordPredicatesAreExact(t *testing.T) {
	t.Parallel()

	if !runsChannel(nil, "data") || !runsChannel([]string{"data"}, "data") || runsChannel([]string{"schema"}, "data") {
		t.Fatal("runsChannel does not preserve all, selected, and excluded semantics")
	}
	for state := Pending; state <= DeadLettered; state++ {
		for _, mode := range []ExecutionMode{OneTime, Repeatable} {
			want := state == Eligible || state == Retryable || state == Deferred || (state == Succeeded && mode == Repeatable)
			if got := canClaimRecord(state, mode); got != want {
				t.Errorf("canClaimRecord(%s, %d) = %t, want %t", state, mode, got, want)
			}
		}
	}
}

func TestRunnerReportChannelsAreSelectedOrSortedUnique(t *testing.T) {
	t.Parallel()

	spec := func(id OperationID, channel string) OperationSpec {
		return OperationSpec{
			ID: id, Version: 1, Checksum: "sum", Description: "description", Channel: channel,
			Policy:  Policy{Mode: OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Second},
			Handler: HandlerFunc(func(context.Context, Attempt) (Output, error) { return Output{}, nil }),
		}
	}
	plan, err := CompilePlan([]OperationSpec{spec("data-a", "data"), spec("schema", "schema"), spec("data-b", "data")}, PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	all := &Runner{plan: plan}
	if got, want := all.reportChannels(), []string{"data", "schema"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("all report channels = %v, want %v", got, want)
	}
	selected := &Runner{plan: plan, options: RunnerOptions{Channels: []string{"schema"}}}
	got := selected.reportChannels()
	if !reflect.DeepEqual(got, []string{"schema"}) {
		t.Fatalf("selected report channels = %v", got)
	}
	got[0] = "mutated"
	if selected.options.Channels[0] != "schema" {
		t.Fatal("report channels alias runner options")
	}
}

func TestNextAttemptSaturatesAtPlatformMaximum(t *testing.T) {
	t.Parallel()

	maximum := ^uint(0)
	if got := nextAttempt(maximum); got != maximum {
		t.Fatalf("nextAttempt(maximum) = %d, want %d", got, maximum)
	}
	if got := nextAttempt(maximum - 1); got != maximum {
		t.Fatalf("nextAttempt(maximum-1) = %d, want %d", got, maximum)
	}
}
