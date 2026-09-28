package sequencer

import (
	"context"
	"errors"
	"reflect"
	"sync"
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
			ID: id, Version: 1, Checksum: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Description: "description", Channel: channel,
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

func TestObserverDropsEventsBeyondItsPendingCapacity(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	delivered := make(chan Event, 18)
	var once sync.Once
	observer := &boundedObserver{observer: ObserverFunc(func(event Event) {
		once.Do(func() { close(started) })
		<-release
		delivered <- event
	})}
	observer.notify(Event{Attempt: 1})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	for attempt := uint(2); attempt <= 18; attempt++ {
		observer.notify(Event{Attempt: attempt})
	}
	close(release)
	deadline := time.After(time.Second)
	for {
		observer.mu.Lock()
		running := observer.running
		observer.mu.Unlock()
		if !running {
			break
		}
		select {
		case <-deadline:
			t.Fatal("observer did not drain its bounded queue")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := len(delivered); got != 17 {
		t.Fatalf("delivered %d events, want one active plus 16 pending", got)
	}
	for attempt := uint(1); attempt <= 17; attempt++ {
		if got := (<-delivered).Attempt; got != attempt {
			t.Fatalf("delivered attempt %d, want %d", got, attempt)
		}
	}
}
