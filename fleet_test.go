package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
	"github.com/faustbrian/golib/pkg/sequencer/sequencertest"
)

func TestFleetStopsAcceptingBeforeCancelingOwnedAttempts(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	canceledWhile := make(chan sequencer.RunnerState, 1)
	spec := validSpec("fleet.lifecycle")
	var fleet *sequencer.Fleet
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		close(started)
		<-ctx.Done()
		canceledWhile <- fleet.State()
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	fleet, err = sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions:  sequencer.RunnerOptions{Owner: "pod-1"},
		ClaimInterval:  time.Millisecond,
		RenewInterval:  5 * time.Millisecond,
		MaxConcurrency: 1,
		ShutdownWait:   time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := fleet.State(); got != sequencer.RunnerStarting {
		t.Fatalf("initial state = %s, want starting", got)
	}
	if fleet.Ready() {
		t.Fatal("starting fleet reported ready")
	}

	runContext, terminate := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- fleet.Run(runContext) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fleet did not accept an operation")
	}
	if got := fleet.State(); got != sequencer.RunnerAccepting {
		t.Fatalf("active state = %s, want accepting", got)
	}
	if !fleet.Ready() {
		t.Fatal("accepting fleet did not report ready")
	}

	terminate()
	select {
	case state := <-canceledWhile:
		if state != sequencer.RunnerDraining {
			t.Fatalf("state when attempt was canceled = %s, want draining", state)
		}
	case <-time.After(time.Second):
		t.Fatal("owned attempt was not canceled")
	}
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fleet did not stop")
	}
	if got := fleet.State(); got != sequencer.RunnerStopped {
		t.Fatalf("terminal state = %s, want stopped", got)
	}
	if fleet.Ready() {
		t.Fatal("stopped fleet reported ready")
	}
	record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if err != nil || record.State != sequencer.Canceled {
		t.Fatalf("Snapshot() = %+v, %v", record, err)
	}
}

func TestFleetRenewsAcceptedAttemptsAndStopsRenewalBeforeCompletion(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	spec := validSpec("fleet.renewal")
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		<-release
		return sequencer.Output{Summary: "done"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := newLeaseTrackingStore()
	var observedMu sync.Mutex
	var observed []sequencer.EventType
	heartbeatAttempts := make(chan uint, 1)
	observer := sequencer.ObserverFunc(func(event sequencer.Event) {
		observedMu.Lock()
		observed = append(observed, event.Type)
		observedMu.Unlock()
		if event.Type == sequencer.EventHeartbeat {
			select {
			case heartbeatAttempts <- event.Attempt:
			default:
			}
		}
	})
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "pod-renew", LeaseDuration: 2 * time.Minute, Observers: []sequencer.Observer{observer},
		},
		ClaimInterval:  time.Millisecond,
		RenewInterval:  2 * time.Millisecond,
		MaxConcurrency: 1,
		ShutdownWait:   time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-store.renewed:
	case <-time.After(time.Second):
		t.Fatal("accepted attempt lease was not renewed")
	}
	close(release)
	select {
	case <-store.completed:
	case <-time.After(time.Second):
		t.Fatal("accepted attempt did not complete")
	}
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got := store.eventsSnapshot(); got[len(got)-1] != "complete" {
		t.Fatalf("lease events = %v; renewal continued after completion", got)
	}
	observedMu.Lock()
	defer observedMu.Unlock()
	heartbeat, completed := -1, -1
	for index, event := range observed {
		if event == sequencer.EventHeartbeat && heartbeat == -1 {
			heartbeat = index
		}
		if event == sequencer.EventCompleted {
			completed = index
		}
	}
	if heartbeat == -1 || completed == -1 || heartbeat >= completed {
		t.Fatalf("observed events = %v; want heartbeat before completion", observed)
	}
	if attempt := <-heartbeatAttempts; attempt != 1 {
		t.Fatalf("heartbeat attempt = %d, want 1", attempt)
	}
}

func TestFleetFailsClosedWhenLeaseRenewalLosesOwnership(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.stale")
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		<-ctx.Done()
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &failingRenewStore{Store: memory.New(), err: sequencer.ErrStaleOwner}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod-stale"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- fleet.Run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, sequencer.ErrStaleOwner) {
			t.Fatalf("Run() error = %v, want stale owner", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fleet did not fail after losing lease ownership")
	}
	if got := fleet.State(); got != sequencer.RunnerFailed {
		t.Fatalf("state = %s, want failed", got)
	}
}

func TestFleetIgnoresRenewalCancellationAfterAttemptStops(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.renewal-cancel")
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		<-ctx.Done()
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &cancelingRenewStore{Store: memory.New(), entered: make(chan struct{})}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod-renewal-cancel"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("lease renewal did not start")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil || fleet.State() != sequencer.RunnerStopped {
		t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
	}
}

func TestFleetDrainOnlyAttemptBecomesUnknownAndCannotCompleteAfterTakeover(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	spec := validSpec("fleet.uncooperative")
	spec.Policy.Cancellation = sequencer.CancellationDrainOnly
	spec.Policy.UnknownOutcome = sequencer.UnknownOutcomeReplayIdempotent
	spec.Policy.Timeout = time.Minute
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		close(started)
		select {
		case <-ctx.Done():
			canceled <- struct{}{}
			<-release
		case <-release:
		}
		return sequencer.Output{Summary: "old owner"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := newLeaseTrackingStore()
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "pod-old", LeaseDuration: 2 * time.Minute,
		},
		ClaimInterval: time.Millisecond, RenewInterval: time.Second,
		MaxConcurrency: 1, ShutdownWait: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, terminate := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("drain-only attempt did not start")
	}
	terminate()
	select {
	case err := <-done:
		if !errors.Is(err, sequencer.ErrShutdownTimeout) {
			t.Fatalf("Run() error = %v, want shutdown timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fleet did not enforce the configured shutdown wait")
	}
	select {
	case <-canceled:
		t.Fatal("drain-only handler received shutdown cancellation")
	default:
	}
	renewalsAtReturn := store.renewalCount()
	time.Sleep(5 * time.Millisecond)
	if got := store.renewalCount(); got != renewalsAtReturn {
		t.Fatalf("renewals after shutdown return = %d, want %d", got, renewalsAtReturn)
	}

	recoveryTime := time.Now().Add(time.Hour)
	if recovered, err := store.RecoverExpired(context.Background(), recoveryTime); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	newClaim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
		OperationIDs: []sequencer.OperationID{spec.ID}, Owner: "pod-new",
		Now: recoveryTime, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), newClaim.Ownership(), recoveryTime); err != nil {
		t.Fatal(err)
	}
	close(release)
	time.Sleep(10 * time.Millisecond)
	record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if err != nil || record.Owner != "pod-new" || record.Fencing != newClaim.Attempt.Fencing || record.State != sequencer.Running {
		t.Fatalf("new ownership changed by stale completion: %+v, %v", record, err)
	}
}

func TestNewFleetRejectsLeaseRenewalThatCannotPrecedeExpiry(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.invalid-renewal")
	spec.Policy.Timeout = time.Second
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sequencer.NewFleet(plan, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod", LeaseDuration: 2 * time.Second},
		ClaimInterval: time.Millisecond, RenewInterval: 2 * time.Second,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if !errors.Is(err, sequencer.ErrInvalidFleet) {
		t.Fatalf("NewFleet() error = %v, want invalid fleet", err)
	}
	invalid := []sequencer.FleetOptions{
		{
			RunnerOptions:  sequencer.RunnerOptions{Owner: "pod"},
			MaxConcurrency: sequencer.MaxFleetConcurrency + 1,
		},
		{
			RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
			ClaimInterval: sequencer.MaxClaimInterval + time.Nanosecond,
		},
		{
			RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
			ShutdownWait:  sequencer.MaxShutdownWait + time.Nanosecond,
		},
	}
	for index, options := range invalid {
		if _, err := sequencer.NewFleet(plan, memory.New(), options); !errors.Is(err, sequencer.ErrInvalidFleet) {
			t.Errorf("invalid options %d error = %v", index, err)
		}
	}
	if _, err := sequencer.NewFleet(plan, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
		ClaimInterval: sequencer.MaxClaimInterval, RenewInterval: sequencer.DefaultRenewInterval,
		MaxConcurrency: sequencer.MaxFleetConcurrency, ShutdownWait: sequencer.MaxShutdownWait,
	}); err != nil {
		t.Fatalf("exact fleet limits error = %v", err)
	}
}

func TestFleetLifecycleTextAndRuntimeBounds(t *testing.T) {
	t.Parallel()

	states := map[sequencer.RunnerState]string{
		sequencer.RunnerStarting: "starting", sequencer.RunnerAccepting: "accepting",
		sequencer.RunnerDraining: "draining", sequencer.RunnerStopped: "stopped",
		sequencer.RunnerFailed: "failed", sequencer.RunnerState(255): "unknown",
	}
	for state, want := range states {
		if got := state.String(); got != want {
			t.Errorf("state %d text = %q, want %q", state, got, want)
		}
	}
	spec := validSpec("fleet.bounds")
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.NewFleet(plan, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "defaults"},
	}); err != nil {
		t.Fatalf("default NewFleet() error = %v", err)
	}
	invalid := []sequencer.FleetOptions{
		{RunnerOptions: sequencer.RunnerOptions{Owner: "pod"}, ClaimInterval: -time.Nanosecond},
		{RunnerOptions: sequencer.RunnerOptions{Owner: "pod"}, RenewInterval: -time.Nanosecond},
		{RunnerOptions: sequencer.RunnerOptions{Owner: "pod"}, ShutdownWait: -time.Nanosecond},
	}
	for index, options := range invalid {
		if _, err := sequencer.NewFleet(plan, memory.New(), options); !errors.Is(err, sequencer.ErrInvalidFleet) {
			t.Errorf("negative options %d error = %v", index, err)
		}
	}
	if _, err := sequencer.NewFleet(nil, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
	}); !errors.Is(err, sequencer.ErrInvalidFleet) {
		t.Fatalf("nil plan error = %v", err)
	}
	tiny := validSpec("fleet.tiny-lease")
	tiny.Policy.Timeout = time.Nanosecond
	tinyPlan, err := sequencer.CompilePlan([]sequencer.OperationSpec{tiny}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sequencer.NewFleet(tinyPlan, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod", LeaseDuration: 2 * time.Nanosecond},
	}); !errors.Is(err, sequencer.ErrInvalidFleet) {
		t.Fatalf("unrenewable lease error = %v", err)
	}
}

func TestFleetCanStopAfterRegistrationWithoutAcceptingWork(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.pre-canceled")
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelAfterRegisterStore{Store: memory.New(), cancel: cancel}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.Run(ctx); err != nil || fleet.State() != sequencer.RunnerStopped {
		t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
	}
	if _, err := store.Snapshot(context.Background(), spec.ID, spec.Version); err != nil {
		t.Fatal(err)
	}
}

func TestFleetDefaultShutdownWaitAllowsCooperativeCompletion(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	spec := validSpec("fleet.default-shutdown")
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		close(started)
		<-ctx.Done()
		time.Sleep(time.Millisecond)
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := sequencer.NewFleet(plan, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"}, ClaimInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fleet did not accept work")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil || fleet.State() != sequencer.RunnerStopped {
		t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
	}
}

func TestFleetFailsClosedOnStoreCandidateOutsideLocalRegistry(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.local")
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &unexpectedClaimStore{Store: memory.New()}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := fleet.Run(ctx); !errors.Is(err, sequencer.ErrInvalidOperation) || fleet.State() != sequencer.RunnerFailed {
		t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
	}
}

func TestFleetFailsClosedOnClaimWithoutDurableBudget(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.missing-budget")
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &zeroBudgetClaimStore{Store: memory.New(), operationID: spec.ID}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fleet.Run(ctx); !errors.Is(err, sequencer.ErrInvalidOperation) || fleet.State() != sequencer.RunnerFailed {
		t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
	}
}

func TestFleetDurableRetriesRemainBoundedAndObservable(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.retry")
	spec.Policy.MaxAttempts, spec.Policy.MaxExceptions = 2, 2
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		if invocations == 1 {
			return sequencer.Output{}, sequencer.Retry(errors.New("transient"))
		}
		return sequencer.Output{Summary: "recovered"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &completionInspectStore{Store: memory.New()}
	succeeded := make(chan struct{}, 1)
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "pod", Observers: []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
				if event.Type == sequencer.EventCompleted && event.State == sequencer.Succeeded {
					succeeded <- struct{}{}
				}
			})},
		},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-succeeded:
	case <-time.After(time.Second):
		t.Fatal("fleet did not complete bounded retry")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 2 || history[0].State != sequencer.Retryable ||
		history[0].ErrorDetail == "" || history[1].State != sequencer.Succeeded {
		t.Fatalf("History() = %+v, %v", history, err)
	}
	completions := store.completionsSnapshot()
	if len(completions) != 2 || completions[0].EligibleAt.IsZero() || completions[0].ErrorDetail == "" ||
		!completions[1].EligibleAt.IsZero() || completions[1].ErrorDetail != "" {
		t.Fatalf("completions = %+v", completions)
	}
}

func TestFleetResetStartsFreshDurableRetryBudget(t *testing.T) {
	t.Parallel()

	invocations := 0
	spec := validSpec("fleet.reset-retry-budget")
	spec.Policy.MaxAttempts, spec.Policy.MaxExceptions = 2, 2
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		if invocations == 2 {
			return sequencer.Output{}, sequencer.Retry(errors.New("transient after reset"))
		}
		return sequencer.Output{Summary: "done"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "setup"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Reset(context.Background(), sequencer.ResetRequest{
		OperationID: spec.ID, Version: spec.Version, Actor: "operator",
		Reason: "explicit replay", At: record.UpdatedAt,
	}); err != nil {
		t.Fatal(err)
	}
	succeeded := make(chan struct{}, 1)
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "pod", Observers: []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
				if event.Type == sequencer.EventCompleted && event.State == sequencer.Succeeded {
					succeeded <- struct{}{}
				}
			})},
		},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-succeeded:
	case <-time.After(time.Second):
		cancel()
		_ = awaitFleetResult(t, done)
		t.Fatal("reset replay did not receive a fresh retry budget")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 3 || history[1].State != sequencer.Retryable || history[2].State != sequencer.Succeeded {
		t.Fatalf("History() = %+v, %v", history, err)
	}
}

func TestFleetRunOwnsOneLifecycle(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	spec := validSpec("fleet.single-run")
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		close(started)
		<-ctx.Done()
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := sequencer.NewFleet(plan, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fleet did not start")
	}
	second := make(chan error, 1)
	go func() { second <- fleet.Run(context.Background()) }()
	select {
	case err := <-second:
		if !errors.Is(err, sequencer.ErrInvalidTransition) {
			t.Fatalf("second Run() error = %v", err)
		}
	case <-time.After(20 * time.Millisecond):
		t.Fatal("second Run() did not fail immediately")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	if err := fleet.Run(context.Background()); !errors.Is(err, sequencer.ErrInvalidTransition) {
		t.Fatalf("Run() after stop error = %v", err)
	}
}

func TestFleetBoundsConcurrentAcceptedAttempts(t *testing.T) {
	t.Parallel()

	started := make(chan sequencer.OperationID, 3)
	release := make(chan struct{}, 3)
	specs := make([]sequencer.OperationSpec, 3)
	for index := range specs {
		spec := validSpec(sequencer.OperationID(fmt.Sprintf("fleet.concurrent-%d", index)))
		spec.Handler = sequencer.HandlerFunc(func(_ context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
			started <- attempt.OperationID
			<-release
			return sequencer.Output{}, nil
		})
		specs[index] = spec
	}
	plan, err := sequencer.CompilePlan(specs, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan struct{}, 3)
	fleet, err := sequencer.NewFleet(plan, memory.New(), sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "pod", Observers: []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
				if event.Type == sequencer.EventCompleted {
					completed <- struct{}{}
				}
			})},
		},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 2, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	runAwaited := false
	defer func() {
		cancel()
		close(release)
		if runAwaited {
			return
		}
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	}()
	go func() { done <- fleet.Run(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("fleet did not fill concurrency")
		}
	}
	select {
	case operation := <-started:
		t.Fatalf("accepted %s above concurrency bound", operation)
	case <-time.After(10 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fleet did not accept after capacity was released")
	}
	release <- struct{}{}
	release <- struct{}{}
	for range 3 {
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatal("accepted operation did not complete")
		}
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	runAwaited = true
}

func TestFleetRecoversExpiredAcceptedAttemptBeforeClaimingTakeover(t *testing.T) {
	t.Parallel()

	initial := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	spec := validSpec("fleet.recovery")
	spec.Policy.MaxAttempts = 2
	spec.Policy.UnknownOutcome = sequencer.UnknownOutcomeReplayIdempotent
	if err := store.Register(context.Background(), []sequencer.Registration{{
		ID: spec.ID, Version: spec.Version, Checksum: spec.Checksum, Channel: spec.Channel,
		UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent,
	}}, initial); err != nil {
		t.Fatal(err)
	}
	old, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: spec.ID, Version: spec.Version, Checksum: spec.Checksum}},
		Owner:      "lost-pod", Now: initial, LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), old.Ownership(), initial); err != nil {
		t.Fatal(err)
	}
	takeover := make(chan sequencer.Attempt, 1)
	spec.Handler = sequencer.HandlerFunc(func(_ context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
		takeover <- attempt
		return sequencer.Output{}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(initial.Add(2 * time.Second))
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "replacement-pod", Clock: clock},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case attempt := <-takeover:
		if attempt.Number != 2 || attempt.Fencing <= old.Attempt.Fencing {
			t.Fatalf("takeover attempt = %+v, old = %+v", attempt, old.Attempt)
		}
	case <-time.After(time.Second):
		t.Fatal("fleet did not recover expired attempt")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 2 || history[0].State != sequencer.Indeterminate || history[0].ErrorDetail != sequencer.ErrUnknownResult.Error() {
		t.Fatalf("History() = %+v, %v", history, err)
	}
}

func TestFleetDoesNotExecuteRecoveredAttemptBeyondSharedBudget(t *testing.T) {
	t.Parallel()

	initial := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	spec := validSpec("fleet.recovery-budget")
	spec.Policy.UnknownOutcome = sequencer.UnknownOutcomeReplayIdempotent
	registration := sequencer.Registration{
		ID: spec.ID, Version: spec.Version, Checksum: spec.Checksum, Channel: spec.Channel,
		UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent,
	}
	if err := store.Register(context.Background(), []sequencer.Registration{registration}, initial); err != nil {
		t.Fatal(err)
	}
	old, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: spec.ID, Version: spec.Version, Checksum: spec.Checksum, Channel: spec.Channel}},
		Owner:      "lost-pod", Now: initial, LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), old.Ownership(), initial); err != nil {
		t.Fatal(err)
	}

	handlerCalled := make(chan struct{}, 1)
	completed := make(chan sequencer.Event, 1)
	var eventsMu sync.Mutex
	var events []sequencer.EventType
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		handlerCalled <- struct{}{}
		return sequencer.Output{Summary: "duplicate"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "replacement", Clock: newManualClock(initial.Add(2 * time.Second)),
			Observers: []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
				eventsMu.Lock()
				events = append(events, event.Type)
				eventsMu.Unlock()
				if event.Type == sequencer.EventCompleted {
					completed <- event
				}
			})},
		},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case event := <-completed:
		if event.State != sequencer.Failed || !errors.Is(event.Err, sequencer.ErrBudgetExhausted) {
			t.Fatalf("completion event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("fleet did not settle the exhausted recovered attempt")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerCalled:
		t.Fatal("handler executed beyond MaxAttempts")
	default:
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 2)
	if err != nil || len(history) != 2 || history[0].State != sequencer.Indeterminate || history[1].State != sequencer.Failed || history[1].ErrorDetail != sequencer.ErrBudgetExhausted.Error() {
		t.Fatalf("History() = %+v, %v", history, err)
	}
	eventsMu.Lock()
	gotEvents := append([]sequencer.EventType(nil), events...)
	eventsMu.Unlock()
	if want := []sequencer.EventType{sequencer.EventClaimed, sequencer.EventCompleted}; !reflect.DeepEqual(gotEvents, want) {
		t.Fatalf("events = %v, want %v", gotEvents, want)
	}
	audit, err := store.Audit(context.Background(), spec.ID, spec.Version, 10)
	if err != nil {
		t.Fatal(err)
	}
	last := audit[len(audit)-1]
	if last.Attempt != 2 || last.From != sequencer.Claimed || last.To != sequencer.Failed {
		t.Fatalf("terminal audit = %+v, want claimed -> failed for attempt 2", last)
	}
}

func TestFleetDoesNotReplayExpiredUnknownWorkByDefault(t *testing.T) {
	t.Parallel()

	initial := time.Date(2026, 8, 10, 15, 0, 0, 0, time.UTC)
	store := memory.New()
	spec := validSpec("fleet.block-unknown")
	if err := store.Register(context.Background(), []sequencer.Registration{{
		ID: spec.ID, Version: spec.Version, Checksum: spec.Checksum, Channel: spec.Channel,
	}}, initial); err != nil {
		t.Fatal(err)
	}
	claim, _ := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: spec.ID, Version: spec.Version, Checksum: spec.Checksum, Channel: spec.Channel}},
		Owner:      "lost", Now: initial, LeaseDuration: time.Second,
	})
	_, _ = store.MarkRunning(context.Background(), claim.Ownership(), initial)
	executed := make(chan struct{}, 1)
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		executed <- struct{}{}
		return sequencer.Output{}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "replacement", Clock: newManualClock(initial.Add(2 * time.Second))},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	recoveryDeadline := time.Now().Add(time.Second)
	for {
		record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
		if snapshotErr != nil {
			t.Fatal(snapshotErr)
		}
		if record.State == sequencer.Indeterminate {
			break
		}
		select {
		case <-executed:
			t.Fatal("indeterminate operation was replayed")
		default:
		}
		if time.Now().After(recoveryDeadline) {
			t.Fatalf("state = %s, want indeterminate", record.State)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executed:
		t.Fatal("indeterminate operation was replayed")
	default:
	}
	record, _ := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if record.State != sequencer.Indeterminate {
		t.Fatalf("state = %s, want indeterminate", record.State)
	}
}

func TestFleetFailsClosedAtEveryDurabilityBoundary(t *testing.T) {
	t.Parallel()

	cause := errors.New("durability unavailable")
	tests := []struct {
		name        string
		faults      sequencertest.Faults
		block       bool
		concurrency uint
	}{
		{name: "register", faults: sequencertest.Faults{Register: cause}},
		{name: "recovery", faults: sequencertest.Faults{RecoverExpired: cause}},
		{name: "claim", faults: sequencertest.Faults{ClaimNext: cause}},
		{name: "mark running", faults: sequencertest.Faults{MarkRunning: cause}},
		{name: "renew", faults: sequencertest.Faults{RenewLease: cause}, block: true},
		{name: "complete", faults: sequencertest.Faults{Complete: cause}, concurrency: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID("fleet.fault-" + strings.ReplaceAll(test.name, " ", "-")))
			if test.block {
				spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
					<-ctx.Done()
					return sequencer.Output{}, ctx.Err()
				})
			}
			plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			store := sequencertest.NewFaultStore(memory.New(), test.faults)
			concurrency := test.concurrency
			if concurrency == 0 {
				concurrency = 1
			}
			fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
				RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
				ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
				MaxConcurrency: concurrency, ShutdownWait: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- fleet.Run(context.Background()) }()
			select {
			case err := <-done:
				if !errors.Is(err, cause) {
					t.Fatalf("Run() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("fleet did not fail closed")
			}
			if fleet.State() != sequencer.RunnerFailed || fleet.Ready() {
				t.Fatalf("state = %s, ready = %t", fleet.State(), fleet.Ready())
			}
		})
	}
}

func TestFleetBoundsDetachedMarkRunning(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.mark-running-timeout")
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingMarkRunningStore{
		Store: memory.New(), entered: make(chan struct{}), returned: make(chan struct{}),
		deadline: make(chan time.Duration, 1),
	}
	const bound = 25 * time.Millisecond
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: bound,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- fleet.Run(context.Background()) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("MarkRunning was not called")
	}
	select {
	case remaining := <-store.deadline:
		if remaining > bound {
			t.Fatalf("MarkRunning deadline remaining = %s, want at most %s", remaining, bound)
		}
	case <-time.After(time.Second):
		t.Fatal("MarkRunning context had no observable deadline")
	}
	if err := awaitFleetResult(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v", err)
	}
	select {
	case <-store.returned:
	default:
		t.Fatal("MarkRunning remained blocked after fleet returned")
	}
}

func TestFleetBoundsEachLeaseRenewal(t *testing.T) {
	t.Parallel()

	spec := validSpec("fleet.renew-timeout")
	spec.Policy.Timeout = 500 * time.Millisecond
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		<-ctx.Done()
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingRenewStore{
		Store: memory.New(), entered: make(chan struct{}), returned: make(chan struct{}),
		deadline: make(chan time.Duration, 1),
	}
	const bound = 20 * time.Millisecond
	const shutdownWait = 100 * time.Millisecond
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod", LeaseDuration: time.Second},
		ClaimInterval: time.Millisecond, RenewInterval: bound,
		MaxConcurrency: 1, ShutdownWait: shutdownWait,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("RenewLease was not called")
	}
	select {
	case remaining := <-store.deadline:
		if remaining > shutdownWait {
			t.Fatalf("renewal deadline remaining = %s, want at most %s", remaining, shutdownWait)
		}
	case <-time.After(time.Second):
		t.Fatal("RenewLease context had no observable deadline")
	}
	select {
	case runErr := <-done:
		if !errors.Is(runErr, context.DeadlineExceeded) {
			t.Fatalf("Run() error = %v, want deadline exceeded", runErr)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("fleet did not bound stalled renewal")
	}
	select {
	case <-store.returned:
	default:
		t.Fatal("RenewLease remained blocked after fleet returned")
	}
	if fleet.State() != sequencer.RunnerFailed {
		t.Fatalf("fleet state = %s, want failed", fleet.State())
	}
}

func TestFleetBoundsRenewalByActualRemainingLease(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	clock := newManualClock(base)
	started := make(chan struct{})
	spec := validSpec("fleet.remaining-lease-timeout")
	spec.Policy.Timeout = 900 * time.Millisecond
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		close(started)
		<-ctx.Done()
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingRenewStore{
		Store: memory.New(), entered: make(chan struct{}), returned: make(chan struct{}),
	}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "pod", Clock: clock, LeaseDuration: time.Second,
		},
		ClaimInterval: time.Millisecond, RenewInterval: 20 * time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: 250 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- fleet.Run(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler was not called")
	}
	clock.mu.Lock()
	clock.now = base.Add(990 * time.Millisecond)
	clock.mu.Unlock()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("RenewLease was not called")
	}
	select {
	case <-store.returned:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("renewal outlived the actual remaining lease window")
	}
	if err := awaitFleetResult(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want deadline exceeded", err)
	}
}

func TestFleetRejectsExpiredAndRegressingRenewalDeadlines(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		advance    time.Duration
		renewUntil time.Time
		want       error
	}{
		{name: "already expired", advance: 200 * time.Millisecond, want: sequencer.ErrStaleOwner},
		{name: "regressing result", renewUntil: time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC), want: sequencer.ErrInvalidLease},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
			clock := newManualClock(base)
			started := make(chan struct{})
			spec := validSpec(sequencer.OperationID("fleet.renew-deadline-" + strings.ReplaceAll(test.name, " ", "-")))
			spec.Policy.Timeout = 150 * time.Millisecond
			spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
				close(started)
				<-ctx.Done()
				return sequencer.Output{}, ctx.Err()
			})
			plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			store := &renewResultStore{Store: memory.New(), until: test.renewUntil}
			fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
				RunnerOptions: sequencer.RunnerOptions{Owner: "pod", Clock: clock, LeaseDuration: 200 * time.Millisecond},
				ClaimInterval: time.Millisecond, RenewInterval: 10 * time.Millisecond,
				MaxConcurrency: 1, ShutdownWait: 100 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- fleet.Run(context.Background()) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("handler was not called")
			}
			if test.advance != 0 {
				clock.mu.Lock()
				clock.now = base.Add(test.advance)
				clock.mu.Unlock()
			}
			if err := awaitFleetResult(t, done); !errors.Is(err, test.want) {
				t.Fatalf("Run() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestFleetBoundsRegistrationRecoveryAndClaimCalls(t *testing.T) {
	t.Parallel()

	for _, boundary := range []string{"register", "recover", "claim"} {
		t.Run(boundary, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID("fleet.store-timeout-" + boundary))
			plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			store := &blockingFleetStore{
				Store: memory.New(), boundary: boundary, entered: make(chan struct{}), returned: make(chan struct{}),
			}
			const bound = 20 * time.Millisecond
			fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
				RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
				ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
				MaxConcurrency: 1, ShutdownWait: bound,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- fleet.Run(ctx) }()
			select {
			case <-store.entered:
			case <-time.After(time.Second):
				t.Fatalf("%s was not called", boundary)
			}
			select {
			case runErr := <-done:
				if !errors.Is(runErr, context.DeadlineExceeded) {
					t.Fatalf("Run() error = %v, want deadline exceeded", runErr)
				}
			case <-time.After(10 * bound):
				cancel()
				t.Fatalf("fleet did not bound stalled %s", boundary)
			}
			select {
			case <-store.returned:
			default:
				t.Fatalf("%s remained blocked after fleet returned", boundary)
			}
			if fleet.State() != sequencer.RunnerFailed {
				t.Fatalf("fleet state = %s, want failed", fleet.State())
			}
		})
	}
}

func TestFleetTreatsCancellationDuringStartupAndPollingAsDrain(t *testing.T) {
	t.Parallel()

	for _, boundary := range []string{"register", "recovery", "claim"} {
		t.Run(boundary, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID("fleet.cancel-" + boundary))
			plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			store := &cancelDuringPollStore{Store: memory.New(), cancel: cancel, boundary: boundary}
			fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
				RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
				ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
				MaxConcurrency: 1, ShutdownWait: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := fleet.Run(ctx); err != nil || fleet.State() != sequencer.RunnerStopped {
				t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
			}
		})
	}
}

func TestFleetDoesNotMaskDurabilityFailureThatRacesCancellation(t *testing.T) {
	t.Parallel()

	cause := errors.New("database failed during termination")
	for _, boundary := range []string{"register", "recovery", "claim"} {
		t.Run(boundary, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID("fleet.cancel-failure-" + boundary))
			plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			store := &cancelDuringPollStore{Store: memory.New(), cancel: cancel, boundary: boundary, err: cause}
			fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
				RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
				ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
				MaxConcurrency: 1, ShutdownWait: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := fleet.Run(ctx); !errors.Is(err, cause) || fleet.State() != sequencer.RunnerFailed {
				t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
			}
		})
	}
}

func TestFleetPreservesApprovalBoundaryForAcceptedAttempts(t *testing.T) {
	t.Parallel()

	called := false
	spec := validSpec("fleet.approval")
	spec.Policy.RequiresApproval = true
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		called = true
		return sequencer.Output{}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	completed := make(chan sequencer.Event, 1)
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner: "pod", Approver: approverStub{approval: sequencer.Approval{Actor: "operator", Reason: "window closed"}},
			Observers: []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
				if event.Type == sequencer.EventCompleted {
					completed <- event
				}
			})},
		},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case event := <-completed:
		if event.State != sequencer.Blocked || !errors.Is(event.Err, sequencer.ErrBlocked) {
			t.Fatalf("completion event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("approval result was not completed")
	}
	cancel()
	if err := awaitFleetResult(t, done); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("handler ran without approval")
	}
	audit, err := store.Audit(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || audit[len(audit)-1].Actor != "operator" || audit[len(audit)-1].Reason != "window closed" {
		t.Fatalf("Audit() = %+v, %v", audit, err)
	}
}

func TestFleetDrainReportsDurabilityFailureFromAcceptedAttempt(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	cause := errors.New("completion unavailable")
	spec := validSpec("fleet.drain-failure")
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		close(started)
		<-ctx.Done()
		return sequencer.Output{}, ctx.Err()
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &completionFailureStore{Store: memory.New(), err: cause}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{Owner: "pod"},
		ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond,
		MaxConcurrency: 1, ShutdownWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("attempt did not start")
	}
	cancel()
	if err := awaitFleetResult(t, done); !errors.Is(err, cause) || fleet.State() != sequencer.RunnerFailed {
		t.Fatalf("Run() error = %v, state = %s", err, fleet.State())
	}
}

func awaitFleetResult(t *testing.T, results <-chan error) error {
	t.Helper()
	select {
	case err := <-results:
		return err
	case <-time.After(time.Second):
		t.Fatal("fleet did not stop within the test bound")
		return nil
	}
}

type leaseTrackingStore struct {
	*memory.Store
	mu        sync.Mutex
	events    []string
	renewed   chan struct{}
	completed chan struct{}
	signals   onceSignals
}

type onceSignals struct {
	renewed   sync.Once
	completed sync.Once
}

type failingRenewStore struct {
	*memory.Store
	err error
}

type cancelingRenewStore struct {
	*memory.Store
	entered chan struct{}
	once    sync.Once
}

type cancelAfterRegisterStore struct {
	*memory.Store
	cancel context.CancelFunc
}

type blockingMarkRunningStore struct {
	*memory.Store
	entered  chan struct{}
	returned chan struct{}
	deadline chan time.Duration
	once     sync.Once
}

type blockingRenewStore struct {
	*memory.Store
	entered  chan struct{}
	returned chan struct{}
	deadline chan time.Duration
	once     sync.Once
}

type blockingFleetStore struct {
	*memory.Store
	boundary string
	entered  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func (store *blockingMarkRunningStore) MarkRunning(ctx context.Context, _ sequencer.Ownership, _ time.Time) (sequencer.AttemptRecord, error) {
	if deadline, ok := ctx.Deadline(); ok && store.deadline != nil {
		store.deadline <- time.Until(deadline)
	}
	store.once.Do(func() { close(store.entered) })
	defer close(store.returned)
	<-ctx.Done()
	return sequencer.AttemptRecord{}, ctx.Err()
}

func (store *blockingRenewStore) RenewLease(ctx context.Context, _ sequencer.Ownership, _ time.Time, _ time.Duration) (time.Time, error) {
	if deadline, ok := ctx.Deadline(); ok && store.deadline != nil {
		store.deadline <- time.Until(deadline)
	}
	store.once.Do(func() { close(store.entered) })
	defer close(store.returned)
	<-ctx.Done()
	return time.Time{}, ctx.Err()
}

func (store *blockingFleetStore) block(ctx context.Context) error {
	store.once.Do(func() { close(store.entered) })
	defer close(store.returned)
	<-ctx.Done()
	return ctx.Err()
}

func (store *blockingFleetStore) Register(ctx context.Context, registrations []sequencer.Registration, now time.Time) error {
	if store.boundary == "register" {
		return store.block(ctx)
	}
	return store.Store.Register(ctx, registrations, now)
}

func (store *blockingFleetStore) RecoverExpired(ctx context.Context, now time.Time) (int, error) {
	if store.boundary == "recover" {
		return 0, store.block(ctx)
	}
	return store.Store.RecoverExpired(ctx, now)
}

func (store *blockingFleetStore) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	if store.boundary == "claim" {
		return sequencer.Claim{}, store.block(ctx)
	}
	return store.Store.ClaimNext(ctx, request)
}

func (store *cancelAfterRegisterStore) Register(ctx context.Context, registrations []sequencer.Registration, now time.Time) error {
	if err := store.Store.Register(ctx, registrations, now); err != nil {
		return err
	}
	store.cancel()
	return nil
}

type unexpectedClaimStore struct{ *memory.Store }

func (store *unexpectedClaimStore) ClaimNext(context.Context, sequencer.ClaimRequest) (sequencer.Claim, error) {
	return sequencer.Claim{Attempt: sequencer.Attempt{
		OperationID: "not-local", Version: 1, Number: 1, Owner: "pod", Fencing: 1,
	}, Budget: sequencer.RetryBudget{Attempt: 1}}, nil
}

type renewResultStore struct {
	*memory.Store
	until time.Time
}

func (store *renewResultStore) RenewLease(context.Context, sequencer.Ownership, time.Time, time.Duration) (time.Time, error) {
	return store.until, nil
}

type zeroBudgetClaimStore struct {
	*memory.Store
	operationID sequencer.OperationID
}

func (store *zeroBudgetClaimStore) ClaimNext(context.Context, sequencer.ClaimRequest) (sequencer.Claim, error) {
	return sequencer.Claim{Attempt: sequencer.Attempt{
		OperationID: store.operationID, Version: 1, Number: 1, Owner: "pod", Fencing: 1,
	}}, nil
}

type completionFailureStore struct {
	*memory.Store
	err error
}

type completionInspectStore struct {
	*memory.Store
	mu          sync.Mutex
	completions []sequencer.Completion
}

func (store *completionInspectStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	store.mu.Lock()
	store.completions = append(store.completions, completion)
	store.mu.Unlock()
	return store.Store.Complete(ctx, completion)
}

func (store *completionInspectStore) completionsSnapshot() []sequencer.Completion {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]sequencer.Completion(nil), store.completions...)
}

type cancelDuringPollStore struct {
	*memory.Store
	cancel   context.CancelFunc
	boundary string
	err      error
}

func (store *cancelDuringPollStore) Register(ctx context.Context, registrations []sequencer.Registration, now time.Time) error {
	if store.boundary == "register" {
		store.cancel()
		if store.err != nil {
			return store.err
		}
		return ctx.Err()
	}
	return store.Store.Register(ctx, registrations, now)
}

func (store *cancelDuringPollStore) RecoverExpired(ctx context.Context, now time.Time) (int, error) {
	if store.boundary == "recovery" {
		store.cancel()
		if store.err != nil {
			return 0, store.err
		}
		return 0, ctx.Err()
	}
	return store.Store.RecoverExpired(ctx, now)
}

func (store *cancelDuringPollStore) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	if store.boundary == "claim" {
		store.cancel()
		if store.err != nil {
			return sequencer.Claim{}, store.err
		}
		return sequencer.Claim{}, ctx.Err()
	}
	return store.Store.ClaimNext(ctx, request)
}

func (store *completionFailureStore) Complete(context.Context, sequencer.Completion) error {
	return store.err
}

func (store *failingRenewStore) RenewLease(context.Context, sequencer.Ownership, time.Time, time.Duration) (time.Time, error) {
	return time.Time{}, store.err
}

func (store *cancelingRenewStore) RenewLease(ctx context.Context, _ sequencer.Ownership, _ time.Time, _ time.Duration) (time.Time, error) {
	store.once.Do(func() { close(store.entered) })
	<-ctx.Done()
	return time.Time{}, ctx.Err()
}

func newLeaseTrackingStore() *leaseTrackingStore {
	return &leaseTrackingStore{
		Store: memory.New(), renewed: make(chan struct{}), completed: make(chan struct{}),
	}
}

func (store *leaseTrackingStore) RenewLease(ctx context.Context, ownership sequencer.Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	store.mu.Lock()
	store.events = append(store.events, "renew")
	store.mu.Unlock()
	store.signals.renewed.Do(func() { close(store.renewed) })
	return store.Store.RenewLease(ctx, ownership, now, duration)
}

func (store *leaseTrackingStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	store.mu.Lock()
	store.events = append(store.events, "complete")
	store.mu.Unlock()
	store.signals.completed.Do(func() { close(store.completed) })
	return store.Store.Complete(ctx, completion)
}

func (store *leaseTrackingStore) eventsSnapshot() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]string(nil), store.events...)
}

func (store *leaseTrackingStore) renewalCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	count := 0
	for _, event := range store.events {
		if event == "renew" {
			count++
		}
	}
	return count
}
