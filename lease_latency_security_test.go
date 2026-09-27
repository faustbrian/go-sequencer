package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/memory"
	"github.com/faustbrian/go-sequencer/v2/sequencertest"
)

type delayedLeaseStore struct {
	*memory.Store
	claimDelay, markDelay, completeDelay time.Duration
}

type nonrenewableSecurityStore struct{ sequencer.Store }

type completionWinsRenewalStore struct {
	*memory.Store
	renewals                     atomic.Int32
	entered, committed, returned chan struct{}
	persistenceClock             bool
}

func (store *completionWinsRenewalStore) RenewLease(ctx context.Context, ownership sequencer.Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	if store.renewals.Add(1) == 1 {
		return store.Store.RenewLease(ctx, ownership, now, duration)
	}
	close(store.entered)
	<-store.committed
	defer close(store.returned)
	return store.Store.RenewLease(context.Background(), ownership, time.Now(), duration)
}

func (store *completionWinsRenewalStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	<-store.entered
	if store.persistenceClock {
		completion.At = time.Now()
	}
	if err := store.Store.Complete(ctx, completion); err != nil {
		return err
	}
	close(store.committed)
	<-store.returned
	return nil
}

func TestCommittedCompletionWinsConcurrentRenewal(t *testing.T) {
	for _, fleetMode := range []bool{false, true} {
		for _, persistenceClock := range []bool{false, true} {
			t.Run(fmt.Sprintf("fleet=%t/persistence-clock=%t", fleetMode, persistenceClock), func(t *testing.T) {
				spec := validSpec("completion.wins")
				spec.Policy.Timeout = 10 * time.Millisecond
				plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
				store := &completionWinsRenewalStore{Store: memory.New(), entered: make(chan struct{}), committed: make(chan struct{}), returned: make(chan struct{}), persistenceClock: persistenceClock}
				completed := make(chan sequencer.Event, 1)
				options := sequencer.RunnerOptions{Owner: "owner", LeaseDuration: 300 * time.Millisecond, Observers: []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
					if event.Type == sequencer.EventCompleted {
						completed <- event
					}
				})}}
				if !fleetMode {
					runner, err := sequencer.NewRunner(plan, store, options)
					if err != nil {
						t.Fatal(err)
					}
					report, err := runner.Execute(context.Background())
					if err != nil || report.Result != sequencer.RunSucceeded {
						t.Fatalf("committed success report=%+v error=%v", report, err)
					}
				} else {
					fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: options, ClaimInterval: time.Millisecond, ShutdownWait: time.Second})
					if err != nil {
						t.Fatal(err)
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					done := make(chan error, 1)
					go func() { done <- fleet.Run(ctx) }()
					select {
					case <-completed:
						cancel()
					case err := <-done:
						t.Fatalf("committed fleet failed: %v", err)
					case <-time.After(time.Second):
						t.Fatal("missing completion observation")
					}
					if err := <-done; err != nil || fleet.State() == sequencer.RunnerFailed {
						t.Fatalf("committed fleet error=%v state=%v", err, fleet.State())
					}
				}
				if !fleetMode {
					select {
					case event := <-completed:
						if event.Err != nil || event.State != sequencer.Succeeded {
							t.Fatalf("event=%+v", event)
						}
					case <-time.After(time.Second):
						t.Fatal("missing completion observation")
					}
				}
				record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
				if err != nil || record.State != sequencer.Succeeded {
					t.Fatalf("persisted state=%v error=%v", record.State, err)
				}
			})
		}
	}
}

func TestRunnerRejectsNonrenewableStoreBeforeEffects(t *testing.T) {
	store := memory.New()
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("lease.required")}, sequencer.PlanOptions{})
	if _, err := sequencer.NewRunner(plan, &nonrenewableSecurityStore{Store: store}, sequencer.RunnerOptions{Owner: "owner"}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("nonrenewable store constructor=%v", err)
	}
	if _, err := store.Snapshot(context.Background(), "lease.required", 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatal("constructor changed store")
	}
}

func TestStandaloneRenewalLossDoesNotStartHandler(t *testing.T) {
	spec := validSpec("lease.loss-before-start")
	called := false
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		called = true
		return sequencer.Output{}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, sequencertest.NewFaultStore(store, sequencertest.Faults{RenewLease: sequencer.ErrStaleOwner}), sequencer.RunnerOptions{Owner: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Execute(context.Background())
	record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if !errors.Is(err, sequencer.ErrStaleOwner) || called || snapshotErr != nil || record.State != sequencer.Claimed {
		t.Fatalf("renewal loss admitted handler: error=%v called=%t state=%v", err, called, record.State)
	}
}

type settlementRenewalLossStore struct {
	*memory.Store
	renewals atomic.Int32
	lost     chan struct{}
}

func (store *settlementRenewalLossStore) RenewLease(ctx context.Context, ownership sequencer.Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	if store.renewals.Add(1) == 1 {
		return store.Store.RenewLease(ctx, ownership, now, duration)
	}
	close(store.lost)
	return time.Time{}, sequencer.ErrInvalidLease
}

func (store *settlementRenewalLossStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	select {
	case <-store.lost:
	case <-ctx.Done():
		return ctx.Err()
	}
	// This partition is a loss that prevents commit, not a concurrent
	// successful commit whose nil result is authoritative.
	<-ctx.Done()
	return store.Store.Complete(ctx, completion)
}

func TestStandaloneRenewalLossDuringSettlementFailsClosed(t *testing.T) {
	spec := validSpec("lease.loss-settlement")
	spec.Policy.Timeout = 10 * time.Millisecond
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := &settlementRenewalLossStore{Store: memory.New(), lost: make(chan struct{})}
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", LeaseDuration: 60 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	report, err := runner.Execute(context.Background())
	if !errors.Is(err, sequencer.ErrInvalidLease) || report.Result == sequencer.RunSucceeded {
		t.Fatalf("renewal loss reported success: report=%+v error=%v", report, err)
	}
}

func TestFleetRenewalLossDuringSettlementFailsClosed(t *testing.T) {
	spec := validSpec("fleet.lease-loss-settlement")
	spec.Policy.Timeout = 10 * time.Millisecond
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := &settlementRenewalLossStore{Store: memory.New(), lost: make(chan struct{})}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: sequencer.RunnerOptions{Owner: "owner", LeaseDuration: 60 * time.Millisecond}, ClaimInterval: time.Millisecond, ShutdownWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fleet.Run(ctx) }()
	select {
	case err = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fleet did not fail closed after renewal loss")
	}
	if !errors.Is(err, sequencer.ErrInvalidLease) || fleet.State() != sequencer.RunnerFailed {
		t.Fatalf("renewal loss error=%v state=%v", err, fleet.State())
	}
}

func (store *delayedLeaseStore) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	claim, err := store.Store.ClaimNext(ctx, request)
	if err != nil {
		return claim, err
	}
	select {
	case <-time.After(store.claimDelay):
	case <-ctx.Done():
		return sequencer.Claim{}, ctx.Err()
	}
	return claim, nil
}

func (store *delayedLeaseStore) MarkRunning(ctx context.Context, ownership sequencer.Ownership, now time.Time) (sequencer.AttemptRecord, error) {
	select {
	case <-time.After(store.markDelay):
	case <-ctx.Done():
		return sequencer.AttemptRecord{}, ctx.Err()
	}
	return store.Store.MarkRunning(ctx, ownership, time.Now())
}

func (store *delayedLeaseStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	select {
	case <-time.After(store.completeDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	// Like PostgreSQL clock_timestamp(), validate at actual persistence time.
	completion.At = time.Now()
	return store.Store.Complete(ctx, completion)
}

func TestStandaloneLeaseCoversPreHandlerAndSettlementLatency(t *testing.T) {
	for _, phase := range []string{"claim-return", "mark-running", "completion"} {
		t.Run(phase, func(t *testing.T) {
			spec := validSpec("lease.latency")
			spec.Policy.Timeout = 10 * time.Millisecond
			release := make(chan struct{})
			defer close(release)
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				<-release
				return sequencer.Output{}, nil
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := &delayedLeaseStore{Store: memory.New()}
			if phase == "claim-return" {
				store.claimDelay = 120 * time.Millisecond
			} else if phase == "mark-running" {
				store.markDelay = 120 * time.Millisecond
			} else {
				store.completeDelay = 120 * time.Millisecond
			}
			runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", HandlerStopWait: 60 * time.Millisecond, LeaseDuration: 170*time.Millisecond + time.Nanosecond})
			if err != nil {
				t.Fatal(err)
			}
			_, err = runner.Execute(context.Background())
			record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
			if !errors.Is(err, sequencer.ErrUnknownResult) || errors.Is(err, sequencer.ErrStaleOwner) || snapshotErr != nil || record.State != sequencer.Indeterminate {
				t.Fatalf("delayed store lost settlement: error=%v state=%v snapshot error=%v", err, record.State, snapshotErr)
			}
		})
	}
}

func TestFleetLeaseCoversPreHandlerAndSettlementLatency(t *testing.T) {
	for _, phase := range []string{"claim-return", "mark-running", "completion"} {
		t.Run(phase, func(t *testing.T) {
			spec := validSpec("fleet.lease-latency")
			spec.Policy.Timeout = 10 * time.Millisecond
			release := make(chan struct{})
			defer close(release)
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				<-release
				return sequencer.Output{}, nil
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := &delayedLeaseStore{Store: memory.New()}
			switch phase {
			case "claim-return":
				store.claimDelay = 120 * time.Millisecond
			case "mark-running":
				store.markDelay = 120 * time.Millisecond
			case "completion":
				store.completeDelay = 200 * time.Millisecond
			}
			fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: sequencer.RunnerOptions{Owner: "owner", HandlerStopWait: 60 * time.Millisecond, LeaseDuration: 170*time.Millisecond + time.Nanosecond}, ClaimInterval: time.Millisecond, ShutdownWait: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- fleet.Run(ctx) }()
			select {
			case err = <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("fleet did not settle unknown callback")
			}
			record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
			if !errors.Is(err, sequencer.ErrUnknownResult) || errors.Is(err, sequencer.ErrStaleOwner) || snapshotErr != nil || record.State != sequencer.Indeterminate {
				t.Fatalf("fleet delayed store lost settlement: error=%v state=%v snapshot error=%v", err, record.State, snapshotErr)
			}
		})
	}
}
