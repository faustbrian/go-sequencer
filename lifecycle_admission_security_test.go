package sequencer_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/memory"
)

type concurrentSecurityTransactions struct{}

func (concurrentSecurityTransactions) Within(ctx context.Context, callback func(context.Context, any) error) error {
	start := make(chan struct{})
	var group sync.WaitGroup
	for range 32 {
		group.Add(1)
		go func() { defer group.Done(); <-start; _ = callback(ctx, struct{}{}) }()
	}
	close(start)
	group.Wait()
	return nil
}

func TestConcurrentTransactionCallbackAdmissionIsExactlyOnce(t *testing.T) {
	spec := validSpec("transaction.concurrent-admission")
	spec.Policy.WithinTransaction = true
	var calls atomic.Int32
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		calls.Add(1)
		return sequencer.Output{Summary: "applied"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Transactions: concurrentSecurityTransactions{}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Execute(context.Background())
	record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if calls.Load() != 1 || !errors.Is(err, sequencer.ErrUnknownResult) || snapshotErr != nil || record.State != sequencer.Indeterminate {
		t.Fatalf("calls=%d error=%v state=%v snapshot error=%v", calls.Load(), err, record.State, snapshotErr)
	}
}

func TestUnsafeClaimOwnersHaveNoEffects(t *testing.T) {
	for _, owner := range []string{"worker password=synthetic-value", "worker\x00identity", "\x00\t"} {
		t.Run(owner, func(t *testing.T) {
			ctx := context.Background()
			now := time.Now()
			store := memory.New()
			if err := store.Register(ctx, []sequencer.Registration{{ID: "owner.audit", Version: 1, Checksum: sequencer.ChecksumBytes([]byte("owner"))}}, now); err != nil {
				t.Fatal(err)
			}
			before, _ := store.Snapshot(ctx, "owner.audit", 1)
			historyBefore, _ := store.History(ctx, "owner.audit", 1, 10)
			auditBefore, _ := store.Audit(ctx, "owner.audit", 1, 10)
			_, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"owner.audit"}, Owner: owner, Now: now, LeaseDuration: time.Minute})
			after, _ := store.Snapshot(ctx, "owner.audit", 1)
			historyAfter, _ := store.History(ctx, "owner.audit", 1, 10)
			auditAfter, _ := store.Audit(ctx, "owner.audit", 1, 10)
			if err == nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(historyBefore, historyAfter) || !reflect.DeepEqual(auditBefore, auditAfter) {
				t.Fatal("unsafe owner accepted or changed durable state")
			}
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("owner.runner")}, sequencer.PlanOptions{})
			if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: owner}); err == nil {
				t.Fatal("unsafe runner owner accepted")
			}
			if _, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: sequencer.RunnerOptions{Owner: owner}}); err == nil {
				t.Fatal("unsafe fleet owner accepted")
			}
		})
	}
}

func TestBlockingObserversDoNotDelayRunnerOrFleetStart(t *testing.T) {
	for _, fleetMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "runner", true: "fleet"}[fleetMode], func(t *testing.T) {
			release := make(chan struct{})
			defer close(release)
			started := make(chan struct{}, 1)
			spec := validSpec("observer.start-admission")
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				started <- struct{}{}
				return sequencer.Output{}, nil
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			observers := make([]sequencer.Observer, 128)
			for index := range observers {
				observers[index] = sequencer.ObserverFunc(func(sequencer.Event) { <-release })
			}
			options := sequencer.RunnerOptions{Owner: "owner", Observers: observers}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			if fleetMode {
				fleet, err := sequencer.NewFleet(plan, memory.New(), sequencer.FleetOptions{RunnerOptions: options, ClaimInterval: time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				go func() { done <- fleet.Run(ctx) }()
			} else {
				runner, err := sequencer.NewRunner(plan, memory.New(), options)
				if err != nil {
					t.Fatal(err)
				}
				go func() { _, err := runner.Execute(ctx); done <- err }()
			}
			select {
			case <-started:
			case <-time.After(200 * time.Millisecond):
				t.Error("observers delayed attempt start")
			}
			cancel()
			select {
			case <-done:
			case <-time.After(4 * time.Second):
				t.Error("runner did not stop")
			}
		})
	}
}

func TestUncooperativeAttemptSettlesWithinAcceptedLease(t *testing.T) {
	spec := validSpec("lease.unknown-settlement")
	spec.Policy.Timeout = 10 * time.Millisecond
	release := make(chan struct{})
	defer close(release)
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		<-release
		return sequencer.Output{}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	options := sequencer.RunnerOptions{Owner: "owner", LeaseDuration: spec.Policy.Timeout + time.Nanosecond, HandlerStopWait: 60 * time.Millisecond}
	runner, err := sequencer.NewRunner(plan, store, options)
	// A constructor may reject an unsafe lease budget instead of renewing it.
	// In that case exercise the same callback against an accepted lease.
	if err != nil {
		options.LeaseDuration = spec.Policy.Timeout + options.HandlerStopWait + 100*time.Millisecond + time.Nanosecond
		runner, err = sequencer.NewRunner(plan, store, options)
	}
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Execute(context.Background())
	record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if !errors.Is(err, sequencer.ErrUnknownResult) || errors.Is(err, sequencer.ErrStaleOwner) || snapshotErr != nil || record.State != sequencer.Indeterminate {
		t.Fatalf("accepted lease lost settlement: error=%v state=%v snapshot error=%v", err, record.State, snapshotErr)
	}
}

func TestTransactionManagerCannotAdmitLateCallback(t *testing.T) {
	spec := validSpec("transaction.late-admission")
	spec.Policy.WithinTransaction = true
	var calls atomic.Int32
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		calls.Add(1)
		return sequencer.Output{}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	var saved func(context.Context, any) error
	runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "owner", Transactions: securityTransactions(func(_ context.Context, callback func(context.Context, any) error) error { saved = callback; return nil })})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("missing callback error=%v", err)
	}
	if err = saved(context.Background(), struct{}{}); !errors.Is(err, sequencer.ErrInvalidRunner) || calls.Load() != 0 {
		t.Fatalf("late callback error=%v calls=%d", err, calls.Load())
	}
}

func TestTransactionManagerEarlyReturnRetainsUnknownCallback(t *testing.T) {
	spec := validSpec("transaction.early-return")
	spec.Policy.WithinTransaction = true
	spec.Policy.Timeout = 10 * time.Millisecond
	started, release, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		calls.Add(1)
		close(started)
		<-release
		return sequencer.Output{Summary: "must not escape"}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", HandlerStopWait: time.Millisecond, Transactions: securityTransactions(func(ctx context.Context, callback func(context.Context, any) error) error {
		go func() { defer close(callbackDone); _ = callback(ctx, struct{}{}) }()
		<-started
		return nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Execute(context.Background())
	record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
	close(release)
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish after cleanup")
	}
	if !errors.Is(err, sequencer.ErrUnknownResult) || snapshotErr != nil || record.State != sequencer.Indeterminate || calls.Load() != 1 {
		t.Fatalf("early manager return error=%v state=%v calls=%d", err, record.State, calls.Load())
	}
}
