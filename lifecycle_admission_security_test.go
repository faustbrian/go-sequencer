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

type acknowledgedSettlementStore struct {
	*memory.Store
	clock      *manualClock
	renewed    chan time.Time
	completing chan struct{}
	settle     <-chan struct{}
}

func (store *acknowledgedSettlementStore) RenewLease(ctx context.Context, ownership sequencer.Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	until, err := store.Store.RenewLease(ctx, ownership, now, duration)
	if err == nil {
		select {
		case store.renewed <- now:
		default:
		}
	}
	return until, err
}

func (store *acknowledgedSettlementStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	close(store.completing)
	select {
	case <-store.settle:
	case <-ctx.Done():
		return ctx.Err()
	}
	completion.At = store.clock.Now()
	return store.Store.Complete(ctx, completion)
}

func TestUncooperativeAttemptSettlesWithinAcceptedLease(t *testing.T) {
	spec := validSpec("lease.unknown-settlement")
	spec.Policy.Timeout = 10 * time.Millisecond
	release, settle := make(chan struct{}), make(chan struct{})
	started := make(chan context.Context, 1)
	handlerStopped := make(chan struct{})
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		defer close(handlerStopped)
		started <- ctx
		<-release
		return sequencer.Output{}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	clock := newManualClock(initial)
	store := &acknowledgedSettlementStore{Store: memory.New(), clock: clock, renewed: make(chan time.Time, 8), completing: make(chan struct{}), settle: settle}
	const lease = 100 * time.Millisecond
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Clock: clock, LeaseDuration: lease, HandlerStopWait: 60 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	result := make(chan error, 1)
	go func() { defer close(finished); _, executeErr := runner.Execute(ctx); result <- executeErr }()
	var releaseOnce, settleOnce sync.Once
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		settleOnce.Do(func() { close(settle) })
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("runner did not stop")
		}
	})
	wait := func(signal <-chan struct{}, phase string) {
		t.Helper()
		select {
		case <-signal:
		case <-time.After(time.Second):
			t.Fatalf("%s did not acknowledge", phase)
		}
	}
	var callbackCtx context.Context
	select {
	case callbackCtx = <-started:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	wait(callbackCtx.Done(), "callback timeout")
	advanceAndRenew := func(at time.Time) {
		t.Helper()
		clock.mu.Lock()
		clock.now = at
		clock.mu.Unlock()
		watchdog := time.NewTimer(time.Second)
		defer watchdog.Stop()
		for {
			select {
			case renewedAt := <-store.renewed:
				if !renewedAt.Before(at) {
					return
				}
			case <-watchdog.C:
				t.Fatalf("no acknowledged renewal at %s", at)
			}
		}
	}
	// Advance only inside the current fenced interval, then acknowledge the
	// real store extension. A frozen clock alone would hide a stopped keeper.
	advanceAndRenew(initial.Add(lease / 2))
	wait(store.completing, "unknown settlement")
	// Cross the ORIGINAL expiry while completion is blocked. The keeper must
	// still renew during settlement, rather than stop with the callback budget.
	advanceAndRenew(initial.Add(lease + lease/5))
	settleOnce.Do(func() { close(settle) })
	wait(finished, "settlement")
	err = <-result
	record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
	history, historyErr := store.History(context.Background(), spec.ID, spec.Version, 10)
	audit, auditErr := store.Audit(context.Background(), spec.ID, spec.Version, 10)
	if !errors.Is(err, sequencer.ErrUnknownResult) || errors.Is(err, sequencer.ErrStaleOwner) || snapshotErr != nil || record.State != sequencer.Indeterminate || !record.LeaseExpiresAt.IsZero() || !record.UpdatedAt.After(initial.Add(lease)) {
		t.Fatalf("renewed lease lost settlement: error=%v record=%+v snapshot error=%v", err, record, snapshotErr)
	}
	if historyErr != nil || len(history) != 1 || history[0].State != sequencer.Indeterminate || auditErr != nil || len(audit) == 0 || audit[len(audit)-1].To != sequencer.Indeterminate {
		t.Fatalf("unknown ledger outcome: history=%+v error=%v audit=%+v error=%v", history, historyErr, audit, auditErr)
	}
	releaseOnce.Do(func() { close(release) })
	wait(handlerStopped, "late callback return")
	after, _ := store.Snapshot(context.Background(), spec.ID, spec.Version)
	afterHistory, _ := store.History(context.Background(), spec.ID, spec.Version, 10)
	afterAudit, _ := store.Audit(context.Background(), spec.ID, spec.Version, 10)
	if !reflect.DeepEqual(record, after) || !reflect.DeepEqual(history, afterHistory) || !reflect.DeepEqual(audit, afterAudit) {
		t.Fatal("late callback changed the unknown ledger")
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
