package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/memory"
)

type securityApprover func(context.Context, sequencer.OperationSpec) (sequencer.Approval, error)

func (approve securityApprover) Approve(ctx context.Context, spec sequencer.OperationSpec) (sequencer.Approval, error) {
	return approve(ctx, spec)
}

// approvalPersistenceClockStore models PostgreSQL persistence-time fencing;
// actual PostgreSQL audit persistence is verified by the hosted integration gate.
type approvalPersistenceClockStore struct{ *memory.Store }

func (store *approvalPersistenceClockStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	completion.At = time.Now()
	return store.Store.Complete(ctx, completion)
}

func TestOversizedApprovalKeepsSafeBlockedAudit(t *testing.T) {
	for _, fleetMode := range []bool{false, true} {
		for _, oversizedActor := range []bool{false, true} {
			for _, persistenceClock := range []bool{false, true} {
				t.Run(fmt.Sprintf("fleet=%t/actor=%t/persistence-clock=%t", fleetMode, oversizedActor, persistenceClock), func(t *testing.T) {
					spec := validSpec("approval.oversized")
					spec.Policy.RequiresApproval = true
					var calls atomic.Int32
					spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
						calls.Add(1)
						return sequencer.Output{}, nil
					})
					plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
					base := memory.New()
					var store sequencer.LeaseStore = base
					if persistenceClock {
						store = &approvalPersistenceClockStore{Store: base}
					}
					options := sequencer.RunnerOptions{Owner: "owner", Approver: securityApprover(func(context.Context, sequencer.OperationSpec) (sequencer.Approval, error) {
						approval := sequencer.Approval{Approved: true, Actor: "operator", Reason: "window"}
						if oversizedActor {
							approval.Actor = strings.Repeat("x", sequencer.DefaultMaxActorBytes+1)
						} else {
							approval.Reason = strings.Repeat("x", sequencer.DefaultMaxReasonBytes+1)
						}
						return approval, nil
					})}
					if !fleetMode {
						runner, err := sequencer.NewRunner(plan, store, options)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrResourceLimit) {
							t.Errorf("resource limit lost: %v", err)
						}
					} else {
						completed := make(chan sequencer.Event, 1)
						options.Observers = []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
							if event.Type == sequencer.EventCompleted {
								completed <- event
							}
						})}
						fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: options, ClaimInterval: time.Millisecond})
						if err != nil {
							t.Fatal(err)
						}
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						done := make(chan error, 1)
						go func() { done <- fleet.Run(ctx) }()
						select {
						case event := <-completed:
							if !errors.Is(event.Err, sequencer.ErrBlocked) {
								t.Errorf("resource limit lost: %v", event.Err)
							}
						case <-time.After(time.Second):
							t.Error("missing completion")
						}
						cancel()
						select {
						case err := <-done:
							if err != nil {
								t.Errorf("fleet shutdown: %v", err)
							}
						case <-time.After(time.Second):
							t.Fatal("fleet shutdown blocked")
						}
					}
					record, err := base.Snapshot(context.Background(), spec.ID, spec.Version)
					if err != nil || record.State != sequencer.Blocked || calls.Load() != 0 {
						t.Errorf("state=%v calls=%d error=%v", record.State, calls.Load(), err)
					}
					audit, err := base.Audit(context.Background(), spec.ID, spec.Version, 10)
					if err != nil || len(audit) == 0 {
						t.Fatalf("audit unavailable: %v", err)
					}
					last := audit[len(audit)-1]
					if last.Actor != "approval" || last.Reason != "invalid approval attribution" {
						t.Errorf("false approval attribution actor=%q reason=%q", last.Actor, last.Reason)
					}
				})
			}
		}
	}
}

func TestApprovalRejectsControlOnlyAttributionBeforeHandler(t *testing.T) {
	for _, fleetMode := range []bool{false, true} {
		for _, invalidActor := range []bool{false, true} {
			t.Run(fmt.Sprintf("fleet=%t/actor=%t", fleetMode, invalidActor), func(t *testing.T) {
				spec := validSpec("approval.invalid-attribution")
				spec.Policy.RequiresApproval = true
				var calls atomic.Int32
				spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
					calls.Add(1)
					return sequencer.Output{}, nil
				})
				plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
				store := memory.New()
				options := sequencer.RunnerOptions{Owner: "owner", Approver: securityApprover(func(context.Context, sequencer.OperationSpec) (sequencer.Approval, error) {
					approval := sequencer.Approval{Approved: true, Actor: "operator", Reason: "change window"}
					if invalidActor {
						approval.Actor = "\x00\x01"
					} else {
						approval.Reason = "\x00\x01"
					}
					return approval, nil
				})}
				if !fleetMode {
					runner, err := sequencer.NewRunner(plan, store, options)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrBlocked) {
						t.Errorf("Execute error=%v", err)
					}
				} else {
					completed := make(chan struct{}, 1)
					options.Observers = []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) {
						if event.Type == sequencer.EventCompleted {
							completed <- struct{}{}
						}
					})}
					fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: options, ClaimInterval: time.Millisecond})
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
						t.Errorf("Fleet error=%v", err)
					case <-time.After(time.Second):
						t.Error("completion missing")
						cancel()
					}
				}
				record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
				if err != nil || record.State != sequencer.Blocked || calls.Load() != 0 {
					t.Errorf("state=%v handler calls=%d error=%v", record.State, calls.Load(), err)
				}
				audit, err := store.Audit(context.Background(), spec.ID, spec.Version, 10)
				if err != nil || len(audit) == 0 {
					t.Fatalf("audit=%v error=%v", audit, err)
				}
				last := audit[len(audit)-1]
				if last.Actor == "" || last.Reason == "" || strings.ContainsAny(last.Actor+last.Reason, "\x00\x01") {
					t.Errorf("unsafe terminal attribution")
				}
			})
		}
	}

}

func TestRunnerApprovalPreservesDecisionAndAudit(t *testing.T) {
	for _, approved := range []bool{false, true} {
		t.Run(map[bool]string{false: "denied", true: "approved"}[approved], func(t *testing.T) {
			spec := validSpec("approval.characterization")
			spec.Policy.RequiresApproval = true
			ran := false
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				ran = true
				return sequencer.Output{Summary: "applied"}, nil
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := memory.New()
			runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Approver: securityApprover(func(context.Context, sequencer.OperationSpec) (sequencer.Approval, error) {
				return sequencer.Approval{Approved: approved, Actor: "operator", Reason: "change window"}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			_, err = runner.Execute(context.Background())
			if ran != approved || (approved && err != nil) || (!approved && !errors.Is(err, sequencer.ErrBlocked)) {
				t.Fatalf("ran=%v error=%v", ran, err)
			}
			audit, err := store.Audit(context.Background(), spec.ID, spec.Version, 10)
			if err != nil || len(audit) == 0 || audit[len(audit)-1].Actor != "operator" || audit[len(audit)-1].Reason != "change window" {
				t.Fatalf("audit=%+v error=%v", audit, err)
			}
		})
	}
}

func TestApprovalDeadlineQuarantinesRunnerAndFleet(t *testing.T) {
	for _, fleetMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "runner", true: "fleet"}[fleetMode], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			started := make(chan struct{})
			handlerStarted := make(chan struct{}, 1)
			spec := validSpec("approval.a-stuck")
			spec.Policy.RequiresApproval = true
			spec.Policy.Timeout = 10 * time.Millisecond
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				handlerStarted <- struct{}{}
				return sequencer.Output{}, nil
			})
			replacement := validSpec("approval.b-replacement")
			replacement.Handler = spec.Handler
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec, replacement}, sequencer.PlanOptions{})
			store := newLeaseTrackingStore()
			options := sequencer.RunnerOptions{Owner: "owner", HandlerStopWait: time.Millisecond, Approver: securityApprover(func(context.Context, sequencer.OperationSpec) (sequencer.Approval, error) {
				close(started)
				<-release
				return sequencer.Approval{Approved: true, Actor: "operator", Reason: "change window"}, nil
			})}
			done := make(chan error, 1)
			var fleet *sequencer.Fleet
			if fleetMode {
				var err error
				fleet, err = sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: options, MaxConcurrency: 1, ClaimInterval: time.Millisecond, RenewInterval: time.Millisecond, ShutdownWait: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				go func() { done <- fleet.Run(ctx) }()
			} else {
				runner, err := sequencer.NewRunner(plan, store, options)
				if err != nil {
					t.Fatal(err)
				}
				go func() { _, runErr := runner.Execute(ctx); done <- runErr }()
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("approval did not start")
			}
			select {
			case err := <-done:
				if !errors.Is(err, sequencer.ErrUnknownResult) {
					t.Fatalf("error=%v, want unknown result", err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("approval bypassed the attempt deadline")
			}
			if fleetMode && (fleet.Ready() || fleet.State() != sequencer.RunnerFailed) {
				t.Fatalf("fleet state=%s ready=%v", fleet.State(), fleet.Ready())
			}
			if fleetMode {
				renewals := store.renewalCount()
				<-time.After(25 * time.Millisecond)
				if store.renewalCount() != renewals {
					t.Fatal("lease renewal continued after unknown outcome")
				}
			}
			select {
			case <-handlerStarted:
				t.Fatal("a handler started while approval retained its slot")
			default:
			}
			history, err := store.History(context.Background(), spec.ID, spec.Version, 1)
			if err != nil || len(history) != 1 || history[0].State != sequencer.Indeterminate {
				t.Fatalf("history=%+v error=%v", history, err)
			}
			close(release)
			select {
			case <-handlerStarted:
				t.Fatal("late approval started a handler after the deadline")
			case <-time.After(25 * time.Millisecond):
			}
		})
	}
}

func TestRunnerApprovalPanicIsContainedAndRedacted(t *testing.T) {
	spec := validSpec("approval.panic")
	spec.Policy.RequiresApproval = true
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Approver: securityApprover(func(context.Context, sequencer.OperationSpec) (sequencer.Approval, error) {
		panic("private approval value")
	})})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Execute(context.Background())
	if !errors.Is(err, sequencer.ErrUnknownResult) || strings.Contains(err.Error(), "private approval value") {
		t.Fatalf("error=%v", err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 1)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Indeterminate || strings.Contains(history[0].ErrorDetail, "private approval value") {
		t.Fatalf("history=%+v error=%v", history, err)
	}
}

type securityTransactions func(context.Context, func(context.Context, any) error) error

func (within securityTransactions) Within(ctx context.Context, callback func(context.Context, any) error) error {
	return within(ctx, callback)
}

func TestTransactionContextRetainsValuesAndParentCancellation(t *testing.T) {
	for _, test := range []struct {
		name              string
		cancelParent      bool
		cancelTransaction bool
	}{
		{name: "deadline"},
		{name: "parent cancellation", cancelParent: true},
		{name: "transaction cancellation", cancelTransaction: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transactionContext, cancelTransaction := context.WithCancel(context.Background())
			defer cancelTransaction()
			release := make(chan struct{})
			defer close(release)
			started := make(chan struct{})
			observed := make(chan error, 1)
			type transactionKey struct{}
			transaction := new(int)
			spec := validSpec("transaction.context")
			spec.Policy.WithinTransaction = true
			spec.Policy.Timeout = 20 * time.Millisecond
			spec.Handler = sequencer.HandlerFunc(func(handlerCtx context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
				if handlerCtx.Value(transactionKey{}) != "transaction value" || attempt.Transaction != transaction {
					observed <- errors.New("transaction values or object were lost")
					return sequencer.Output{}, errors.New("invalid transaction context")
				}
				close(started)
				select {
				case <-handlerCtx.Done():
					observed <- handlerCtx.Err()
					return sequencer.Output{}, handlerCtx.Err()
				case <-release:
					return sequencer.Output{}, nil
				}
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "owner", Transactions: securityTransactions(func(_ context.Context, callback func(context.Context, any) error) error {
				return callback(context.WithValue(transactionContext, transactionKey{}, "transaction value"), transaction)
			})})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, runErr := runner.Execute(ctx); done <- runErr }()
			select {
			case <-started:
			case err := <-observed:
				t.Fatal(err)
			case <-time.After(time.Second):
				t.Fatal("handler did not start")
			}
			want := context.DeadlineExceeded
			if test.cancelParent {
				cancel()
				want = context.Canceled
			}
			if test.cancelTransaction {
				cancelTransaction()
				want = context.Canceled
			}
			select {
			case got := <-observed:
				if !errors.Is(got, want) {
					t.Fatalf("handler context error=%v want=%v", got, want)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("transaction context lost parent cancellation")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("runner did not settle")
			}
		})
	}
}

func TestTransactionCallbackAfterDeadlineCannotStartHandler(t *testing.T) {
	spec := validSpec("transaction.late-callback")
	spec.Policy.WithinTransaction = true
	spec.Policy.Timeout = 10 * time.Millisecond
	handlerStarted := make(chan struct{}, 1)
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		handlerStarted <- struct{}{}
		return sequencer.Output{}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "owner", Transactions: securityTransactions(func(ctx context.Context, callback func(context.Context, any) error) error {
		<-ctx.Done()
		return callback(context.Background(), new(int))
	})})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Execute(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	select {
	case <-handlerStarted:
		t.Fatal("late transaction callback started handler")
	default:
	}
}
