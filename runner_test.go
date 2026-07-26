package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
	"github.com/faustbrian/golib/pkg/sequencer/sequencertest"
)

func TestRunnerExecutesPlanInOrderAndReportsDurableResults(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var executed []sequencer.OperationID
	operation := func(id sequencer.OperationID, dependencies ...sequencer.OperationID) sequencer.OperationSpec {
		spec := validSpec(id)
		spec.Dependencies = dependencies
		spec.Handler = sequencer.HandlerFunc(func(_ context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
			mu.Lock()
			defer mu.Unlock()
			executed = append(executed, attempt.OperationID)
			return sequencer.Output{Summary: "applied"}, nil
		})
		return spec
	}
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{
		operation("postal", "locations"), operation("locations", "countries"), operation("countries"),
	}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(time.Date(2026, 7, 19, 8, 0, 0, 0, time.UTC))
	runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "replica-1", Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	report, err := runner.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got, want := executed, []sequencer.OperationID{"countries", "locations", "postal"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("executed = %v, want %v", got, want)
	}
	if report.Result != sequencer.RunSucceeded || len(report.Operations) != 3 {
		t.Fatalf("report = %+v", report)
	}
}

func TestRunnerRetriesTypedFailuresWithinBudget(t *testing.T) {
	t.Parallel()

	spec := validSpec("retry")
	spec.Policy.MaxAttempts = 2
	spec.Policy.MaxExceptions = 2
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		if invocations == 1 {
			return sequencer.Output{}, sequencer.Retry(errors.New("busy"))
		}
		return sequencer.Output{Summary: "recovered"}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica", Clock: newManualClock(time.Now())})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	history, err := store.History(context.Background(), "retry", 1, 10)
	if err != nil || len(history) != 2 || history[0].State != sequencer.Retryable || history[1].State != sequencer.Succeeded {
		t.Fatalf("History() = %+v, %v", history, err)
	}
}

func TestRunnerUsesOneLocalTransactionPerAttempt(t *testing.T) {
	t.Parallel()

	spec := validSpec("transactional")
	spec.Policy.WithinTransaction = true
	tx := &struct{ name string }{"tx-1"}
	spec.Handler = sequencer.HandlerFunc(func(_ context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
		if attempt.Transaction != tx {
			t.Fatalf("transaction = %v, want injected transaction", attempt.Transaction)
		}
		return sequencer.Output{}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	manager := &transactionManager{transaction: tx}
	runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{
		Owner: "replica", Clock: newManualClock(time.Now()), Transactions: manager,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manager.calls != 1 {
		t.Fatalf("transaction calls = %d, want 1", manager.calls)
	}
}

func TestRunnerRequiresDeclaredApprovalAndEnvironment(t *testing.T) {
	t.Parallel()

	spec := validSpec("protected")
	spec.Environments = []string{"production"}
	spec.Policy.RequiresApproval = true
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	_, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "replica", Environment: "staging"})
	if !errors.Is(err, sequencer.ErrEnvironmentForbidden) {
		t.Fatalf("NewRunner() error = %v", err)
	}
	_, err = sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "replica", Environment: "production"})
	if !errors.Is(err, sequencer.ErrApprovalRequired) {
		t.Fatalf("NewRunner() error = %v", err)
	}
}

func TestRunnerAuditsApprovalDenialAsBlocked(t *testing.T) {
	t.Parallel()

	spec := validSpec("approval")
	spec.Policy.RequiresApproval = true
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{
		Owner: "replica", Approver: approverStub{approval: sequencer.Approval{Actor: "operator", Reason: "change window closed"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrBlocked) {
		t.Fatalf("Execute() error = %v", err)
	}
	record, err := store.Snapshot(context.Background(), "approval", 1)
	if err != nil || record.State != sequencer.Blocked {
		t.Fatalf("Snapshot() = %+v, %v", record, err)
	}
	audit, err := store.Audit(context.Background(), "approval", 1, 10)
	if err != nil || audit[len(audit)-1].Actor != "operator" || audit[len(audit)-1].Reason != "change window closed" {
		t.Fatalf("Audit() = %+v, %v", audit, err)
	}
}

func TestRunnerAuditsConditionalSkipWithoutInvokingHandler(t *testing.T) {
	t.Parallel()

	spec := validSpec("conditional")
	called := false
	spec.Condition = sequencer.ConditionFunc(func(context.Context, sequencer.Attempt) (sequencer.Decision, error) {
		return sequencer.Decision{Run: false, Reason: "already normalized"}, nil
	})
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		called = true
		return sequencer.Output{}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("handler ran after condition denied execution")
	}
	record, _ := store.Snapshot(context.Background(), "conditional", 1)
	if record.State != sequencer.Skipped {
		t.Fatalf("state = %s", record.State)
	}
}

func TestRunnerSanitizesFailuresAndRecoversPanics(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		handler sequencer.Handler
	}{
		{"error", sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{}, errors.New("secret token abc")
		})},
		{"panic", sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			panic("secret panic")
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID(test.name))
			spec.Handler = test.handler
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := memory.New()
			runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
			if _, err := runner.Execute(context.Background()); err == nil {
				t.Fatal("Execute() error = nil")
			}
			history, _ := store.History(context.Background(), spec.ID, 1, 10)
			if history[0].ErrorDetail != sequencer.ErrPermanent.Error() {
				t.Fatalf("persisted error detail = %q", history[0].ErrorDetail)
			}
		})
	}
}

func TestRunnerFailsClosedOnOversizedOutput(t *testing.T) {
	t.Parallel()

	spec := validSpec("oversized")
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		return sequencer.Output{Summary: string(make([]byte, sequencer.DefaultMaxOutputBytes+1))}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Execute() error = %v, want ErrResourceLimit", err)
	}
	history, _ := store.History(context.Background(), "oversized", 1, 10)
	if len(history) != 1 || history[0].Output.Summary != "" || history[0].State != sequencer.Failed {
		t.Fatalf("History() = %+v", history)
	}

	tooMany := make(map[string]string, sequencer.DefaultMaxOutputMetadata+1)
	for index := 0; index <= sequencer.DefaultMaxOutputMetadata; index++ {
		tooMany[string(rune('a'+index))] = "value"
	}
	outputs := []sequencer.Output{
		{Metadata: tooMany},
		{Metadata: map[string]string{"": "value"}},
		{Metadata: map[string]string{"key": string(make([]byte, 4_097))}},
	}
	for index, output := range outputs {
		spec := validSpec(sequencer.OperationID(fmt.Sprintf("output-%d", index)))
		spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) { return output, nil })
		plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
		runner, _ := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "replica"})
		if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Errorf("output %d error = %v", index, err)
		}
	}

	validOutput := validSpec("valid-output")
	validOutput.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		return sequencer.Output{Summary: "done\n", Metadata: map[string]string{"count\n": "1\n"}}, nil
	})
	plan, _ = sequencer.CompilePlan([]sequencer.OperationSpec{validOutput}, sequencer.PlanOptions{})
	validStore := memory.New()
	runner, _ = sequencer.NewRunner(plan, validStore, sequencer.RunnerOptions{Owner: "replica"})
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	history, _ = validStore.History(context.Background(), "valid-output", 1, 1)
	if history[0].Output.Summary != "done" || history[0].Output.Metadata["count"] != "1" {
		t.Fatalf("sanitized output = %+v", history[0].Output)
	}
}

func TestRunnerConstructorValidation(t *testing.T) {
	t.Parallel()

	store := memory.New()
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a")}, sequencer.PlanOptions{})
	if _, err := sequencer.NewRunner(nil, store, sequencer.RunnerOptions{Owner: "owner"}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("nil plan error = %v", err)
	}
	if _, err := sequencer.NewRunner(plan, nil, sequencer.RunnerOptions{Owner: "owner"}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("nil store error = %v", err)
	}
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("empty owner error = %v", err)
	}
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", LeaseDuration: -time.Second}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("negative lease error = %v", err)
	}
	observers := make([]sequencer.Observer, 129)
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Observers: observers}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("observer limit error = %v", err)
	}
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", LeaseDuration: time.Second}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("lease timeout error = %v", err)
	}
	transactional := validSpec("transactional-missing")
	transactional.Policy.WithinTransaction = true
	plan, _ = sequencer.CompilePlan([]sequencer.OperationSpec{transactional}, sequencer.PlanOptions{})
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner"}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("transaction manager error = %v", err)
	}
}

func TestRunnerFaultBoundariesAndAllowedFailure(t *testing.T) {
	t.Parallel()

	cause := errors.New("store failure")
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a")}, sequencer.PlanOptions{})
	for name, faults := range map[string]sequencertest.Faults{
		"register": {Register: cause},
		"claim":    {ClaimNext: cause},
		"running":  {MarkRunning: cause},
		"complete": {Complete: cause},
	} {
		t.Run(name, func(t *testing.T) {
			store := sequencertest.NewFaultStore(memory.New(), faults)
			runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner"})
			if _, err := runner.Execute(context.Background()); !errors.Is(err, cause) {
				t.Fatalf("Execute() error = %v", err)
			}
		})
	}

	first := validSpec("allowed")
	first.Policy.AllowedFailure = true
	first.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		return sequencer.Output{}, errors.New("failed")
	})
	second := validSpec("later")
	called := false
	second.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		called = true
		return sequencer.Output{}, nil
	})
	plan, _ = sequencer.CompilePlan([]sequencer.OperationSpec{first, second}, sequencer.PlanOptions{})
	runner, _ := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "owner"})
	report, err := runner.Execute(context.Background())
	if err != nil || report.Result != sequencer.RunPartial || !called || len(report.Operations) != 2 {
		t.Fatalf("report = %+v, called = %t, error = %v", report, called, err)
	}
}

func TestRunnerObserverApprovalConditionAndFailureClassifications(t *testing.T) {
	t.Parallel()

	events := 0
	observer := sequencer.ObserverFunc(func(sequencer.Event) { events++ })
	spec := validSpec("approved")
	spec.Policy.RequiresApproval = true
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	runner, _ := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{
		Owner: "owner", Approver: approverStub{approval: sequencer.Approval{Approved: true, Actor: "op", Reason: "ticket"}},
		Observers: []sequencer.Observer{nil, observer},
	})
	if _, err := runner.Execute(context.Background()); err != nil || events != 3 {
		t.Fatalf("Execute() error = %v, events = %d", err, events)
	}
	if report, err := runner.Execute(context.Background()); err != nil || report.Operations[0].State != sequencer.Succeeded {
		t.Fatalf("second Execute() = %+v, %v", report, err)
	}

	classifications := []struct {
		name string
		err  error
		want sequencer.State
	}{
		{"skip", sequencer.Skip(errors.New("skip")), sequencer.Skipped},
		{"block", sequencer.Block(errors.New("block")), sequencer.Blocked},
		{"cancel", context.Canceled, sequencer.Canceled},
		{"timeout", context.DeadlineExceeded, sequencer.Failed},
		{"unknown", sequencer.UnknownResult(errors.New("unknown")), sequencer.Failed},
	}
	for _, test := range classifications {
		t.Run(test.name, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID(test.name))
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				return sequencer.Output{}, test.err
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := memory.New()
			runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner"})
			_, _ = runner.Execute(context.Background())
			record, _ := store.Snapshot(context.Background(), spec.ID, 1)
			if record.State != test.want {
				t.Fatalf("state = %s, want %s", record.State, test.want)
			}
		})
	}

	condition := validSpec("condition-error")
	condition.Condition = sequencer.ConditionFunc(func(context.Context, sequencer.Attempt) (sequencer.Decision, error) {
		return sequencer.Decision{}, errors.New("condition")
	})
	plan, _ = sequencer.CompilePlan([]sequencer.OperationSpec{condition}, sequencer.PlanOptions{})
	runner, _ = sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "owner"})
	if _, err := runner.Execute(context.Background()); err == nil {
		t.Fatal("condition error = nil")
	}
	emptyReason := validSpec("condition-empty")
	emptyReason.Condition = sequencer.ConditionFunc(func(context.Context, sequencer.Attempt) (sequencer.Decision, error) {
		return sequencer.Decision{}, nil
	})
	plan, _ = sequencer.CompilePlan([]sequencer.OperationSpec{emptyReason}, sequencer.PlanOptions{})
	runner, _ = sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "owner"})
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatalf("empty condition reason error = %v", err)
	}
}

type approverStub struct {
	approval sequencer.Approval
	err      error
}

func (stub approverStub) Approve(context.Context, sequencer.OperationSpec) (sequencer.Approval, error) {
	return stub.approval, stub.err
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(now time.Time) *manualClock { return &manualClock{now: now} }
func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

type transactionManager struct {
	transaction any
	calls       int
}

func (manager *transactionManager) Within(_ context.Context, execute func(context.Context, any) error) error {
	manager.calls++
	return execute(context.Background(), manager.transaction)
}
