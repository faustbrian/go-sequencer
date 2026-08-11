package sequencer_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/goretry"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
	"github.com/faustbrian/golib/pkg/sequencer/sequencertest"
)

func TestRunnerExecutesPlanInOrderAndReportsDurableResults(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var executed []sequencer.OperationID
	operation := func(id sequencer.OperationID, dependencies ...sequencer.OperationID) sequencer.OperationSpec {
		spec := validSpec(id)
		for _, dependency := range dependencies {
			spec.DependencyRefs = append(spec.DependencyRefs, sequencer.DependencyRef{ID: dependency, Version: 1, Checksum: "sha256:0123456789abcdef"})
		}
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

func TestRunnerTreatsPersistedOneTimeSkipAsComplete(t *testing.T) {
	t.Parallel()

	spec := validSpec("conditional-skip")
	evaluations := 0
	spec.Condition = sequencer.ConditionFunc(func(context.Context, sequencer.Attempt) (sequencer.Decision, error) {
		evaluations++
		return sequencer.Decision{Run: false, Reason: "already satisfied"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := runner.Execute(context.Background())
	if err != nil || first.Operations[0].State != sequencer.Skipped {
		t.Fatalf("first Execute() = %+v, %v", first, err)
	}
	second, err := runner.Execute(context.Background())
	if err != nil || second.Result != sequencer.RunSucceeded || len(second.Operations) != 1 ||
		second.Operations[0].State != sequencer.Skipped || second.Operations[0].Attempts != 1 {
		t.Fatalf("second Execute() = %+v, %v", second, err)
	}
	if evaluations != 1 {
		t.Fatalf("condition evaluations = %d, want 1", evaluations)
	}
}

func TestRunnerExplicitlyResetsAndExecutesRepeatableSuccess(t *testing.T) {
	t.Parallel()

	spec := validSpec("repeatable")
	spec.Policy.Mode = sequencer.Repeatable
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		return sequencer.Output{Summary: "applied"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{
		Owner: "replica", Clock: newManualClock(time.Date(2026, 8, 10, 17, 0, 0, 0, time.UTC)),
	})
	if err != nil {
		t.Fatal(err)
	}
	for run := 1; run <= 2; run++ {
		report, executeErr := runner.Execute(context.Background())
		if executeErr != nil || report.Result != sequencer.RunSucceeded || report.Operations[0].State != sequencer.Succeeded {
			t.Fatalf("Execute(%d) = %+v, %v", run, report, executeErr)
		}
	}
	if invocations != 2 {
		t.Fatalf("handler invocations = %d, want 2", invocations)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 2 {
		t.Fatalf("History() = %+v, %v", history, err)
	}
	audit, err := store.Audit(context.Background(), spec.ID, spec.Version, 20)
	if err != nil {
		t.Fatal(err)
	}
	foundReset := false
	for _, event := range audit {
		if event.From == sequencer.Succeeded && event.To == sequencer.Eligible &&
			event.Actor == "replica" && event.Reason == "repeatable execution requested" {
			foundReset = true
		}
	}
	if !foundReset {
		t.Fatalf("repeatable reset audit missing: %+v", audit)
	}
}

func TestRunnerBoundsRepeatableResetTimeAndPropagatesResetFailure(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC)
	clock := newManualClock(base.Add(time.Minute))
	spec := validSpec("repeatable-reset-failure")
	spec.Policy.Mode = sequencer.Repeatable
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	first, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica", Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	clock.now = base
	clock.mu.Unlock()
	cause := errors.New("reset unavailable")
	failing := &resetFailureStore{Store: store, err: cause}
	runner, err := sequencer.NewRunner(plan, failing, sequencer.RunnerOptions{Owner: "replica", Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); !errors.Is(err, cause) {
		t.Fatalf("Execute() error = %v", err)
	}
	if !failing.request.At.Equal(record.UpdatedAt) {
		t.Fatalf("Reset() at = %s, want %s", failing.request.At, record.UpdatedAt)
	}
}

func TestRunnerRejectsClaimWithoutDurableBudget(t *testing.T) {
	t.Parallel()

	spec := validSpec("missing-durable-budget")
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := &zeroBudgetStore{Store: memory.New()}
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Execute() error = %v", err)
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
	store := &completionInspectStore{Store: memory.New()}
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
	completions := store.completionsSnapshot()
	if len(completions) != 2 || completions[0].EligibleAt.IsZero() || !completions[1].EligibleAt.IsZero() {
		t.Fatalf("completion eligibility = %+v", completions)
	}
}

func TestRunnerStopsDurableRetriesAtTheExactAttemptBudget(t *testing.T) {
	t.Parallel()

	spec := validSpec("retry-exhausted")
	spec.Policy.MaxAttempts, spec.Policy.MaxExceptions = 2, 2
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		return sequencer.Output{}, sequencer.Retry(errors.New("still busy"))
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := runner.Execute(ctx); !errors.Is(err, sequencer.ErrRetryable) {
		t.Fatalf("Execute() error = %v", err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 2 || invocations != 2 || history[1].State != sequencer.Failed {
		t.Fatalf("History() = %+v, %v; invocations = %d", history, err, invocations)
	}
}

func TestRunnerStopsDurableRetriesAtTheLowerExceptionBudget(t *testing.T) {
	t.Parallel()

	spec := validSpec("retry-exception-exhausted")
	spec.Policy.MaxAttempts, spec.Policy.MaxExceptions = 4, 2
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		return sequencer.Output{}, sequencer.Retry(errors.New("still busy"))
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrRetryable) {
		t.Fatalf("Execute() error = %v", err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 2 || invocations != 2 || history[1].State != sequencer.Failed {
		t.Fatalf("History() = %+v, %v; invocations = %d", history, err, invocations)
	}
}

func TestRunnerStopsDurableRetriesAtTheLowerAttemptBudget(t *testing.T) {
	t.Parallel()

	spec := validSpec("retry-attempt-exhausted")
	spec.Policy.MaxAttempts, spec.Policy.MaxExceptions = 2, 4
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		return sequencer.Output{}, sequencer.Retry(errors.New("still busy"))
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrRetryable) {
		t.Fatalf("Execute() error = %v", err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 2 || invocations != 2 || history[1].State != sequencer.Failed {
		t.Fatalf("History() = %+v, %v; invocations = %d", history, err, invocations)
	}
}

func TestRunnerUsesExactlyOneInlineRetryLoopWithSharedBudget(t *testing.T) {
	t.Parallel()

	adapter, err := goretry.New(inlineRetryPolicy{attempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	spec := validSpec("inline-retry")
	spec.Policy.RetryMode = sequencer.InlineRetries
	spec.Policy.MaxAttempts = 2
	spec.Policy.MaxExceptions = 2
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
		err := adapter.Do(ctx, attempt.Budget, func(context.Context) error {
			invocations++
			if invocations == 1 {
				return errors.New("transient")
			}
			return nil
		})
		if err != nil {
			return sequencer.Output{}, err
		}
		return sequencer.Output{Summary: "recovered inline"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "pod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Succeeded || invocations != 2 {
		t.Fatalf("History() = %+v, %v; inline invocations = %d", history, err, invocations)
	}
}

func TestRunnerInlineRetryBudgetUsesTheLowerExceptionBound(t *testing.T) {
	t.Parallel()

	adapter, err := goretry.New(inlineRetryPolicy{attempts: 4})
	if err != nil {
		t.Fatal(err)
	}
	spec := validSpec("inline-exceptions")
	spec.Policy.RetryMode = sequencer.InlineRetries
	spec.Policy.MaxAttempts = 4
	spec.Policy.MaxExceptions = 2
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
		err := adapter.Do(ctx, attempt.Budget, func(context.Context) error {
			invocations++
			return errors.New("transient")
		})
		return sequencer.Output{}, err
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "pod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrBudgetExhausted) || invocations != 2 {
		t.Fatalf("Execute() error = %v, inline invocations = %d", err, invocations)
	}
}

func TestRunnerInlineRetryBudgetUsesTheLowerAttemptBound(t *testing.T) {
	t.Parallel()

	adapter, err := goretry.New(inlineRetryPolicy{attempts: 4})
	if err != nil {
		t.Fatal(err)
	}
	spec := validSpec("inline-attempts")
	spec.Policy.RetryMode = sequencer.InlineRetries
	spec.Policy.MaxAttempts = 2
	spec.Policy.MaxExceptions = 4
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
		err := adapter.Do(ctx, attempt.Budget, func(context.Context) error {
			invocations++
			return errors.New("transient")
		})
		return sequencer.Output{}, err
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "pod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrBudgetExhausted) || invocations != 2 {
		t.Fatalf("Execute() error = %v, inline invocations = %d", err, invocations)
	}
}

func TestRunnerInlineRetryableFailureDoesNotStartADurableRetry(t *testing.T) {
	t.Parallel()

	spec := validSpec("inline-no-durable-retry")
	spec.Policy.RetryMode = sequencer.InlineRetries
	spec.Policy.MaxAttempts, spec.Policy.MaxExceptions = 2, 2
	invocations := 0
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		invocations++
		return sequencer.Output{}, sequencer.Retry(errors.New("inline owner exhausted"))
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	store := memory.New()
	runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "pod"})
	if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrRetryable) {
		t.Fatalf("Execute() error = %v", err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Failed || invocations != 1 {
		t.Fatalf("History() = %+v, %v; invocations = %d", history, err, invocations)
	}
}

func TestRunnerPinsLocalRegistryAcrossRollingDeploymentAndRollback(t *testing.T) {
	t.Parallel()

	store := memory.New()
	now := time.Now()
	if err := store.Register(context.Background(), []sequencer.Registration{{
		ID: "rolling", Version: 2, Checksum: "sha256:v2", Channel: "deploy",
	}}, now); err != nil {
		t.Fatal(err)
	}
	executed := make([]uint, 0, 2)
	operation := func(version uint, checksum string) sequencer.OperationSpec {
		spec := validSpec("rolling")
		spec.Version, spec.Checksum = version, checksum
		spec.Handler = sequencer.HandlerFunc(func(_ context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
			executed = append(executed, attempt.Version)
			return sequencer.Output{}, nil
		})
		return spec
	}
	oldPlan, err := sequencer.CompilePlan([]sequencer.OperationSpec{operation(1, "sha256:v1")}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	oldRunner, err := sequencer.NewRunner(oldPlan, store, sequencer.RunnerOptions{Owner: "old-pod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldRunner.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(executed) != 1 || executed[0] != 1 {
		t.Fatalf("old binary executions = %v", executed)
	}

	newPlan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{operation(2, "sha256:v2")}, sequencer.PlanOptions{})
	newRunner, _ := sequencer.NewRunner(newPlan, store, sequencer.RunnerOptions{Owner: "new-pod"})
	if _, err := newRunner.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(executed) != 2 || executed[1] != 2 {
		t.Fatalf("new binary executions = %v", executed)
	}
	if report, err := oldRunner.Execute(context.Background()); err != nil || report.Operations[0].State != sequencer.Succeeded {
		t.Fatalf("rollback Execute() = %+v, %v", report, err)
	}
	if len(executed) != 2 {
		t.Fatalf("rollback executions = %v, want no re-execution", executed)
	}
	driftPlan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{operation(1, "sha256:changed")}, sequencer.PlanOptions{})
	driftRunner, _ := sequencer.NewRunner(driftPlan, store, sequencer.RunnerOptions{Owner: "drifted-pod"})
	if _, err := driftRunner.Execute(context.Background()); !errors.Is(err, sequencer.ErrChecksumDrift) {
		t.Fatalf("drifted Execute() error = %v", err)
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

func TestRunnerRejectsTransactionManagerContractViolations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		manager     transactionManagerFunc
		handlerErr  error
		want        error
		wantState   sequencer.State
		invocations int
	}{
		{
			name:    "callback omitted",
			manager: func(context.Context, func(context.Context, any) error) error { return nil },
			want:    sequencer.ErrInvalidRunner, wantState: sequencer.Failed,
		},
		{
			name: "callback repeated",
			manager: func(ctx context.Context, execute func(context.Context, any) error) error {
				_ = execute(ctx, &struct{}{})
				return execute(ctx, &struct{}{})
			},
			want: sequencer.ErrUnknownResult, wantState: sequencer.Indeterminate, invocations: 1,
		},
		{
			name: "callback error swallowed",
			manager: func(ctx context.Context, execute func(context.Context, any) error) error {
				_ = execute(ctx, &struct{}{})
				return nil
			},
			handlerErr: errors.New("handler failed"),
			want:       sequencer.ErrUnknownResult, wantState: sequencer.Indeterminate, invocations: 1,
		},
		{
			name: "callback error replaced",
			manager: func(ctx context.Context, execute func(context.Context, any) error) error {
				_ = execute(ctx, &struct{}{})
				return errors.New("manager replaced callback error")
			},
			handlerErr: errors.New("handler failed"),
			want:       sequencer.ErrUnknownResult, wantState: sequencer.Indeterminate, invocations: 1,
		},
		{
			name: "nil transaction",
			manager: func(ctx context.Context, execute func(context.Context, any) error) error {
				return execute(ctx, nil)
			},
			want: sequencer.ErrInvalidRunner, wantState: sequencer.Failed,
		},
		{
			name: "manager panic before callback",
			manager: func(context.Context, func(context.Context, any) error) error {
				panic("manager panic")
			},
			want: sequencer.ErrInvalidRunner, wantState: sequencer.Failed,
		},
		{
			name: "manager panic after callback",
			manager: func(ctx context.Context, execute func(context.Context, any) error) error {
				_ = execute(ctx, &struct{}{})
				panic("manager panic")
			},
			want: sequencer.ErrUnknownResult, wantState: sequencer.Indeterminate, invocations: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invocations := 0
			spec := validSpec(sequencer.OperationID("transaction-contract-" + strings.ReplaceAll(test.name, " ", "-")))
			spec.Policy.WithinTransaction = true
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				invocations++
				return sequencer.Output{}, test.handlerErr
			})
			plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			store := memory.New()
			runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "pod", Transactions: test.manager})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runner.Execute(context.Background()); !errors.Is(err, test.want) {
				t.Fatalf("Execute() error = %v, want %v", err, test.want)
			}
			if invocations != test.invocations {
				t.Fatalf("handler invocations = %d, want %d", invocations, test.invocations)
			}
			record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
			if err != nil || record.State != test.wantState {
				t.Fatalf("Snapshot() = %+v, %v; want %s", record, err, test.wantState)
			}
		})
	}
}

func TestRunnerRejectsSuccessReturnedAfterAttemptDeadline(t *testing.T) {
	t.Parallel()

	spec := validSpec("deadline.ignored")
	spec.Policy.Timeout = time.Millisecond
	spec.Handler = sequencer.HandlerFunc(func(ctx context.Context, _ sequencer.Attempt) (sequencer.Output, error) {
		<-ctx.Done()
		return sequencer.Output{Summary: "late success"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := runner.Execute(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || len(report.Operations) != 1 || report.Operations[0].State != sequencer.Failed {
		t.Fatalf("Execute() report = %+v, error = %v", report, err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].State != sequencer.Failed || history[0].Output.Summary != "" {
		t.Fatalf("history = %+v", history)
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

func TestRunnerRejectsUnpersistableAuditMetadataBeforeHandlerEffects(t *testing.T) {
	t.Parallel()

	for name, approval := range map[string]sequencer.Approval{
		"actor": {
			Approved: true, Actor: strings.Repeat("a", sequencer.DefaultMaxActorBytes+1), Reason: "approved",
		},
		"reason": {
			Approved: true, Actor: "operator", Reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1),
		},
	} {
		t.Run("approval "+name, func(t *testing.T) {
			called := false
			spec := validSpec(sequencer.OperationID("approval-" + name))
			spec.Policy.RequiresApproval = true
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				called = true
				return sequencer.Output{}, nil
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := memory.New()
			runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Approver: approverStub{approval: approval}})
			report, err := runner.Execute(context.Background())
			if !errors.Is(err, sequencer.ErrResourceLimit) || called || report.Operations[0].State != sequencer.Blocked {
				t.Fatalf("Execute() = %+v, %v; called = %t", report, err, called)
			}
		})
	}

	called := false
	exactApproval := sequencer.Approval{
		Approved: true,
		Actor:    strings.Repeat("a", sequencer.DefaultMaxActorBytes),
		Reason:   strings.Repeat("r", sequencer.DefaultMaxReasonBytes),
	}
	exactSpec := validSpec("approval-exact-boundaries")
	exactSpec.Policy.RequiresApproval = true
	exactSpec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		called = true
		return sequencer.Output{}, nil
	})
	exactPlan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{exactSpec}, sequencer.PlanOptions{})
	exactStore := memory.New()
	exactRunner, _ := sequencer.NewRunner(exactPlan, exactStore, sequencer.RunnerOptions{
		Owner: "owner", Approver: approverStub{approval: exactApproval},
	})
	exactReport, err := exactRunner.Execute(context.Background())
	if err != nil || !called || exactReport.Operations[0].State != sequencer.Succeeded {
		t.Fatalf("exact-boundary Execute() = %+v, %v; called = %t", exactReport, err, called)
	}
	exactAudit, err := exactStore.Audit(context.Background(), exactSpec.ID, exactSpec.Version, 10)
	if err != nil || len(exactAudit) == 0 || exactAudit[len(exactAudit)-1].Actor != exactApproval.Actor ||
		exactAudit[len(exactAudit)-1].Reason != exactApproval.Reason {
		t.Fatalf("exact-boundary Audit() = %+v, %v", exactAudit, err)
	}

	called = false
	spec := validSpec("condition-reason-limit")
	spec.Condition = sequencer.ConditionFunc(func(context.Context, sequencer.Attempt) (sequencer.Decision, error) {
		return sequencer.Decision{Reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1)}, nil
	})
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		called = true
		return sequencer.Output{}, nil
	})
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	runner, _ := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{Owner: "owner"})
	report, err := runner.Execute(context.Background())
	if !errors.Is(err, sequencer.ErrResourceLimit) || called || report.Operations[0].State != sequencer.Failed {
		t.Fatalf("Execute() = %+v, %v; called = %t", report, err, called)
	}
}

func TestRunnerAcceptsExactObserverAndLeaseBoundaries(t *testing.T) {
	t.Parallel()

	spec := validSpec("runner.bounds")
	spec.Policy.Timeout = time.Second - time.Nanosecond
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	observers := make([]sequencer.Observer, 128)
	if _, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{
		Owner: "pod", LeaseDuration: time.Second, Observers: observers,
	}); err != nil {
		t.Fatalf("exact runner limits error = %v", err)
	}
	if _, err := sequencer.NewRunner(plan, memory.New(), sequencer.RunnerOptions{
		Owner: "pod", LeaseDuration: time.Second, Observers: append(observers, nil),
	}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("observer overflow error = %v", err)
	}
	equal := validSpec("runner.equal-lease")
	equal.Policy.Timeout = time.Second
	equalPlan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{equal}, sequencer.PlanOptions{})
	if _, err := sequencer.NewRunner(equalPlan, memory.New(), sequencer.RunnerOptions{
		Owner: "pod", LeaseDuration: time.Second,
	}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("equal timeout and lease error = %v", err)
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

func TestRunnerAuditsOnlyActualConditionReasons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		condition  sequencer.Condition
		wantActor  string
		wantReason string
	}{
		{name: "no-condition", wantActor: "replica", wantReason: "completed"},
		{
			name: "empty-decline",
			condition: sequencer.ConditionFunc(func(context.Context, sequencer.Attempt) (sequencer.Decision, error) {
				return sequencer.Decision{Run: false}, nil
			}),
			wantActor: "condition", wantReason: "condition declined execution",
		},
		{
			name: "exact-decline-reason",
			condition: sequencer.ConditionFunc(func(context.Context, sequencer.Attempt) (sequencer.Decision, error) {
				return sequencer.Decision{Run: false, Reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes)}, nil
			}),
			wantActor: "condition", wantReason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID("audit-" + test.name))
			spec.Condition = test.condition
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := memory.New()
			runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
			_, _ = runner.Execute(context.Background())
			audit, err := store.Audit(context.Background(), spec.ID, spec.Version, 10)
			if err != nil {
				t.Fatal(err)
			}
			completed := audit[len(audit)-1]
			if completed.Actor != test.wantActor || completed.Reason != test.wantReason {
				t.Fatalf("completion audit = %+v", completed)
			}
		})
	}
}

func TestRunnerSanitizesFailuresAndRecoversPanics(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		handler sequencer.Handler
		wantErr error
		detail  error
		state   sequencer.State
	}{
		{"error", sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{}, errors.New("secret token abc")
		}), nil, sequencer.ErrPermanent, sequencer.Failed},
		{"panic", sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			panic("secret panic")
		}), sequencer.ErrUnknownResult, sequencer.ErrUnknownResult, sequencer.Indeterminate},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := validSpec(sequencer.OperationID(test.name))
			spec.Handler = test.handler
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			store := memory.New()
			runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
			if _, err := runner.Execute(context.Background()); err == nil || test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Execute() error = %v, want %v", err, test.wantErr)
			}
			history, _ := store.History(context.Background(), spec.ID, 1, 10)
			if history[0].ErrorDetail != test.detail.Error() || history[0].State != test.state {
				t.Fatalf("persisted error detail = %q", history[0].ErrorDetail)
			}
		})
	}
}

func TestRunnerDoesNotPersistOutputFromFailedAttempt(t *testing.T) {
	t.Parallel()

	spec := validSpec("failed-output")
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		return sequencer.Output{Summary: "secret partial result", Metadata: map[string]string{"token": "secret"}}, errors.New("failed")
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "pod"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Execute(context.Background()); err == nil {
		t.Fatal("Execute() error = nil")
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 1)
	if err != nil || history[0].Output.Summary != "" || history[0].Output.Metadata != nil {
		t.Fatalf("History() = %+v, %v", history, err)
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
		{Metadata: map[string]string{strings.Repeat("k", 129): "value"}},
		{Metadata: map[string]string{"key": string(make([]byte, 4_097))}},
		{Metadata: map[string]string{"same key": "first", "same\tkey": "second"}},
		{Metadata: func() map[string]string {
			metadata := make(map[string]string, sequencer.DefaultMaxOutputMetadata)
			for index := range sequencer.DefaultMaxOutputMetadata {
				metadata[fmt.Sprintf("key-%d", index)] = strings.Repeat("v", 4_096)
			}
			return metadata
		}()},
	}
	for index, output := range outputs {
		spec := validSpec(sequencer.OperationID(fmt.Sprintf("output-%d", index)))
		spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) { return output, nil })
		plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
		store := memory.New()
		runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "replica"})
		if _, err := runner.Execute(context.Background()); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Errorf("output %d error = %v", index, err)
		}
		record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
		if err != nil || record.State != sequencer.Failed {
			t.Errorf("output %d snapshot = %+v, %v; want failed", index, record, err)
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

	exactRawSummary := validSpec("exact-raw-summary")
	exactRawSummary.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		return sequencer.Output{Summary: strings.Repeat(" ", sequencer.DefaultMaxOutputBytes)}, nil
	})
	plan, _ = sequencer.CompilePlan([]sequencer.OperationSpec{exactRawSummary}, sequencer.PlanOptions{})
	exactRawStore := memory.New()
	runner, _ = sequencer.NewRunner(plan, exactRawStore, sequencer.RunnerOptions{Owner: "replica"})
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatalf("exact raw summary error = %v", err)
	}
	history, _ = exactRawStore.History(context.Background(), exactRawSummary.ID, exactRawSummary.Version, 1)
	if len(history) != 1 || history[0].State != sequencer.Succeeded || history[0].Output.Summary != "" {
		t.Fatalf("exact raw summary history = %+v", history)
	}

	exactMetadata := make(map[string]string, sequencer.DefaultMaxOutputMetadata)
	exactMetadata[strings.Repeat("k", 128)] = strings.Repeat("v", 4_096)
	for index := 1; index < sequencer.DefaultMaxOutputMetadata; index++ {
		exactMetadata[fmt.Sprintf("key-%d", index)] = "value"
	}
	exactOutput := validSpec("exact-output")
	encodedEmptySummary, err := json.Marshal(sequencer.Output{Metadata: exactMetadata})
	if err != nil {
		t.Fatal(err)
	}
	exactSummaryBytes := sequencer.DefaultMaxOutputBytes - len(encodedEmptySummary)
	exactOutput.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		return sequencer.Output{Summary: strings.Repeat("s", exactSummaryBytes), Metadata: exactMetadata}, nil
	})
	plan, _ = sequencer.CompilePlan([]sequencer.OperationSpec{exactOutput}, sequencer.PlanOptions{})
	exactStore := memory.New()
	runner, _ = sequencer.NewRunner(plan, exactStore, sequencer.RunnerOptions{Owner: "replica"})
	if _, err := runner.Execute(context.Background()); err != nil {
		t.Fatalf("exact bounded output error = %v", err)
	}
	history, _ = exactStore.History(context.Background(), exactOutput.ID, exactOutput.Version, 1)
	if len(history[0].Output.Summary) != exactSummaryBytes ||
		len(history[0].Output.Metadata) != sequencer.DefaultMaxOutputMetadata ||
		len(history[0].Output.Metadata[strings.Repeat("k", 128)]) != 4_096 {
		t.Fatalf("exact bounded output = %+v", history[0].Output)
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
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: strings.Repeat("o", sequencer.DefaultMaxActorBytes+1)}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("owner overflow error = %v", err)
	}
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: strings.Repeat("o", sequencer.DefaultMaxActorBytes)}); err != nil {
		t.Fatalf("exact owner limit error = %v", err)
	}
	if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Channels: []string{"missing"}}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("unknown channel error = %v", err)
	}
	for _, channels := range [][]string{{"invalid channel"}, {"", ""}} {
		if _, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner", Channels: channels}); !errors.Is(err, sequencer.ErrInvalidRunner) {
			t.Fatalf("channels %v error = %v", channels, err)
		}
	}
	channelSpec := validSpec("channel")
	channelSpec.Channel = "data"
	channelPlan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{channelSpec}, sequencer.PlanOptions{})
	if _, err := sequencer.NewRunner(channelPlan, store, sequencer.RunnerOptions{Owner: "owner", Channels: []string{"data", "data"}}); !errors.Is(err, sequencer.ErrInvalidRunner) {
		t.Fatalf("duplicate channels error = %v", err)
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

func TestRunnerFiltersChannelsButRegistersCompleteCrossChannelPlan(t *testing.T) {
	t.Parallel()

	dependency := validSpec("schema")
	dependency.Channel = "schema"
	dependent := validSpec("backfill")
	dependent.Channel = "data"
	dependent.DependencyRefs = []sequencer.DependencyRef{referenceTo(dependency)}
	called := false
	dependent.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		called = true
		return sequencer.Output{}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{dependent, dependency}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "data-runner", Channels: []string{"data"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := runner.Execute(context.Background())
	if !errors.Is(err, sequencer.ErrNoEligibleOperation) || called {
		t.Fatalf("Execute() = %+v, %v; dependent called = %t", report, err, called)
	}
	dependencyRecord, snapshotErr := store.Snapshot(context.Background(), dependency.ID, dependency.Version)
	if snapshotErr != nil || dependencyRecord.Channel != "schema" {
		t.Fatalf("complete plan was not registered: %+v, %v", dependencyRecord, snapshotErr)
	}
	if len(report.Channels) != 1 || report.Channels[0] != "data" || len(report.Operations) != 1 || report.Operations[0].Channel != "data" {
		t.Fatalf("scoped report = %+v", report)
	}
}

func TestRunnerFaultBoundariesAndAllowedFailure(t *testing.T) {
	t.Parallel()

	cause := errors.New("store failure")
	plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a")}, sequencer.PlanOptions{})
	for name, faults := range map[string]sequencertest.Faults{
		"register": {Register: cause},
		"snapshot": {Snapshot: cause},
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
		{"unknown", sequencer.UnknownResult(errors.New("unknown")), sequencer.Indeterminate},
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

func TestRunnerPersistsUnknownHandlerOutcomeWithoutAuthorizingReplay(t *testing.T) {
	t.Parallel()

	calls := 0
	spec := validSpec("unknown-handler-outcome")
	spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
		calls++
		return sequencer.Output{}, sequencer.UnknownResult(errors.New("effect outcome unavailable"))
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := runner.Execute(context.Background())
	if !errors.Is(err, sequencer.ErrUnknownResult) || report.Operations[0].State != sequencer.Indeterminate {
		t.Fatalf("Execute() = %+v, %v", report, err)
	}
	record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
	if snapshotErr != nil || record.State != sequencer.Indeterminate || calls != 1 {
		t.Fatalf("Snapshot() = %+v, %v; calls = %d", record, snapshotErr, calls)
	}
	report, err = runner.Execute(context.Background())
	if !errors.Is(err, sequencer.ErrUnknownResult) || report.Operations[0].State != sequencer.Indeterminate || calls != 1 {
		t.Fatalf("replay Execute() = %+v, %v; calls = %d", report, err, calls)
	}
}

func TestRunnerMakesDeclaredDeadLettersObservable(t *testing.T) {
	t.Parallel()

	for _, allowed := range []bool{false, true} {
		spec := validSpec(sequencer.OperationID(fmt.Sprintf("dead-letter-%t", allowed)))
		spec.Policy.DeadLetter = true
		spec.Policy.AllowedFailure = allowed
		spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{}, sequencer.Permanent(errors.New("permanent"))
		})
		plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
		store := memory.New()
		runner, _ := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "owner"})
		report, err := runner.Execute(context.Background())
		if !allowed && err == nil {
			t.Fatal("Execute() error = nil")
		}
		if allowed && err != nil {
			t.Fatalf("Execute() allowed failure error = %v", err)
		}
		wantResult := sequencer.RunFailed
		if allowed {
			wantResult = sequencer.RunPartial
		}
		if report.Result != wantResult || report.Operations[0].State != sequencer.DeadLettered {
			t.Fatalf("report = %+v, want result %v and dead-lettered state", report, wantResult)
		}
		record, _ := store.Snapshot(context.Background(), spec.ID, spec.Version)
		if record.State != sequencer.DeadLettered || !record.DeadLetter {
			t.Fatalf("record = %+v", record)
		}
		rerun, rerunErr := runner.Execute(context.Background())
		if !allowed && rerunErr == nil {
			t.Fatal("rerun error = nil")
		}
		if allowed && rerunErr != nil {
			t.Fatalf("allowed rerun error = %v", rerunErr)
		}
		if rerun.Operations[0].State != sequencer.DeadLettered || rerun.Operations[0].Attempts != record.AttemptNumber {
			t.Fatalf("rerun report = %+v", rerun)
		}
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

type resetFailureStore struct {
	*memory.Store
	err     error
	request sequencer.ResetRequest
}

func (store *resetFailureStore) Reset(_ context.Context, request sequencer.ResetRequest) error {
	store.request = request
	return store.err
}

type zeroBudgetStore struct{ *memory.Store }

func (store *zeroBudgetStore) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	claim, err := store.Store.ClaimNext(ctx, request)
	claim.Budget = sequencer.RetryBudget{}
	return claim, err
}

type transactionManagerFunc func(context.Context, func(context.Context, any) error) error

func (function transactionManagerFunc) Within(ctx context.Context, execute func(context.Context, any) error) error {
	return function(ctx, execute)
}

type inlineRetryPolicy struct{ attempts int }

func (policy inlineRetryPolicy) Do(ctx context.Context, operation func(context.Context) error) error {
	var err error
	for range policy.attempts {
		err = operation(ctx)
		if err == nil || errors.Is(err, sequencer.ErrBudgetExhausted) {
			return err
		}
	}
	return err
}

func (manager *transactionManager) Within(_ context.Context, execute func(context.Context, any) error) error {
	manager.calls++
	return execute(context.Background(), manager.transaction)
}
