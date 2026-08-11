package sequencer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
	"github.com/faustbrian/golib/pkg/sequencer/sequencertest"
)

func TestRunnerCrashAfterLocalTransactionCommitLeavesUnknownDurableOutcome(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 10, 15, 0, 0, 0, time.UTC)
	completionLost := errors.New("ledger completion unavailable")
	commits, effects := 0, 0
	transaction := &struct{ local bool }{local: true}
	manager := commitRecordingTransactionManager{transaction: transaction, commits: &commits}
	spec := validSpec("crash.after-local-commit")
	spec.Policy.WithinTransaction = true
	spec.Handler = sequencer.HandlerFunc(func(_ context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
		if attempt.Transaction != manager.transaction {
			t.Fatalf("transaction = %v, want local transaction", attempt.Transaction)
		}
		effects++
		return sequencer.Output{Summary: "effect committed"}, nil
	})
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ledger := memory.New()
	store := sequencertest.NewFaultStore(ledger, sequencertest.Faults{Complete: completionLost})
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{
		Owner: "pod-old", Clock: newManualClock(now), Transactions: manager,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := runner.Execute(context.Background()); !errors.Is(err, completionLost) {
		t.Fatalf("Execute() error = %v", err)
	}
	if effects != 1 || commits != 1 {
		t.Fatalf("effects = %d, commits = %d; want one committed local effect", effects, commits)
	}
	record, err := ledger.Snapshot(context.Background(), spec.ID, spec.Version)
	if err != nil || record.State != sequencer.Running || record.Owner != "pod-old" {
		t.Fatalf("pre-recovery record = %+v, %v", record, err)
	}
	if recovered, err := ledger.RecoverExpired(context.Background(), now.Add(sequencer.DefaultLeaseDuration)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	record, err = ledger.Snapshot(context.Background(), spec.ID, spec.Version)
	history, historyErr := ledger.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || historyErr != nil || record.State != sequencer.Indeterminate || record.Owner != "" ||
		len(history) != 1 || history[0].State != sequencer.Indeterminate || history[0].ErrorDetail != sequencer.ErrUnknownResult.Error() {
		t.Fatalf("recovered record = %+v, history = %+v, errors = %v, %v", record, history, err, historyErr)
	}
	if effects != 1 {
		t.Fatalf("recovery repeated local effect: %d", effects)
	}
}

type commitRecordingTransactionManager struct {
	transaction any
	commits     *int
}

func (manager commitRecordingTransactionManager) Within(ctx context.Context, execute func(context.Context, any) error) error {
	if err := execute(ctx, manager.transaction); err != nil {
		return err
	}
	*manager.commits++
	return nil
}
