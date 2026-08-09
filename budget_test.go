package sequencer_test

import (
	"errors"
	"sync"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestExecutionBudgetIsFiniteNilSafeAndConcurrent(t *testing.T) {
	t.Parallel()

	if _, err := sequencer.NewExecutionBudget(0); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("NewExecutionBudget(0) error = %v", err)
	}
	var nilBudget *sequencer.ExecutionBudget
	if !errors.Is(nilBudget.Take(), sequencer.ErrBudgetExhausted) || nilBudget.Remaining() != 0 {
		t.Fatal("nil budget did not fail closed")
	}
	budget, err := sequencer.NewExecutionBudget(64)
	if err != nil || budget.Remaining() != 64 {
		t.Fatalf("budget = %v, %v", budget, err)
	}
	sequential, err := sequencer.NewExecutionBudget(2)
	if err != nil || sequential.Take() != nil || sequential.Remaining() != 1 {
		t.Fatalf("sequential budget remaining = %d, error = %v", sequential.Remaining(), err)
	}
	if sequential.Take() != nil || sequential.Remaining() != 0 || !errors.Is(sequential.Take(), sequencer.ErrBudgetExhausted) {
		t.Fatalf("sequential budget did not exhaust at exactly two executions")
	}
	var wait sync.WaitGroup
	results := make(chan error, 128)
	for range 128 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			results <- budget.Take()
		}()
	}
	wait.Wait()
	close(results)
	accepted, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, sequencer.ErrBudgetExhausted):
			rejected++
		default:
			t.Fatalf("Take() error = %v", err)
		}
	}
	if accepted != 64 || rejected != 64 || budget.Remaining() != 0 {
		t.Fatalf("accepted = %d, rejected = %d, remaining = %d", accepted, rejected, budget.Remaining())
	}
}
