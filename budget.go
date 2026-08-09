package sequencer

import (
	"errors"
	"sync/atomic"
)

// ErrBudgetExhausted reports an attempted execution beyond a shared bound.
var ErrBudgetExhausted = errors.New("sequencer: execution budget exhausted")

// ExecutionBudget bounds all callback executions performed by one retry
// owner. It is safe for concurrent use and must not be copied after use.
type ExecutionBudget struct {
	limit uint64
	used  atomic.Uint64
}

// NewExecutionBudget constructs a finite non-zero shared execution budget.
func NewExecutionBudget(limit uint) (*ExecutionBudget, error) {
	if limit == 0 {
		return nil, ErrResourceLimit
	}
	return &ExecutionBudget{limit: uint64(limit)}, nil
}

// Take reserves one execution or reports that the shared bound is exhausted.
func (budget *ExecutionBudget) Take() error {
	if budget == nil {
		return ErrBudgetExhausted
	}
	for {
		used := budget.used.Load()
		if used >= budget.limit {
			return ErrBudgetExhausted
		}
		if budget.used.CompareAndSwap(used, used+1) {
			return nil
		}
	}
}

// Remaining returns the number of callback executions still available.
func (budget *ExecutionBudget) Remaining() uint {
	if budget == nil {
		return 0
	}
	used := budget.used.Load()
	return uint(max(budget.limit-used, 0))
}
