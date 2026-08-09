package goretry_test

import (
	"context"
	"errors"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/goretry"
)

func TestClassifierMapsSequencerRetryability(t *testing.T) {
	t.Parallel()

	classifier := goretry.Classifier{}
	if got := classifier.Classify(sequencer.Retry(errors.New("busy"))); got != goretry.Retryable {
		t.Fatalf("retry classification = %v", got)
	}
	if got := classifier.Classify(sequencer.Permanent(errors.New("bad"))); got != goretry.Permanent {
		t.Fatalf("permanent classification = %v", got)
	}
}

func TestAdapterUsesExternalBoundedPolicy(t *testing.T) {
	t.Parallel()

	policy := &policyStub{}
	adapter, err := goretry.New(policy)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := sequencer.NewExecutionBudget(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Do(context.Background(), budget, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if policy.calls != 1 {
		t.Fatalf("policy calls = %d", policy.calls)
	}
}

func TestAdapterValidationAndPolicyFailure(t *testing.T) {
	t.Parallel()

	if _, err := goretry.New(nil); !errors.Is(err, goretry.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	policy := &policyStub{err: errors.New("budget")}
	adapter, _ := goretry.New(policy)
	budget, _ := sequencer.NewExecutionBudget(1)
	if err := adapter.Do(context.Background(), budget, nil); !errors.Is(err, goretry.ErrInvalidAdapter) {
		t.Fatalf("Do(nil) error = %v", err)
	}
	if err := adapter.Do(context.Background(), budget, func(context.Context) error { return nil }); !errors.Is(err, policy.err) {
		t.Fatalf("Do() error = %v", err)
	}
}

func TestAdapterCannotExceedTheSharedExecutionBudget(t *testing.T) {
	t.Parallel()

	adapter, err := goretry.New(repeatingPolicy{attempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	budget, err := sequencer.NewExecutionBudget(2)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	err = adapter.Do(context.Background(), budget, func(context.Context) error {
		calls++
		return errors.New("retry")
	})
	if !errors.Is(err, sequencer.ErrBudgetExhausted) || calls != 2 || budget.Remaining() != 0 {
		t.Fatalf("Do() error = %v, calls = %d, remaining = %d", err, calls, budget.Remaining())
	}
}

type policyStub struct {
	calls int
	err   error
}

type repeatingPolicy struct{ attempts int }

func (policy repeatingPolicy) Do(ctx context.Context, operation func(context.Context) error) error {
	var err error
	for range policy.attempts {
		err = operation(ctx)
	}
	return err
}

func (policy *policyStub) Do(ctx context.Context, operation func(context.Context) error) error {
	policy.calls++
	if policy.err != nil {
		return policy.err
	}
	return operation(ctx)
}
