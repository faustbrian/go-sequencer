package goretry_test

import (
	"context"
	"errors"
	"testing"

	sequencer "github.com/faustbrian/go-sequencer"
	sequencerretry "github.com/faustbrian/go-sequencer/adapters/retry"
	"github.com/faustbrian/go-sequencer/goretry"
)

func TestFacadeDelegatesToCanonicalAdapter(t *testing.T) {
	t.Parallel()

	if !errors.Is(goretry.ErrInvalidAdapter, sequencerretry.ErrInvalidAdapter) {
		t.Fatal("legacy and canonical sentinels differ")
	}
	if _, err := goretry.New(nil); !errors.Is(err, goretry.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	adapter, err := goretry.New(facadePolicy{})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	budget, err := sequencer.NewExecutionBudget(1)
	if err != nil {
		t.Fatalf("NewExecutionBudget(): %v", err)
	}
	called := false
	if err := adapter.Do(context.Background(), budget, func(context.Context) error {
		called = true
		return nil
	}); err != nil || !called {
		t.Fatalf("Do() = %v, called = %v", err, called)
	}
	if got := (goretry.Classifier{}).Classify(sequencer.Retry(errors.New("busy"))); got != goretry.Retryable {
		t.Fatalf("Classify(retry) = %v", got)
	}
	if got, want := uint8((goretry.Classifier{}).Classify(sequencer.Retry(errors.New("busy")))),
		uint8((sequencerretry.Classifier{}).Classify(sequencer.Retry(errors.New("busy")))); got != want {
		t.Fatalf("legacy classification = %v, canonical = %v", got, want)
	}
	if got := (goretry.Classifier{}).Classify(errors.New("bad")); got != goretry.Permanent {
		t.Fatalf("Classify(permanent) = %v", got)
	}
}

type facadePolicy struct{}

func (facadePolicy) Do(ctx context.Context, operation func(context.Context) error) error {
	return operation(ctx)
}
