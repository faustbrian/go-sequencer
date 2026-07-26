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
	if err := adapter.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
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
	if err := adapter.Do(context.Background(), nil); !errors.Is(err, goretry.ErrInvalidAdapter) {
		t.Fatalf("Do(nil) error = %v", err)
	}
	if err := adapter.Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, policy.err) {
		t.Fatalf("Do() error = %v", err)
	}
}

type policyStub struct {
	calls int
	err   error
}

func (policy *policyStub) Do(ctx context.Context, operation func(context.Context) error) error {
	policy.calls++
	if policy.err != nil {
		return policy.err
	}
	return operation(ctx)
}
