package goidempotency_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sequenceridempotency "github.com/faustbrian/go-sequencer/adapters/idempotency"
	"github.com/faustbrian/go-sequencer/goidempotency"
)

func TestFacadeDelegatesToCanonicalAdapter(t *testing.T) {
	t.Parallel()

	if !errors.Is(goidempotency.ErrInvalidAdapter, sequenceridempotency.ErrInvalidAdapter) {
		t.Fatal("legacy and canonical sentinels differ")
	}
	if goidempotency.DefaultCleanupTimeout != sequenceridempotency.DefaultCleanupTimeout ||
		goidempotency.MaxCleanupTimeout != sequenceridempotency.MaxCleanupTimeout {
		t.Fatal("legacy and canonical cleanup defaults differ")
	}
	if _, err := goidempotency.New(nil); !errors.Is(err, goidempotency.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	if _, err := goidempotency.NewWithCleanupTimeout(facadeGate{}, 0); !errors.Is(err, goidempotency.ErrInvalidAdapter) {
		t.Fatalf("NewWithCleanupTimeout(0) error = %v", err)
	}
	adapter, err := goidempotency.New(facadeGate{})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	called := false
	if err := adapter.Do(context.Background(), "key", func(context.Context) error {
		called = true
		return nil
	}); err != nil || !called {
		t.Fatalf("Do() = %v, called = %v", err, called)
	}
	failure := errors.New("failed")
	if err := adapter.Do(context.Background(), "key", func(context.Context) error {
		return failure
	}); !errors.Is(err, failure) {
		t.Fatalf("Do(failure) = %v", err)
	}
	if goidempotency.DefaultCleanupTimeout != 5*time.Second || goidempotency.MaxCleanupTimeout != time.Minute {
		t.Fatal("cleanup defaults changed")
	}
}

type facadeGate struct{}

func (facadeGate) Begin(context.Context, string) (goidempotency.Token, bool, error) {
	return struct{}{}, true, nil
}

func (facadeGate) Complete(context.Context, goidempotency.Token) error { return nil }

func (facadeGate) Fail(context.Context, goidempotency.Token, error) error { return nil }
