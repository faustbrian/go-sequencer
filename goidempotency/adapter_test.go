package goidempotency_test

import (
	"context"
	"errors"
	"testing"

	"github.com/faustbrian/golib/pkg/sequencer/goidempotency"
)

func TestAdapterExecutesOnlyAcquiredKeyAndCompletes(t *testing.T) {
	t.Parallel()

	gate := &gateStub{execute: true}
	adapter, err := goidempotency.New(gate)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := adapter.Do(context.Background(), "sequencer/postal/1", func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !called || !gate.completed {
		t.Fatalf("called = %t, completed = %t", called, gate.completed)
	}
}

func TestAdapterFailureAndValidationPaths(t *testing.T) {
	t.Parallel()

	if _, err := goidempotency.New(nil); !errors.Is(err, goidempotency.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	adapter, _ := goidempotency.New(&gateStub{})
	if err := adapter.Do(context.Background(), "", func(context.Context) error { return nil }); !errors.Is(err, goidempotency.ErrInvalidAdapter) {
		t.Fatalf("Do(empty) error = %v", err)
	}
	if err := adapter.Do(context.Background(), "key", nil); !errors.Is(err, goidempotency.ErrInvalidAdapter) {
		t.Fatalf("Do(nil) error = %v", err)
	}
	cause := errors.New("unavailable")
	adapter, _ = goidempotency.New(&gateStub{beginErr: cause})
	if err := adapter.Do(context.Background(), "key", func(context.Context) error { return nil }); !errors.Is(err, cause) {
		t.Fatalf("begin error = %v", err)
	}
	execution, failure := errors.New("execution"), errors.New("record failure")
	adapter, _ = goidempotency.New(&gateStub{execute: true, failErr: failure})
	err := adapter.Do(context.Background(), "key", func(context.Context) error { return execution })
	if !errors.Is(err, execution) || !errors.Is(err, failure) {
		t.Fatalf("execution error = %v", err)
	}
	adapter, _ = goidempotency.New(&gateStub{execute: true, completeErr: cause})
	if err := adapter.Do(context.Background(), "key", func(context.Context) error { return nil }); !errors.Is(err, cause) {
		t.Fatalf("complete error = %v", err)
	}
}

type gateStub struct {
	execute     bool
	completed   bool
	beginErr    error
	failErr     error
	completeErr error
}

func (gate *gateStub) Begin(context.Context, string) (goidempotency.Token, bool, error) {
	return "token", gate.execute, gate.beginErr
}
func (gate *gateStub) Complete(context.Context, goidempotency.Token) error {
	gate.completed = true
	return gate.completeErr
}
func (gate *gateStub) Fail(context.Context, goidempotency.Token, error) error { return gate.failErr }
