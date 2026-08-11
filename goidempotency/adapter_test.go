package goidempotency_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/goidempotency"
)

var errMissingCleanupDeadline = errors.New("cleanup context has no deadline")

func TestAdapterBoundsDetachedCleanupAfterCallerCancellation(t *testing.T) {
	t.Parallel()

	const cleanupTimeout = 20 * time.Millisecond
	tests := []struct {
		name      string
		execution error
	}{
		{name: "complete"},
		{name: "fail", execution: errors.New("execution")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gate := &blockingCleanupGate{execute: true}
			adapter, err := goidempotency.NewWithCleanupTimeout(gate, cleanupTimeout)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			err = adapter.Do(ctx, "key", func(context.Context) error {
				cancel()
				return test.execution
			})
			if !gate.hadDeadline {
				t.Fatal("cleanup context had no deadline")
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cleanup error = %v, want deadline exceeded", err)
			}
			if !errors.Is(err, sequencer.ErrUnknownResult) {
				t.Fatalf("cleanup error = %v, want ErrUnknownResult", err)
			}
			if test.execution != nil && !errors.Is(err, test.execution) {
				t.Fatalf("cleanup error = %v, want joined execution error", err)
			}
		})
	}
}

func TestNewWithCleanupTimeoutRejectsUnboundedDurations(t *testing.T) {
	t.Parallel()

	for _, timeout := range []time.Duration{-time.Nanosecond, 0, goidempotency.MaxCleanupTimeout + time.Nanosecond} {
		if _, err := goidempotency.NewWithCleanupTimeout(&gateStub{}, timeout); !errors.Is(err, goidempotency.ErrInvalidAdapter) {
			t.Fatalf("NewWithCleanupTimeout(%s) error = %v", timeout, err)
		}
	}
	if _, err := goidempotency.NewWithCleanupTimeout(&gateStub{}, goidempotency.MaxCleanupTimeout); err != nil {
		t.Fatalf("exact maximum cleanup timeout error = %v", err)
	}
}

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
	if !called || !gate.completed || !gate.completeHadDeadline {
		t.Fatalf("called = %t, completed = %t, cleanup deadline = %t", called, gate.completed, gate.completeHadDeadline)
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
	adapter, _ = goidempotency.New(&gateStub{execute: true, beginErr: cause})
	if err := adapter.Do(context.Background(), "key", func(context.Context) error { return nil }); !errors.Is(err, cause) {
		t.Fatalf("begin error = %v", err)
	}
	called := false
	adapter, _ = goidempotency.New(&gateStub{})
	if err := adapter.Do(context.Background(), "key", func(context.Context) error {
		called = true
		return nil
	}); err != nil || called {
		t.Fatalf("unacquired key error = %v, called = %t", err, called)
	}
	called = false
	adapter, _ = goidempotency.New(&gateStub{execute: true, nilToken: true})
	if err := adapter.Do(context.Background(), "key", func(context.Context) error {
		called = true
		return nil
	}); !errors.Is(err, goidempotency.ErrInvalidAdapter) || called {
		t.Fatalf("nil token error = %v, called = %t", err, called)
	}
	execution, failure := errors.New("execution"), errors.New("record failure")
	adapter, _ = goidempotency.New(&gateStub{execute: true})
	if err := adapter.Do(context.Background(), "key", func(context.Context) error { return execution }); !errors.Is(err, execution) {
		t.Fatalf("recorded execution error = %v", err)
	}
	adapter, _ = goidempotency.New(&gateStub{execute: true, failErr: failure})
	err := adapter.Do(context.Background(), "key", func(context.Context) error { return execution })
	if !errors.Is(err, execution) || !errors.Is(err, failure) {
		t.Fatalf("execution error = %v", err)
	}
	if !errors.Is(err, sequencer.ErrUnknownResult) {
		t.Fatalf("execution error = %v, want ErrUnknownResult", err)
	}
	adapter, _ = goidempotency.New(&gateStub{execute: true, completeErr: cause})
	if err := adapter.Do(context.Background(), "key", func(context.Context) error { return nil }); !errors.Is(err, cause) || !errors.Is(err, sequencer.ErrUnknownResult) {
		t.Fatalf("complete error = %v", err)
	}
}

type gateStub struct {
	execute             bool
	completed           bool
	completeHadDeadline bool
	beginErr            error
	failErr             error
	completeErr         error
	nilToken            bool
}

func (gate *gateStub) Begin(context.Context, string) (goidempotency.Token, bool, error) {
	if gate.nilToken {
		return nil, gate.execute, gate.beginErr
	}
	return "token", gate.execute, gate.beginErr
}
func (gate *gateStub) Complete(ctx context.Context, _ goidempotency.Token) error {
	gate.completed = true
	_, gate.completeHadDeadline = ctx.Deadline()
	return gate.completeErr
}
func (gate *gateStub) Fail(context.Context, goidempotency.Token, error) error { return gate.failErr }

type blockingCleanupGate struct {
	execute     bool
	hadDeadline bool
}

func (gate *blockingCleanupGate) Begin(context.Context, string) (goidempotency.Token, bool, error) {
	return "token", gate.execute, nil
}

func (gate *blockingCleanupGate) Complete(ctx context.Context, _ goidempotency.Token) error {
	return gate.block(ctx)
}

func (gate *blockingCleanupGate) Fail(ctx context.Context, _ goidempotency.Token, _ error) error {
	return gate.block(ctx)
}

func (gate *blockingCleanupGate) block(ctx context.Context) error {
	_, gate.hadDeadline = ctx.Deadline()
	if !gate.hadDeadline {
		return errMissingCleanupDeadline
	}
	<-ctx.Done()
	return ctx.Err()
}
