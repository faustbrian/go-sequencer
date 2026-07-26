package scheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/sequencer/scheduler"
)

func TestAdapterSchedulesExplicitEligibility(t *testing.T) {
	t.Parallel()

	destination := &schedulerStub{}
	adapter, err := scheduler.New(destination)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)
	if err := adapter.Defer(context.Background(), "postal.backfill", 2, at); err != nil {
		t.Fatal(err)
	}
	if destination.request.OperationID != "postal.backfill" || !destination.request.EligibleAt.Equal(at) {
		t.Fatalf("scheduled = %+v", destination.request)
	}
}

func TestAdapterRejectsInvalidDependenciesAndRequests(t *testing.T) {
	t.Parallel()

	if _, err := scheduler.New(nil); !errors.Is(err, scheduler.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	adapter, _ := scheduler.New(&schedulerStub{})
	if err := adapter.Defer(context.Background(), "", 0, time.Time{}); !errors.Is(err, scheduler.ErrInvalidAdapter) {
		t.Fatalf("Defer(invalid) error = %v", err)
	}
	cause := errors.New("schedule")
	adapter, _ = scheduler.New(&schedulerStub{err: cause})
	if err := adapter.Defer(context.Background(), "a", 1, time.Now()); !errors.Is(err, cause) {
		t.Fatalf("Defer() error = %v", err)
	}
}

type schedulerStub struct {
	request scheduler.Request
	err     error
}

func (stub *schedulerStub) Schedule(_ context.Context, request scheduler.Request) error {
	stub.request = request
	return stub.err
}
