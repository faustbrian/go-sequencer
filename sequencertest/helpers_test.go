package sequencertest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
	"github.com/faustbrian/golib/pkg/sequencer/sequencertest"
)

func TestClockAndOperationAreDeterministic(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	clock := sequencertest.NewClock(start)
	clock.Advance(time.Minute)
	if got := clock.Now(); !got.Equal(start.Add(time.Minute)) {
		t.Fatalf("Now() = %v", got)
	}
	spec := sequencertest.Operation("fixture", nil)
	if spec.Checksum == "" || spec.Handler == nil || spec.Policy.MaxAttempts == 0 {
		t.Fatalf("Operation() = %+v", spec)
	}
	if _, err := spec.Handler.Handle(context.Background(), sequencer.Attempt{}); err != nil {
		t.Fatalf("fixture handler error = %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("negative clock advance did not panic")
		}
	}()
	clock.Advance(-time.Second)
}

func TestFaultStoreInjectsCrashBoundary(t *testing.T) {
	t.Parallel()

	cause := errors.New("crash")
	store := sequencertest.NewFaultStore(memory.New(), sequencertest.Faults{Complete: cause})
	err := store.Complete(context.Background(), sequencer.Completion{})
	if !errors.Is(err, cause) {
		t.Fatalf("Complete() error = %v", err)
	}
}

func TestFaultStoreForwardsEveryStoreBoundary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now()
	store := sequencertest.NewFaultStore(memory.New(), sequencertest.Faults{})
	registration := sequencer.Registration{ID: "a", Version: 1, Checksum: "sum"}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "owner", Now: now, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(ctx, "a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.History(ctx, "a", 1, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Audit(ctx, "a", 1, 10); err != nil {
		t.Fatal(err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: "a", Version: 1, Actor: "op", Reason: "retry", At: now}); err != nil {
		t.Fatal(err)
	}

	recovery := sequencertest.NewFaultStore(memory.New(), sequencertest.Faults{})
	if err := recovery.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "owner", Now: now, LeaseDuration: time.Second}); err != nil {
		t.Fatal(err)
	}
	if recovered, err := recovery.RecoverExpired(ctx, now.Add(2*time.Second)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
}

func TestFaultStoreInjectsEveryConfiguredBoundary(t *testing.T) {
	t.Parallel()

	cause := errors.New("fault")
	store := sequencertest.NewFaultStore(memory.New(), sequencertest.Faults{
		Register: cause, ClaimNext: cause, MarkRunning: cause,
		RecoverExpired: cause, Snapshot: cause, History: cause,
		Audit: cause, Reset: cause,
	})
	checks := []func() error{
		func() error { return store.Register(context.Background(), nil, time.Now()) },
		func() error { _, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{}); return err },
		func() error {
			_, err := store.MarkRunning(context.Background(), sequencer.Ownership{}, time.Now())
			return err
		},
		func() error { _, err := store.RecoverExpired(context.Background(), time.Now()); return err },
		func() error { _, err := store.Snapshot(context.Background(), "a", 1); return err },
		func() error { _, err := store.History(context.Background(), "a", 1, 1); return err },
		func() error { _, err := store.Audit(context.Background(), "a", 1, 1); return err },
		func() error { return store.Reset(context.Background(), sequencer.ResetRequest{}) },
	}
	for index, check := range checks {
		if err := check(); !errors.Is(err, cause) {
			t.Errorf("check %d error = %v", index, err)
		}
	}
}
