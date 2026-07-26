package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

func TestStoreValidationInspectionAndResetEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now()
	store := memory.New()
	if err := store.Register(ctx, []sequencer.Registration{{}}, now); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Register(invalid) error = %v", err)
	}
	registration := sequencer.Registration{ID: "a", Version: 1, Checksum: "sum"}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatalf("Register(same) error = %v", err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("ClaimNext(invalid) error = %v", err)
	}
	if _, err := store.MarkRunning(ctx, sequencer.Ownership{OperationID: "missing"}, now); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("MarkRunning(missing) error = %v", err)
	}
	if _, err := store.Snapshot(ctx, "missing", 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("Snapshot(missing) error = %v", err)
	}
	if _, err := store.History(ctx, "missing", 1, 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("History(missing) error = %v", err)
	}
	if _, err := store.Audit(ctx, "missing", 1, 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("Audit(missing) error = %v", err)
	}
	if _, err := store.History(ctx, "a", 1, 0); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("History(limit) error = %v", err)
	}
	if _, err := store.Audit(ctx, "a", 1, sequencer.DefaultMaxHistory+1); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Audit(limit) error = %v", err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset(invalid) error = %v", err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: "missing", Version: 1, Actor: "a", Reason: "r", At: now}); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("Reset(missing) error = %v", err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: "a", Version: 1, Actor: "a", Reason: "r", At: now}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset(eligible) error = %v", err)
	}
	if recovered, err := store.RecoverExpired(ctx, now); err != nil || recovered != 0 {
		t.Fatalf("RecoverExpired(eligible) = %d, %v", recovered, err)
	}
	blocked := memory.New()
	if err := blocked.Register(ctx, []sequencer.Registration{{ID: "dependent", Version: 1, Checksum: "sum", Dependencies: []sequencer.OperationID{"missing"}}}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := blocked.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"dependent"}, Owner: "owner", Now: now, LeaseDuration: time.Minute}); !errors.Is(err, sequencer.ErrNoEligibleOperation) {
		t.Fatalf("dependency claim error = %v", err)
	}
}

func TestStoreDeferredRetryResetAndDefensiveCopies(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now()
	store := memory.New()
	if err := store.Register(ctx, []sequencer.Registration{{ID: "a", Version: 1, Checksum: "sum"}}, now); err != nil {
		t.Fatal(err)
	}
	claim, _ := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "owner", Now: now, LeaseDuration: time.Minute})
	if recovered, err := store.RecoverExpired(ctx, now); err != nil || recovered != 0 {
		t.Fatalf("RecoverExpired(active) = %d, %v", recovered, err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); !errors.Is(err, sequencer.ErrInvalidTransition) {
		t.Fatalf("MarkRunning(twice) error = %v", err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: sequencer.Ownership{OperationID: "missing"}, State: sequencer.Succeeded}); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("Complete(missing) error = %v", err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Eligible}); !errors.Is(err, sequencer.ErrInvalidTransition) {
		t.Fatalf("Complete(state) error = %v", err)
	}
	metadata := map[string]string{"count": "1"}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Deferred, At: now, EligibleAt: now.Add(time.Minute), Output: sequencer.Output{Metadata: metadata}}); err != nil {
		t.Fatal(err)
	}
	metadata["count"] = "changed"
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "early", Now: now, LeaseDuration: time.Minute}); !errors.Is(err, sequencer.ErrNoEligibleOperation) {
		t.Fatalf("early claim error = %v", err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "owner-2", Now: now.Add(time.Minute), LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: now.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	history, _ := store.History(ctx, "a", 1, 10)
	if history[0].Output.Metadata["count"] != "1" {
		t.Fatalf("stored metadata = %+v", history[0].Output.Metadata)
	}
	history[0].Output.Metadata["count"] = "mutated"
	history, _ = store.History(ctx, "a", 1, 10)
	if history[0].Output.Metadata["count"] != "1" {
		t.Fatal("history output is mutable")
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: "a", Version: 1, Actor: "operator", Reason: "approved", At: now.Add(2 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	record, _ := store.Snapshot(ctx, "a", 1)
	record.Dependencies = append(record.Dependencies, "mutated")
	recordAgain, _ := store.Snapshot(ctx, "a", 1)
	if len(recordAgain.Dependencies) != 0 || recordAgain.State != sequencer.Eligible {
		t.Fatalf("record = %+v", recordAgain)
	}
}

func TestStoreHonorsCanceledContexts(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := memory.New()
	checks := []func() error{
		func() error { return store.Register(ctx, nil, time.Now()) },
		func() error { _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{}); return err },
		func() error { _, err := store.MarkRunning(ctx, sequencer.Ownership{}, time.Now()); return err },
		func() error { return store.Complete(ctx, sequencer.Completion{}) },
		func() error { _, err := store.RecoverExpired(ctx, time.Now()); return err },
		func() error { _, err := store.Snapshot(ctx, "a", 1); return err },
		func() error { _, err := store.History(ctx, "a", 1, 1); return err },
		func() error { _, err := store.Audit(ctx, "a", 1, 1); return err },
		func() error { return store.Reset(ctx, sequencer.ResetRequest{}) },
	}
	for index, check := range checks {
		if err := check(); !errors.Is(err, context.Canceled) {
			t.Errorf("check %d error = %v", index, err)
		}
	}
}
