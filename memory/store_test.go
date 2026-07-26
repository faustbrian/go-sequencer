package memory_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

func TestStoreClaimsExactlyOnceAndEnforcesOwnership(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 7, 19, 7, 0, 0, 0, time.UTC)
	store := memory.New()
	register(t, store, "a", "sha256:a", now)

	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		OperationIDs: []sequencer.OperationID{"a"}, Owner: "replica-1",
		Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}
	if claim.Attempt.Number != 1 || claim.Attempt.Fencing != 1 {
		t.Fatalf("claim = %+v", claim)
	}
	_, err = store.MarkRunning(ctx, sequencer.Ownership{
		OperationID: "a", Version: 1, Owner: "replica-2", Fencing: 1,
	}, now)
	if !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("MarkRunning() error = %v, want ErrStaleOwner", err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		t.Fatalf("MarkRunning(owner) error = %v", err)
	}
	if err := store.Complete(ctx, sequencer.Completion{
		Ownership: claim.Ownership(), State: sequencer.Succeeded,
		At: now.Add(time.Second), Output: sequencer.Output{Summary: "ok"},
	}); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		OperationIDs: []sequencer.OperationID{"a"}, Owner: "replica-2",
		Now: now.Add(time.Minute), LeaseDuration: time.Minute,
	}); !errors.Is(err, sequencer.ErrNoEligibleOperation) {
		t.Fatalf("second ClaimNext() error = %v", err)
	}
	history, err := store.History(ctx, "a", 1, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Succeeded {
		t.Fatalf("History() = %+v, %v", history, err)
	}
}

func TestStoreConcurrentClaimHasSingleWinner(t *testing.T) {
	t.Parallel()

	now := time.Now()
	store := memory.New()
	register(t, store, "a", "sha256:a", now)
	var wait sync.WaitGroup
	winners := make(chan sequencer.Claim, 32)
	for index := range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
				OperationIDs: []sequencer.OperationID{"a"}, Owner: fmt.Sprintf("owner-%d", index),
				Now: now, LeaseDuration: time.Minute,
			})
			if err == nil {
				winners <- claim
			} else if !errors.Is(err, sequencer.ErrNoEligibleOperation) {
				t.Errorf("ClaimNext() error = %v", err)
			}
		}()
	}
	wait.Wait()
	close(winners)
	if got := len(winners); got != 1 {
		t.Fatalf("winners = %d, want 1", got)
	}
}

func TestStoreFailsClosedOnChecksumDriftAndRecoversExpiredClaim(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now()
	store := memory.New()
	register(t, store, "a", "sha256:a", now)
	if err := store.Register(ctx, []sequencer.Registration{{ID: "a", Version: 1, Checksum: "sha256:changed"}}, now); !errors.Is(err, sequencer.ErrChecksumDrift) {
		t.Fatalf("Register drift error = %v", err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "one", Now: now, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpired(ctx, now.Add(2*time.Second)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	next, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "two", Now: now.Add(3 * time.Second), LeaseDuration: time.Second})
	if err != nil || next.Attempt.Number != claim.Attempt.Number+1 || next.Attempt.Fencing <= claim.Attempt.Fencing {
		t.Fatalf("recovered claim = %+v, %v", next, err)
	}
}

func register(t *testing.T, store *memory.Store, id sequencer.OperationID, checksum string, now time.Time) {
	t.Helper()
	if err := store.Register(context.Background(), []sequencer.Registration{{ID: id, Version: 1, Checksum: checksum}}, now); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
}
