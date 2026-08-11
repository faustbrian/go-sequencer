package sequencer_test

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

func TestLeaseRenewalCompletionRecoveryAndTakeoverRaceKeepsOneOwner(t *testing.T) {
	t.Parallel()

	for iteration := range 64 {
		store := memory.New()
		id := sequencer.OperationID(fmt.Sprintf("race.lifecycle-%d", iteration))
		base := time.Unix(1_000+int64(iteration), 0)
		registration := sequencer.Registration{ID: id, Version: 1, Checksum: "sha256:race", UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent}
		if err := store.Register(context.Background(), []sequencer.Registration{registration}, base); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: registration.Checksum}}, Owner: "old", Now: base, LeaseDuration: 10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(context.Background(), claim.Ownership(), base); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		results := make(chan error, 3)
		var group sync.WaitGroup
		group.Add(3)
		go func() {
			defer group.Done()
			<-start
			_, renewErr := store.RenewLease(context.Background(), claim.Ownership(), base.Add(5*time.Second), 10*time.Second)
			results <- renewErr
		}()
		go func() {
			defer group.Done()
			<-start
			results <- store.Complete(context.Background(), sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: base.Add(5 * time.Second)})
		}()
		go func() {
			defer group.Done()
			<-start
			_, recoveryErr := store.RecoverExpired(context.Background(), base.Add(20*time.Second))
			results <- recoveryErr
		}()
		close(start)
		group.Wait()
		close(results)
		for result := range results {
			if result != nil && !errors.Is(result, sequencer.ErrStaleOwner) && !errors.Is(result, sequencer.ErrInvalidTransition) {
				t.Fatalf("race result = %v", result)
			}
		}

		record, err := store.Snapshot(context.Background(), id, 1)
		if err != nil {
			t.Fatal(err)
		}
		if record.State == sequencer.Eligible {
			newClaim, claimErr := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
				Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: registration.Checksum}}, Owner: "new", Now: base.Add(21 * time.Second), LeaseDuration: time.Minute,
			})
			if claimErr != nil {
				t.Fatal(claimErr)
			}
			if newClaim.Attempt.Fencing <= claim.Attempt.Fencing {
				t.Fatalf("takeover fence = %d, old = %d", newClaim.Attempt.Fencing, claim.Attempt.Fencing)
			}
		}
		if err := store.Complete(context.Background(), sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: base.Add(22 * time.Second)}); !errors.Is(err, sequencer.ErrStaleOwner) && !errors.Is(err, sequencer.ErrInvalidTransition) {
			t.Fatalf("stale completion error = %v", err)
		}
	}
}

func TestResetCompletionRaceNeverLeavesOldOwnershipWritable(t *testing.T) {
	t.Parallel()

	for iteration := range 64 {
		store := memory.New()
		id := sequencer.OperationID(fmt.Sprintf("race.reset-%d", iteration))
		base := time.Unix(2_000+int64(iteration), 0)
		registration := sequencer.Registration{ID: id, Version: 1, Checksum: "sha256:reset"}
		if err := store.Register(context.Background(), []sequencer.Registration{registration}, base); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: registration.Checksum}}, Owner: "old", Now: base, LeaseDuration: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(context.Background(), claim.Ownership(), base); err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			results <- store.Complete(context.Background(), sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: base.Add(time.Second)})
		}()
		go func() {
			<-start
			results <- store.Reset(context.Background(), sequencer.ResetRequest{OperationID: id, Version: 1, Actor: "operator", Reason: "race audit", At: base.Add(2 * time.Second)})
		}()
		close(start)
		for range 2 {
			result := <-results
			if result != nil && !errors.Is(result, sequencer.ErrResetForbidden) {
				t.Fatalf("race result = %v", result)
			}
		}
		if err := store.Complete(context.Background(), sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: base.Add(3 * time.Second)}); err == nil {
			t.Fatal("old ownership completed after completion/reset race")
		}
	}
}

func TestMixedRegistryRegistrationAndClaimRaceKeepsExactGeneration(t *testing.T) {
	t.Parallel()

	for iteration := range 64 {
		store := memory.New()
		id := sequencer.OperationID(fmt.Sprintf("race.registry-%d", iteration))
		base := time.Unix(3_000+int64(iteration), 0)
		v1 := sequencer.Registration{ID: id, Version: 1, Checksum: "sha256:v1"}
		if err := store.Register(context.Background(), []sequencer.Registration{v1}, base); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		registered := make(chan error, 1)
		claimed := make(chan sequencer.Claim, 1)
		claimErrors := make(chan error, 1)
		go func() {
			<-start
			registered <- store.Register(context.Background(), []sequencer.Registration{{ID: id, Version: 2, Checksum: "sha256:v2"}}, base)
		}()
		go func() {
			<-start
			claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: v1.Checksum}}, Owner: "old-binary", Now: base, LeaseDuration: time.Minute})
			claimed <- claim
			claimErrors <- err
		}()
		close(start)
		if err := <-registered; err != nil {
			t.Fatal(err)
		}
		claim, err := <-claimed, <-claimErrors
		if err != nil || claim.Attempt.Version != 1 {
			t.Fatalf("claim = %+v, %v", claim, err)
		}
	}
}

func TestStaleCompensationOwnerCannotCompleteAfterTakeover(t *testing.T) {
	t.Parallel()

	store := memory.New()
	base := time.Unix(4_000, 0)
	forward := sequencer.DependencyRef{ID: "forward", Version: 1, Checksum: "sha256:forward"}
	compensation := sequencer.Registration{ID: "compensate", Version: 1, Checksum: "sha256:compensate", DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward, UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent}
	if err := store.Register(context.Background(), []sequencer.Registration{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}, compensation}, base); err != nil {
		t.Fatal(err)
	}
	forwardClaim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: 1, Checksum: forward.Checksum}}, Owner: "forward-owner", Now: base, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), forwardClaim.Ownership(), base); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), sequencer.Completion{Ownership: forwardClaim.Ownership(), State: sequencer.Succeeded, At: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	old, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: 1, Checksum: compensation.Checksum}}, Owner: "old-compensator", Now: base.Add(2 * time.Second), LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), old.Ownership(), base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecoverExpired(context.Background(), base.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	newClaim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: 1, Checksum: compensation.Checksum}}, Owner: "new-compensator", Now: base.Add(5 * time.Second), LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), sequencer.Completion{Ownership: old.Ownership(), State: sequencer.Succeeded, At: base.Add(6 * time.Second)}); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("stale compensation completion error = %v", err)
	}
	if newClaim.Attempt.Fencing <= old.Attempt.Fencing {
		t.Fatalf("new compensation fence = %d, old = %d", newClaim.Attempt.Fencing, old.Attempt.Fencing)
	}
}
