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

func TestStaleCompensationCannotCompleteAfterForwardGenerationAdvances(t *testing.T) {
	t.Parallel()

	store := memory.New()
	ctx := context.Background()
	base := time.Unix(5_000, 0)
	forward := sequencer.DependencyRef{ID: "forward-generation", Version: 1, Checksum: "sha256:forward-generation"}
	compensation := sequencer.Registration{
		ID: "compensate-generation", Version: 1, Checksum: "sha256:compensate-generation",
		DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
	}
	if err := store.Register(ctx, []sequencer.Registration{
		{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum},
		compensation,
	}, base); err != nil {
		t.Fatal(err)
	}
	firstForward, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
		Owner:      "forward-generation-one", Now: base, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, firstForward.Ownership(), base); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: firstForward.Ownership(), State: sequencer.Succeeded, At: base.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	staleCompensation, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
		Owner:      "compensator-generation-one", Now: base.Add(2 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, staleCompensation.Ownership(), base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = store.Reset(ctx, sequencer.ResetRequest{
		OperationID: forward.ID, Version: forward.Version, At: base.Add(3 * time.Second),
		Actor: "operator", Reason: "new forward generation",
	}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset() during compensation error = %v, want reset forbidden", err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: staleCompensation.Ownership(), State: sequencer.Succeeded, At: base.Add(3 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err = store.Reset(ctx, sequencer.ResetRequest{
		OperationID: compensation.ID, Version: compensation.Version, At: base.Add(4 * time.Second),
		Actor: "operator", Reason: "replay same forward generation",
	}); err != nil {
		t.Fatalf("same-generation compensation reset error = %v", err)
	}
	sameGenerationCompensation, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
		Owner:      "compensator-generation-one-replay", Now: base.Add(5 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, sameGenerationCompensation.Ownership(), base.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: sameGenerationCompensation.Ownership(), State: sequencer.Succeeded, At: base.Add(6 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err = store.Reset(ctx, sequencer.ResetRequest{
		OperationID: forward.ID, Version: forward.Version, At: base.Add(7 * time.Second),
		Actor: "operator", Reason: "new forward generation",
	}); err != nil {
		t.Fatalf("Reset() after compensation error = %v", err)
	}
	if err = store.Reset(ctx, sequencer.ResetRequest{
		OperationID: compensation.ID, Version: compensation.Version, At: base.Add(8 * time.Second),
		Actor: "operator", Reason: "replay while forward is eligible",
	}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("compensation reset during forward reset error = %v, want reset forbidden", err)
	}
	secondForward, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
		Owner:      "forward-generation-two", Now: base.Add(9 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, secondForward.Ownership(), base.Add(9*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: secondForward.Ownership(), State: sequencer.Succeeded, At: base.Add(10 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: sameGenerationCompensation.Ownership(), State: sequencer.Succeeded, At: base.Add(11 * time.Second),
	}); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("stale compensation completion error = %v, want stale owner", err)
	}
	if err = store.Reset(ctx, sequencer.ResetRequest{
		OperationID: compensation.ID, Version: compensation.Version, At: base.Add(12 * time.Second),
		Actor: "operator", Reason: "replay stale compensation",
	}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("stale compensation reset error = %v, want reset forbidden", err)
	}
}

func TestForwardResetWaitsForRecoveredCompensation(t *testing.T) {
	t.Parallel()

	store := memory.New()
	ctx := context.Background()
	base := time.Unix(6_000, 0)
	forward := sequencer.DependencyRef{ID: "recovered-forward", Version: 1, Checksum: "sha256:recovered-forward"}
	compensation := sequencer.Registration{
		ID: "recovered-compensation", Version: 1, Checksum: "sha256:recovered-compensation",
		DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
		UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent,
	}
	if err := store.Register(ctx, []sequencer.Registration{
		{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}, compensation,
	}, base); err != nil {
		t.Fatal(err)
	}
	forwardClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
		Owner:      "recovered-forward-owner", Now: base, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, forwardClaim.Ownership(), base); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: forwardClaim.Ownership(), State: sequencer.Succeeded, At: base.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	compensationClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
		Owner:      "recovered-compensation-owner", Now: base.Add(2 * time.Second), LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, compensationClaim.Ownership(), base.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpired(ctx, base.Add(4*time.Second)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	reset := sequencer.ResetRequest{
		OperationID: forward.ID, Version: forward.Version, At: base.Add(5 * time.Second),
		Actor: "operator", Reason: "new forward generation",
	}
	if err = store.Reset(ctx, reset); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset() during recovered compensation error = %v, want reset forbidden", err)
	}
	takeover, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
		Owner:      "recovered-compensation-takeover", Now: base.Add(6 * time.Second), LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, takeover.Ownership(), base.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: takeover.Ownership(), State: sequencer.Failed, At: base.Add(7 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	reset.At = base.Add(8 * time.Second)
	if err = store.Reset(ctx, reset); err != nil {
		t.Fatalf("Reset() after terminal compensation error = %v", err)
	}
}

func TestForwardResetFencesEveryActiveCompensationState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		replayIdempotent bool
		activate         func(*memory.Store, sequencer.Claim, time.Time) error
	}{
		{name: "claimed", activate: func(_ *memory.Store, _ sequencer.Claim, _ time.Time) error { return nil }},
		{name: "running", activate: func(store *memory.Store, claim sequencer.Claim, base time.Time) error {
			_, err := store.MarkRunning(context.Background(), claim.Ownership(), base.Add(3*time.Second))
			return err
		}},
		{name: "retryable", activate: func(store *memory.Store, claim sequencer.Claim, base time.Time) error {
			if _, err := store.MarkRunning(context.Background(), claim.Ownership(), base.Add(3*time.Second)); err != nil {
				return err
			}
			return store.Complete(context.Background(), sequencer.Completion{
				Ownership: claim.Ownership(), State: sequencer.Retryable,
				At: base.Add(4 * time.Second), EligibleAt: base.Add(time.Hour), RetryException: true,
			})
		}},
		{name: "deferred", activate: func(store *memory.Store, claim sequencer.Claim, base time.Time) error {
			if _, err := store.MarkRunning(context.Background(), claim.Ownership(), base.Add(3*time.Second)); err != nil {
				return err
			}
			return store.Complete(context.Background(), sequencer.Completion{
				Ownership: claim.Ownership(), State: sequencer.Deferred,
				At: base.Add(4 * time.Second), EligibleAt: base.Add(time.Hour),
			})
		}},
		{name: "indeterminate", activate: func(store *memory.Store, _ sequencer.Claim, base time.Time) error {
			_, err := store.RecoverExpired(context.Background(), base.Add(5*time.Second))
			return err
		}},
		{name: "replay eligible", replayIdempotent: true, activate: func(store *memory.Store, _ sequencer.Claim, base time.Time) error {
			_, err := store.RecoverExpired(context.Background(), base.Add(5*time.Second))
			return err
		}},
	}
	for index, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			store := memory.New()
			ctx := context.Background()
			base := time.Unix(7_000+int64(index*100), 0)
			forward := sequencer.DependencyRef{
				ID: sequencer.OperationID(fmt.Sprintf("state-forward-%d", index)), Version: 1,
				Checksum: fmt.Sprintf("sha256:state-forward-%d", index),
			}
			compensation := sequencer.Registration{
				ID: sequencer.OperationID(fmt.Sprintf("state-compensation-%d", index)), Version: 1,
				Checksum:       fmt.Sprintf("sha256:state-compensation-%d", index),
				DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
			}
			if test.replayIdempotent {
				compensation.UnknownOutcome = sequencer.UnknownOutcomeReplayIdempotent
			}
			if err := store.Register(ctx, []sequencer.Registration{
				{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}, compensation,
			}, base); err != nil {
				t.Fatal(err)
			}
			forwardClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
				Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
				Owner:      "state-forward-owner", Now: base, LeaseDuration: time.Minute,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.MarkRunning(ctx, forwardClaim.Ownership(), base); err != nil {
				t.Fatal(err)
			}
			if err = store.Complete(ctx, sequencer.Completion{
				Ownership: forwardClaim.Ownership(), State: sequencer.Succeeded, At: base.Add(time.Second),
			}); err != nil {
				t.Fatal(err)
			}
			leaseDuration := time.Minute
			if test.name == "indeterminate" || test.name == "replay eligible" {
				leaseDuration = time.Second
			}
			claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
				Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
				Owner:      "state-compensation-owner", Now: base.Add(2 * time.Second), LeaseDuration: leaseDuration,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = test.activate(store, claim, base); err != nil {
				t.Fatal(err)
			}
			if err = store.Reset(ctx, sequencer.ResetRequest{
				OperationID: forward.ID, Version: forward.Version, At: base.Add(10 * time.Second),
				Actor: "operator", Reason: "state fence",
			}); !errors.Is(err, sequencer.ErrResetForbidden) {
				t.Fatalf("Reset() error = %v, want reset forbidden", err)
			}
		})
	}
}

func TestForwardResetCountsEveryActiveCompensation(t *testing.T) {
	t.Parallel()

	store := memory.New()
	ctx := context.Background()
	base := time.Unix(8_000, 0)
	forward := sequencer.DependencyRef{ID: "counted-forward", Version: 1, Checksum: "sha256:counted-forward"}
	compensations := []sequencer.Registration{
		{ID: "counted-compensation-a", Version: 1, Checksum: "sha256:counted-compensation-a", DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward},
		{ID: "counted-compensation-b", Version: 1, Checksum: "sha256:counted-compensation-b", DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward},
	}
	registrations := append([]sequencer.Registration{{ID: forward.ID, Version: 1, Checksum: forward.Checksum}}, compensations...)
	if err := store.Register(ctx, registrations, base); err != nil {
		t.Fatal(err)
	}
	forwardClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: 1, Checksum: forward.Checksum}},
		Owner:      "counted-forward-owner", Now: base, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, forwardClaim.Ownership(), base); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{Ownership: forwardClaim.Ownership(), State: sequencer.Succeeded, At: base.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	claims := make([]sequencer.Claim, len(compensations))
	for index, compensation := range compensations {
		claims[index], err = store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: 1, Checksum: compensation.Checksum}},
			Owner:      fmt.Sprintf("counted-owner-%d", index), Now: base.Add(2 * time.Second), LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(ctx, claims[index].Ownership(), base.Add(2*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	reset := sequencer.ResetRequest{OperationID: forward.ID, Version: 1, Actor: "operator", Reason: "count fence"}
	for index, claim := range claims {
		if err = store.Complete(ctx, sequencer.Completion{
			Ownership: claim.Ownership(), State: sequencer.Succeeded, At: base.Add(time.Duration(3+index) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		reset.At = base.Add(time.Duration(5+index) * time.Second)
		if err = store.Reset(ctx, reset); index == 0 && !errors.Is(err, sequencer.ErrResetForbidden) {
			t.Fatalf("Reset() after first compensation error = %v, want reset forbidden", err)
		} else if index == 1 && err != nil {
			t.Fatalf("Reset() after all compensations error = %v", err)
		}
	}
}
