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

func TestStoreReplicaRaceAuthorizesExactlyOneClaim(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC)
	store := memory.New()
	registration := sequencer.Registration{ID: "race.claim", Version: 1, Checksum: "sha256:race"}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}

	const replicas = 32
	start := make(chan struct{})
	results := make(chan claimResult, replicas)
	var workers sync.WaitGroup
	for replica := 0; replica < replicas; replica++ {
		workers.Add(1)
		go func(replica int) {
			defer workers.Done()
			<-start
			claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
				Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: 1, Checksum: registration.Checksum}},
				Owner:      fmt.Sprintf("replica-%d", replica), Now: now, LeaseDuration: time.Minute,
			})
			results <- claimResult{claim: claim, err: err}
		}(replica)
	}
	close(start)
	workers.Wait()
	close(results)

	winners := 0
	var winner sequencer.Claim
	for result := range results {
		if result.err == nil {
			winners++
			winner = result.claim
			continue
		}
		if !errors.Is(result.err, sequencer.ErrNoEligibleOperation) {
			t.Fatalf("losing ClaimNext() error = %v", result.err)
		}
	}
	if winners != 1 || winner.Attempt.Number != 1 || winner.Attempt.Fencing != 1 {
		t.Fatalf("winners = %d, claim = %+v", winners, winner)
	}
	history, err := store.History(ctx, registration.ID, registration.Version, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Claimed {
		t.Fatalf("History() = %+v, %v", history, err)
	}
}

func TestStoreCrashFailoverFencesEveryStaleOwnerTransition(t *testing.T) {
	t.Parallel()

	for _, crashState := range []sequencer.State{sequencer.Claimed, sequencer.Running} {
		t.Run(crashState.String(), func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 8, 10, 17, 0, 0, 0, time.UTC)
			store := memory.New()
			registration := sequencer.Registration{
				ID: "failover." + sequencer.OperationID(crashState.String()), Version: 1, Checksum: "sha256:failover",
				UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent,
			}
			if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
				t.Fatal(err)
			}
			oldClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "pod-old", Now: now, LeaseDuration: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if crashState == sequencer.Running {
				if _, err := store.MarkRunning(ctx, oldClaim.Ownership(), now); err != nil {
					t.Fatal(err)
				}
			}
			if recovered, err := store.RecoverExpired(ctx, now.Add(time.Second)); err != nil || recovered != 1 {
				t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
			}
			newClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "pod-new", Now: now.Add(time.Second), LeaseDuration: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			if newClaim.Attempt.Number != 2 || newClaim.Attempt.Fencing <= oldClaim.Attempt.Fencing {
				t.Fatalf("takeover claim = %+v, old = %+v", newClaim, oldClaim)
			}
			if _, err := store.MarkRunning(ctx, oldClaim.Ownership(), now.Add(2*time.Second)); !errors.Is(err, sequencer.ErrStaleOwner) {
				t.Fatalf("stale MarkRunning() error = %v", err)
			}
			if err := store.Complete(ctx, sequencer.Completion{Ownership: oldClaim.Ownership(), State: sequencer.Succeeded, At: now.Add(2 * time.Second)}); !errors.Is(err, sequencer.ErrStaleOwner) {
				t.Fatalf("stale Complete() error = %v", err)
			}
			if _, err := store.MarkRunning(ctx, newClaim.Ownership(), now.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := store.Complete(ctx, sequencer.Completion{Ownership: newClaim.Ownership(), State: sequencer.Succeeded, At: now.Add(3 * time.Second)}); err != nil {
				t.Fatal(err)
			}
			record, _ := store.Snapshot(ctx, registration.ID, registration.Version)
			history, _ := store.History(ctx, registration.ID, registration.Version, 10)
			audit, _ := store.Audit(ctx, registration.ID, registration.Version, 20)
			if record.State != sequencer.Succeeded || record.Owner != "" || len(history) != 2 ||
				history[0].State != sequencer.Indeterminate || history[1].State != sequencer.Succeeded {
				t.Fatalf("record = %+v, history = %+v", record, history)
			}
			for _, event := range audit {
				if event.Owner == "pod-old" && event.Attempt == 2 {
					t.Fatalf("stale owner authorized takeover attempt: %+v", event)
				}
			}
		})
	}
}

type claimResult struct {
	claim sequencer.Claim
	err   error
}
