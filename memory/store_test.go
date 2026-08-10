package memory_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	audit, err := store.Audit(ctx, "a", 1, 10)
	if err != nil || audit[len(audit)-1].Owner != claim.Attempt.Owner || audit[len(audit)-1].Fencing != claim.Attempt.Fencing {
		t.Fatalf("completion audit = %+v, %v", audit, err)
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
	audit, err := store.Audit(ctx, "a", 1, 10)
	if err != nil || audit[len(audit)-2].Owner != claim.Attempt.Owner || audit[len(audit)-1].Owner != claim.Attempt.Owner {
		t.Fatalf("recovery audit = %+v, %v", audit, err)
	}
	next, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "two", Now: now.Add(3 * time.Second), LeaseDuration: time.Second})
	if err != nil || next.Attempt.Number != claim.Attempt.Number+1 || next.Attempt.Fencing <= claim.Attempt.Fencing {
		t.Fatalf("recovered claim = %+v, %v", next, err)
	}
}

func TestStoreRenewsOnlyTheCurrentOwnedLease(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	store := memory.New()
	register(t, store, "renew", "sha256:renew", now)
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		OperationIDs: []sequencer.OperationID{"renew"}, Owner: "pod-1",
		Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantUntil := now.Add(90 * time.Second)
	gotUntil, err := store.RenewLease(ctx, claim.Ownership(), now.Add(30*time.Second), time.Minute)
	if err != nil || !gotUntil.Equal(wantUntil) {
		t.Fatalf("RenewLease() = %s, %v; want %s", gotUntil, err, wantUntil)
	}
	record, err := store.Snapshot(ctx, "renew", 1)
	if err != nil || !record.LeaseExpiresAt.Equal(wantUntil) {
		t.Fatalf("Snapshot() = %+v, %v", record, err)
	}
	stale := claim.Ownership()
	stale.Fencing--
	if _, err := store.RenewLease(ctx, stale, now, time.Minute); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("stale RenewLease() error = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.RenewLease(canceled, claim.Ownership(), now, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled RenewLease() error = %v", err)
	}
	if _, err := store.RenewLease(ctx, claim.Ownership(), time.Time{}, time.Minute); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("zero-time RenewLease() error = %v", err)
	}
	if _, err := store.RenewLease(ctx, claim.Ownership(), now, 0); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("zero-duration RenewLease() error = %v", err)
	}
	if _, err := store.RenewLease(ctx, claim.Ownership(), now.Add(time.Minute), 0); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("non-regressing zero-duration RenewLease() error = %v", err)
	}
	if _, err := store.RenewLease(ctx, claim.Ownership(), now.Add(time.Minute), -time.Second); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("negative-duration RenewLease() error = %v", err)
	}
	if _, err := store.RenewLease(ctx, claim.Ownership(), now.Add(-time.Second), time.Minute); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("regressing RenewLease() error = %v", err)
	}
	unchanged, err := store.RenewLease(ctx, claim.Ownership(), now.Add(time.Minute), time.Second)
	if err != nil || !unchanged.Equal(wantUntil) {
		t.Fatalf("non-extending RenewLease() = %s, %v", unchanged, err)
	}
}

func TestStoreRejectsOwnershipWritesAtLeaseExpiry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	newClaim := func(id sequencer.OperationID) (*memory.Store, sequencer.Claim) {
		store := memory.New()
		register(t, store, id, "sha256:"+string(id), now)
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{id}, Owner: "expired-owner",
			Now: now, LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		return store, claim
	}

	markStore, markClaim := newClaim("expired-mark")
	if _, err := markStore.MarkRunning(ctx, markClaim.Ownership(), now.Add(time.Minute)); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("expired MarkRunning() error = %v", err)
	}
	renewStore, renewClaim := newClaim("expired-renew")
	if _, err := renewStore.RenewLease(ctx, renewClaim.Ownership(), now.Add(time.Minute), time.Minute); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("expired RenewLease() error = %v", err)
	}
	completeStore, completeClaim := newClaim("expired-complete")
	if _, err := completeStore.MarkRunning(ctx, completeClaim.Ownership(), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := completeStore.Complete(ctx, sequencer.Completion{
		Ownership: completeClaim.Ownership(), State: sequencer.Succeeded, At: now.Add(time.Minute),
	}); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("expired Complete() error = %v", err)
	}
}

func TestStoreClaimsOnlyTheLocalBinaryOperationVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 11, 0, 0, 0, time.UTC)
	store := memory.New()
	if err := store.Register(ctx, []sequencer.Registration{
		{ID: "rolling", Version: 1, Checksum: "sha256:v1"},
		{ID: "rolling", Version: 2, Checksum: "sha256:v2"},
	}, now); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "rolling", Version: 1, Checksum: "sha256:v1"}},
		Owner:      "old-binary", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Attempt.Version != 1 {
		t.Fatalf("claimed version = %d, want local version 1", claim.Attempt.Version)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "rolling", Version: 2, Checksum: "sha256:changed"}},
		Owner:      "drifted-binary", Now: now, LeaseDuration: time.Minute,
	}); !errors.Is(err, sequencer.ErrChecksumDrift) {
		t.Fatalf("checksum-mismatched claim error = %v", err)
	}
}

func TestStoreValidatesRegistrationAndContinuesAfterExistingIdentity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	invalid := []sequencer.Registration{
		{Version: 1, Checksum: "sha256:value"},
		{ID: "value", Checksum: "sha256:value"},
		{ID: "value", Version: 1},
	}
	for _, registration := range invalid {
		store := memory.New()
		if err := store.Register(context.Background(), []sequencer.Registration{registration}, now); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Fatalf("Register(%+v) error = %v", registration, err)
		}
	}

	store := memory.New()
	register(t, store, "existing", "sha256:existing", now)
	if err := store.Register(context.Background(), []sequencer.Registration{
		{ID: "existing", Version: 1, Checksum: "sha256:existing"},
		{ID: "new", Version: 1, Checksum: "sha256:new"},
	}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(context.Background(), "new", 1); err != nil {
		t.Fatalf("registration after existing identity was not stored: %v", err)
	}
}

func TestStoreRegistrationIsAtomicAndRejectsDependencyDrift(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC)
	store := memory.New()
	if err := store.Register(ctx, []sequencer.Registration{
		{ID: "dependency-a", Version: 1, Checksum: "sha256:a"},
		{ID: "dependency-b", Version: 1, Checksum: "sha256:b"},
		{ID: "existing", Version: 1, Checksum: "sha256:existing", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency-a", Version: 1, Checksum: "sha256:a"}}},
	}, now); err != nil {
		t.Fatal(err)
	}

	err := store.Register(ctx, []sequencer.Registration{
		{ID: "must-not-persist", Version: 1, Checksum: "sha256:new"},
		{ID: "existing", Version: 1, Checksum: "sha256:existing", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency-b", Version: 1, Checksum: "sha256:b"}}},
	}, now.Add(time.Minute))
	if !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("Register(dependency drift) error = %v", err)
	}
	if _, err := store.Snapshot(ctx, "must-not-persist", 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("partial registration persisted: %v", err)
	}

	if err := store.Register(ctx, []sequencer.Registration{
		{ID: "existing", Version: 1, Checksum: "sha256:existing", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency-a", Version: 1, Checksum: "sha256:a"}}},
	}, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("Register(same dependencies) error = %v", err)
	}
	for _, reference := range []sequencer.DependencyRef{
		{ID: "dependency-a", Version: 2, Checksum: "sha256:a"},
		{ID: "dependency-a", Version: 1, Checksum: "sha256:changed"},
	} {
		err := store.Register(ctx, []sequencer.Registration{{
			ID: "existing", Version: 1, Checksum: "sha256:existing",
			DependencyRefs: []sequencer.DependencyRef{reference},
		}}, now.Add(3*time.Minute))
		if !errors.Is(err, sequencer.ErrDefinitionDrift) {
			t.Fatalf("Register(exact dependency drift %+v) error = %v", reference, err)
		}
	}
}

func TestStoreCanonicalizesExactDependencyOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 9, 30, 0, 0, time.UTC)
	store := memory.New()
	references := []sequencer.DependencyRef{
		{ID: "b", Version: 2, Checksum: "sha256:b"},
		{ID: "a", Version: 1, Checksum: "sha256:a"},
	}
	registration := sequencer.Registration{ID: "dependent", Version: 1, Checksum: "sha256:dependent", DependencyRefs: references}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	slices.Reverse(references)
	registration.DependencyRefs = references
	if err := store.Register(ctx, []sequencer.Registration{registration}, now.Add(time.Minute)); err != nil {
		t.Fatalf("Register(equivalent order) error = %v", err)
	}
	record, err := store.Snapshot(ctx, registration.ID, registration.Version)
	if err != nil || len(record.DependencyRefs) != 2 || record.DependencyRefs[0].ID != "a" || record.DependencyRefs[1].ID != "b" {
		t.Fatalf("Snapshot canonical refs = %+v, %v", record.DependencyRefs, err)
	}
}

func TestStorePinsDependencyEligibilityToExactIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	store := memory.New()
	registration := sequencer.Registration{
		ID: "dependent", Version: 1, Checksum: "sha256:dependent",
		DependencyRefs: []sequencer.DependencyRef{{ID: "dependency", Version: 1, Checksum: "sha256:dependency-v1"}},
	}
	if err := store.Register(ctx, []sequencer.Registration{
		{ID: "dependency", Version: 1, Checksum: "sha256:dependency-v1"},
		{ID: "dependency", Version: 2, Checksum: "sha256:dependency-v2"},
		registration,
	}, now); err != nil {
		t.Fatal(err)
	}

	complete := func(version uint, checksum string) {
		t.Helper()
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: "dependency", Version: version, Checksum: checksum}},
			Owner:      "owner", Now: now, LeaseDuration: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			t.Fatal(err)
		}
		if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: now}); err != nil {
			t.Fatal(err)
		}
	}
	complete(2, "sha256:dependency-v2")
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: registration.Version, Checksum: registration.Checksum}},
		Owner:      "owner", Now: now, LeaseDuration: time.Second,
	}); !errors.Is(err, sequencer.ErrNoEligibleOperation) {
		t.Fatalf("dependent claimed after only newer dependency succeeded: %v", err)
	}
	complete(1, "sha256:dependency-v1")
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: registration.Version, Checksum: registration.Checksum}},
		Owner:      "owner", Now: now, LeaseDuration: time.Second,
	}); err != nil {
		t.Fatalf("dependent not claimed after exact dependency succeeded: %v", err)
	}

	record, err := store.Snapshot(ctx, registration.ID, registration.Version)
	if err != nil {
		t.Fatal(err)
	}
	record.DependencyRefs[0].Checksum = "mutated"
	again, err := store.Snapshot(ctx, registration.ID, registration.Version)
	if err != nil || again.DependencyRefs[0].Checksum != "sha256:dependency-v1" {
		t.Fatalf("Snapshot dependency refs are mutable: %+v, %v", again.DependencyRefs, err)
	}
}

func TestStoreValidatesClaimFieldsIndependentlyAndSkipsIneligibleCandidates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 13, 0, 0, 0, time.UTC)
	store := memory.New()
	register(t, store, "ready", "sha256:ready", now)
	for _, request := range []sequencer.ClaimRequest{
		{OperationIDs: []sequencer.OperationID{"ready"}, Now: now, LeaseDuration: time.Second},
		{OperationIDs: []sequencer.OperationID{"ready"}, Owner: "owner", Now: now},
		{OperationIDs: []sequencer.OperationID{"ready"}, Owner: "owner", LeaseDuration: time.Second},
		{OperationIDs: []sequencer.OperationID{"ready"}, Owner: "owner", Now: now, LeaseDuration: -time.Second},
	} {
		if _, err := store.ClaimNext(ctx, request); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Fatalf("ClaimNext(%+v) error = %v", request, err)
		}
	}

	if err := store.Register(ctx, []sequencer.Registration{
		{ID: "blocked", Version: 1, Checksum: "sha256:blocked", DependencyRefs: []sequencer.DependencyRef{{ID: "missing", Version: 1, Checksum: "sha256:missing"}}},
	}, now); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		OperationIDs: []sequencer.OperationID{"blocked", "ready"},
		Owner:        "owner", Now: now, LeaseDuration: time.Second,
	})
	if err != nil || claim.Attempt.OperationID != "ready" {
		t.Fatalf("ClaimNext() = %+v, %v; want ready after blocked", claim, err)
	}
}

func TestStorePersistsRetryAndDeferredEligibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC)
	for _, state := range []sequencer.State{sequencer.Retryable, sequencer.Deferred} {
		store := memory.New()
		register(t, store, sequencer.OperationID(state), "sha256:state", now)
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{sequencer.OperationID(state)},
			Owner:        "owner", Now: now, LeaseDuration: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			t.Fatal(err)
		}
		eligibleAt := now.Add(time.Minute)
		if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: state, At: now, EligibleAt: eligibleAt}); err != nil {
			t.Fatal(err)
		}
		record, err := store.Snapshot(ctx, sequencer.OperationID(state), 1)
		if err != nil || !record.EligibleAt.Equal(eligibleAt) {
			t.Fatalf("Snapshot(%s) = %+v, %v", state, record, err)
		}
	}
}

func TestStoreRecoverySkipsUnexpiredAndTerminalEntries(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 15, 0, 0, 0, time.UTC)
	store := memory.New()
	for _, id := range []sequencer.OperationID{"terminal", "unexpired", "expired"} {
		register(t, store, id, "sha256:"+string(id), now)
	}
	terminal, _ := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"terminal"}, Owner: "owner", Now: now, LeaseDuration: time.Minute})
	if _, err := store.MarkRunning(ctx, terminal.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: terminal.Ownership(), State: sequencer.Succeeded, At: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"unexpired"}, Owner: "owner", Now: now, LeaseDuration: 2 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"expired"}, Owner: "owner", Now: now, LeaseDuration: time.Second}); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpired(ctx, now.Add(time.Minute)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v; want only expired", recovered, err)
	}
}

func TestStoreHistoryAuditResetAndDependenciesEnforceExactBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 16, 0, 0, 0, time.UTC)
	store := memory.New()
	register(t, store, "history", "sha256:history", now)
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"history"}, Owner: "owner", Now: now, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Retryable, At: now, EligibleAt: now}); err != nil {
		t.Fatal(err)
	}
	claim, err = store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"history"}, Owner: "owner", Now: now, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Failed, At: now}); err != nil {
		t.Fatal(err)
	}
	history, err := store.History(ctx, "history", 1, 1)
	if err != nil || len(history) != 1 || history[0].Number != 2 {
		t.Fatalf("History(limit 1) = %+v, %v", history, err)
	}
	audit, err := store.Audit(ctx, "history", 1, 1)
	if err != nil || len(audit) != 1 || audit[0].To != sequencer.Failed {
		t.Fatalf("Audit(limit 1) = %+v, %v", audit, err)
	}
	for _, limit := range []int{0, sequencer.DefaultMaxHistory + 1} {
		if _, err := store.History(ctx, "history", 1, limit); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Fatalf("History(limit %d) error = %v", limit, err)
		}
		if _, err := store.Audit(ctx, "history", 1, limit); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Fatalf("Audit(limit %d) error = %v", limit, err)
		}
	}
	if _, err := store.History(ctx, "history", 1, sequencer.DefaultMaxHistory); err != nil {
		t.Fatalf("History(exact maximum) error = %v", err)
	}
	if _, err := store.Audit(ctx, "history", 1, sequencer.DefaultMaxHistory); err != nil {
		t.Fatalf("Audit(exact maximum) error = %v", err)
	}

	for _, request := range []sequencer.ResetRequest{
		{Reason: "reason", At: now},
		{Actor: "actor", At: now},
		{Actor: "actor", Reason: "reason"},
	} {
		if err := store.Reset(ctx, request); !errors.Is(err, sequencer.ErrResetForbidden) {
			t.Fatalf("Reset(%+v) error = %v", request, err)
		}
	}

	if err := store.Register(ctx, []sequencer.Registration{
		{ID: "dependency", Version: 1, Checksum: "sha256:dependency-v1"},
		{ID: "dependency", Version: 2, Checksum: "sha256:dependency-v2"},
		{ID: "dependent", Version: 1, Checksum: "sha256:dependent", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency", Version: 1, Checksum: "sha256:dependency-v1"}}},
	}, now); err != nil {
		t.Fatal(err)
	}
	dependency, err := store.ClaimNext(ctx, sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: "dependency", Version: 1}}, Owner: "owner", Now: now, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, dependency.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: dependency.Ownership(), State: sequencer.Skipped, At: now}); err != nil {
		t.Fatal(err)
	}
	dependent, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"dependent"}, Owner: "owner", Now: now, LeaseDuration: time.Second})
	if err != nil || dependent.Attempt.OperationID != "dependent" {
		t.Fatalf("dependent claim = %+v, %v", dependent, err)
	}
}

func register(t *testing.T, store *memory.Store, id sequencer.OperationID, checksum string, now time.Time) {
	t.Helper()
	if err := store.Register(context.Background(), []sequencer.Registration{{ID: id, Version: 1, Checksum: checksum}}, now); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
}
