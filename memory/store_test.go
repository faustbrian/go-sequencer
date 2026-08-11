package memory_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
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
	if claim.Budget.Attempt != 1 || claim.Budget.Exceptions != 0 {
		t.Fatalf("claim budget = %+v", claim.Budget)
	}
	invalidOwnership := claim.Ownership()
	invalidOwnership.Owner = strings.Repeat("o", sequencer.DefaultMaxActorBytes+1)
	if _, err := store.MarkRunning(ctx, invalidOwnership, now); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("MarkRunning(invalid ownership) error = %v", err)
	}
	if _, err := store.RenewLease(ctx, invalidOwnership, now, time.Minute); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("RenewLease(invalid ownership) error = %v", err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: invalidOwnership, State: sequencer.Succeeded, At: now}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Complete(invalid ownership) error = %v", err)
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
	registration := sequencer.Registration{ID: "a", Version: 1, Checksum: "sha256:a", UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	drifted := registration
	drifted.Checksum = "sha256:changed"
	if err := store.Register(ctx, []sequencer.Registration{drifted}, now); !errors.Is(err, sequencer.ErrChecksumDrift) {
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
	mismatchStore, mismatchClaim := newClaim("completion-source-mismatch")
	if err := mismatchStore.Complete(ctx, sequencer.Completion{
		Ownership: mismatchClaim.Ownership(), State: sequencer.Failed, At: now,
	}); !errors.Is(err, sequencer.ErrInvalidTransition) {
		t.Fatalf("Complete(source mismatch) error = %v", err)
	}
	mismatchRecord, err := mismatchStore.Snapshot(ctx, "completion-source-mismatch", 1)
	if err != nil || mismatchRecord.State != sequencer.Claimed {
		t.Fatalf("Snapshot(source mismatch) = %+v, %v", mismatchRecord, err)
	}

	for _, markAt := range []time.Time{{}, now.Add(-time.Nanosecond)} {
		store, claim := newClaim("invalid-mark-time")
		if _, err := store.MarkRunning(ctx, claim.Ownership(), markAt); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Fatalf("MarkRunning(%s) error = %v", markAt, err)
		}
		record, err := store.Snapshot(ctx, "invalid-mark-time", 1)
		if err != nil || record.State != sequencer.Claimed || !record.UpdatedAt.Equal(now) {
			t.Fatalf("Snapshot(invalid mark time) = %+v, %v", record, err)
		}
	}
	for _, completeAt := range []time.Time{{}, now.Add(-time.Nanosecond)} {
		store, claim := newClaim("invalid-complete-time")
		if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			t.Fatal(err)
		}
		if err := store.Complete(ctx, sequencer.Completion{
			Ownership: claim.Ownership(), State: sequencer.Succeeded, At: completeAt,
		}); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Fatalf("Complete(%s) error = %v", completeAt, err)
		}
		record, err := store.Snapshot(ctx, "invalid-complete-time", 1)
		if err != nil || record.State != sequencer.Running || !record.UpdatedAt.Equal(now) {
			t.Fatalf("Snapshot(invalid complete time) = %+v, %v", record, err)
		}
	}
	if _, err := memory.New().RecoverExpired(ctx, time.Time{}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("RecoverExpired(zero time) error = %v", err)
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
		{ID: "Invalid", Version: 1, Checksum: "sha256:value"},
		{ID: "value", Version: 1, Checksum: "sha256:value", Channel: "Invalid Channel"},
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

func TestStoreRejectsEveryInvalidRegistrationBoundaryAtomically(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 12, 30, 0, 0, time.UTC)
	dependency := sequencer.DependencyRef{ID: "dependency", Version: 1, Checksum: "sha256:dependency"}
	invalid := []sequencer.Registration{
		{ID: "legacy", Version: 1, Checksum: "sum", Dependencies: []sequencer.OperationID{"dependency"}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{Version: 1, Checksum: "sum"}}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "Invalid", Version: 1, Checksum: "sum"}}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "operation", Version: 1, Checksum: "sum"}}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency", Checksum: "sum"}}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency", Version: 1}}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{dependency, dependency}},
		{ID: "operation", Version: 1, Checksum: "sum", Compensates: &dependency},
	}
	for _, registration := range invalid {
		if err := memory.New().Register(ctx, []sequencer.Registration{registration}, now); err == nil {
			t.Fatalf("Register(%+v) error = nil", registration)
		}
	}
	if err := memory.New().Register(ctx, nil, time.Time{}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Register(zero time) error = %v", err)
	}
	tooMany := make([]sequencer.DependencyRef, sequencer.DefaultMaxDependencies+1)
	for index := range tooMany {
		tooMany[index] = sequencer.DependencyRef{
			ID: sequencer.OperationID(fmt.Sprintf("dependency-%d", index)), Version: 1, Checksum: "sum",
		}
	}
	if err := memory.New().Register(ctx, []sequencer.Registration{{
		ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: tooMany,
	}}, now); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Register(dependency overflow) error = %v", err)
	}
	for _, registration := range []sequencer.Registration{
		{ID: "operation", Version: 1, Checksum: strings.Repeat("c", 513)},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency", Version: 1, Checksum: strings.Repeat("c", 513)}}},
	} {
		if err := memory.New().Register(ctx, []sequencer.Registration{registration}, now); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Fatalf("Register(checksum overflow) error = %v", err)
		}
	}
	exactDependencyChecksum := strings.Repeat("c", sequencer.DefaultMaxChecksumBytes)
	exactDependencyRefs := make([]sequencer.DependencyRef, sequencer.DefaultMaxDependencies)
	for index := range exactDependencyRefs {
		exactDependencyRefs[index] = sequencer.DependencyRef{
			ID: sequencer.OperationID(fmt.Sprintf("exact-dependency-%d", index)), Version: 1, Checksum: "sum",
		}
	}
	exactDependencyRefs[0].Checksum = exactDependencyChecksum
	if err := memory.New().Register(ctx, []sequencer.Registration{{
		ID: "operation", Version: 1, Checksum: "sum",
		DependencyRefs: exactDependencyRefs,
	}}, now); err != nil {
		t.Fatalf("Register(exact dependency bounds) error = %v", err)
	}

	compensating := sequencer.Registration{
		ID: "compensating", Version: 1, Checksum: "sum",
		DependencyRefs: []sequencer.DependencyRef{dependency}, Compensates: &dependency,
	}
	store := memory.New()
	if err := store.Register(ctx, []sequencer.Registration{compensating}, now); err != nil {
		t.Fatal(err)
	}
	if err := memory.New().Register(ctx, []sequencer.Registration{compensating, compensating}, now); err != nil {
		t.Fatalf("Register(equivalent duplicate) error = %v", err)
	}
	duplicateBatch := memory.New()
	if err := duplicateBatch.Register(ctx, []sequencer.Registration{
		compensating,
		compensating,
		{ID: "after-duplicate", Version: 1, Checksum: "sum"},
	}, now); err != nil {
		t.Fatalf("Register(duplicate followed by distinct identity) error = %v", err)
	}
	if _, err := duplicateBatch.Snapshot(ctx, "after-duplicate", 1); err != nil {
		t.Fatalf("registration after in-batch duplicate was not stored: %v", err)
	}
	duplicate := compensating
	duplicate.Channel = "other"
	if err := memory.New().Register(ctx, []sequencer.Registration{compensating, duplicate}, now); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("Register(duplicate drift) error = %v", err)
	}
	record, err := store.Snapshot(ctx, compensating.ID, compensating.Version)
	if err != nil || record.Compensates == nil || *record.Compensates != dependency {
		t.Fatalf("Snapshot(compensation) = %+v, %v", record, err)
	}
}

func TestStoreRegistrationComparesCompensationPresenceAndIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 12, 45, 0, 0, time.UTC)
	first := sequencer.DependencyRef{ID: "first", Version: 1, Checksum: "sha256:first"}
	second := sequencer.DependencyRef{ID: "second", Version: 1, Checksum: "sha256:second"}
	dependencies := []sequencer.DependencyRef{first, second}
	tests := []struct {
		name    string
		stored  *sequencer.DependencyRef
		current *sequencer.DependencyRef
		want    error
	}{
		{"both absent", nil, nil, nil},
		{"new compensation", nil, &first, sequencer.ErrDefinitionDrift},
		{"removed compensation", &first, nil, sequencer.ErrDefinitionDrift},
		{"same compensation", &first, &first, nil},
		{"changed compensation", &first, &second, sequencer.ErrDefinitionDrift},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := memory.New()
			registration := sequencer.Registration{
				ID: "operation", Version: 1, Checksum: "sum",
				DependencyRefs: dependencies, Compensates: test.stored,
			}
			if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
				t.Fatal(err)
			}
			registration.Compensates = test.current
			err := store.Register(ctx, []sequencer.Registration{registration}, now.Add(time.Second))
			if !errors.Is(err, test.want) {
				t.Fatalf("Register() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestStoreResolveUnknownHonorsContextAndMissingIdentity(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	request := sequencer.ReconcileRequest{
		OperationID: "missing", Version: 1, Attempt: 1, Fencing: 1,
		Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "verified", At: time.Now(),
	}
	store := memory.New()
	if err := store.ResolveUnknown(canceled, request); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveUnknown(canceled) error = %v", err)
	}
	if err := store.ResolveUnknown(context.Background(), request); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("ResolveUnknown(missing) error = %v", err)
	}
}

func TestStoreResolveUnknownValidatesEveryAdministrativeFieldIndependently(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 12, 50, 0, 0, time.UTC)
	valid := sequencer.ReconcileRequest{
		OperationID: "missing", Version: 1, Attempt: 1, Fencing: 1,
		Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "verified", At: now,
	}
	tests := []struct {
		name   string
		mutate func(*sequencer.ReconcileRequest)
	}{
		{"operation ID", func(request *sequencer.ReconcileRequest) { request.OperationID = "" }},
		{"malformed operation ID", func(request *sequencer.ReconcileRequest) { request.OperationID = "Invalid" }},
		{"version", func(request *sequencer.ReconcileRequest) { request.Version = 0 }},
		{"attempt", func(request *sequencer.ReconcileRequest) { request.Attempt = 0 }},
		{"fencing", func(request *sequencer.ReconcileRequest) { request.Fencing = 0 }},
		{"timestamp", func(request *sequencer.ReconcileRequest) { request.At = time.Time{} }},
		{"actor", func(request *sequencer.ReconcileRequest) { request.Actor = "" }},
		{"actor limit", func(request *sequencer.ReconcileRequest) {
			request.Actor = strings.Repeat("a", sequencer.DefaultMaxActorBytes+1)
		}},
		{"reason", func(request *sequencer.ReconcileRequest) { request.Reason = "" }},
		{"reason limit", func(request *sequencer.ReconcileRequest) {
			request.Reason = strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if err := memory.New().ResolveUnknown(ctx, request); !errors.Is(err, sequencer.ErrReconcileForbidden) {
				t.Fatalf("ResolveUnknown() error = %v", err)
			}
		})
	}
	for _, request := range []sequencer.ReconcileRequest{
		func() sequencer.ReconcileRequest {
			request := valid
			request.Actor = strings.Repeat("a", sequencer.DefaultMaxActorBytes)
			return request
		}(),
		func() sequencer.ReconcileRequest {
			request := valid
			request.Reason = strings.Repeat("r", sequencer.DefaultMaxReasonBytes)
			return request
		}(),
	} {
		if err := memory.New().ResolveUnknown(ctx, request); !errors.Is(err, sequencer.ErrNotFound) {
			t.Fatalf("ResolveUnknown(exact bound) error = %v", err)
		}
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

func TestStoreRejectsRegistrationAndClaimChannelDrift(t *testing.T) {
	t.Parallel()

	store := memory.New()
	now := time.Now()
	registration := sequencer.Registration{ID: "operation", Version: 1, Checksum: "sum", Channel: "deploy"}
	if err := store.Register(context.Background(), []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	drifted := registration
	drifted.Channel = "maintenance"
	if err := store.Register(context.Background(), []sequencer.Registration{drifted}, now); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("Register(channel drift) error = %v", err)
	}
	_, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: registration.Version, Checksum: registration.Checksum, Channel: "maintenance"}},
		Owner:      "owner", Now: now, LeaseDuration: time.Minute,
	})
	if !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("ClaimNext(channel drift) error = %v", err)
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
		{OperationIDs: []sequencer.OperationID{"ready"}, Owner: strings.Repeat("o", sequencer.DefaultMaxActorBytes+1), Now: now, LeaseDuration: time.Second},
		{OperationIDs: []sequencer.OperationID{"ready"}, Owner: "owner", Now: now},
		{OperationIDs: []sequencer.OperationID{"ready"}, Owner: "owner", LeaseDuration: time.Second},
		{OperationIDs: []sequencer.OperationID{"ready"}, Owner: "owner", Now: now, LeaseDuration: -time.Second},
	} {
		if _, err := store.ClaimNext(ctx, request); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Fatalf("ClaimNext(%+v) error = %v", request, err)
		}
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "ready", Version: 1, Checksum: strings.Repeat("c", 513)}},
		Owner:      "owner", Now: now, LeaseDuration: time.Second,
	}); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("ClaimNext(checksum overflow) error = %v", err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "ready", Version: 1, Channel: "Invalid Channel"}},
		Owner:      "owner", Now: now, LeaseDuration: time.Second,
	}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("ClaimNext(invalid channel) error = %v", err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "Invalid"}},
		Owner:      "owner", Now: now, LeaseDuration: time.Second,
	}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("ClaimNext(invalid operation ID) error = %v", err)
	}
	tooMany := make([]sequencer.OperationID, sequencer.DefaultMaxOperations+1)
	for index := range tooMany {
		tooMany[index] = "missing"
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		OperationIDs: tooMany, Owner: "owner", Now: now, LeaseDuration: time.Second,
	}); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("ClaimNext(candidate overflow) error = %v", err)
	}
	store = memory.New()
	exactOwner := strings.Repeat("o", sequencer.DefaultMaxActorBytes)
	exactChecksum := strings.Repeat("c", sequencer.DefaultMaxChecksumBytes)
	register(t, store, "ready", exactChecksum, now)
	exactCandidates := make([]sequencer.ClaimCandidate, sequencer.DefaultMaxOperations)
	for index := range exactCandidates {
		exactCandidates[index] = sequencer.ClaimCandidate{ID: "missing"}
	}
	exactCandidates[0] = sequencer.ClaimCandidate{ID: "ready", Version: 1, Checksum: exactChecksum}
	exactClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: exactCandidates, Owner: exactOwner, Now: now, LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatalf("ClaimNext(exact bounds) error = %v", err)
	}
	if _, err := store.MarkRunning(ctx, exactClaim.Ownership(), now); err != nil {
		t.Fatalf("MarkRunning(exact owner bound) error = %v", err)
	}
	store = memory.New()
	register(t, store, "ready", "sha256:ready", now)

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

func TestStoreCompleteBoundsAuditMetadataWithoutChangingState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC)
	newRunning := func() (*memory.Store, sequencer.Claim) {
		t.Helper()
		store := memory.New()
		register(t, store, "bounded-completion", "sha256:bounded", now)
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{"bounded-completion"}, Owner: "owner",
			Now: now, LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			t.Fatal(err)
		}
		return store, claim
	}

	for _, test := range []struct {
		name   string
		actor  string
		reason string
		output sequencer.Output
	}{
		{name: "actor overflow", actor: strings.Repeat("a", sequencer.DefaultMaxActorBytes+1), reason: "completed"},
		{name: "reason overflow", actor: "operator", reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1)},
		{name: "encoded output overflow", actor: "operator", reason: "completed", output: sequencer.Output{
			Metadata: map[string]string{"large": strings.Repeat("v", sequencer.DefaultMaxOutputBytes)},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, claim := newRunning()
			err := store.Complete(ctx, sequencer.Completion{
				Ownership: claim.Ownership(), State: sequencer.Succeeded, At: now,
				Actor: test.actor, Reason: test.reason, Output: test.output,
			})
			if !errors.Is(err, sequencer.ErrResourceLimit) {
				t.Fatalf("Complete() error = %v, want ErrResourceLimit", err)
			}
			record, snapshotErr := store.Snapshot(ctx, "bounded-completion", 1)
			if snapshotErr != nil || record.State != sequencer.Running {
				t.Fatalf("Snapshot() = %+v, %v; want running", record, snapshotErr)
			}
		})
	}
	invalidStore, invalidClaim := newRunning()
	if err := invalidStore.Complete(ctx, sequencer.Completion{
		Ownership: invalidClaim.Ownership(), State: sequencer.Succeeded, At: now,
		RetryException: true,
	}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Complete(invalid retry exception) error = %v", err)
	}
	invalidRecord, err := invalidStore.Snapshot(ctx, "bounded-completion", 1)
	if err != nil || invalidRecord.State != sequencer.Running || invalidRecord.RetryExceptions != 0 {
		t.Fatalf("Snapshot(invalid retry exception) = %+v, %v", invalidRecord, err)
	}
	missingStore, missingClaim := newRunning()
	if err := missingStore.Complete(ctx, sequencer.Completion{
		Ownership: missingClaim.Ownership(), State: sequencer.Retryable, At: now, EligibleAt: now,
	}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Complete(missing retry exception) error = %v", err)
	}

	store, claim := newRunning()
	actor := strings.Repeat("a", sequencer.DefaultMaxActorBytes)
	reason := strings.Repeat("r", sequencer.DefaultMaxReasonBytes)
	if err := store.Complete(ctx, sequencer.Completion{
		Ownership: claim.Ownership(), State: sequencer.Succeeded, At: now,
		Actor: actor, Reason: reason,
	}); err != nil {
		t.Fatalf("Complete(exact bounds) error = %v", err)
	}
	audit, err := store.Audit(ctx, "bounded-completion", 1, 10)
	if err != nil || audit[len(audit)-1].Actor != actor || audit[len(audit)-1].Reason != reason {
		t.Fatalf("Audit() = %+v, %v", audit, err)
	}
}

func TestStorePersistsRetryAndDeferredEligibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC)
	for _, state := range []sequencer.State{sequencer.Retryable, sequencer.Deferred} {
		id := sequencer.OperationID(state.String())
		store := memory.New()
		register(t, store, id, "sha256:state", now)
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{id},
			Owner:        "owner", Now: now, LeaseDuration: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			t.Fatal(err)
		}
		eligibleAt := now.Add(time.Minute)
		if err := store.Complete(ctx, sequencer.Completion{
			Ownership: claim.Ownership(), State: state, At: now, EligibleAt: eligibleAt,
			RetryException: state == sequencer.Retryable,
		}); err != nil {
			t.Fatal(err)
		}
		record, err := store.Snapshot(ctx, id, 1)
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

func TestStoreRecoversExpiredWorkInDeterministicBoundedBatches(t *testing.T) {
	t.Parallel()

	const recoveryBatchSize = 32
	ctx := context.Background()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	for index := recoveryBatchSize; index >= 0; index-- {
		id := sequencer.OperationID(fmt.Sprintf("recovery-%02d", index))
		register(t, store, id, "sha256:"+string(id), now)
		if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{id}, Owner: "owner", Now: now,
			LeaseDuration: time.Second,
		}); err != nil {
			t.Fatal(err)
		}
	}

	if recovered, err := store.RecoverExpired(ctx, now.Add(time.Minute)); err != nil || recovered != recoveryBatchSize {
		t.Fatalf("first RecoverExpired() = %d, %v; want %d", recovered, err, recoveryBatchSize)
	}
	for index := 0; index < recoveryBatchSize; index++ {
		id := sequencer.OperationID(fmt.Sprintf("recovery-%02d", index))
		record, err := store.Snapshot(ctx, id, 1)
		if err != nil || record.State != sequencer.Indeterminate {
			t.Fatalf("Snapshot(%s) = %+v, %v; want indeterminate", id, record, err)
		}
	}
	remaining, err := store.Snapshot(ctx, "recovery-32", 1)
	if err != nil || remaining.State != sequencer.Claimed {
		t.Fatalf("remaining Snapshot() = %+v, %v; want claimed", remaining, err)
	}
	if recovered, err := store.RecoverExpired(ctx, now.Add(time.Minute)); err != nil || recovered != 1 {
		t.Fatalf("second RecoverExpired() = %d, %v; want 1", recovered, err)
	}
}

func TestStoreBlocksUnknownExpiredWorkUntilExplicitResolution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	registration := sequencer.Registration{ID: "unknown", Version: 1, Checksum: "sum"}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "old", Now: now, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpired(ctx, now.Add(2*time.Second)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	record, _ := store.Snapshot(ctx, registration.ID, registration.Version)
	history, _ := store.History(ctx, registration.ID, registration.Version, 10)
	if record.State != sequencer.Indeterminate || len(history) != 1 || history[0].State != sequencer.Indeterminate {
		t.Fatalf("recovered record = %+v, history = %+v", record, history)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "new", Now: now.Add(3 * time.Second), LeaseDuration: time.Second}); !errors.Is(err, sequencer.ErrNoEligibleOperation) {
		t.Fatalf("ClaimNext(indeterminate) error = %v", err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: registration.ID, Version: 1, Actor: "operator", Reason: "unsafe generic reset", At: now.Add(3 * time.Second)}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset(indeterminate) error = %v", err)
	}
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{OperationID: registration.ID, Version: 1, Attempt: claim.Attempt.Number, Fencing: claim.Attempt.Fencing, Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "stale observation", At: now.Add(time.Second)}); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("ResolveUnknown(stale time) error = %v", err)
	}
	staleFence := claim.Attempt.Fencing - 1
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{OperationID: registration.ID, Version: 1, Attempt: claim.Attempt.Number, Fencing: staleFence, Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "stale fence", At: now.Add(3 * time.Second)}); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("ResolveUnknown(stale fence) error = %v", err)
	}
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{OperationID: registration.ID, Version: 1, Attempt: claim.Attempt.Number, Fencing: claim.Attempt.Fencing, Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "effect absent", At: now.Add(3 * time.Second)}); err != nil {
		t.Fatalf("ResolveUnknown(retry) error = %v", err)
	}
	record, err = store.Snapshot(ctx, registration.ID, registration.Version)
	if err != nil || !record.EligibleAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("resolved retry eligibility = %s, %v", record.EligibleAt, err)
	}
	next, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "new", Now: now.Add(3 * time.Second), LeaseDuration: time.Second})
	if err != nil || next.Attempt.Number != 2 || next.Attempt.Fencing <= claim.Attempt.Fencing {
		t.Fatalf("resolved claim = %+v, %v", next, err)
	}
}

func TestStoreReplaysExpiredWorkOnlyForExplicitIdempotencyPolicy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 13, 0, 0, 0, time.UTC)
	store := memory.New()
	registration := sequencer.Registration{ID: "idempotent", Version: 1, Checksum: "sum", UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	claim, _ := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "old", Now: now, LeaseDuration: time.Second})
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpired(ctx, now.Add(2*time.Second)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	record, _ := store.Snapshot(ctx, registration.ID, registration.Version)
	history, _ := store.History(ctx, registration.ID, registration.Version, 10)
	audit, _ := store.Audit(ctx, registration.ID, registration.Version, 10)
	if record.State != sequencer.Eligible || history[0].State != sequencer.Indeterminate || audit[len(audit)-2].To != sequencer.Indeterminate || audit[len(audit)-1].From != sequencer.Indeterminate || audit[len(audit)-1].To != sequencer.Eligible {
		t.Fatalf("record = %+v, history = %+v, audit = %+v", record, history, audit)
	}
}

func TestStoreResolvesUnknownAndResumesCanceledWithAttributedBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, 8, 10, 14, 0, 0, 0, time.UTC)
	for _, resolution := range []sequencer.ReconcileResolution{sequencer.ReconcileSucceeded, sequencer.ReconcileFailed} {
		store := memory.New()
		registration := sequencer.Registration{ID: sequencer.OperationID(resolution.String()), Version: 1, Checksum: "sum", DeadLetter: resolution == sequencer.ReconcileFailed}
		if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
			t.Fatal(err)
		}
		claim, _ := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "old", Now: now, LeaseDuration: time.Second})
		if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			t.Fatal(err)
		}
		_, _ = store.RecoverExpired(ctx, now.Add(2*time.Second))
		if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{OperationID: registration.ID, Version: 1, Attempt: claim.Attempt.Number, Fencing: claim.Attempt.Fencing, Resolution: resolution, Actor: "operator", Reason: "reconciled", At: now.Add(3 * time.Second)}); err != nil {
			t.Fatal(err)
		}
		record, _ := store.Snapshot(ctx, registration.ID, 1)
		want := sequencer.Succeeded
		if resolution == sequencer.ReconcileFailed {
			want = sequencer.DeadLettered
		}
		if record.State != want {
			t.Fatalf("resolution %s state = %s, want %s", resolution, record.State, want)
		}
		if !record.EligibleAt.Equal(now) {
			t.Fatalf("resolution %s changed terminal eligibility to %s", resolution, record.EligibleAt)
		}
		if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{OperationID: registration.ID, Version: 1, Attempt: claim.Attempt.Number, Fencing: claim.Attempt.Fencing, Resolution: resolution, Actor: "operator", Reason: "again", At: now.Add(4 * time.Second)}); !errors.Is(err, sequencer.ErrReconcileForbidden) {
			t.Fatalf("second resolution error = %v", err)
		}
	}

	store := memory.New()
	register(t, store, "canceled-resume", "sum", now)
	claim, _ := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"canceled-resume"}, Owner: "old", Now: now, LeaseDuration: time.Minute})
	_, _ = store.MarkRunning(ctx, claim.Ownership(), now)
	if err := store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Canceled, At: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: "canceled-resume", Version: 1, Actor: "operator", Reason: "resume", At: now.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}

	tooLong := strings.Repeat("x", sequencer.DefaultMaxReasonBytes+1)
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{Actor: "operator", Reason: tooLong, At: now, Resolution: sequencer.ReconcileRetry}); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("unbounded resolution error = %v", err)
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
	if err := store.Complete(ctx, sequencer.Completion{
		Ownership: claim.Ownership(), State: sequencer.Retryable, At: now, EligibleAt: now, RetryException: true,
	}); err != nil {
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

func TestStoreResetEnforcesAdministrativeBoundsAndMonotonicTime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	terminalStore := func(t *testing.T) *memory.Store {
		t.Helper()
		store := memory.New()
		register(t, store, "reset-bounds", "sha256:reset-bounds", now)
		claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{"reset-bounds"}, Owner: "owner",
			Now: now, LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(context.Background(), claim.Ownership(), now); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(context.Background(), sequencer.Completion{
			Ownership: claim.Ownership(), State: sequencer.Failed, At: now.Add(time.Second),
		}); err != nil {
			t.Fatal(err)
		}
		return store
	}

	exact := terminalStore(t)
	if err := exact.Reset(context.Background(), sequencer.ResetRequest{
		OperationID: "reset-bounds", Version: 1,
		Actor:  strings.Repeat("a", sequencer.DefaultMaxActorBytes),
		Reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes),
		At:     now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("Reset(exact bounds) error = %v", err)
	}

	for name, request := range map[string]sequencer.ResetRequest{
		"missing operation id": {
			Version: 1, Actor: "actor", Reason: "reason", At: now.Add(2 * time.Second),
		},
		"malformed operation id": {
			OperationID: "Invalid", Version: 1, Actor: "actor", Reason: "reason", At: now.Add(2 * time.Second),
		},
		"missing version": {
			OperationID: "reset-bounds", Actor: "actor", Reason: "reason", At: now.Add(2 * time.Second),
		},
		"actor overflow": {
			OperationID: "reset-bounds", Version: 1,
			Actor: strings.Repeat("a", sequencer.DefaultMaxActorBytes+1), Reason: "reason", At: now.Add(2 * time.Second),
		},
		"reason overflow": {
			OperationID: "reset-bounds", Version: 1,
			Actor: "actor", Reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1), At: now.Add(2 * time.Second),
		},
		"stale time": {
			OperationID: "reset-bounds", Version: 1,
			Actor: "actor", Reason: "reason", At: now,
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := terminalStore(t)
			if err := store.Reset(context.Background(), request); !errors.Is(err, sequencer.ErrResetForbidden) {
				t.Fatalf("Reset() error = %v", err)
			}
			record, err := store.Snapshot(context.Background(), "reset-bounds", 1)
			if err != nil || record.State != sequencer.Failed || !record.UpdatedAt.Equal(now.Add(time.Second)) {
				t.Fatalf("Snapshot() = %+v, %v", record, err)
			}
		})
	}
}

func register(t *testing.T, store *memory.Store, id sequencer.OperationID, checksum string, now time.Time) {
	t.Helper()
	if err := store.Register(context.Background(), []sequencer.Registration{{ID: id, Version: 1, Checksum: checksum}}, now); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
}
