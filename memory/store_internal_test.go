package memory

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestStoreInternalOrderingAndMissingLatest(t *testing.T) {
	t.Parallel()

	store := New()
	if latest := store.latest("missing"); latest != nil {
		t.Fatalf("latest(missing) = %+v", latest)
	}
	reference := sequencer.DependencyRef{ID: "same", Version: 1, Checksum: "sum"}
	if order := compareDependencyRefs(reference, reference); order != 0 {
		t.Fatalf("compareDependencyRefs(equal) = %d", order)
	}
}

func TestStoreRejectsOwnershipAndRetryCounterOverflow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	for _, state := range []sequencer.State{sequencer.Eligible, sequencer.Retryable, sequencer.Deferred} {
		for _, mutate := range []func(*sequencer.Record){
			func(record *sequencer.Record) { record.AttemptNumber = ^uint(0) },
			func(record *sequencer.Record) { record.RunAttempt = ^uint(0) },
			func(record *sequencer.Record) { record.Fencing = math.MaxUint64 },
		} {
			store := New()
			identifier := key{id: "overflow", version: 1}
			record := sequencer.Record{
				Registration: sequencer.Registration{ID: identifier.id, Version: identifier.version, Checksum: "sum"},
				State:        state, EligibleAt: now, UpdatedAt: now,
			}
			mutate(&record)
			store.entries[identifier] = &entry{record: record}
			store.versions[identifier.id] = []uint{identifier.version}
			if _, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
				OperationIDs: []sequencer.OperationID{identifier.id}, Owner: "owner", Now: now, LeaseDuration: time.Minute,
			}); !errors.Is(err, sequencer.ErrResourceLimit) {
				t.Fatalf("ClaimNext(overflow %+v) error = %v", record, err)
			}
			if current := store.entries[identifier].record; current.State != state || current.Owner != "" {
				t.Fatalf("overflow claim mutated record: %+v", current)
			}
		}
	}

	store := New()
	if err := store.Register(context.Background(), []sequencer.Registration{{ID: "retry-overflow", Version: 1, Checksum: "sum"}}, now); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
		OperationIDs: []sequencer.OperationID{"retry-overflow"}, Owner: "owner", Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), claim.Ownership(), now); err != nil {
		t.Fatal(err)
	}
	store.entries[key{id: "retry-overflow", version: 1}].record.RetryExceptions = ^uint(0)
	if err := store.Complete(context.Background(), sequencer.Completion{
		Ownership: claim.Ownership(), State: sequencer.Retryable, At: now,
		EligibleAt: now, RetryException: true,
	}); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Complete(retry overflow) error = %v", err)
	}
	current := store.entries[key{id: "retry-overflow", version: 1}].record
	if current.State != sequencer.Running || current.RetryExceptions != ^uint(0) {
		t.Fatalf("retry overflow mutated record: %+v", current)
	}
}
