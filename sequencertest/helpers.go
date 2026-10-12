// Package sequencertest provides deterministic operations, clocks, and fault
// injection helpers for sequencer consumers and store conformance suites.
//
// Helpers own no timers or background work. Fixture checksums hash the operation
// ID, not handler source; they are test identities, not production provenance.
package sequencertest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
)

// Clock is a concurrency-safe manually advanced clock.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock constructs a clock at an exact instant.
func NewClock(now time.Time) *Clock { return &Clock{now: now} }

// Now returns the current test instant.
func (clock *Clock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

// Advance moves time forward and panics on a negative duration.
func (clock *Clock) Advance(duration time.Duration) {
	if duration < 0 {
		panic("sequencertest: clock cannot move backwards")
	}
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
}

// Operation creates a one-time version-one fixture with Channel test and a one-minute
// timeout. ID still needs to pass NewOperation validation; its checksum hashes ID
// rather than the handler. A nil handler selects a no-op, but typed nil is retained.
func Operation(id sequencer.OperationID, handler sequencer.Handler) sequencer.OperationSpec {
	if handler == nil {
		handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{}, nil
		})
	}
	digest := sha256.Sum256([]byte(id))
	return sequencer.OperationSpec{
		ID: id, Version: 1, Checksum: "sha256:" + hex.EncodeToString(digest[:]),
		Description: "deterministic test operation", Channel: "test",
		Policy:  sequencer.Policy{Mode: sequencer.OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Minute},
		Handler: handler,
	}
}

// Faults inject errors at durable store boundaries.
type Faults struct {
	// Register is returned before delegation when non-nil.
	Register error
	// ClaimNext is returned before delegation when non-nil.
	ClaimNext error
	// MarkRunning is returned before delegation when non-nil.
	MarkRunning error
	// RenewLease is returned before delegation when non-nil.
	RenewLease error
	// Complete is returned before delegation when non-nil.
	Complete error
	// RecoverExpired is returned before delegation when non-nil.
	RecoverExpired error
	// Snapshot is returned before delegation when non-nil.
	Snapshot error
	// History is returned before delegation when non-nil.
	History error
	// Audit is returned before delegation when non-nil.
	Audit error
	// Reset is returned before delegation when non-nil.
	Reset error
}

// FaultStore wraps a real store while injecting selected boundary failures.
type FaultStore struct {
	sequencer.Store
	faults Faults
}

// NewFaultStore copies fault selection and borrows the store and error values.
// It does not validate a nil store; unfaulted methods need a usable delegate.
// RenewLease returns ErrInvalidLease if the delegate lacks LeaseStore capability.
func NewFaultStore(store sequencer.Store, faults Faults) *FaultStore {
	return &FaultStore{Store: store, faults: faults}
}

// Register injects the configured fault or delegates registration.
func (store *FaultStore) Register(ctx context.Context, registrations []sequencer.Registration, now time.Time) error {
	if store.faults.Register != nil {
		return store.faults.Register
	}
	return store.Store.Register(ctx, registrations, now)
}

// ClaimNext injects the configured fault or delegates claiming.
func (store *FaultStore) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	if store.faults.ClaimNext != nil {
		return sequencer.Claim{}, store.faults.ClaimNext
	}
	return store.Store.ClaimNext(ctx, request)
}

// MarkRunning injects the configured fault or delegates the transition.
func (store *FaultStore) MarkRunning(ctx context.Context, ownership sequencer.Ownership, now time.Time) (sequencer.AttemptRecord, error) {
	if store.faults.MarkRunning != nil {
		return sequencer.AttemptRecord{}, store.faults.MarkRunning
	}
	return store.Store.MarkRunning(ctx, ownership, now)
}

// RenewLease injects the configured fault or delegates fenced renewal.
func (store *FaultStore) RenewLease(ctx context.Context, ownership sequencer.Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	if store.faults.RenewLease != nil {
		return time.Time{}, store.faults.RenewLease
	}
	leaseStore, ok := store.Store.(sequencer.LeaseStore)
	if !ok {
		return time.Time{}, sequencer.ErrInvalidLease
	}
	return leaseStore.RenewLease(ctx, ownership, now, duration)
}

// Complete injects the configured fault or delegates completion.
func (store *FaultStore) Complete(ctx context.Context, completion sequencer.Completion) error {
	if store.faults.Complete != nil {
		return store.faults.Complete
	}
	return store.Store.Complete(ctx, completion)
}

// RecoverExpired injects the configured fault or delegates recovery.
func (store *FaultStore) RecoverExpired(ctx context.Context, now time.Time) (int, error) {
	if store.faults.RecoverExpired != nil {
		return 0, store.faults.RecoverExpired
	}
	return store.Store.RecoverExpired(ctx, now)
}

// Snapshot injects the configured fault or delegates inspection.
func (store *FaultStore) Snapshot(ctx context.Context, id sequencer.OperationID, version uint) (sequencer.Record, error) {
	if store.faults.Snapshot != nil {
		return sequencer.Record{}, store.faults.Snapshot
	}
	return store.Store.Snapshot(ctx, id, version)
}

// History injects the configured fault or delegates history retrieval.
func (store *FaultStore) History(ctx context.Context, id sequencer.OperationID, version uint, limit int) ([]sequencer.AttemptRecord, error) {
	if store.faults.History != nil {
		return nil, store.faults.History
	}
	return store.Store.History(ctx, id, version, limit)
}

// Audit injects the configured fault or delegates audit retrieval.
func (store *FaultStore) Audit(ctx context.Context, id sequencer.OperationID, version uint, limit int) ([]sequencer.AuditEvent, error) {
	if store.faults.Audit != nil {
		return nil, store.faults.Audit
	}
	return store.Store.Audit(ctx, id, version, limit)
}

// Reset injects the configured fault or delegates replay authorization.
func (store *FaultStore) Reset(ctx context.Context, request sequencer.ResetRequest) error {
	if store.faults.Reset != nil {
		return store.faults.Reset
	}
	return store.Store.Reset(ctx, request)
}

var _ sequencer.Store = (*FaultStore)(nil)
var _ sequencer.LeaseStore = (*FaultStore)(nil)
