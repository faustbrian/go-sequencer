// Package memory provides a concurrency-safe deterministic sequencer store.
package memory

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

type key struct {
	id      sequencer.OperationID
	version uint
}

type entry struct {
	record   sequencer.Record
	attempts []sequencer.AttemptRecord
	audit    []sequencer.AuditEvent
}

// Store is a mutex-serialized reference implementation suitable for tests.
type Store struct {
	mu      sync.Mutex
	entries map[key]*entry
}

// New constructs an empty store without background goroutines.
func New() *Store { return &Store{entries: make(map[key]*entry)} }

// Register stores immutable operation identities and rejects checksum drift.
func (store *Store) Register(ctx context.Context, registrations []sequencer.Registration, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, registration := range registrations {
		if registration.ID == "" || registration.Version == 0 || registration.Checksum == "" {
			return sequencer.ErrInvalidOperation
		}
		identifier := key{registration.ID, registration.Version}
		if current, exists := store.entries[identifier]; exists {
			if current.record.Checksum != registration.Checksum {
				return fmt.Errorf("%w: %s version %d", sequencer.ErrChecksumDrift, registration.ID, registration.Version)
			}
			continue
		}
		registration.Dependencies = slices.Clone(registration.Dependencies)
		store.entries[identifier] = &entry{record: sequencer.Record{
			Registration: registration, State: sequencer.Eligible,
			EligibleAt: now, UpdatedAt: now,
		}}
		store.entries[identifier].appendAudit(sequencer.Pending, sequencer.Eligible, now, "", "registered")
	}
	return nil
}

// ClaimNext atomically claims the first dependency-ready operation.
func (store *Store) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	if err := ctx.Err(); err != nil {
		return sequencer.Claim{}, err
	}
	if request.Owner == "" || request.LeaseDuration <= 0 || request.Now.IsZero() {
		return sequencer.Claim{}, sequencer.ErrInvalidOperation
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, id := range request.OperationIDs {
		current := store.latest(id)
		if current == nil || current.record.EligibleAt.After(request.Now) || !store.dependenciesSucceeded(current.record.Dependencies) {
			continue
		}
		if current.record.State == sequencer.Retryable || current.record.State == sequencer.Deferred {
			from := current.record.State
			current.record.State = sequencer.Eligible
			current.appendAudit(from, sequencer.Eligible, request.Now, "system", "eligibility reached")
		}
		if current.record.State != sequencer.Eligible {
			continue
		}
		from := current.record.State
		current.record.State = sequencer.Claimed
		current.record.Owner = request.Owner
		current.record.Fencing++
		current.record.AttemptNumber++
		current.record.LeaseExpiresAt = request.Now.Add(request.LeaseDuration)
		current.record.UpdatedAt = request.Now
		attempt := sequencer.Attempt{
			OperationID: current.record.ID, Version: current.record.Version,
			Number: current.record.AttemptNumber, Owner: request.Owner,
			Fencing: current.record.Fencing, StartedAt: request.Now,
		}
		current.attempts = append(current.attempts, sequencer.AttemptRecord{Attempt: attempt, State: sequencer.Claimed})
		current.appendAudit(from, sequencer.Claimed, request.Now, request.Owner, "claimed")
		return sequencer.Claim{Attempt: attempt, Until: current.record.LeaseExpiresAt}, nil
	}
	return sequencer.Claim{}, sequencer.ErrNoEligibleOperation
}

// MarkRunning records handler execution under the current fencing proof.
func (store *Store) MarkRunning(ctx context.Context, ownership sequencer.Ownership, now time.Time) (sequencer.AttemptRecord, error) {
	if err := ctx.Err(); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, err := store.owned(ownership)
	if err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if err := sequencer.ValidateTransition(current.record.State, sequencer.Running); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	from := current.record.State
	current.record.State = sequencer.Running
	current.record.UpdatedAt = now
	attempt := &current.attempts[len(current.attempts)-1]
	attempt.State = sequencer.Running
	current.appendAudit(from, sequencer.Running, now, ownership.Owner, "started")
	return cloneAttempt(*attempt), nil
}

// Complete atomically persists an attempt outcome and current projection.
func (store *Store) Complete(ctx context.Context, completion sequencer.Completion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, err := store.owned(completion.Ownership)
	if err != nil {
		return err
	}
	if err := sequencer.ValidateTransition(current.record.State, completion.State); err != nil {
		return err
	}
	from := current.record.State
	current.record.State = completion.State
	current.record.UpdatedAt = completion.At
	current.record.LeaseExpiresAt = time.Time{}
	current.record.Owner = ""
	if completion.State == sequencer.Retryable || completion.State == sequencer.Deferred {
		current.record.EligibleAt = completion.EligibleAt
	}
	attempt := &current.attempts[len(current.attempts)-1]
	attempt.State = completion.State
	attempt.CompletedAt = completion.At
	attempt.ErrorDetail = sequencer.SanitizePersistenceText(completion.ErrorDetail, sequencer.DefaultMaxErrorBytes)
	attempt.Output = cloneOutput(completion.Output)
	actor, reason := completion.Actor, completion.Reason
	if actor == "" {
		actor = completion.Owner
	}
	if reason == "" {
		reason = "completed"
	}
	current.appendAudit(from, completion.State, completion.At, actor, reason)
	return nil
}

// RecoverExpired releases expired leases for explicit re-execution.
func (store *Store) RecoverExpired(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	recovered := 0
	for _, current := range store.entries {
		if (current.record.State != sequencer.Claimed && current.record.State != sequencer.Running) || current.record.LeaseExpiresAt.After(now) {
			continue
		}
		from := current.record.State
		attempt := &current.attempts[len(current.attempts)-1]
		attempt.State = sequencer.Retryable
		attempt.CompletedAt = now
		attempt.ErrorDetail = sequencer.ErrUnknownResult.Error()
		current.record.State = sequencer.Eligible
		current.record.Owner = ""
		current.record.LeaseExpiresAt = time.Time{}
		current.record.EligibleAt = now
		current.record.UpdatedAt = now
		current.appendAudit(from, sequencer.Retryable, now, "system", "lease expired; outcome unknown")
		current.appendAudit(sequencer.Retryable, sequencer.Eligible, now, "system", "recovered")
		recovered++
	}
	return recovered, nil
}

// Snapshot returns a copy of one current operation projection.
func (store *Store) Snapshot(ctx context.Context, id sequencer.OperationID, version uint) (sequencer.Record, error) {
	if err := ctx.Err(); err != nil {
		return sequencer.Record{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.entries[key{id, version}]
	if current == nil {
		return sequencer.Record{}, sequencer.ErrNotFound
	}
	return cloneRecord(current.record), nil
}

// History returns bounded attempt records in execution order.
func (store *Store) History(ctx context.Context, id sequencer.OperationID, version uint, limit int) ([]sequencer.AttemptRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.entries[key{id, version}]
	if current == nil {
		return nil, sequencer.ErrNotFound
	}
	if limit < 1 || limit > sequencer.DefaultMaxHistory {
		return nil, sequencer.ErrResourceLimit
	}
	start := max(0, len(current.attempts)-limit)
	result := make([]sequencer.AttemptRecord, len(current.attempts)-start)
	for index := range result {
		result[index] = cloneAttempt(current.attempts[start+index])
	}
	return result, nil
}

// Audit returns bounded append-only events in occurrence order.
func (store *Store) Audit(ctx context.Context, id sequencer.OperationID, version uint, limit int) ([]sequencer.AuditEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.entries[key{id, version}]
	if current == nil {
		return nil, sequencer.ErrNotFound
	}
	if limit < 1 || limit > sequencer.DefaultMaxHistory {
		return nil, sequencer.ErrResourceLimit
	}
	start := max(0, len(current.audit)-limit)
	return slices.Clone(current.audit[start:]), nil
}

// Reset performs an explicit attributable replay authorization.
func (store *Store) Reset(ctx context.Context, request sequencer.ResetRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.Actor == "" || request.Reason == "" || request.At.IsZero() {
		return sequencer.ErrResetForbidden
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.entries[key{request.OperationID, request.Version}]
	if current == nil {
		return sequencer.ErrNotFound
	}
	if current.record.State != sequencer.Succeeded && current.record.State != sequencer.Failed && current.record.State != sequencer.Blocked {
		return sequencer.ErrResetForbidden
	}
	from := current.record.State
	current.record.State = sequencer.Eligible
	current.record.EligibleAt = request.At
	current.record.UpdatedAt = request.At
	current.record.Owner = ""
	current.appendAudit(from, sequencer.Eligible, request.At, request.Actor, request.Reason)
	return nil
}

func (store *Store) latest(id sequencer.OperationID) *entry {
	var selected *entry
	for identifier, candidate := range store.entries {
		if identifier.id == id && (selected == nil || identifier.version > selected.record.Version) {
			selected = candidate
		}
	}
	return selected
}

func (store *Store) dependenciesSucceeded(dependencies []sequencer.OperationID) bool {
	for _, dependency := range dependencies {
		current := store.latest(dependency)
		if current == nil || (current.record.State != sequencer.Succeeded && current.record.State != sequencer.Skipped) {
			return false
		}
	}
	return true
}

func (store *Store) owned(ownership sequencer.Ownership) (*entry, error) {
	current := store.entries[key{ownership.OperationID, ownership.Version}]
	if current == nil {
		return nil, sequencer.ErrNotFound
	}
	if current.record.Owner != ownership.Owner || current.record.Fencing != ownership.Fencing {
		return nil, fmt.Errorf("%w: %s", sequencer.ErrStaleOwner, ownership.OperationID)
	}
	return current, nil
}

func (current *entry) appendAudit(from, to sequencer.State, at time.Time, actor, reason string) {
	current.audit = append(current.audit, sequencer.AuditEvent{
		OperationID: current.record.ID, Version: current.record.Version,
		Attempt: current.record.AttemptNumber, From: from, To: to, At: at,
		Owner: current.record.Owner, Fencing: current.record.Fencing,
		Actor: actor, Reason: reason,
	})
}

func cloneRecord(record sequencer.Record) sequencer.Record {
	record.Dependencies = slices.Clone(record.Dependencies)
	return record
}

func cloneAttempt(attempt sequencer.AttemptRecord) sequencer.AttemptRecord {
	attempt.Output = cloneOutput(attempt.Output)
	return attempt
}

func cloneOutput(output sequencer.Output) sequencer.Output {
	output.Summary = sequencer.SanitizePersistenceText(output.Summary, sequencer.DefaultMaxOutputBytes)
	if output.Metadata == nil {
		return output
	}
	metadata := make(map[string]string, len(output.Metadata))
	for key, value := range output.Metadata {
		metadata[key] = value
	}
	output.Metadata = metadata
	return output
}

var _ sequencer.Store = (*Store)(nil)
var _ = errors.Is
