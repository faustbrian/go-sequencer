// Package memory provides a concurrency-safe deterministic sequencer store.
package memory

import (
	"context"
	"encoding/json"
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
	record              sequencer.Record
	compensationFencing uint64
	attempts            []sequencer.AttemptRecord
	audit               []sequencer.AuditEvent
}

// Store is a mutex-serialized reference implementation suitable for tests.
type Store struct {
	mu                  sync.Mutex
	entries             map[key]*entry
	versions            map[sequencer.OperationID][]uint
	activeCompensations map[sequencer.DependencyRef]uint
	leases              leaseQueue
	leased              map[key]*leasedEntry
}

// New constructs an empty store without background goroutines.
func New() *Store {
	return &Store{
		entries:             make(map[key]*entry),
		versions:            make(map[sequencer.OperationID][]uint),
		activeCompensations: make(map[sequencer.DependencyRef]uint),
		leased:              make(map[key]*leasedEntry),
	}
}

// Register stores immutable operation identities and rejects checksum drift.
func (store *Store) Register(ctx context.Context, registrations []sequencer.Registration, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if now.IsZero() {
		return sequencer.ErrInvalidOperation
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	normalized := make(map[key]sequencer.Registration, len(registrations))
	for _, registration := range registrations {
		if !registration.ID.Valid() || registration.Version == 0 || registration.Checksum == "" ||
			(registration.Channel != "" && !sequencer.OperationID(registration.Channel).Valid()) ||
			registration.UnknownOutcome > sequencer.UnknownOutcomeReplayIdempotent {
			return sequencer.ErrInvalidOperation
		}
		if len(registration.Checksum) > sequencer.DefaultMaxChecksumBytes {
			return sequencer.ErrResourceLimit
		}
		if len(registration.Dependencies) > 0 {
			return sequencer.ErrUnpinnedDependency
		}
		if len(registration.DependencyRefs) > sequencer.DefaultMaxDependencies {
			return sequencer.ErrResourceLimit
		}
		registration.DependencyRefs = slices.Clone(registration.DependencyRefs)
		registration.Compensates = cloneDependencyRef(registration.Compensates)
		slices.SortFunc(registration.DependencyRefs, compareDependencyRefs)
		for index, dependency := range registration.DependencyRefs {
			if len(dependency.Checksum) > sequencer.DefaultMaxChecksumBytes {
				return sequencer.ErrResourceLimit
			}
			if !dependency.ID.Valid() || dependency.ID == registration.ID || dependency.Version == 0 || dependency.Checksum == "" ||
				(index > 0 && dependency.ID == registration.DependencyRefs[index-1].ID) {
				return sequencer.ErrInvalidOperation
			}
		}
		if registration.Compensates != nil && !slices.Contains(registration.DependencyRefs, *registration.Compensates) {
			return sequencer.ErrInvalidOperation
		}
		registration.Dependencies = slices.Clone(registration.Dependencies)
		identifier := key{registration.ID, registration.Version}
		if pending, exists := normalized[identifier]; exists {
			if pending.Checksum != registration.Checksum || pending.Channel != registration.Channel ||
				!slices.Equal(pending.DependencyRefs, registration.DependencyRefs) || !equalDependencyRef(pending.Compensates, registration.Compensates) ||
				pending.UnknownOutcome != registration.UnknownOutcome || pending.DeadLetter != registration.DeadLetter {
				return fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, registration.ID, registration.Version)
			}
			continue
		}
		if current, exists := store.entries[identifier]; exists {
			if current.record.Checksum != registration.Checksum {
				return fmt.Errorf("%w: %s version %d", sequencer.ErrChecksumDrift, registration.ID, registration.Version)
			}
			if current.record.Channel != registration.Channel || !slices.Equal(current.record.DependencyRefs, registration.DependencyRefs) ||
				!equalDependencyRef(current.record.Compensates, registration.Compensates) || current.record.UnknownOutcome != registration.UnknownOutcome ||
				current.record.DeadLetter != registration.DeadLetter {
				return fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, registration.ID, registration.Version)
			}
			continue
		}
		normalized[identifier] = registration
	}
	for identifier, registration := range normalized {
		store.entries[identifier] = &entry{record: sequencer.Record{
			Registration: registration, State: sequencer.Eligible,
			EligibleAt: now, UpdatedAt: now,
		}}
		store.versions[registration.ID] = append(store.versions[registration.ID], registration.Version)
		slices.Sort(store.versions[registration.ID])
		store.entries[identifier].appendAudit(sequencer.Pending, sequencer.Eligible, now, "", "registered")
	}
	return nil
}

func equalDependencyRef(left, right *sequencer.DependencyRef) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func cloneDependencyRef(reference *sequencer.DependencyRef) *sequencer.DependencyRef {
	if reference == nil {
		return nil
	}
	cloned := *reference
	return &cloned
}

func (store *Store) hasActiveCompensation(reference sequencer.DependencyRef) bool {
	return store.activeCompensations[reference] > 0
}

func compensationActive(record sequencer.Record) bool {
	if record.Compensates == nil {
		return false
	}
	return record.State == sequencer.Claimed || record.State == sequencer.Running ||
		record.State == sequencer.Retryable || record.State == sequencer.Deferred ||
		record.State == sequencer.Indeterminate ||
		record.State == sequencer.Eligible && record.AttemptNumber > 0
}

func (store *Store) setState(current *entry, state sequencer.State) {
	wasActive := compensationActive(current.record)
	current.record.State = state
	isActive := compensationActive(current.record)
	if wasActive == isActive || current.record.Compensates == nil {
		return
	}
	if isActive {
		store.activeCompensations[*current.record.Compensates]++
		return
	}
	store.activeCompensations[*current.record.Compensates]--
}

func ownershipValid(ownership sequencer.Ownership) bool {
	return ownership.OperationID.Valid() &&
		len(ownership.Owner) <= sequencer.DefaultMaxActorBytes
}

// ClaimNext atomically claims the first dependency-ready operation.
func (store *Store) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	if err := ctx.Err(); err != nil {
		return sequencer.Claim{}, err
	}
	if request.Owner == "" || len(request.Owner) > sequencer.DefaultMaxActorBytes || request.LeaseDuration <= 0 || request.Now.IsZero() ||
		(len(request.Candidates) == 0 && len(request.OperationIDs) == 0) {
		return sequencer.Claim{}, sequencer.ErrInvalidOperation
	}
	selectedCount := len(request.Candidates)
	if selectedCount == 0 {
		selectedCount = len(request.OperationIDs)
	}
	if selectedCount > sequencer.DefaultMaxOperations {
		return sequencer.Claim{}, sequencer.ErrResourceLimit
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	candidates := request.Candidates
	if len(candidates) == 0 {
		candidates = make([]sequencer.ClaimCandidate, len(request.OperationIDs))
		for index, id := range request.OperationIDs {
			candidates[index] = sequencer.ClaimCandidate{ID: id}
		}
	}
	for _, candidate := range candidates {
		if !candidate.ID.Valid() {
			return sequencer.Claim{}, sequencer.ErrInvalidOperation
		}
		if len(candidate.Checksum) > sequencer.DefaultMaxChecksumBytes {
			return sequencer.Claim{}, sequencer.ErrResourceLimit
		}
		if candidate.Channel != "" && !sequencer.OperationID(candidate.Channel).Valid() {
			return sequencer.Claim{}, sequencer.ErrInvalidOperation
		}
	}
	for _, candidate := range candidates {
		current := store.latest(candidate.ID)
		if candidate.Version != 0 {
			current = store.entries[key{candidate.ID, candidate.Version}]
		}
		if current != nil && candidate.Checksum != "" && current.record.Checksum != candidate.Checksum {
			return sequencer.Claim{}, fmt.Errorf("%w: %s version %d", sequencer.ErrChecksumDrift, candidate.ID, candidate.Version)
		}
		if current != nil && candidate.Channel != "" && current.record.Channel != candidate.Channel {
			return sequencer.Claim{}, fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, candidate.ID, candidate.Version)
		}
		if current == nil || current.record.EligibleAt.After(request.Now) || !store.dependenciesSucceeded(current.record.DependencyRefs) {
			continue
		}
		if (current.record.State == sequencer.Eligible || current.record.State == sequencer.Retryable || current.record.State == sequencer.Deferred) &&
			(current.record.AttemptNumber == ^uint(0) || current.record.RunAttempt == ^uint(0) || current.record.Fencing == ^uint64(0)) {
			return sequencer.Claim{}, sequencer.ErrResourceLimit
		}
		if current.record.State == sequencer.Retryable || current.record.State == sequencer.Deferred {
			from := current.record.State
			store.setState(current, sequencer.Eligible)
			current.appendAudit(from, sequencer.Eligible, request.Now, "system", "eligibility reached")
		}
		if current.record.State != sequencer.Eligible {
			continue
		}
		if current.record.Compensates != nil && current.compensationFencing == 0 {
			// Dependency readiness above proves this exact forward definition exists.
			forward := store.entries[key{current.record.Compensates.ID, current.record.Compensates.Version}]
			current.compensationFencing = forward.record.Fencing
		}
		from := current.record.State
		store.setState(current, sequencer.Claimed)
		current.record.Owner = request.Owner
		current.record.Fencing++
		current.record.AttemptNumber++
		current.record.RunAttempt++
		current.record.LeaseExpiresAt = request.Now.Add(request.LeaseDuration)
		current.record.UpdatedAt = request.Now
		store.setLease(key{current.record.ID, current.record.Version}, current.record.LeaseExpiresAt)
		attempt := sequencer.Attempt{
			OperationID: current.record.ID, Version: current.record.Version,
			Number: current.record.AttemptNumber, Owner: request.Owner,
			Fencing: current.record.Fencing, StartedAt: request.Now,
		}
		current.attempts = append(current.attempts, sequencer.AttemptRecord{Attempt: attempt, State: sequencer.Claimed})
		current.appendAudit(from, sequencer.Claimed, request.Now, request.Owner, "claimed")
		return sequencer.Claim{
			Attempt: attempt, Until: current.record.LeaseExpiresAt,
			Budget: sequencer.RetryBudget{Attempt: current.record.RunAttempt, Exceptions: current.record.RetryExceptions},
		}, nil
	}
	return sequencer.Claim{}, sequencer.ErrNoEligibleOperation
}

// MarkRunning records handler execution under the current fencing proof.
func (store *Store) MarkRunning(ctx context.Context, ownership sequencer.Ownership, now time.Time) (sequencer.AttemptRecord, error) {
	if err := ctx.Err(); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if !ownershipValid(ownership) {
		return sequencer.AttemptRecord{}, sequencer.ErrInvalidOperation
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, err := store.owned(ownership)
	if err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if now.IsZero() || now.Before(current.record.UpdatedAt) {
		return sequencer.AttemptRecord{}, sequencer.ErrInvalidOperation
	}
	if !now.Before(current.record.LeaseExpiresAt) {
		return sequencer.AttemptRecord{}, sequencer.ErrStaleOwner
	}
	if err := sequencer.ValidateTransition(current.record.State, sequencer.Running); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	from := current.record.State
	store.setState(current, sequencer.Running)
	current.record.UpdatedAt = now
	attempt := &current.attempts[len(current.attempts)-1]
	attempt.State = sequencer.Running
	current.appendAudit(from, sequencer.Running, now, ownership.Owner, "started")
	return cloneAttempt(*attempt), nil
}

// RenewLease extends a claimed or running attempt under its current fencing proof.
func (store *Store) RenewLease(ctx context.Context, ownership sequencer.Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if !ownershipValid(ownership) {
		return time.Time{}, sequencer.ErrInvalidOperation
	}
	if duration <= 0 || now.IsZero() {
		return time.Time{}, sequencer.ErrInvalidLease
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, err := store.owned(ownership)
	if err != nil {
		return time.Time{}, err
	}
	if !now.Before(current.record.LeaseExpiresAt) {
		return time.Time{}, sequencer.ErrStaleOwner
	}
	if now.Before(current.record.UpdatedAt) {
		return time.Time{}, sequencer.ErrInvalidLease
	}
	until := now.Add(duration)
	if until.After(current.record.LeaseExpiresAt) {
		current.record.LeaseExpiresAt = until
		current.record.UpdatedAt = now
		store.setLease(key{current.record.ID, current.record.Version}, until)
	}
	return current.record.LeaseExpiresAt, nil
}

// Complete atomically persists an attempt outcome and current projection.
func (store *Store) Complete(ctx context.Context, completion sequencer.Completion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !ownershipValid(completion.Ownership) {
		return sequencer.ErrInvalidOperation
	}
	from := completion.From
	if from == 0 {
		from = sequencer.Running
	}
	if err := sequencer.ValidateTransition(from, completion.State); err != nil {
		return err
	}
	if completion.State == sequencer.Retryable && !completion.RetryException ||
		completion.RetryException && completion.State != sequencer.Retryable && completion.State != sequencer.Failed && completion.State != sequencer.DeadLettered {
		return sequencer.ErrInvalidOperation
	}
	actor, reason := completion.Actor, completion.Reason
	if actor == "" {
		actor = completion.Owner
	}
	if reason == "" {
		reason = "completed"
	}
	if len(actor) > sequencer.DefaultMaxActorBytes || len(reason) > sequencer.DefaultMaxReasonBytes {
		return sequencer.ErrResourceLimit
	}
	output, err := json.Marshal(completion.Output)
	if err != nil || len(output) > sequencer.DefaultMaxOutputBytes {
		return sequencer.ErrResourceLimit
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, err := store.owned(completion.Ownership)
	if err != nil {
		return err
	}
	if current.record.State != from {
		return sequencer.ErrInvalidTransition
	}
	if completion.At.IsZero() || completion.At.Before(current.record.UpdatedAt) {
		return sequencer.ErrInvalidOperation
	}
	if !completion.At.Before(current.record.LeaseExpiresAt) {
		return sequencer.ErrStaleOwner
	}
	if completion.RetryException && current.record.RetryExceptions == ^uint(0) {
		return sequencer.ErrResourceLimit
	}
	store.setState(current, completion.State)
	current.record.UpdatedAt = completion.At
	current.record.LeaseExpiresAt = time.Time{}
	store.clearLease(key{current.record.ID, current.record.Version})
	if completion.State == sequencer.Retryable || completion.State == sequencer.Deferred {
		current.record.EligibleAt = completion.EligibleAt
	}
	attempt := &current.attempts[len(current.attempts)-1]
	attempt.State = completion.State
	attempt.CompletedAt = completion.At
	attempt.ErrorDetail = sequencer.SanitizePersistenceText(completion.ErrorDetail, sequencer.DefaultMaxErrorBytes)
	attempt.Output = cloneOutput(completion.Output)
	if completion.RetryException {
		current.record.RetryExceptions++
	}
	current.appendAudit(from, completion.State, completion.At, actor, reason)
	current.record.Owner = ""
	return nil
}

// RecoverExpired records expired attempts as indeterminate. Only an explicit
// idempotent-replay policy makes the operation eligible again automatically.
func (store *Store) RecoverExpired(ctx context.Context, now time.Time) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if now.IsZero() {
		return 0, sequencer.ErrInvalidOperation
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	recovered := 0
	for range sequencer.DefaultRecoveryBatchSize {
		identifier, ok := store.popExpiredLease(now)
		if ok {
			current := store.entries[identifier]
			from := current.record.State
			attempt := &current.attempts[len(current.attempts)-1]
			attempt.State = sequencer.Indeterminate
			attempt.CompletedAt = now
			attempt.ErrorDetail = sequencer.ErrUnknownResult.Error()
			store.setState(current, sequencer.Indeterminate)
			current.record.LeaseExpiresAt = time.Time{}
			current.record.UpdatedAt = now
			current.appendAudit(from, sequencer.Indeterminate, now, "system", "lease expired; outcome unknown")
			if current.record.UnknownOutcome == sequencer.UnknownOutcomeReplayIdempotent {
				store.setState(current, sequencer.Eligible)
				current.record.EligibleAt = now
				current.appendAudit(sequencer.Indeterminate, sequencer.Eligible, now, "system", "idempotent replay authorized")
			}
			current.record.Owner = ""
			recovered++
		}
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
	if !request.OperationID.Valid() || request.Version == 0 || request.At.IsZero() ||
		request.Actor == "" || len(request.Actor) > sequencer.DefaultMaxActorBytes ||
		request.Reason == "" || len(request.Reason) > sequencer.DefaultMaxReasonBytes {
		return sequencer.ErrResetForbidden
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.entries[key{request.OperationID, request.Version}]
	if current == nil {
		return sequencer.ErrNotFound
	}
	if current.record.State != sequencer.Succeeded && current.record.State != sequencer.Failed && current.record.State != sequencer.Blocked &&
		current.record.State != sequencer.Canceled && current.record.State != sequencer.DeadLettered {
		return sequencer.ErrResetForbidden
	}
	if request.At.Before(current.record.UpdatedAt) {
		return sequencer.ErrResetForbidden
	}
	if current.record.Compensates != nil {
		// Claiming a compensation proves this exact immutable forward definition
		// exists and binds a nonzero generation before the compensation can
		// reach a resettable state.
		forward := store.entries[key{current.record.Compensates.ID, current.record.Compensates.Version}]
		if (forward.record.State != sequencer.Succeeded && forward.record.State != sequencer.Skipped) ||
			current.compensationFencing != forward.record.Fencing {
			return sequencer.ErrResetForbidden
		}
	}
	forward := sequencer.DependencyRef{ID: current.record.ID, Version: current.record.Version, Checksum: current.record.Checksum}
	if store.hasActiveCompensation(forward) {
		return sequencer.ErrResetForbidden
	}
	from := current.record.State
	store.setState(current, sequencer.Eligible)
	current.record.EligibleAt = request.At
	current.record.UpdatedAt = request.At
	current.record.Owner = ""
	current.record.RunAttempt = 0
	current.record.RetryExceptions = 0
	current.appendAudit(from, sequencer.Eligible, request.At, request.Actor, request.Reason)
	return nil
}

// ResolveUnknown atomically applies one attributed decision to an
// indeterminate operation. A second or stale decision fails closed.
func (store *Store) ResolveUnknown(ctx context.Context, request sequencer.ReconcileRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !request.OperationID.Valid() || request.Version == 0 || request.Attempt == 0 || request.Fencing == 0 || request.At.IsZero() ||
		request.Actor == "" || len(request.Actor) > sequencer.DefaultMaxActorBytes ||
		request.Reason == "" || len(request.Reason) > sequencer.DefaultMaxReasonBytes ||
		request.Resolution < sequencer.ReconcileSucceeded || request.Resolution > sequencer.ReconcileFailed {
		return sequencer.ErrReconcileForbidden
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.entries[key{request.OperationID, request.Version}]
	if current == nil {
		return sequencer.ErrNotFound
	}
	if current.record.State != sequencer.Indeterminate || current.record.AttemptNumber != request.Attempt || current.record.Fencing != request.Fencing {
		return sequencer.ErrReconcileForbidden
	}
	if request.At.Before(current.record.UpdatedAt) {
		return sequencer.ErrReconcileForbidden
	}
	to := sequencer.Eligible
	switch request.Resolution {
	case sequencer.ReconcileSucceeded:
		to = sequencer.Succeeded
	case sequencer.ReconcileRetry:
		to = sequencer.Eligible
	case sequencer.ReconcileFailed:
		to = sequencer.Failed
		if current.record.DeadLetter {
			to = sequencer.DeadLettered
		}
	}
	from := current.record.State
	store.setState(current, to)
	current.record.UpdatedAt = request.At
	if to == sequencer.Eligible {
		current.record.EligibleAt = request.At
	}
	current.appendAudit(from, to, request.At, request.Actor, request.Reason)
	return nil
}

func (store *Store) latest(id sequencer.OperationID) *entry {
	versions := store.versions[id]
	if len(versions) == 0 {
		return nil
	}
	return store.entries[key{id, versions[len(versions)-1]}]
}

func (store *Store) dependenciesSucceeded(dependencies []sequencer.DependencyRef) bool {
	for _, dependency := range dependencies {
		current := store.entries[key{dependency.ID, dependency.Version}]
		if current == nil || current.record.Checksum != dependency.Checksum ||
			(current.record.State != sequencer.Succeeded && current.record.State != sequencer.Skipped) {
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
	record.DependencyRefs = slices.Clone(record.DependencyRefs)
	record.Compensates = cloneDependencyRef(record.Compensates)
	record.Dependencies = slices.Clone(record.Dependencies)
	return record
}

func compareDependencyRefs(left, right sequencer.DependencyRef) int {
	if left.ID < right.ID {
		return -1
	}
	if left.ID > right.ID {
		return 1
	}
	return 0
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
var _ sequencer.LeaseStore = (*Store)(nil)
var _ sequencer.ReconciliationStore = (*Store)(nil)
var _ = errors.Is
