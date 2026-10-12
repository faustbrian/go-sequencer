package sequencer

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var (
	bearerCredentialPattern = regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9._~+/=-]+`)
	namedCredentialPattern  = regexp.MustCompile(`(?i)\b(authorization|password|passwd|secret|token|api[_-]?key|credential)s?\b\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^\s,;]+)`)
)

const persistenceSanitizerLookaheadBytes = 4 << 10

var (
	// ErrPermanent marks a non-retryable operation failure.
	ErrPermanent = errors.New("sequencer: permanent failure")
	// ErrRetryable marks an explicitly retryable operation failure.
	ErrRetryable = errors.New("sequencer: retryable failure")
	// ErrSkipped marks an intentional no-op outcome.
	ErrSkipped = errors.New("sequencer: skipped")
	// ErrBlocked marks an operation awaiting external intervention.
	ErrBlocked = errors.New("sequencer: blocked")
	// ErrStaleOwner reports an expired or superseded fencing proof.
	ErrStaleOwner = errors.New("sequencer: stale owner")
	// ErrCanceled reports context or administrative cancellation.
	ErrCanceled = errors.New("sequencer: canceled")
	// ErrTimeout reports an attempt exceeding its declared timeout.
	ErrTimeout = errors.New("sequencer: timeout")
	// ErrUnknownResult reports an indeterminate durable outcome.
	ErrUnknownResult = errors.New("sequencer: unknown result")
	// ErrRollback reports a failed compensation operation.
	ErrRollback = errors.New("sequencer: rollback failure")
	// ErrChecksumDrift reports changed code for an existing version.
	ErrChecksumDrift = errors.New("sequencer: checksum drift")
	// ErrDefinitionDrift reports changed durable metadata for an existing version.
	ErrDefinitionDrift = errors.New("sequencer: definition drift")
	// ErrNoEligibleOperation reports that no dependency-ready work exists.
	ErrNoEligibleOperation = errors.New("sequencer: no eligible operation")
	// ErrNotFound reports an unknown operation version.
	ErrNotFound = errors.New("sequencer: operation not found")
	// ErrResetForbidden reports an invalid or unattributed replay request.
	ErrResetForbidden = errors.New("sequencer: reset forbidden")
	// ErrReconcileForbidden reports an invalid or stale unknown-outcome decision.
	ErrReconcileForbidden = errors.New("sequencer: reconciliation forbidden")
	// ErrInvalidLease reports a non-positive or regressing lease renewal.
	ErrInvalidLease = errors.New("sequencer: invalid lease renewal")
)

const (
	// DefaultRecoveryBatchSize bounds one expired-lease recovery transaction.
	DefaultRecoveryBatchSize = 32
)

type classifiedError struct {
	kind  error
	cause error
}

func (failure classifiedError) Error() string {
	return failure.kind.Error() + ": " + failure.cause.Error()
}
func (failure classifiedError) Unwrap() []error { return []error{failure.kind, failure.cause} }

func classify(kind, cause error) error {
	if cause == nil {
		cause = kind
	}
	return classifiedError{kind: kind, cause: cause}
}

// Permanent wraps a failure that must not be retried.
// The wrapper preserves errors.Is for classification and cause, and renders the
// cause in Error; it is not sanitized for logs. Nil cause retains the classification.
func Permanent(cause error) error { return classify(ErrPermanent, cause) }

// Retry wraps a transient failure eligible for another bounded attempt.
// The wrapper preserves errors.Is for classification and cause, and renders the
// cause in Error; it is not sanitized for logs. Nil cause retains the classification.
func Retry(cause error) error { return classify(ErrRetryable, cause) }

// Skip wraps the reason an operation intentionally did no work.
// The wrapper preserves errors.Is for classification and cause, and renders the
// cause in Error; it is not sanitized for logs. Nil cause retains the classification.
func Skip(cause error) error { return classify(ErrSkipped, cause) }

// Block wraps the reason an operation requires external intervention.
// The wrapper preserves errors.Is for classification and cause, and renders the
// cause in Error; it is not sanitized for logs. Nil cause retains the classification.
func Block(cause error) error { return classify(ErrBlocked, cause) }

// UnknownResult wraps an error whose durable outcome cannot be determined.
// The wrapper preserves errors.Is for classification and cause, and renders the
// cause in Error; it is not sanitized for logs. Nil cause retains the classification.
func UnknownResult(cause error) error { return classify(ErrUnknownResult, cause) }

// RollbackFailure wraps a failed compensation operation.
// The wrapper preserves errors.Is for classification and cause, and renders the
// cause in Error; it is not sanitized for logs. Nil cause retains the classification.
func RollbackFailure(cause error) error { return classify(ErrRollback, cause) }

// Registration is durable operation identity and dependency metadata.
type Registration struct {
	// ID identifies the immutable operation.
	ID OperationID
	// Version is nonzero; a changed definition requires a new version.
	Version uint
	// Checksum is the canonical caller-declared definition digest.
	Checksum string
	// Channel is the durable execution group; direct registration permits empty legacy channels.
	Channel string
	// DependencyRefs are exact prerequisites, copied and canonicalized by the reference stores.
	DependencyRefs []DependencyRef
	// Compensates binds this independent reverse operation to an exact forward dependency.
	Compensates *DependencyRef
	// UnknownOutcome controls recovery eligibility, not verification of external idempotency.
	UnknownOutcome UnknownOutcomePolicy
	// DeadLetter selects terminal dead-letter state when failure is resolved.
	DeadLetter bool
	// Dependencies is retained for source compatibility and rejected when non-empty.
	Dependencies []OperationID
}

// Record is the current-state projection for one operation version.
type Record struct {
	Registration
	// State is the current ledger projection.
	State State
	// AttemptNumber counts lifetime claims and is not reset by replay authorization.
	AttemptNumber uint
	// RunAttempt counts claims since the last attributed reset.
	RunAttempt uint
	// RetryExceptions counts typed retry failures in the current replay epoch.
	RetryExceptions uint
	// Owner is current claim identity, cleared after completion or recovery.
	Owner string
	// Fencing is the latest claim token, retained after ownership is cleared.
	Fencing uint64
	// LeaseExpiresAt bounds current ownership; absent leases use provider-specific zero/epoch representation.
	LeaseExpiresAt time.Time
	// EligibleAt is the earliest provider-time instant when retry/defer work can be claimed.
	EligibleAt time.Time
	// UpdatedAt is the latest projection update, including lease-only heartbeats.
	UpdatedAt time.Time
}

// AttemptRecord is the durable summary of one execution attempt.
type AttemptRecord struct {
	Attempt
	// State is this historical attempt outcome or current live state.
	State State
	// CompletedAt is absent while live; memory uses zero time and PostgreSQL reads epoch.
	CompletedAt time.Time
	// ErrorDetail is bounded sanitized text; runners supply stable classifications.
	ErrorDetail string
	// Output is prepared durable summary data, not handler payload history.
	Output Output
}

// AuditEvent is one append-only state or administration record.
type AuditEvent struct {
	// OperationID identifies the audited definition.
	OperationID OperationID
	// Version is the exact definition version.
	Version uint
	// Attempt is the lifetime attempt number; registration uses zero.
	Attempt uint
	// From is the source state; registration starts from Pending.
	From State
	// To is the resulting state, including administrative eligibility changes.
	To State
	// At is the recorded transition time, using the store time authority.
	At time.Time
	// Owner records transition ownership even when the projection subsequently clears it.
	Owner string
	// Fencing records the generation at the transition.
	Fencing uint64
	// Actor is bounded sanitized attribution, not an authentication proof.
	Actor string
	// Reason is bounded sanitized audit explanation.
	Reason string
}

// ClaimRequest selects the first eligible operation in deterministic plan order.
type ClaimRequest struct {
	// Candidates are the exact definitions present in the claiming binary.
	Candidates []ClaimCandidate
	// OperationIDs is retained for callers that do not operate mixed binaries.
	// Fleet runners use Candidates so an old pod cannot claim a newer version.
	OperationIDs []OperationID
	// Owner must preserve its exact bytes under sanitization and fit DefaultMaxActorBytes.
	Owner string
	// Now is the memory-store time authority; PostgreSQL ignores it in favor of server time.
	Now time.Time
	// LeaseDuration is positive; PostgreSQL truncates it to milliseconds and requires at least one millisecond.
	LeaseDuration time.Duration
}

// ClaimCandidate pins one locally executable operation definition.
type ClaimCandidate struct {
	// ID selects an operation in the request order.
	ID OperationID
	// Version pins a definition; zero is the legacy latest-version selector.
	Version uint
	// Checksum pins the declared digest; empty leaves it unconstrained for legacy callers.
	Checksum string
	// Channel constrains the durable group; empty leaves it unconstrained.
	Channel string
}

// Ownership is the proof required for attempt transitions.
type Ownership struct {
	// OperationID identifies the owned operation.
	OperationID OperationID
	// Version selects the exact ledger definition.
	Version uint
	// Owner must equal the accepted claim identity.
	Owner string
	// Fencing must equal the current claim token; stale proofs cannot mutate ownership.
	Fencing uint64
}

// Claim contains one newly-owned durable attempt.
type Claim struct {
	// Attempt is the newly accepted lifetime invocation and proof.
	Attempt Attempt
	// Until is the provider-authoritative lease expiry.
	Until time.Time
	// Budget is atomic replay-epoch usage for this claim, not an independent retry allowance.
	Budget RetryBudget
}

// RetryBudget is the durable policy usage for the current replay epoch.
// Attempt includes the newly claimed attempt; Exceptions includes only prior
// explicitly retryable failures.
type RetryBudget struct {
	// Attempt includes the newly claimed attempt in the replay epoch.
	Attempt uint
	// Exceptions includes only preceding typed retry failures in that epoch.
	Exceptions uint
}

// Ownership returns the transition proof for this claim.
func (claim Claim) Ownership() Ownership {
	return Ownership{OperationID: claim.Attempt.OperationID, Version: claim.Attempt.Version, Owner: claim.Attempt.Owner, Fencing: claim.Attempt.Fencing}
}

// Completion records a terminal or retryable attempt outcome.
type Completion struct {
	Ownership
	// From is the fenced source state. Zero retains the historical Running default.
	From State
	// State is the target attempt outcome, validated against From.
	State State
	// At is the memory transition time; PostgreSQL records server time instead.
	At time.Time
	// EligibleAt supplies the next claim instant for Retryable or Deferred.
	EligibleAt time.Time
	// ErrorDetail is sanitized and bounded; direct callers must omit arbitrary secret formats.
	ErrorDetail string
	// Output is defensively prepared; runners discard output when callbacks fail.
	Output Output
	// Actor defaults to Owner when empty and is sanitized with a raw byte bound.
	Actor string
	// Reason defaults to completed when empty and is sanitized with a raw byte bound.
	Reason string
	// RetryException must be true for Retryable, and may also mark Failed or DeadLettered
	// when retry exhaustion terminates an explicitly retryable failure.
	RetryException bool
}

// ResetRequest is an explicit, attributable replay authorization.
type ResetRequest struct {
	// OperationID selects the exact replay target.
	OperationID OperationID
	// Version selects its immutable definition.
	Version uint
	// Actor must be nonempty bounded attributable text.
	Actor string
	// Reason must be a nonempty bounded explanation for replay.
	Reason string
	// At must not regress memory ledger time; PostgreSQL reset uses server time instead.
	At time.Time
}

const (
	// DefaultMaxActorBytes bounds an administrative principal in one request.
	DefaultMaxActorBytes = 255
	// DefaultMaxReasonBytes bounds an administrative explanation in one request.
	DefaultMaxReasonBytes = 4 << 10
)

// ReconcileResolution is an explicit decision about an indeterminate attempt.
type ReconcileResolution uint8

const (
	// ReconcileSucceeded records that the exact indeterminate attempt succeeded.
	ReconcileSucceeded ReconcileResolution = iota + 1
	// ReconcileRetry authorizes another attempt without resetting its retry epoch.
	ReconcileRetry
	// ReconcileFailed records that the exact indeterminate attempt failed.
	ReconcileFailed
)

// String returns stable administrative decision text.
func (resolution ReconcileResolution) String() string {
	switch resolution {
	case ReconcileSucceeded:
		return "succeeded"
	case ReconcileRetry:
		return "retry"
	case ReconcileFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ReconcileRequest is an attributed resolution of one unknown durable result.
type ReconcileRequest struct {
	// OperationID selects the indeterminate operation.
	OperationID OperationID
	// Version selects its immutable definition.
	Version uint
	// Attempt must match the exact lifetime attempt being resolved.
	Attempt uint
	// Fencing must match that attempt generation.
	Fencing uint64
	// Resolution explicitly confirms success, authorizes retry or records definite failure.
	Resolution ReconcileResolution
	// Actor must be nonempty bounded attributable text.
	Actor string
	// Reason must be a nonempty bounded explanation of the external-effect decision.
	Reason string
	// At must be nonzero and no earlier than the current projection in both reference stores.
	At time.Time
}

// Store is the transactional durability boundary used by runners and workers.
// Implementations must serialize claims, fenced transitions and administrative
// resets, honor contexts, and preserve projection/attempt/audit atomicity.
// Reference stores differ in time authority and absent-time representation;
// this interface does not make application effects atomic with ledger updates.
// Reset must reject a new forward generation while any operation compensating
// that exact ID, version, and checksum has active or replayable ownership. A
// compensation replay must remain bound to the same forward fencing generation.
type Store interface {
	// Register atomically inserts a batch of immutable definitions or rejects drift.
	// It must not replace existing attempt or audit history.
	Register(context.Context, []Registration, time.Time) error
	// ClaimNext atomically chooses dependency-ready work in candidate order and
	// returns a new lifetime attempt, fencing token and durable replay-epoch budget.
	// Exact Candidates must not cross into another binary version or checksum.
	ClaimNext(context.Context, ClaimRequest) (Claim, error)
	// MarkRunning requires current unexpired ownership and records the attempt start boundary.
	MarkRunning(context.Context, Ownership, time.Time) (AttemptRecord, error)
	// Complete atomically settles projection, attempt and audit under current unexpired ownership.
	// A commit acknowledgement failure may be ErrUnknownResult, not proof that nothing committed.
	Complete(context.Context, Completion) error
	// RecoverExpired settles at most DefaultRecoveryBatchSize expired leases.
	RecoverExpired(context.Context, time.Time) (int, error)
	// Snapshot returns one current projection; reference stores return independent metadata.
	Snapshot(context.Context, OperationID, uint) (Record, error)
	// History returns the latest limit attempts in ascending attempt order,
	// with limit in [1, DefaultMaxHistory].
	History(context.Context, OperationID, uint, int) ([]AttemptRecord, error)
	// Audit returns the latest limit events in append order, with the same limit range.
	Audit(context.Context, OperationID, uint, int) ([]AuditEvent, error)
	// Reset returns ErrResetForbidden while a related compensation is claimed,
	// running, retryable, deferred, indeterminate, or eligible after an attempt,
	// and when a compensation replay no longer matches its forward generation.
	Reset(context.Context, ResetRequest) error
}

// LeaseStore extends Store with fenced renewal for long-lived attempts.
// Renewal proves only that ownership is current; it does not prove that an
// external side effect is still running or can be canceled.
type LeaseStore interface {
	Store
	// RenewLease compares current claimed/running proof and extends without shortening expiry.
	// Providers must honor context cancellation and reject expired or stale ownership.
	RenewLease(context.Context, Ownership, time.Time, time.Duration) (time.Time, error)
}

// ReconciliationStore extends Store with explicit unknown-outcome resolution.
type ReconciliationStore interface {
	Store
	// ResolveUnknown atomically applies one attributed decision to the exact indeterminate
	// attempt and fencing token; stale or repeated decisions fail closed.
	ResolveUnknown(context.Context, ReconcileRequest) error
}

// SanitizePersistenceText redacts common credential forms, removes control
// characters, and applies a byte bound. Work is bounded to maximum plus a
// fixed credential-pattern lookahead. Applications must still avoid passing
// arbitrary payloads, stack traces, and unknown secret formats.
func SanitizePersistenceText(value string, maximum int) string {
	maximum = max(maximum, 0)
	if maximum == 0 {
		return ""
	}
	limit := min(len(value), maximum)
	value = value[:limit+min(len(value)-limit, persistenceSanitizerLookaheadBytes)]
	value = bearerCredentialPattern.ReplaceAllString(value, "Bearer [REDACTED]")
	value = namedCredentialPattern.ReplaceAllString(value, "$1=[REDACTED]")
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	var bounded strings.Builder
	bounded.Grow(min(len(value), maximum))
	for _, character := range value {
		width := utf8.RuneLen(character)
		if bounded.Len()+width > maximum {
			break
		}
		bounded.WriteRune(character)
	}
	return bounded.String()
}
