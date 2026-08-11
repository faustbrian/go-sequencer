package sequencer

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

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
func Permanent(cause error) error { return classify(ErrPermanent, cause) }

// Retry wraps a transient failure eligible for another bounded attempt.
func Retry(cause error) error { return classify(ErrRetryable, cause) }

// Skip wraps the reason an operation intentionally did no work.
func Skip(cause error) error { return classify(ErrSkipped, cause) }

// Block wraps the reason an operation requires external intervention.
func Block(cause error) error { return classify(ErrBlocked, cause) }

// UnknownResult wraps an error whose durable outcome cannot be determined.
func UnknownResult(cause error) error { return classify(ErrUnknownResult, cause) }

// RollbackFailure wraps a failed compensation operation.
func RollbackFailure(cause error) error { return classify(ErrRollback, cause) }

// Registration is durable operation identity and dependency metadata.
type Registration struct {
	ID             OperationID
	Version        uint
	Checksum       string
	Channel        string
	DependencyRefs []DependencyRef
	Compensates    *DependencyRef
	UnknownOutcome UnknownOutcomePolicy
	DeadLetter     bool
	// Dependencies is retained for source compatibility and rejected when non-empty.
	Dependencies []OperationID
}

// Record is the current-state projection for one operation version.
type Record struct {
	Registration
	State           State
	AttemptNumber   uint
	RunAttempt      uint
	RetryExceptions uint
	Owner           string
	Fencing         uint64
	LeaseExpiresAt  time.Time
	EligibleAt      time.Time
	UpdatedAt       time.Time
}

// AttemptRecord is the durable summary of one execution attempt.
type AttemptRecord struct {
	Attempt
	State       State
	CompletedAt time.Time
	ErrorDetail string
	Output      Output
}

// AuditEvent is one append-only state or administration record.
type AuditEvent struct {
	OperationID OperationID
	Version     uint
	Attempt     uint
	From        State
	To          State
	At          time.Time
	Owner       string
	Fencing     uint64
	Actor       string
	Reason      string
}

// ClaimRequest selects the first eligible operation in deterministic plan order.
type ClaimRequest struct {
	// Candidates are the exact definitions present in the claiming binary.
	Candidates []ClaimCandidate
	// OperationIDs is retained for callers that do not operate mixed binaries.
	// Fleet runners use Candidates so an old pod cannot claim a newer version.
	OperationIDs  []OperationID
	Owner         string
	Now           time.Time
	LeaseDuration time.Duration
}

// ClaimCandidate pins one locally executable operation definition.
type ClaimCandidate struct {
	ID       OperationID
	Version  uint
	Checksum string
	Channel  string
}

// Ownership is the proof required for attempt transitions.
type Ownership struct {
	OperationID OperationID
	Version     uint
	Owner       string
	Fencing     uint64
}

// Claim contains one newly-owned durable attempt.
type Claim struct {
	Attempt Attempt
	Until   time.Time
	Budget  RetryBudget
}

// RetryBudget is the durable policy usage for the current replay epoch.
// Attempt includes the newly claimed attempt; Exceptions includes only prior
// explicitly retryable failures.
type RetryBudget struct {
	Attempt    uint
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
	From           State
	State          State
	At             time.Time
	EligibleAt     time.Time
	ErrorDetail    string
	Output         Output
	Actor          string
	Reason         string
	RetryException bool
}

// ResetRequest is an explicit, attributable replay authorization.
type ResetRequest struct {
	OperationID OperationID
	Version     uint
	Actor       string
	Reason      string
	At          time.Time
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
	OperationID OperationID
	Version     uint
	Attempt     uint
	Fencing     uint64
	Resolution  ReconcileResolution
	Actor       string
	Reason      string
	At          time.Time
}

// Store is the transactional durability boundary used by runners and workers.
type Store interface {
	Register(context.Context, []Registration, time.Time) error
	ClaimNext(context.Context, ClaimRequest) (Claim, error)
	MarkRunning(context.Context, Ownership, time.Time) (AttemptRecord, error)
	Complete(context.Context, Completion) error
	// RecoverExpired settles at most DefaultRecoveryBatchSize expired leases.
	RecoverExpired(context.Context, time.Time) (int, error)
	Snapshot(context.Context, OperationID, uint) (Record, error)
	History(context.Context, OperationID, uint, int) ([]AttemptRecord, error)
	Audit(context.Context, OperationID, uint, int) ([]AuditEvent, error)
	Reset(context.Context, ResetRequest) error
}

// LeaseStore extends Store with fenced renewal for long-lived attempts.
// Renewal proves only that ownership is current; it does not prove that an
// external side effect is still running or can be canceled.
type LeaseStore interface {
	Store
	RenewLease(context.Context, Ownership, time.Time, time.Duration) (time.Time, error)
}

// ReconciliationStore extends Store with explicit unknown-outcome resolution.
type ReconciliationStore interface {
	Store
	ResolveUnknown(context.Context, ReconcileRequest) error
}

// SanitizePersistenceText removes control characters and applies a byte bound.
// Applications should pass pre-redacted summaries; arbitrary errors, payloads,
// stack traces, and secrets must not be persisted.
func SanitizePersistenceText(value string, maximum int) string {
	maximum = max(maximum, 0)
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
