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
	// ErrNoEligibleOperation reports that no dependency-ready work exists.
	ErrNoEligibleOperation = errors.New("sequencer: no eligible operation")
	// ErrNotFound reports an unknown operation version.
	ErrNotFound = errors.New("sequencer: operation not found")
	// ErrResetForbidden reports an invalid or unattributed replay request.
	ErrResetForbidden = errors.New("sequencer: reset forbidden")
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
	ID           OperationID
	Version      uint
	Checksum     string
	Dependencies []OperationID
}

// Record is the current-state projection for one operation version.
type Record struct {
	Registration
	State          State
	AttemptNumber  uint
	Owner          string
	Fencing        uint64
	LeaseExpiresAt time.Time
	EligibleAt     time.Time
	UpdatedAt      time.Time
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
	OperationIDs  []OperationID
	Owner         string
	Now           time.Time
	LeaseDuration time.Duration
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
}

// Ownership returns the transition proof for this claim.
func (claim Claim) Ownership() Ownership {
	return Ownership{OperationID: claim.Attempt.OperationID, Version: claim.Attempt.Version, Owner: claim.Attempt.Owner, Fencing: claim.Attempt.Fencing}
}

// Completion records a terminal or retryable attempt outcome.
type Completion struct {
	Ownership
	State       State
	At          time.Time
	EligibleAt  time.Time
	ErrorDetail string
	Output      Output
	Actor       string
	Reason      string
}

// ResetRequest is an explicit, attributable replay authorization.
type ResetRequest struct {
	OperationID OperationID
	Version     uint
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
	RecoverExpired(context.Context, time.Time) (int, error)
	Snapshot(context.Context, OperationID, uint) (Record, error)
	History(context.Context, OperationID, uint, int) ([]AttemptRecord, error)
	Audit(context.Context, OperationID, uint, int) ([]AuditEvent, error)
	Reset(context.Context, ResetRequest) error
}

// SanitizePersistenceText removes control characters and applies a byte bound.
// Applications should pass pre-redacted summaries; arbitrary errors, payloads,
// stack traces, and secrets must not be persisted.
func SanitizePersistenceText(value string, maximum int) string {
	if maximum <= 0 {
		return ""
	}
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= maximum && utf8.ValidString(value) {
		return value
	}
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
