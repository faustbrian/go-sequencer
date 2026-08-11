// Package sequencer plans and executes durable, explicitly ordered operations.
package sequencer

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"
)

var (
	// ErrInvalidOperation reports an incomplete or unsafe operation definition.
	ErrInvalidOperation = errors.New("sequencer: invalid operation")
	// ErrDuplicateOperation reports two definitions with the same identifier.
	ErrDuplicateOperation = errors.New("sequencer: duplicate operation")
	// ErrMissingDependency reports a dependency absent from a compiled plan.
	ErrMissingDependency = errors.New("sequencer: missing dependency")
	// ErrDependencyCycle reports a cycle in the operation graph.
	ErrDependencyCycle = errors.New("sequencer: dependency cycle")
	// ErrResourceLimit reports input beyond an explicit package bound.
	ErrResourceLimit = errors.New("sequencer: resource limit exceeded")
	// ErrUnpinnedDependency reports a dependency without an exact durable identity.
	ErrUnpinnedDependency = errors.New("sequencer: unpinned dependency")
)

const (
	// DefaultMaxOperations bounds one immutable plan.
	DefaultMaxOperations = 10_000
	// DefaultMaxDependencies bounds direct dependencies per operation.
	DefaultMaxDependencies = 256
	// DefaultMaxChecksumBytes bounds one reviewed definition checksum.
	DefaultMaxChecksumBytes = 512
	// DefaultMaxDescriptionBytes bounds one operation description.
	DefaultMaxDescriptionBytes = 4 << 10
	// DefaultMaxTags bounds tags per operation.
	DefaultMaxTags = 64
	// DefaultMaxTagBytes bounds one operation tag.
	DefaultMaxTagBytes = 255
	// DefaultMaxEnvironments bounds environment selectors per operation.
	DefaultMaxEnvironments = 64
	// DefaultMaxEnvironmentBytes bounds one environment selector.
	DefaultMaxEnvironmentBytes = 255
	// DefaultMaxGraphDepth bounds dependency traversal depth.
	DefaultMaxGraphDepth = 1_024
	// DefaultMaxOutputBytes bounds persisted output summaries.
	DefaultMaxOutputBytes = 64 << 10
	// DefaultMaxErrorBytes bounds persisted sanitized error details.
	DefaultMaxErrorBytes = 16 << 10
	// DefaultMaxOutputMetadata bounds structured output entries.
	DefaultMaxOutputMetadata = 64
	// DefaultMaxHistory bounds one history or audit request.
	DefaultMaxHistory = 10_000
)

var identifierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,254}$`)

// OperationID is a stable identifier shared by code, the ledger, and audit logs.
type OperationID string

// Valid reports whether the identifier satisfies the stable lowercase,
// 255-byte operation identifier grammar.
func (id OperationID) Valid() bool { return identifierPattern.MatchString(string(id)) }

// DependencyRef pins one prerequisite to an exact durable definition.
type DependencyRef struct {
	ID       OperationID
	Version  uint
	Checksum string
}

// ExecutionMode controls whether an operation may be replayed normally.
type ExecutionMode uint8

const (
	// OneTime permits one successful execution for an identifier and version.
	OneTime ExecutionMode = iota + 1
	// Repeatable permits explicitly requested executions after success.
	Repeatable
)

// CancellationMode defines whether process shutdown may cancel an accepted
// handler. It does not make cancellation capable of stopping external effects.
type CancellationMode uint8

const (
	// CancellationCooperative delivers shutdown cancellation to the handler.
	CancellationCooperative CancellationMode = iota
	// CancellationDrainOnly keeps the handler context alive while shutdown waits.
	// If the wait expires, the runner fails and Kubernetes must terminate the pod;
	// the durable lease later recovers the attempt with an unknown outcome.
	CancellationDrainOnly
)

// RetryMode selects the sole owner of an operation's retry loop.
type RetryMode uint8

const (
	// DurableRetries creates a new fenced ledger attempt for each retry.
	DurableRetries RetryMode = iota
	// InlineRetries delegates the only retry loop to a bounded handler adapter.
	InlineRetries
)

// UnknownOutcomePolicy controls whether lease recovery may authorize replay.
type UnknownOutcomePolicy uint8

const (
	// UnknownOutcomeBlock requires explicit reconciliation before replay.
	UnknownOutcomeBlock UnknownOutcomePolicy = iota
	// UnknownOutcomeReplayIdempotent declares that replay is protected by an
	// application-owned idempotency boundary.
	UnknownOutcomeReplayIdempotent
)

// Policy declares bounded execution and failure behavior.
type Policy struct {
	Mode              ExecutionMode
	MaxAttempts       uint
	MaxExceptions     uint
	Timeout           time.Duration
	WithinTransaction bool
	RequiresApproval  bool
	AllowedFailure    bool
	DeadLetter        bool
	Cancellation      CancellationMode
	RetryMode         RetryMode
	UnknownOutcome    UnknownOutcomePolicy
}

// Output is the bounded, non-secret result safe to retain in the ledger.
type Output struct {
	Summary  string
	Metadata map[string]string
}

// Attempt identifies one durable invocation and its ownership proof.
type Attempt struct {
	OperationID OperationID
	Version     uint
	Number      uint
	Owner       string
	Fencing     uint64
	StartedAt   time.Time
	Transaction any
	// Budget is non-nil only for InlineRetries. Every inline callback
	// execution must consume it.
	Budget *ExecutionBudget
}

// Handler executes one local attempt. Dependencies belong in the concrete
// handler value; the sequencer never performs global dependency lookup.
type Handler interface {
	Handle(context.Context, Attempt) (Output, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Attempt) (Output, error)

// Handle invokes the adapted function.
func (function HandlerFunc) Handle(ctx context.Context, attempt Attempt) (Output, error) {
	return function(ctx, attempt)
}

// Decision is an auditable conditional execution result.
type Decision struct {
	Run    bool
	Reason string
}

// Condition decides whether a declared operation should run.
type Condition interface {
	Evaluate(context.Context, Attempt) (Decision, error)
}

// ConditionFunc adapts a function to Condition.
type ConditionFunc func(context.Context, Attempt) (Decision, error)

// Evaluate invokes the adapted condition function.
func (function ConditionFunc) Evaluate(ctx context.Context, attempt Attempt) (Decision, error) {
	return function(ctx, attempt)
}

// OperationSpec is the complete declarative definition of an operation.
type OperationSpec struct {
	ID             OperationID
	Version        uint
	Checksum       string
	Description    string
	Tags           []string
	Channel        string
	DependencyRefs []DependencyRef
	// Compensates identifies the exact forward operation related to this
	// independent operation. It must also be an explicit dependency.
	Compensates *DependencyRef
	// Dependencies is retained for source compatibility. Non-empty legacy
	// references are rejected because selecting a dependency by ID is unsafe
	// when multiple binary versions share a ledger.
	Dependencies []OperationID
	Environments []string
	Policy       Policy
	Condition    Condition
	Handler      Handler
}

// Operation is an immutable validated operation.
type Operation struct{ spec OperationSpec }

// NewOperation validates and freezes a definition.
func NewOperation(spec OperationSpec) (Operation, error) {
	if !spec.ID.Valid() || spec.Version == 0 ||
		spec.Checksum == "" || len(spec.Checksum) > DefaultMaxChecksumBytes ||
		spec.Description == "" || len(spec.Description) > DefaultMaxDescriptionBytes ||
		!identifierPattern.MatchString(spec.Channel) ||
		spec.Handler == nil || spec.Policy.MaxAttempts == 0 || spec.Policy.MaxExceptions == 0 ||
		(spec.Policy.Mode != OneTime && spec.Policy.Mode != Repeatable) ||
		spec.Policy.Cancellation > CancellationDrainOnly ||
		spec.Policy.RetryMode > InlineRetries ||
		spec.Policy.UnknownOutcome > UnknownOutcomeReplayIdempotent ||
		spec.Policy.Timeout <= 0 || len(spec.DependencyRefs) > DefaultMaxDependencies ||
		len(spec.Tags) > DefaultMaxTags || len(spec.Environments) > DefaultMaxEnvironments {
		return Operation{}, ErrInvalidOperation
	}
	for _, tag := range spec.Tags {
		if tag == "" || len(tag) > DefaultMaxTagBytes {
			return Operation{}, ErrInvalidOperation
		}
	}
	for _, environment := range spec.Environments {
		if environment == "" || len(environment) > DefaultMaxEnvironmentBytes {
			return Operation{}, ErrInvalidOperation
		}
	}
	if len(spec.Dependencies) > 0 {
		return Operation{}, ErrUnpinnedDependency
	}
	seen := make(map[OperationID]struct{}, len(spec.DependencyRefs))
	compensationDependency := false
	for _, dependency := range spec.DependencyRefs {
		if dependency.ID == spec.ID || !dependency.ID.Valid() ||
			dependency.Version == 0 || dependency.Checksum == "" || len(dependency.Checksum) > DefaultMaxChecksumBytes {
			return Operation{}, fmt.Errorf("%w: invalid dependency %q", ErrInvalidOperation, dependency.ID)
		}
		if _, duplicate := seen[dependency.ID]; duplicate {
			return Operation{}, fmt.Errorf("%w: duplicate dependency %q", ErrInvalidOperation, dependency.ID)
		}
		seen[dependency.ID] = struct{}{}
		if spec.Compensates != nil && dependency == *spec.Compensates {
			compensationDependency = true
		}
	}
	if spec.Compensates != nil && (!compensationDependency || spec.Compensates.ID == spec.ID ||
		spec.Compensates.Version == 0 || spec.Compensates.Checksum == "") {
		return Operation{}, fmt.Errorf("%w: invalid compensation dependency", ErrInvalidOperation)
	}
	return Operation{spec: cloneSpec(spec)}, nil
}

// Spec returns a defensive copy of the operation definition.
func (operation Operation) Spec() OperationSpec { return cloneSpec(operation.spec) }

func cloneSpec(spec OperationSpec) OperationSpec {
	spec.Tags = slices.Clone(spec.Tags)
	spec.DependencyRefs = slices.Clone(spec.DependencyRefs)
	if spec.Compensates != nil {
		compensates := *spec.Compensates
		spec.Compensates = &compensates
	}
	spec.Dependencies = slices.Clone(spec.Dependencies)
	spec.Environments = slices.Clone(spec.Environments)
	return spec
}
