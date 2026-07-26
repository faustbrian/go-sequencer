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
)

const (
	// DefaultMaxOperations bounds one immutable plan.
	DefaultMaxOperations = 10_000
	// DefaultMaxDependencies bounds direct dependencies per operation.
	DefaultMaxDependencies = 256
	// DefaultMaxTags bounds tags per operation.
	DefaultMaxTags = 64
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

// ExecutionMode controls whether an operation may be replayed normally.
type ExecutionMode uint8

const (
	// OneTime permits one successful execution for an identifier and version.
	OneTime ExecutionMode = iota + 1
	// Repeatable permits explicitly requested executions after success.
	Repeatable
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
	ID           OperationID
	Version      uint
	Checksum     string
	Description  string
	Tags         []string
	Channel      string
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
	if !identifierPattern.MatchString(string(spec.ID)) || spec.Version == 0 ||
		spec.Checksum == "" || spec.Description == "" || spec.Channel == "" ||
		spec.Handler == nil || spec.Policy.MaxAttempts == 0 || spec.Policy.MaxExceptions == 0 ||
		spec.Policy.MaxExceptions > spec.Policy.MaxAttempts ||
		(spec.Policy.Mode != OneTime && spec.Policy.Mode != Repeatable) ||
		spec.Policy.Timeout <= 0 || len(spec.Dependencies) > DefaultMaxDependencies ||
		len(spec.Tags) > DefaultMaxTags {
		return Operation{}, ErrInvalidOperation
	}
	seen := make(map[OperationID]struct{}, len(spec.Dependencies))
	for _, dependency := range spec.Dependencies {
		if dependency == spec.ID || !identifierPattern.MatchString(string(dependency)) {
			return Operation{}, fmt.Errorf("%w: invalid dependency %q", ErrInvalidOperation, dependency)
		}
		if _, duplicate := seen[dependency]; duplicate {
			return Operation{}, fmt.Errorf("%w: duplicate dependency %q", ErrInvalidOperation, dependency)
		}
		seen[dependency] = struct{}{}
	}
	return Operation{spec: cloneSpec(spec)}, nil
}

// Spec returns a defensive copy of the operation definition.
func (operation Operation) Spec() OperationSpec { return cloneSpec(operation.spec) }

func cloneSpec(spec OperationSpec) OperationSpec {
	spec.Tags = slices.Clone(spec.Tags)
	spec.Dependencies = slices.Clone(spec.Dependencies)
	spec.Environments = slices.Clone(spec.Environments)
	return spec
}
