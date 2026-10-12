// Package sequencer plans and executes durable, dependency-ordered application
// operations. It is not a numeric sequence allocator or a schema migration engine.
//
// CompilePlan copies definition metadata and orders exact dependency identities;
// handlers and other interface collaborators remain application-owned. NewRunner
// executes selected channels synchronously; NewFleet owns one leaderless Run
// lifecycle. Constructors start no background work. Collaborators must remain
// valid for concurrent callbacks and honor contexts; no constructor detects all
// typed-nil implementations or makes mutable callback state immutable.
//
// Stores own fenced claims, attempt history and audit transitions, not external
// effects. A lease or cancellation cannot stop a paused process from writing;
// protected resources must check fencing. Application transactions are separate
// from ledger claim/completion, and an ambiguous effect requires reconciliation
// or application-owned idempotency before replay. In-process returned errors may
// retain sensitive causes; runners persist classifications and observers receive
// classifications rather than those original causes.
//
// Use non-nil contexts. Arbitrary callbacks cannot be preempted: an unresponsive
// callback can retain a quarantined goroutine, and observer delivery has no join
// or flush contract. Callers own database pools, transports, process supervision
// and cleanup of borrowed collaborators.
package sequencer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
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
	// DefaultMaxChecksumBytes is the exact encoded length of one canonical
	// SHA-256 definition checksum.
	DefaultMaxChecksumBytes = len(sha256Prefix) + sha256.Size*2
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
	// DefaultMaxOutputBytes bounds raw summaries and the complete encoded persisted Output.
	DefaultMaxOutputBytes = 64 << 10
	// DefaultMaxErrorBytes bounds persisted sanitized error details.
	DefaultMaxErrorBytes = 16 << 10
	// DefaultMaxOutputMetadata bounds structured output entries.
	DefaultMaxOutputMetadata = 64
	// DefaultMaxHistory bounds one history or audit request.
	DefaultMaxHistory = 10_000
)

var identifierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,254}$`)

const sha256Prefix = "sha256:"

// ValidChecksum checks only the canonical sha256: plus 64 lowercase hex grammar.
// It does not authenticate source or compare a digest to an executable handler.
func ValidChecksum(value string) bool {
	if len(value) != len(sha256Prefix)+64 || !strings.HasPrefix(value, sha256Prefix) || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value[len(sha256Prefix):])
	return err == nil
}

// ChecksumBytes hashes the supplied bytes into canonical SHA-256 text.
// Selecting reviewed source bytes and binding them to a definition are caller responsibilities.
func ChecksumBytes(source []byte) string {
	digest := sha256.Sum256(source)
	return sha256Prefix + hex.EncodeToString(digest[:])
}

// OperationID is a stable identifier shared by code, the ledger, and audit logs.
type OperationID string

// Valid reports whether the identifier satisfies the stable lowercase,
// 255-byte operation identifier grammar.
func (id OperationID) Valid() bool { return identifierPattern.MatchString(string(id)) }

// DependencyRef pins one prerequisite to an exact durable definition.
type DependencyRef struct {
	// ID identifies the prerequisite operation.
	ID OperationID
	// Version is its exact nonzero definition version.
	Version uint
	// Checksum pins the canonical digest declared for that version.
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
	// Mode distinguishes ordinary one-time completion from explicit repeatable execution.
	Mode ExecutionMode
	// MaxAttempts bounds durable attempts in a replay epoch; it must be nonzero.
	MaxAttempts uint
	// MaxExceptions independently bounds explicitly retryable failures; it must be nonzero.
	MaxExceptions uint
	// Timeout is the positive deadline shared by approval, condition, transaction and handler callbacks.
	// It must be strictly shorter than the runner lease duration.
	Timeout time.Duration
	// WithinTransaction requires an injected manager for a local application transaction,
	// not an atomic transaction with ledger claim or completion.
	WithinTransaction bool
	// RequiresApproval asks the application approver before condition or handler execution.
	RequiresApproval bool
	// AllowedFailure permits a partial aggregate report; it does not satisfy dependencies.
	AllowedFailure bool
	// DeadLetter selects DeadLettered instead of Failed for definite terminal failures.
	DeadLetter bool
	// Cancellation controls fleet shutdown delivery, not the operation deadline.
	Cancellation CancellationMode
	// RetryMode chooses durable attempts or a single externally owned inline retry loop.
	RetryMode RetryMode
	// UnknownOutcome defaults to blocking expired unknown work; automatic replay
	// requires an application-owned idempotency guarantee.
	UnknownOutcome UnknownOutcomePolicy
}

// Output is application-selected summary data, not an automatically secret-free value.
// PreparePersistenceOutput bounds and redacts known forms before ledger storage;
// applications must exclude other credentials, personal data and payloads themselves.
type Output struct {
	// Summary is an intentionally non-secret result summary, bounded before persistence.
	Summary string
	// Metadata contains at most DefaultMaxOutputMetadata entries with non-secret values.
	// Persistence prepares a new map; callers must not mutate this map during preparation.
	Metadata map[string]string
}

// Attempt identifies one durable invocation and its ownership proof.
type Attempt struct {
	// OperationID identifies the claimed definition.
	OperationID OperationID
	// Version is the exact locally executable version.
	Version uint
	// Number is the lifetime attempt number, distinct from the replay-epoch budget.
	Number uint
	// Owner is the exact accepted fencing identity.
	Owner string
	// Fencing is the monotonically increasing claim token; protected external writes
	// must check it independently of the ledger.
	Fencing uint64
	// StartedAt is the claim timestamp supplied by the store.
	StartedAt time.Time
	// Transaction is a borrowed application handle only when WithinTransaction is enabled.
	// It is valid within the manager callback, not for later asynchronous work.
	Transaction any
	// Budget is non-nil only for InlineRetries. Every inline callback
	// execution must consume it.
	Budget *ExecutionBudget
}

// Handler executes one local attempt. Dependencies belong in the concrete
// handler value; the sequencer never performs global dependency lookup.
type Handler interface {
	// Handle performs the application effect with the attempt context and fencing proof.
	// Implementations own effect safety and cancellation cooperation; replay
	// idempotency is application-owned, not required for non-replayed effects.
	// Returning an error cannot prove that an external effect did not commit.
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
	// Run admits handler execution when true; false records a conditional skip.
	Run bool
	// Reason is bounded audit text for a skip, not a place for payloads or credentials.
	Reason string
}

// Condition decides whether a declared operation should run.
type Condition interface {
	// Evaluate runs before Handle within the same attempt deadline.
	// A false Run skips the handler; an error is a permanent condition failure.
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
	// ID is the stable lowercase identifier shared with the ledger.
	ID OperationID
	// Version is a nonzero immutable definition version, not an automatically allocated number.
	Version uint
	// Checksum is the caller-declared canonical SHA-256 definition identity.
	// Validation does not recompute a digest from Handler or authenticate its source.
	Checksum string
	// Description must be nonempty and at most DefaultMaxDescriptionBytes.
	Description string
	// Tags are copied metadata, limited by DefaultMaxTags and DefaultMaxTagBytes.
	Tags []string
	// Channel is a required identifier selecting an execution group, not a transport topic.
	Channel string
	// DependencyRefs pin prerequisites by exact ID, version and checksum, with no duplicate IDs.
	DependencyRefs []DependencyRef
	// Compensates identifies the exact forward operation related to this
	// independent operation. It must also be an explicit dependency.
	Compensates *DependencyRef
	// Dependencies is retained for source compatibility. Non-empty legacy
	// references are rejected because selecting a dependency by ID is unsafe
	// when multiple binary versions share a ledger.
	Dependencies []OperationID
	// Environments is a copied allowlist; empty permits every environment.
	Environments []string
	// Policy defines finite execution and replay behavior.
	Policy Policy
	// Condition is an optional borrowed callback, not a cloned collaborator.
	Condition Condition
	// Handler is a required borrowed callback; a typed-nil implementation is not detected.
	Handler Handler
}

// Operation contains validated immutable metadata and borrowed callback interfaces.
// Its zero value is not a validated definition.
type Operation struct{ spec OperationSpec }

// NewOperation validates identity, policies and collection bounds, then copies metadata.
// It returns no partial operation on error; Handler and Condition are borrowed,
// not cloned or authenticated by Checksum.
func NewOperation(spec OperationSpec) (Operation, error) {
	if !spec.ID.Valid() || spec.Version == 0 ||
		!ValidChecksum(spec.Checksum) ||
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
			dependency.Version == 0 || !ValidChecksum(dependency.Checksum) {
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
		spec.Compensates.Version == 0 || !ValidChecksum(spec.Compensates.Checksum)) {
		return Operation{}, fmt.Errorf("%w: invalid compensation dependency", ErrInvalidOperation)
	}
	return Operation{spec: cloneSpec(spec)}, nil
}

// Spec copies slices and the compensation reference; Handler and Condition still
// refer to the same application collaborators.
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
