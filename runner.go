package sequencer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
)

var (
	// ErrInvalidRunner reports invalid runner configuration.
	ErrInvalidRunner = errors.New("sequencer: invalid runner")
	// ErrEnvironmentForbidden reports an operation excluded from the environment.
	ErrEnvironmentForbidden = errors.New("sequencer: environment forbidden")
	// ErrApprovalRequired reports a missing approval provider.
	ErrApprovalRequired = errors.New("sequencer: approval provider required")
)

// DefaultLeaseDuration is the default upper bound for one claimed attempt.
const DefaultLeaseDuration = 5 * time.Minute

// Clock makes execution and recovery decisions deterministic.
type Clock interface{ Now() time.Time }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// TransactionManager scopes exactly one local attempt transaction.
type TransactionManager interface {
	Within(context.Context, func(context.Context, any) error) error
}

// Approval is an application-authorized and attributable execution decision.
type Approval struct {
	Approved bool
	Actor    string
	Reason   string
}

// Approver supplies application-owned authorization for declared operations.
type Approver interface {
	Approve(context.Context, OperationSpec) (Approval, error)
}

// EventType identifies a runner lifecycle boundary.
type EventType uint8

const (
	// EventClaimed reports durable ownership acquisition.
	EventClaimed EventType = iota + 1
	// EventRunning reports the start of handler execution.
	EventRunning
	// EventCompleted reports a durably recorded outcome.
	EventCompleted
	// EventHeartbeat reports a successful fenced lease renewal.
	EventHeartbeat
)

// Event carries bounded execution metadata to an observer.
type Event struct {
	Type      EventType
	Operation OperationID
	Channel   string
	Attempt   uint
	State     State
	At        time.Time
	Err       error
}

// Observer receives synchronous lifecycle notifications and must return
// promptly without network I/O. Fleet observers may be called concurrently
// for different accepted attempts.
type Observer interface{ Observe(Event) }

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(Event)

// Observe invokes the adapted function synchronously.
func (function ObserverFunc) Observe(event Event) { function(event) }

// RunnerOptions configures bounded synchronous execution.
type RunnerOptions struct {
	Owner         string
	Environment   string
	Channels      []string
	Clock         Clock
	LeaseDuration time.Duration
	Transactions  TransactionManager
	Approver      Approver
	Observers     []Observer
}

// RunResult summarizes the complete plan without hiding allowed failures.
type RunResult uint8

const (
	// RunSucceeded means every operation succeeded or was skipped.
	RunSucceeded RunResult = iota + 1
	// RunPartial means an allowed failure occurred.
	RunPartial
	// RunFailed means a required operation failed.
	RunFailed
)

// OperationResult reports one terminal operation decision.
type OperationResult struct {
	OperationID OperationID
	Version     uint
	Channel     string
	Attempts    uint
	State       State
	Err         error
}

// Report is the complete synchronous execution result.
type Report struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Result     RunResult
	Channels   []string
	Operations []OperationResult
}

// Runner coordinates durable claims and local handlers without hidden workers.
type Runner struct {
	plan    *Plan
	store   Store
	options RunnerOptions
}

func runsChannel(channels []string, channel string) bool {
	return len(channels) == 0 || slices.Contains(channels, channel)
}

func (runner *Runner) reportChannels() []string {
	if len(runner.options.Channels) != 0 {
		return slices.Clone(runner.options.Channels)
	}
	channels := make([]string, 0)
	seen := make(map[string]struct{})
	for _, operation := range runner.plan.operations {
		if _, exists := seen[operation.spec.Channel]; !exists {
			seen[operation.spec.Channel] = struct{}{}
			channels = append(channels, operation.spec.Channel)
		}
	}
	slices.Sort(channels)
	return channels
}

// NewRunner validates execution dependencies and declared constraints.
func NewRunner(plan *Plan, store Store, options RunnerOptions) (*Runner, error) {
	if plan == nil || store == nil || options.Owner == "" || len(options.Owner) > DefaultMaxActorBytes {
		return nil, ErrInvalidRunner
	}
	if options.LeaseDuration < 0 || len(options.Observers) > 128 {
		return nil, ErrInvalidRunner
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = DefaultLeaseDuration
	}
	options.Observers = slices.Clone(options.Observers)
	options.Channels = slices.Clone(options.Channels)
	slices.Sort(options.Channels)
	seenChannels := make(map[string]struct{}, len(options.Channels))
	for _, channel := range options.Channels {
		if !identifierPattern.MatchString(channel) {
			return nil, ErrInvalidRunner
		}
		if _, exists := seenChannels[channel]; exists {
			return nil, ErrInvalidRunner
		}
		seenChannels[channel] = struct{}{}
	}
	knownChannels := make(map[string]struct{})
	for _, operation := range plan.operations {
		knownChannels[operation.spec.Channel] = struct{}{}
	}
	for _, channel := range options.Channels {
		if _, exists := knownChannels[channel]; !exists {
			return nil, fmt.Errorf("%w: unknown channel %q", ErrInvalidRunner, channel)
		}
	}
	for _, operation := range plan.operations {
		spec := operation.spec
		if runsChannel(options.Channels, spec.Channel) {
			if len(spec.Environments) > 0 && !slices.Contains(spec.Environments, options.Environment) {
				return nil, fmt.Errorf("%w: %s", ErrEnvironmentForbidden, spec.ID)
			}
			if spec.Policy.RequiresApproval && options.Approver == nil {
				return nil, fmt.Errorf("%w: %s", ErrApprovalRequired, spec.ID)
			}
			if spec.Policy.WithinTransaction && options.Transactions == nil {
				return nil, fmt.Errorf("%w: transaction manager required for %s", ErrInvalidRunner, spec.ID)
			}
			if spec.Policy.Timeout >= options.LeaseDuration {
				return nil, fmt.Errorf("%w: timeout must be shorter than lease for %s", ErrInvalidRunner, spec.ID)
			}
		}
	}
	return &Runner{plan: plan, store: store, options: options}, nil
}

// Execute runs the immutable plan synchronously under durable ownership.
func (runner *Runner) Execute(ctx context.Context) (Report, error) {
	report := Report{StartedAt: runner.options.Clock.Now(), Result: RunSucceeded, Channels: runner.reportChannels()}
	registrations := make([]Registration, 0, len(runner.plan.operations))
	for _, operation := range runner.plan.operations {
		registrations = append(registrations, Registration{
			ID: operation.spec.ID, Version: operation.spec.Version,
			Checksum: operation.spec.Checksum, Channel: operation.spec.Channel,
			DependencyRefs: slices.Clone(operation.spec.DependencyRefs),
			Compensates:    operation.spec.Compensates, UnknownOutcome: operation.spec.Policy.UnknownOutcome,
			DeadLetter: operation.spec.Policy.DeadLetter,
		})
	}
	if err := runner.store.Register(ctx, registrations, report.StartedAt); err != nil {
		return report, err
	}

	for _, operation := range runner.plan.operations {
		if !runsChannel(runner.options.Channels, operation.spec.Channel) {
			continue
		}
		result, err := runner.executeOperation(ctx, operation)
		report.Operations = append(report.Operations, result)
		if err == nil {
			continue
		}
		if operation.spec.Policy.AllowedFailure {
			report.Result = RunPartial
			continue
		}
		report.Result = RunFailed
		report.FinishedAt = runner.options.Clock.Now()
		return report, err
	}
	report.FinishedAt = runner.options.Clock.Now()
	return report, nil
}

func (runner *Runner) executeOperation(ctx context.Context, operation Operation) (OperationResult, error) {
	spec := operation.spec
	result := OperationResult{OperationID: spec.ID, Version: spec.Version, Channel: spec.Channel}
	record, err := runner.store.Snapshot(ctx, spec.ID, spec.Version)
	if err != nil {
		result.Err = err
		return result, err
	}
	if record.State == Skipped || record.State == RolledBack || (record.State == Succeeded && spec.Policy.Mode == OneTime) {
		result.State = record.State
		result.Attempts = record.AttemptNumber
		return result, nil
	}
	if record.State == Succeeded && spec.Policy.Mode == Repeatable {
		resetAt := runner.options.Clock.Now()
		if resetAt.Before(record.UpdatedAt) {
			resetAt = record.UpdatedAt
		}
		if err := runner.store.Reset(ctx, ResetRequest{
			OperationID: spec.ID,
			Version:     spec.Version,
			Actor:       runner.options.Owner,
			Reason:      "repeatable execution requested",
			At:          resetAt,
		}); err != nil {
			result.State = record.State
			result.Attempts = record.AttemptNumber
			result.Err = err
			return result, err
		}
	}
	if !canClaimRecord(record.State, spec.Policy.Mode) {
		result.State = record.State
		result.Attempts = record.AttemptNumber
		result.Err = durableStateError(record.State)
		return result, result.Err
	}
	for {
		now := runner.options.Clock.Now()
		claim, err := runner.store.ClaimNext(ctx, ClaimRequest{
			Candidates: []ClaimCandidate{{ID: spec.ID, Version: spec.Version, Checksum: spec.Checksum, Channel: spec.Channel}}, Owner: runner.options.Owner,
			Now: now, LeaseDuration: runner.options.LeaseDuration,
		})
		if err != nil {
			result.Err = err
			return result, err
		}
		if claim.Budget.Attempt == 0 {
			result.Err = ErrInvalidOperation
			return result, result.Err
		}
		if claim.Attempt.OperationID != spec.ID || claim.Attempt.Version != spec.Version {
			result.Err = ErrDefinitionDrift
			return result, result.Err
		}
		result.Attempts = claim.Attempt.Number
		runner.observe(Event{Type: EventClaimed, Operation: spec.ID, Channel: spec.Channel, Attempt: claim.Attempt.Number, State: Claimed, At: now})
		if executionErr := attemptBudgetError(claim.Budget, spec.Policy); executionErr != nil {
			state := classifyState(executionErr, spec.Policy, claim.Budget.Attempt, claim.Budget.Exceptions)
			completion := Completion{
				Ownership: claim.Ownership(), From: Claimed, State: state,
				At: runner.options.Clock.Now(), ErrorDetail: persistentErrorDetail(executionErr),
			}
			if err := runner.store.Complete(ctx, completion); err != nil {
				result.Err = err
				return result, err
			}
			result.State, result.Err = state, executionErr
			runner.observe(Event{Type: EventCompleted, Operation: spec.ID, Channel: spec.Channel, Attempt: claim.Attempt.Number, State: state, At: completion.At, Err: executionErr})
			return result, executionErr
		}
		if _, err := runner.store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			result.Err = err
			return result, err
		}
		runner.observe(Event{Type: EventRunning, Operation: spec.ID, Channel: spec.Channel, Attempt: claim.Attempt.Number, State: Running, At: now})

		var output Output
		var actor, reason string
		output, actor, reason, executionErr := runner.runAttempt(ctx, spec, claim.Attempt)
		retryException := spec.Policy.RetryMode == DurableRetries && errors.Is(executionErr, ErrRetryable)
		exceptions := claim.Budget.Exceptions
		if retryException {
			exceptions = nextAttempt(exceptions)
		}
		state := classifyState(executionErr, spec.Policy, claim.Budget.Attempt, exceptions)
		completion := Completion{
			Ownership: claim.Ownership(), State: state,
			At: runner.options.Clock.Now(), Output: output,
			Actor: actor, Reason: reason, RetryException: retryException,
		}
		if executionErr != nil {
			completion.ErrorDetail = persistentErrorDetail(executionErr)
		}
		if state == Retryable {
			completion.EligibleAt = completion.At
		}
		if err := runner.store.Complete(ctx, completion); err != nil {
			result.Err = err
			return result, err
		}
		result.State, result.Err = state, executionErr
		runner.observe(Event{Type: EventCompleted, Operation: spec.ID, Channel: spec.Channel, Attempt: claim.Attempt.Number, State: state, At: completion.At, Err: executionErr})
		if state == Retryable {
			continue
		}
		if state == Succeeded || state == Skipped {
			return result, nil
		}
		return result, executionErr
	}
}

func canClaimRecord(state State, mode ExecutionMode) bool {
	return state == Eligible || state == Retryable || state == Deferred || (state == Succeeded && mode == Repeatable)
}

func durableStateError(state State) error {
	switch state {
	case Blocked:
		return ErrBlocked
	case Canceled:
		return ErrCanceled
	case Indeterminate:
		return ErrUnknownResult
	case Pending, Eligible, Claimed, Running, Succeeded, Skipped, Failed,
		Retryable, Deferred, RolledBack, DeadLettered:
		return ErrPermanent
	default:
		return ErrInvalidOperation
	}
}

func nextAttempt(current uint) uint {
	if current == ^uint(0) {
		return current
	}
	return current + 1
}

func (runner *Runner) runAttempt(ctx context.Context, spec OperationSpec, attempt Attempt) (Output, string, string, error) {
	var actor, reason string
	if spec.Policy.RequiresApproval {
		approval, err := runner.options.Approver.Approve(ctx, cloneSpec(spec))
		actor, reason = approval.Actor, approval.Reason
		if len(actor) > DefaultMaxActorBytes || len(reason) > DefaultMaxReasonBytes {
			return Output{}, "", "", Block(ErrResourceLimit)
		}
		if err != nil || !approval.Approved || actor == "" || reason == "" {
			return Output{}, actor, reason, Block(errors.Join(ErrBlocked, err))
		}
	}
	if spec.Policy.RetryMode == InlineRetries {
		attempt.Budget, _ = NewExecutionBudget(min(spec.Policy.MaxAttempts, spec.Policy.MaxExceptions))
	}
	output, conditionReason, err := runner.invoke(ctx, spec, attempt)
	if conditionReason != "" {
		actor, reason = "condition", conditionReason
	}
	if err != nil {
		return Output{}, actor, reason, err
	}
	output, err = prepareOutput(output)
	return output, actor, reason, err
}

func (runner *Runner) invoke(ctx context.Context, spec OperationSpec, attempt Attempt) (Output, string, error) {
	ctx, cancel := context.WithTimeout(ctx, spec.Policy.Timeout)
	defer cancel()
	if !spec.Policy.WithinTransaction {
		output, reason, err := executeAttempt(ctx, spec, attempt)
		return attemptContextOutcome(ctx, spec.Policy.Cancellation, output, reason, err)
	}
	var output Output
	var reason string
	var callbackErr error
	calls := 0
	contractErr := fmt.Errorf("%w: transaction manager contract violation", ErrInvalidRunner)
	managerPanicked := false
	var managerErr error
	func() {
		defer func() {
			if recover() != nil {
				managerPanicked = true
			}
		}()
		managerErr = runner.options.Transactions.Within(ctx, func(transactionContext context.Context, transaction any) error {
			calls++
			if calls != 1 {
				callbackErr = contractErr
				return callbackErr
			}
			if transactionContext == nil || transaction == nil {
				callbackErr = contractErr
				return callbackErr
			}
			attempt.Transaction = transaction
			output, reason, callbackErr = executeAttempt(transactionContext, spec, attempt)
			return callbackErr
		})
	}()
	var result Output
	var resultReason string
	var resultErr error
	switch {
	case calls == 0:
		resultErr = errors.Join(contractErr, managerErr)
	case calls != 1, managerPanicked:
		resultErr = UnknownResult(errors.Join(contractErr, callbackErr, managerErr))
	case callbackErr != nil && (managerErr == nil || !errors.Is(managerErr, callbackErr)):
		resultErr = UnknownResult(errors.Join(contractErr, callbackErr, managerErr))
	case callbackErr == nil && managerErr != nil:
		resultErr = UnknownResult(managerErr)
	default:
		result, resultReason, resultErr = output, reason, managerErr
	}
	return attemptContextOutcome(ctx, spec.Policy.Cancellation, result, resultReason, resultErr)
}

func attemptContextOutcome(ctx context.Context, cancellation CancellationMode, output Output, reason string, err error) (Output, string, error) {
	if errors.Is(err, ErrUnknownResult) || ctx.Err() == nil {
		return output, reason, err
	}
	if cancellation == CancellationDrainOnly {
		return Output{}, "", UnknownResult(ctx.Err())
	}
	return Output{}, "", ctx.Err()
}

func executeAttempt(ctx context.Context, spec OperationSpec, attempt Attempt) (output Output, reason string, err error) {
	defer func() {
		if recover() != nil {
			output = Output{}
			reason = ""
			err = UnknownResult(errors.New("sequencer: handler panic"))
		}
	}()
	if spec.Condition != nil {
		decision, conditionErr := spec.Condition.Evaluate(ctx, attempt)
		if conditionErr != nil {
			return Output{}, "", Permanent(conditionErr)
		}
		if !decision.Run {
			if len(decision.Reason) > DefaultMaxReasonBytes {
				return Output{}, "", Permanent(ErrResourceLimit)
			}
			if decision.Reason == "" {
				decision.Reason = "condition declined execution"
			}
			return Output{}, decision.Reason, Skip(ErrSkipped)
		}
	}
	output, err = spec.Handler.Handle(ctx, attempt)
	return output, "", err
}

func classifyState(err error, policy Policy, attempt, exceptions uint) State {
	if err == nil {
		return Succeeded
	}
	switch {
	case errors.Is(err, ErrSkipped):
		return Skipped
	case errors.Is(err, ErrBlocked):
		return Blocked
	case errors.Is(err, ErrUnknownResult):
		return Indeterminate
	case errors.Is(err, context.Canceled), errors.Is(err, ErrCanceled):
		return Canceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrTimeout):
		return terminalFailureState(policy)
	}
	if policy.RetryMode != DurableRetries {
		return terminalFailureState(policy)
	}
	if !errors.Is(err, ErrRetryable) {
		return terminalFailureState(policy)
	}
	if attempt >= policy.MaxAttempts || exceptions >= policy.MaxExceptions {
		return terminalFailureState(policy)
	}
	return Retryable
}

func attemptBudgetError(budget RetryBudget, policy Policy) error {
	if budget.Attempt > policy.MaxAttempts {
		return ErrBudgetExhausted
	}
	return nil
}

func terminalFailureState(policy Policy) State {
	if policy.DeadLetter {
		return DeadLettered
	}
	return Failed
}

func persistentErrorDetail(err error) string {
	switch {
	case errors.Is(err, ErrRetryable):
		return ErrRetryable.Error()
	case errors.Is(err, ErrSkipped):
		return ErrSkipped.Error()
	case errors.Is(err, ErrBlocked):
		return ErrBlocked.Error()
	case errors.Is(err, ErrUnknownResult):
		return ErrUnknownResult.Error()
	case errors.Is(err, context.Canceled), errors.Is(err, ErrCanceled):
		return ErrCanceled.Error()
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrTimeout):
		return ErrTimeout.Error()
	case errors.Is(err, ErrBudgetExhausted):
		return ErrBudgetExhausted.Error()
	default:
		return ErrPermanent.Error()
	}
}

func prepareOutput(output Output) (Output, error) {
	if len(output.Summary) > DefaultMaxOutputBytes || len(output.Metadata) > DefaultMaxOutputMetadata {
		return Output{}, ErrResourceLimit
	}
	output.Summary = SanitizePersistenceText(output.Summary, DefaultMaxOutputBytes)
	metadata := make(map[string]string, len(output.Metadata))
	for key, value := range output.Metadata {
		if key == "" || len(key) > 128 || len(value) > 4_096 {
			return Output{}, ErrResourceLimit
		}
		sanitizedKey := SanitizePersistenceText(key, 128)
		if _, duplicate := metadata[sanitizedKey]; duplicate {
			return Output{}, ErrResourceLimit
		}
		metadata[sanitizedKey] = SanitizePersistenceText(value, 4_096)
	}
	if output.Metadata != nil {
		output.Metadata = metadata
	}
	encoded, err := json.Marshal(output)
	if err != nil || len(encoded) > DefaultMaxOutputBytes {
		return Output{}, ErrResourceLimit
	}
	return output, nil
}

func (runner *Runner) observe(event Event) {
	for _, observer := range runner.options.Observers {
		if observer != nil {
			observer.Observe(event)
		}
	}
}
