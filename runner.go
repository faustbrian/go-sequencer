package sequencer

import (
	"context"
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
)

// Event carries bounded execution metadata to an observer.
type Event struct {
	Type      EventType
	Operation OperationID
	Attempt   uint
	State     State
	At        time.Time
	Err       error
}

// Observer receives synchronous lifecycle notifications.
type Observer interface{ Observe(Event) }

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(Event)

// Observe invokes the adapted function synchronously.
func (function ObserverFunc) Observe(event Event) { function(event) }

// RunnerOptions configures bounded synchronous execution.
type RunnerOptions struct {
	Owner         string
	Environment   string
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
	Attempts    uint
	State       State
	Err         error
}

// Report is the complete synchronous execution result.
type Report struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Result     RunResult
	Operations []OperationResult
}

// Runner coordinates durable claims and local handlers without hidden workers.
type Runner struct {
	plan    *Plan
	store   Store
	options RunnerOptions
}

// NewRunner validates execution dependencies and declared constraints.
func NewRunner(plan *Plan, store Store, options RunnerOptions) (*Runner, error) {
	if plan == nil || store == nil || options.Owner == "" {
		return nil, ErrInvalidRunner
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = DefaultLeaseDuration
	}
	if options.LeaseDuration < 0 || len(options.Observers) > 128 {
		return nil, ErrInvalidRunner
	}
	options.Observers = slices.Clone(options.Observers)
	for _, operation := range plan.operations {
		spec := operation.spec
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
	return &Runner{plan: plan, store: store, options: options}, nil
}

// Execute runs the immutable plan synchronously under durable ownership.
func (runner *Runner) Execute(ctx context.Context) (Report, error) {
	report := Report{StartedAt: runner.options.Clock.Now(), Result: RunSucceeded}
	registrations := make([]Registration, 0, len(runner.plan.operations))
	for _, operation := range runner.plan.operations {
		registrations = append(registrations, Registration{
			ID: operation.spec.ID, Version: operation.spec.Version,
			Checksum:     operation.spec.Checksum,
			Dependencies: slices.Clone(operation.spec.Dependencies),
		})
	}
	if err := runner.store.Register(ctx, registrations, report.StartedAt); err != nil {
		return report, err
	}

	for _, operation := range runner.plan.operations {
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
	result := OperationResult{OperationID: spec.ID, Version: spec.Version}
	if record, err := runner.store.Snapshot(ctx, spec.ID, spec.Version); err == nil && record.State == Succeeded && spec.Policy.Mode == OneTime {
		result.State = Succeeded
		result.Attempts = record.AttemptNumber
		return result, nil
	}
	exceptions := uint(0)
	for attemptsThisRun := uint(1); ; attemptsThisRun++ {
		now := runner.options.Clock.Now()
		claim, err := runner.store.ClaimNext(ctx, ClaimRequest{
			OperationIDs: []OperationID{spec.ID}, Owner: runner.options.Owner,
			Now: now, LeaseDuration: runner.options.LeaseDuration,
		})
		if err != nil {
			result.Err = err
			return result, err
		}
		result.Attempts = claim.Attempt.Number
		runner.observe(Event{Type: EventClaimed, Operation: spec.ID, Attempt: claim.Attempt.Number, State: Claimed, At: now})
		if _, err := runner.store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
			result.Err = err
			return result, err
		}
		runner.observe(Event{Type: EventRunning, Operation: spec.ID, Attempt: claim.Attempt.Number, State: Running, At: now})

		var output Output
		var executionErr error
		var actor, reason string
		if spec.Policy.RequiresApproval {
			approval, approvalErr := runner.options.Approver.Approve(ctx, cloneSpec(spec))
			actor, reason = approval.Actor, approval.Reason
			if approvalErr != nil || !approval.Approved || actor == "" || reason == "" {
				if approvalErr == nil {
					approvalErr = ErrBlocked
				}
				executionErr = Block(approvalErr)
			}
		}
		if executionErr == nil {
			output, reason, executionErr = runner.invoke(ctx, spec, claim.Attempt)
			if reason != "" {
				actor = "condition"
			}
		}
		if executionErr == nil {
			output, executionErr = prepareOutput(output)
		}
		if executionErr != nil && !errors.Is(executionErr, ErrSkipped) && !errors.Is(executionErr, ErrBlocked) {
			exceptions++
		}
		state := classifyState(executionErr, attemptsThisRun, spec.Policy.MaxAttempts, exceptions, spec.Policy.MaxExceptions)
		completion := Completion{
			Ownership: claim.Ownership(), State: state,
			At: runner.options.Clock.Now(), Output: output,
			Actor: actor, Reason: reason,
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
		runner.observe(Event{Type: EventCompleted, Operation: spec.ID, Attempt: claim.Attempt.Number, State: state, At: completion.At, Err: executionErr})
		if state == Retryable {
			continue
		}
		if state == Succeeded || state == Skipped {
			return result, nil
		}
		return result, executionErr
	}
}

func (runner *Runner) invoke(ctx context.Context, spec OperationSpec, attempt Attempt) (Output, string, error) {
	if spec.Policy.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, spec.Policy.Timeout)
		defer cancel()
	}
	if !spec.Policy.WithinTransaction {
		return executeAttempt(ctx, spec, attempt)
	}
	var output Output
	var reason string
	err := runner.options.Transactions.Within(ctx, func(transactionContext context.Context, transaction any) error {
		attempt.Transaction = transaction
		var err error
		output, reason, err = executeAttempt(transactionContext, spec, attempt)
		return err
	})
	return output, reason, err
}

func executeAttempt(ctx context.Context, spec OperationSpec, attempt Attempt) (output Output, reason string, err error) {
	defer func() {
		if recover() != nil {
			output = Output{}
			reason = ""
			err = Permanent(errors.New("sequencer: handler panic"))
		}
	}()
	if spec.Condition != nil {
		decision, conditionErr := spec.Condition.Evaluate(ctx, attempt)
		if conditionErr != nil {
			return Output{}, "", Permanent(conditionErr)
		}
		if !decision.Run {
			if decision.Reason == "" {
				decision.Reason = "condition declined execution"
			}
			return Output{}, decision.Reason, Skip(ErrSkipped)
		}
	}
	output, err = spec.Handler.Handle(ctx, attempt)
	return output, "", err
}

func classifyState(err error, attempt, maximum, exceptions, maxExceptions uint) State {
	if err == nil {
		return Succeeded
	}
	switch {
	case errors.Is(err, ErrSkipped):
		return Skipped
	case errors.Is(err, ErrBlocked):
		return Blocked
	case errors.Is(err, context.Canceled), errors.Is(err, ErrCanceled):
		return Canceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrTimeout):
		return Failed
	case errors.Is(err, ErrRetryable) && attempt < maximum && exceptions < maxExceptions:
		return Retryable
	default:
		return Failed
	}
}

func persistentErrorDetail(err error) string {
	switch {
	case errors.Is(err, ErrRetryable):
		return ErrRetryable.Error()
	case errors.Is(err, ErrSkipped):
		return ErrSkipped.Error()
	case errors.Is(err, ErrBlocked):
		return ErrBlocked.Error()
	case errors.Is(err, context.Canceled), errors.Is(err, ErrCanceled):
		return ErrCanceled.Error()
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrTimeout):
		return ErrTimeout.Error()
	case errors.Is(err, ErrUnknownResult):
		return ErrUnknownResult.Error()
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
		metadata[SanitizePersistenceText(key, 128)] = SanitizePersistenceText(value, 4_096)
	}
	if output.Metadata != nil {
		output.Metadata = metadata
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
