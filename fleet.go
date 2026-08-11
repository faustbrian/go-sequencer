package sequencer

import (
	"context"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"sync"
	"time"
)

var (
	// ErrInvalidFleet reports invalid long-running runner configuration.
	ErrInvalidFleet = errors.New("sequencer: invalid fleet runner")
	// ErrShutdownTimeout reports accepted attempts that outlived the shutdown bound.
	ErrShutdownTimeout = errors.New("sequencer: shutdown wait exceeded")
)

const (
	// DefaultClaimInterval bounds how often an idle runner polls for work.
	DefaultClaimInterval = time.Second
	// DefaultRenewInterval bounds how often owned leases are renewed.
	DefaultRenewInterval = DefaultLeaseDuration / 3
	// DefaultShutdownWait bounds graceful drain before the runner reports failure.
	DefaultShutdownWait = 30 * time.Second
	// DefaultMaxConcurrency bounds simultaneous accepted attempts per runner.
	DefaultMaxConcurrency = 1
	// MaxFleetConcurrency prevents configuration-sized worker allocations.
	MaxFleetConcurrency = 1_024
	// MaxClaimInterval keeps loss-of-readiness and recovery polling observable.
	MaxClaimInterval = time.Minute
	// MaxShutdownWait caps graceful termination within an operationally finite bound.
	MaxShutdownWait = 30 * time.Minute
)

// RunnerState is the observable lifecycle of a long-running fleet runner.
type RunnerState uint8

const (
	// RunnerStarting is validating and registering its local operation plan.
	RunnerStarting RunnerState = iota + 1
	// RunnerAccepting is ready and may claim eligible operations.
	RunnerAccepting
	// RunnerDraining has stopped claiming and is settling accepted attempts.
	RunnerDraining
	// RunnerStopped has settled all accepted attempts and owns no workers.
	RunnerStopped
	// RunnerFailed stopped accepting because a lifecycle or durability boundary failed.
	RunnerFailed
)

var runnerStateNames = map[RunnerState]string{
	RunnerStarting:  "starting",
	RunnerAccepting: "accepting",
	RunnerDraining:  "draining",
	RunnerStopped:   "stopped",
	RunnerFailed:    "failed",
}

// String returns stable runner lifecycle text.
func (state RunnerState) String() string {
	if name, ok := runnerStateNames[state]; ok {
		return name
	}
	return "unknown"
}

// FleetOptions configures one explicitly owned long-running runner lifecycle.
type FleetOptions struct {
	RunnerOptions
	ClaimInterval  time.Duration
	RenewInterval  time.Duration
	MaxConcurrency uint
	ShutdownWait   time.Duration
}

// Fleet claims locally registered operations without a leader. A stopped Run
// owns no goroutines. A shutdown-timeout or lease-loss failure requires the
// process manager to terminate any handler that could not cooperate.
type Fleet struct {
	plan       *Plan
	store      LeaseStore
	options    FleetOptions
	operations map[OperationID]Operation

	mu            sync.RWMutex
	state         RunnerState
	started       bool
	workers       sync.WaitGroup
	renewalStarts sync.WaitGroup
	renewals      sync.WaitGroup
}

type attemptExecutionResult struct {
	output Output
	actor  string
	reason string
	err    error
}

// NewFleet validates a long-running runner without starting background work.
func NewFleet(plan *Plan, store LeaseStore, options FleetOptions) (*Fleet, error) {
	if options.ClaimInterval < 0 || options.RenewInterval < 0 || options.ShutdownWait < 0 {
		return nil, ErrInvalidFleet
	}
	if options.ClaimInterval == 0 {
		options.ClaimInterval = DefaultClaimInterval
	}
	if options.MaxConcurrency == 0 {
		options.MaxConcurrency = DefaultMaxConcurrency
	}
	if options.ShutdownWait == 0 {
		options.ShutdownWait = DefaultShutdownWait
	}
	if options.ClaimInterval > MaxClaimInterval || options.ShutdownWait > MaxShutdownWait ||
		options.MaxConcurrency > MaxFleetConcurrency {
		return nil, ErrInvalidFleet
	}
	runner, err := NewRunner(plan, store, options.RunnerOptions)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFleet, err)
	}
	options.RunnerOptions = runner.options
	if options.RenewInterval == 0 {
		options.RenewInterval = options.LeaseDuration / 3
	}
	if options.RenewInterval <= 0 || options.RenewInterval >= options.LeaseDuration {
		return nil, ErrInvalidFleet
	}
	operations := make(map[OperationID]Operation, len(plan.operations))
	for _, operation := range plan.operations {
		operations[operation.spec.ID] = operation
	}
	return &Fleet{plan: runner.plan, store: store, options: options, operations: operations, state: RunnerStarting}, nil
}

// State returns the current observable lifecycle state.
func (fleet *Fleet) State() RunnerState {
	fleet.mu.RLock()
	defer fleet.mu.RUnlock()
	return fleet.state
}

// Ready reports whether Kubernetes may route work to this runner. Draining,
// stopped, and failed runners are never ready and cannot initiate new claims.
func (fleet *Fleet) Ready() bool { return fleet.State() == RunnerAccepting }

func (fleet *Fleet) setState(state RunnerState) {
	fleet.mu.Lock()
	fleet.state = state
	fleet.mu.Unlock()
}

func (fleet *Fleet) admissionOpen() bool {
	fleet.mu.RLock()
	defer fleet.mu.RUnlock()
	return fleet.state == RunnerAccepting
}

func (fleet *Fleet) beginRun() bool {
	fleet.mu.Lock()
	defer fleet.mu.Unlock()
	if fleet.started {
		return false
	}
	fleet.started = true
	return true
}

// Run registers the local binary plan, accepts leaderless claims, and drains
// accepted work after cancellation. Cancellation is treated as SIGTERM: the
// accepting state ends before attempt contexts are canceled.
func (fleet *Fleet) Run(ctx context.Context) error {
	if !fleet.beginRun() {
		return ErrInvalidTransition
	}
	registrations := make([]Registration, 0, len(fleet.plan.operations))
	candidates := make([]ClaimCandidate, 0, len(fleet.plan.operations))
	for _, operation := range fleet.plan.operations {
		registrations = append(registrations, Registration{
			ID: operation.spec.ID, Version: operation.spec.Version,
			Checksum: operation.spec.Checksum, Channel: operation.spec.Channel, DependencyRefs: slices.Clone(operation.spec.DependencyRefs),
			Compensates: operation.spec.Compensates, UnknownOutcome: operation.spec.Policy.UnknownOutcome, DeadLetter: operation.spec.Policy.DeadLetter,
		})
		if runsChannel(fleet.options.Channels, operation.spec.Channel) {
			candidates = append(candidates, ClaimCandidate{ID: operation.spec.ID, Version: operation.spec.Version, Checksum: operation.spec.Checksum, Channel: operation.spec.Channel})
		}
	}
	registerContext, cancelRegister := context.WithTimeout(ctx, fleet.options.ShutdownWait)
	registerErr := fleet.store.Register(registerContext, registrations, fleet.options.Clock.Now())
	cancelRegister()
	if registerErr != nil {
		if isRunCancellation(ctx, registerErr) {
			fleet.setState(RunnerDraining)
			fleet.setState(RunnerStopped)
			return nil
		}
		fleet.setState(RunnerFailed)
		return registerErr
	}

	workContext, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()
	renewalContext, cancelRenewals := context.WithCancel(context.Background())
	defer func() {
		cancelRenewals()
		fleet.renewalStarts.Wait()
		fleet.renewals.Wait()
	}()
	results := make(chan error, fleet.options.MaxConcurrency)
	active := uint64(0)
	fleet.setState(RunnerAccepting)

	for {
		if err := ctx.Err(); err != nil {
			fleet.setState(RunnerDraining)
			cancelWork()
			err := fleet.waitForDrain(results, active)
			cancelRenewals()
			return err
		}
		if active >= uint64(fleet.options.MaxConcurrency) {
			if err := fleet.waitForCapacity(ctx, results, &active); err != nil {
				if !isRunCancellation(ctx, err) {
					cancelWork()
					_ = fleet.waitForDrain(results, active)
					cancelRenewals()
					fleet.setState(RunnerFailed)
					return err
				}
				fleet.setState(RunnerDraining)
				cancelWork()
				err := fleet.waitForDrain(results, active)
				cancelRenewals()
				return err
			}
			continue
		}

		now := fleet.options.Clock.Now()
		recoveryContext, cancelRecovery := context.WithTimeout(ctx, fleet.options.ShutdownWait)
		_, recoveryErr := fleet.store.RecoverExpired(recoveryContext, now)
		cancelRecovery()
		if recoveryErr != nil {
			if isRunCancellation(ctx, recoveryErr) {
				fleet.setState(RunnerDraining)
				cancelWork()
				err := fleet.waitForDrain(results, active)
				cancelRenewals()
				return err
			}
			cancelWork()
			_ = fleet.waitForDrain(results, active)
			cancelRenewals()
			fleet.setState(RunnerFailed)
			return recoveryErr
		}
		if !fleet.admissionOpen() {
			cancelWork()
			err := fleet.waitForDrain(results, active)
			cancelRenewals()
			fleet.setState(RunnerFailed)
			return err
		}
		claimContext, cancelClaim := context.WithTimeout(ctx, fleet.options.ShutdownWait)
		claim, err := fleet.store.ClaimNext(claimContext, ClaimRequest{
			Candidates: candidates, Owner: fleet.options.Owner, Now: now,
			LeaseDuration: fleet.options.LeaseDuration,
		})
		cancelClaim()
		if err != nil {
			if isRunCancellation(ctx, err) {
				fleet.setState(RunnerDraining)
				cancelWork()
				err := fleet.waitForDrain(results, active)
				cancelRenewals()
				return err
			}
			if errors.Is(err, ErrNoEligibleOperation) {
				if err := fleet.waitForPoll(ctx, results, &active); err != nil {
					if !isRunCancellation(ctx, err) {
						cancelWork()
						_ = fleet.waitForDrain(results, active)
						cancelRenewals()
						fleet.setState(RunnerFailed)
						return err
					}
					fleet.setState(RunnerDraining)
					cancelWork()
					err := fleet.waitForDrain(results, active)
					cancelRenewals()
					return err
				}
				continue
			}
			cancelWork()
			_ = fleet.waitForDrain(results, active)
			cancelRenewals()
			fleet.setState(RunnerFailed)
			return err
		}
		if claim.Budget.Attempt == 0 {
			cancelWork()
			_ = fleet.waitForDrain(results, active)
			cancelRenewals()
			fleet.setState(RunnerFailed)
			return ErrInvalidOperation
		}
		operation, ok := fleet.operations[claim.Attempt.OperationID]
		if !ok {
			cancelWork()
			_ = fleet.waitForDrain(results, active)
			cancelRenewals()
			fleet.setState(RunnerFailed)
			return ErrInvalidOperation
		}
		if claim.Attempt.Version != operation.spec.Version {
			cancelWork()
			_ = fleet.waitForDrain(results, active)
			cancelRenewals()
			fleet.setState(RunnerFailed)
			return ErrDefinitionDrift
		}
		if !fleet.admitClaim(workContext, renewalContext, operation, claim, results, now) {
			settlementErr := fleet.settleUnadmittedClaim(claim, operation.spec.Channel)
			cancelWork()
			drainErr := fleet.waitForDrain(results, active)
			cancelRenewals()
			fleet.setState(RunnerFailed)
			return errors.Join(drainErr, settlementErr)
		}
		active, _ = bits.Add64(active, 1, 0)
	}
}

func (fleet *Fleet) admitClaim(workContext, renewalContext context.Context, operation Operation, claim Claim, results chan<- error, now time.Time) bool {
	start := make(chan struct{})
	fleet.mu.Lock()
	open := fleet.state == RunnerAccepting
	if open {
		fleet.renewalStarts.Add(1)
		fleet.workers.Go(func() {
			<-start
			results <- fleet.executeClaim(workContext, renewalContext, operation, claim)
		})
	}
	fleet.mu.Unlock()
	if open {
		fleet.observe(Event{Type: EventClaimed, Operation: claim.Attempt.OperationID, Channel: operation.spec.Channel, Attempt: claim.Attempt.Number, State: Claimed, At: now})
		close(start)
	}
	return open
}

func (fleet *Fleet) settleUnadmittedClaim(claim Claim, channel string) error {
	completion := Completion{
		Ownership: claim.Ownership(), From: Claimed, State: Canceled,
		At: fleet.options.Clock.Now(), ErrorDetail: ErrCanceled.Error(),
		Actor: fleet.options.Owner, Reason: "fleet admission closed",
	}
	completionContext, cancel := context.WithTimeout(context.Background(), fleet.options.ShutdownWait)
	defer cancel()
	if err := fleet.store.Complete(completionContext, completion); err != nil {
		return fmt.Errorf("sequencer: settle claim after admission closed: %w", err)
	}
	fleet.observe(Event{Type: EventCompleted, Operation: claim.Attempt.OperationID, Channel: channel, Attempt: claim.Attempt.Number, State: Canceled, At: completion.At, Err: ErrCanceled})
	return nil
}

func isRunCancellation(ctx context.Context, err error) bool {
	return ctx.Err() != nil && !errors.Is(err, ErrUnknownResult) && errors.Is(err, ctx.Err())
}

func (fleet *Fleet) waitForCapacity(ctx context.Context, results <-chan error, active *uint64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-results:
		*active, _ = bits.Sub64(*active, 1, 0)
		return err
	}
}

func (fleet *Fleet) waitForPoll(ctx context.Context, results <-chan error, active *uint64) error {
	timer := time.NewTimer(fleet.options.ClaimInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case err := <-results:
		*active, _ = bits.Sub64(*active, 1, 0)
		return err
	}
}

func (fleet *Fleet) waitForDrain(results <-chan error, active uint64) error {
	if active == 0 {
		fleet.workers.Wait()
		fleet.setState(RunnerStopped)
		return nil
	}
	timer := time.NewTimer(fleet.options.ShutdownWait)
	defer timer.Stop()
	var drainErr error
	for range active {
		select {
		case err := <-results:
			if drainErr == nil && err != nil {
				drainErr = err
			}
		case <-timer.C:
			fleet.setState(RunnerFailed)
			return ErrShutdownTimeout
		}
	}
	if drainErr != nil {
		fleet.workers.Wait()
		fleet.setState(RunnerFailed)
		return drainErr
	}
	fleet.workers.Wait()
	fleet.setState(RunnerStopped)
	return nil
}

func (fleet *Fleet) executeClaim(ctx, renewalParent context.Context, operation Operation, claim Claim) error {
	if executionErr := attemptBudgetError(claim.Budget, operation.spec.Policy); executionErr != nil {
		fleet.renewalStarts.Done()
		state := classifyState(executionErr, operation.spec.Policy, claim.Budget.Attempt, claim.Budget.Exceptions)
		completion := Completion{
			Ownership: claim.Ownership(), From: Claimed, State: state,
			At: fleet.options.Clock.Now(), ErrorDetail: persistentErrorDetail(executionErr),
		}
		completionContext, cancelCompletion := context.WithTimeout(context.WithoutCancel(ctx), fleet.options.ShutdownWait)
		err := fleet.store.Complete(completionContext, completion)
		cancelCompletion()
		if err != nil {
			return fmt.Errorf("sequencer: complete accepted attempt: %w", err)
		}
		fleet.observe(Event{Type: EventCompleted, Operation: claim.Attempt.OperationID, Channel: operation.spec.Channel, Attempt: claim.Attempt.Number, State: state, At: completion.At, Err: executionErr})
		return nil
	}
	attemptParent := ctx
	if operation.spec.Policy.Cancellation == CancellationDrainOnly {
		attemptParent = context.Background()
	}
	attemptContext, cancelAttempt := context.WithCancel(attemptParent)
	defer cancelAttempt()
	renewContext, stopRenewal := context.WithCancel(renewalParent)
	renewalReady := make(chan struct{})
	renewalStopped := make(chan struct{})
	renewalError := make(chan error, 1)
	fleet.renewals.Go(func() {
		fleet.renewLease(renewContext, renewalReady, cancelAttempt, claim.Ownership(), claim.Attempt.Number, claim.Until, renewalStopped, renewalError)
	})
	fleet.renewalStarts.Done()
	now := fleet.options.Clock.Now()
	markContext, cancelMark := context.WithTimeout(context.WithoutCancel(ctx), fleet.options.ShutdownWait)
	_, markErr := fleet.store.MarkRunning(markContext, claim.Ownership(), now)
	cancelMark()
	if markErr != nil {
		stopRenewal()
		<-renewalStopped
		return fmt.Errorf("sequencer: mark accepted attempt running: %w", markErr)
	}
	close(renewalReady)
	fleet.observe(Event{Type: EventRunning, Operation: claim.Attempt.OperationID, Channel: operation.spec.Channel, Attempt: claim.Attempt.Number, State: Running, At: now})

	worker := &Runner{options: fleet.options.RunnerOptions}
	executionResult := make(chan attemptExecutionResult, 1)
	go func() {
		output, actor, reason, err := worker.runAttempt(attemptContext, operation.spec, claim.Attempt)
		executionResult <- attemptExecutionResult{output: output, actor: actor, reason: reason, err: err}
	}()
	result, renewalErr := waitForAttempt(executionResult, renewalError, stopRenewal, renewalStopped)
	if renewalErr != nil {
		fleet.setState(RunnerFailed)
		return fmt.Errorf("sequencer: renew accepted attempt lease: %w", renewalErr)
	}
	output, actor, reason, executionErr := result.output, result.actor, result.reason, result.err
	retryException := operation.spec.Policy.RetryMode == DurableRetries && errors.Is(executionErr, ErrRetryable)
	exceptions := claim.Budget.Exceptions
	if retryException {
		exceptions = nextAttempt(exceptions)
	}
	state := classifyState(executionErr, operation.spec.Policy, claim.Budget.Attempt, exceptions)
	completion := Completion{
		Ownership: claim.Ownership(), State: state, At: fleet.options.Clock.Now(),
		Output: output, Actor: actor, Reason: reason, RetryException: retryException,
	}
	if executionErr != nil {
		completion.ErrorDetail = persistentErrorDetail(executionErr)
	}
	if state == Retryable {
		completion.EligibleAt = completion.At
	}
	completionContext, cancel := context.WithTimeout(context.Background(), fleet.options.ShutdownWait)
	defer cancel()
	if err := fleet.store.Complete(completionContext, completion); err != nil {
		return fmt.Errorf("sequencer: complete accepted attempt: %w", err)
	}
	fleet.observe(Event{Type: EventCompleted, Operation: claim.Attempt.OperationID, Channel: operation.spec.Channel, Attempt: claim.Attempt.Number, State: state, At: completion.At, Err: executionErr})
	return nil
}

func waitForAttempt(execution <-chan attemptExecutionResult, renewalFailure <-chan error, stopRenewal context.CancelFunc, renewalStopped <-chan struct{}) (attemptExecutionResult, error) {
	select {
	case result := <-execution:
		stopRenewal()
		<-renewalStopped
		select {
		case err := <-renewalFailure:
			return attemptExecutionResult{}, err
		default:
			return result, nil
		}
	case err := <-renewalFailure:
		stopRenewal()
		<-renewalStopped
		return attemptExecutionResult{}, err
	}
}

func (fleet *Fleet) renewLease(ctx context.Context, ready <-chan struct{}, cancelAttempt context.CancelFunc, ownership Ownership, attempt uint, until time.Time, stopped chan<- struct{}, failed chan<- error) {
	defer close(stopped)
	select {
	case <-ctx.Done():
		return
	case <-ready:
	}
	ticker := time.NewTicker(fleet.options.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := fleet.options.Clock.Now()
			remaining := until.Sub(now)
			if remaining <= 0 {
				failed <- ErrStaleOwner
				cancelAttempt()
				return
			}
			renewalTimeout := min(fleet.options.ShutdownWait, remaining)
			renewContext, cancel := context.WithTimeout(ctx, renewalTimeout)
			renewedUntil, err := fleet.store.RenewLease(renewContext, ownership, now, fleet.options.LeaseDuration)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				failed <- err
				cancelAttempt()
				return
			}
			if !renewedUntil.After(now) {
				failed <- ErrInvalidLease
				cancelAttempt()
				return
			}
			until = renewedUntil
			fleet.observe(Event{Type: EventHeartbeat, Operation: ownership.OperationID, Attempt: attempt, State: Running, At: now})
		}
	}
}

func (fleet *Fleet) observe(event Event) {
	for _, observer := range fleet.options.Observers {
		if observer != nil {
			observer.Observe(event)
		}
	}
}
