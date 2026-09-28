package sequencer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type observedRenewalStore struct {
	Store
	renewed chan time.Time
	entered chan struct{}
}

type deferredFailureStore struct {
	LeaseStore
	fleet    *Fleet
	renewals atomic.Int32
	markErr  error
}

type waitForRenewalError struct {
	fleet *Fleet
	cause error
}

func (failure waitForRenewalError) Error() string {
	failure.fleet.renewals.Wait()
	return failure.cause.Error()
}

func (failure waitForRenewalError) Unwrap() error { return failure.cause }

func (store *deferredFailureStore) RenewLease(_ context.Context, _ Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	if store.renewals.Add(1) == 1 {
		return now.Add(duration), nil
	}
	return time.Time{}, ErrStaleOwner
}

func (store *deferredFailureStore) MarkRunning(context.Context, Ownership, time.Time) (AttemptRecord, error) {
	return AttemptRecord{}, waitForRenewalError{fleet: store.fleet, cause: store.markErr}
}

func TestFleetRetainsRenewalFailureArrivingWhileStoreErrorIsReported(t *testing.T) {
	storeFailure := errors.New("mark transition unavailable")
	store := &deferredFailureStore{markErr: storeFailure}
	fleet := &Fleet{store: store, options: FleetOptions{
		RunnerOptions: RunnerOptions{Clock: wallClock{}, LeaseDuration: 300 * time.Millisecond},
		RenewInterval: 100 * time.Millisecond, ShutdownWait: time.Second,
	}}
	store.fleet = fleet
	claim := renewalClaim(time.Now().Add(300 * time.Millisecond))
	claim.Budget.Attempt = 1
	operation := Operation{spec: OperationSpec{ID: claim.Attempt.OperationID, Version: claim.Attempt.Version,
		Policy: Policy{Mode: OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: 10 * time.Millisecond}}}
	fleet.renewalStarts.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := fleet.executeClaim(ctx, ctx, operation, claim)
	if !errors.Is(err, storeFailure) || !errors.Is(err, ErrStaleOwner) || fleet.State() != RunnerFailed {
		t.Fatalf("executeClaim() error = %v, state = %s; want both failures and failed state", err, fleet.State())
	}
}

func (store *observedRenewalStore) RenewLease(ctx context.Context, _ Ownership, now time.Time, duration time.Duration) (time.Time, error) {
	if store.entered != nil {
		close(store.entered)
		<-ctx.Done()
		return time.Time{}, ctx.Err()
	}
	store.renewed <- now
	return now.Add(duration), nil
}

func renewalClaim(until time.Time) Claim {
	return Claim{Attempt: Attempt{OperationID: "renewal", Version: 1, Number: 1, Owner: "runner", Fencing: 1}, Until: until}
}

func TestAcceptedClaimRenewsBeforeHandlerAdmission(t *testing.T) {
	store := &observedRenewalStore{renewed: make(chan time.Time, 2)}
	fleet := &Fleet{store: store, options: FleetOptions{
		RunnerOptions: RunnerOptions{Clock: wallClock{}, LeaseDuration: time.Second},
		RenewInterval: 500 * time.Millisecond, ShutdownWait: 150 * time.Millisecond,
	}}
	claim := renewalClaim(time.Now().Add(time.Second))
	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		stop, failed := fleet.startClaimRenewal(context.Background(), claim, ready, func() {})
		defer func() { _ = stop() }()
		select {
		case <-store.renewed:
		default:
			result <- errors.New("handler admitted before first fenced renewal")
			return
		}
		select {
		case err := <-failed:
			result <- err
		default:
			result <- nil
		}
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first renewal did not establish ownership")
	}
}

func TestStandaloneClaimRenewsAgainBeforeLeaseExpiry(t *testing.T) {
	const lease = 300 * time.Millisecond
	store := &observedRenewalStore{renewed: make(chan time.Time, 3)}
	runner := &Runner{store: store, options: RunnerOptions{Clock: wallClock{}, LeaseDuration: lease}}
	claim := renewalClaim(time.Now().Add(lease))
	stop, failed := runner.maintainClaim(context.Background(), claim, func() {})
	defer func() { _ = stop() }()
	var first time.Time
	select {
	case first = <-store.renewed:
	case err := <-failed:
		t.Fatalf("first renewal failed: %v", err)
	case <-time.After(lease):
		t.Fatal("initial renewal did not establish ownership")
	}
	select {
	case second := <-store.renewed:
		if !second.Before(first.Add(lease)) {
			t.Fatalf("next renewal at %s after current lease expiry %s", second, first.Add(lease))
		}
	case err := <-failed:
		t.Fatalf("renewal failed before the current lease expired: %v", err)
	case <-time.After(600 * time.Millisecond):
		t.Fatal("no second renewal before the current lease expired")
	}
}

func TestInitialRenewalCancellationReportsTheLostPreflight(t *testing.T) {
	store := &observedRenewalStore{entered: make(chan struct{})}
	fleet := &Fleet{store: store, options: FleetOptions{
		RunnerOptions: RunnerOptions{Clock: wallClock{}, LeaseDuration: time.Second},
		RenewInterval: 100 * time.Millisecond, ShutdownWait: time.Second,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		stop, failed := fleet.startClaimRenewal(ctx, renewalClaim(time.Now().Add(time.Second)), make(chan struct{}), func() {})
		defer func() { _ = stop() }()
		select {
		case err := <-failed:
			result <- err
		default:
			result <- errors.New("preflight cancellation lost")
		}
	}()
	select {
	case <-store.entered:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("initial renewal never entered the store")
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("preflight error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("preflight cancellation did not settle")
	}
}

func TestAttemptCompletionDoesNotMaskConcurrentRenewalFailure(t *testing.T) {
	t.Parallel()

	cause := errors.New("lease lost")
	execution := make(chan attemptExecutionResult, 1)
	execution <- attemptExecutionResult{output: Output{Summary: "uncommitted"}}
	renewalFailure := make(chan error, 1)
	renewalFailure <- cause
	_, err := waitForAttempt(execution, renewalFailure)
	if !errors.Is(err, cause) {
		t.Fatalf("waitForAttempt() error = %v, want lease failure", err)
	}
}

func TestFleetDrainRetainsFirstWorkerFailure(t *testing.T) {
	t.Parallel()

	cause := errors.New("first completion failed")
	second := errors.New("second completion failed")
	results := make(chan error, 2)
	results <- cause
	results <- second
	fleet := &Fleet{
		options: FleetOptions{ShutdownWait: time.Second},
		state:   RunnerDraining,
	}

	if err := fleet.waitForDrain(results, 2); !errors.Is(err, cause) {
		t.Fatalf("waitForDrain() error = %v", err)
	}
	if fleet.State() != RunnerFailed {
		t.Fatalf("state = %s, want failed", fleet.State())
	}
}
