package golease_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/sequencer/golease"
)

var errMissingReleaseDeadline = errors.New("release context has no deadline")

func TestAdapterBoundsDetachedReleaseAfterCallerCancellation(t *testing.T) {
	t.Parallel()

	const cleanupTimeout = 20 * time.Millisecond
	execution := errors.New("execution")
	handle := &blockingHandle{owner: "owner", fencing: 1}
	adapter, err := golease.NewWithCleanupTimeout(acquirerStub{handle: handle}, cleanupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = adapter.WithClaim(ctx, "key", time.Second, func(context.Context, golease.Ownership) error {
		cancel()
		return execution
	})
	if !handle.hadDeadline {
		t.Fatal("release context had no deadline")
	}
	if !errors.Is(err, execution) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("joined error = %v", err)
	}
}

func TestNewWithCleanupTimeoutRejectsUnboundedDurations(t *testing.T) {
	t.Parallel()

	for _, timeout := range []time.Duration{-time.Nanosecond, 0, golease.MaxCleanupTimeout + time.Nanosecond} {
		if _, err := golease.NewWithCleanupTimeout(acquirerStub{}, timeout); !errors.Is(err, golease.ErrInvalidAdapter) {
			t.Fatalf("NewWithCleanupTimeout(%s) error = %v", timeout, err)
		}
	}
	if _, err := golease.NewWithCleanupTimeout(acquirerStub{}, golease.MaxCleanupTimeout); err != nil {
		t.Fatalf("exact maximum cleanup timeout error = %v", err)
	}
}

func TestAdapterPassesFencingProofAndReleases(t *testing.T) {
	t.Parallel()

	handle := &handleStub{owner: "replica", fencing: 42}
	adapter, err := golease.New(acquirerStub{handle: handle})
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.WithClaim(context.Background(), "postal.backfill", time.Minute, func(_ context.Context, ownership golease.Ownership) error {
		if ownership.Owner != "replica" || ownership.Fencing != 42 {
			t.Fatalf("ownership = %+v", ownership)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !handle.released || !handle.releaseHadDeadline {
		t.Fatalf("released = %t, cleanup deadline = %t", handle.released, handle.releaseHadDeadline)
	}
}

func TestAdapterValidationAndFailurePaths(t *testing.T) {
	t.Parallel()

	if _, err := golease.New(nil); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	adapter, _ := golease.New(acquirerStub{handle: &handleStub{owner: "owner", fencing: 1}})
	if err := adapter.WithClaim(context.Background(), "", time.Second, func(context.Context, golease.Ownership) error { return nil }); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("invalid input error = %v", err)
	}
	if err := adapter.WithClaim(context.Background(), "key", 0, func(context.Context, golease.Ownership) error { return nil }); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("zero TTL error = %v", err)
	}
	if err := adapter.WithClaim(context.Background(), "key", time.Second, nil); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("nil execute error = %v", err)
	}
	cause := errors.New("unavailable")
	adapter, _ = golease.New(acquirerStub{err: cause})
	if err := adapter.WithClaim(context.Background(), "key", time.Second, func(context.Context, golease.Ownership) error { return nil }); !errors.Is(err, cause) {
		t.Fatalf("acquire error = %v", err)
	}
	for _, handle := range []golease.Handle{nil, &handleStub{fencing: 1}, &handleStub{owner: "owner"}} {
		adapter, _ = golease.New(acquirerStub{handle: handle})
		if err := adapter.WithClaim(context.Background(), "key", time.Second, func(context.Context, golease.Ownership) error { return nil }); !errors.Is(err, golease.ErrInvalidAdapter) {
			t.Fatalf("invalid handle %+v error = %v", handle, err)
		}
	}
	execution, release := errors.New("execution"), errors.New("release")
	adapter, _ = golease.New(acquirerStub{handle: &handleStub{owner: "owner", fencing: 1, releaseErr: release}})
	err := adapter.WithClaim(context.Background(), "key", time.Second, func(context.Context, golease.Ownership) error { return execution })
	if !errors.Is(err, execution) || !errors.Is(err, release) {
		t.Fatalf("joined error = %v", err)
	}
}

type acquirerStub struct {
	handle golease.Handle
	err    error
}

func (stub acquirerStub) Acquire(context.Context, string, time.Duration) (golease.Handle, error) {
	return stub.handle, stub.err
}

type handleStub struct {
	owner              string
	fencing            uint64
	released           bool
	releaseHadDeadline bool
	releaseErr         error
}

func (handle *handleStub) Owner() string   { return handle.owner }
func (handle *handleStub) Fencing() uint64 { return handle.fencing }
func (handle *handleStub) Release(ctx context.Context) error {
	handle.released = true
	_, handle.releaseHadDeadline = ctx.Deadline()
	return handle.releaseErr
}

type blockingHandle struct {
	owner       string
	fencing     uint64
	hadDeadline bool
}

func (handle *blockingHandle) Owner() string   { return handle.owner }
func (handle *blockingHandle) Fencing() uint64 { return handle.fencing }
func (handle *blockingHandle) Release(ctx context.Context) error {
	_, handle.hadDeadline = ctx.Deadline()
	if !handle.hadDeadline {
		return errMissingReleaseDeadline
	}
	<-ctx.Done()
	return ctx.Err()
}
