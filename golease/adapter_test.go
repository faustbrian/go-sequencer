package golease_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/faustbrian/golib/pkg/sequencer/golease"
)

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
	if !handle.released {
		t.Fatal("lease was not released")
	}
}

func TestAdapterValidationAndFailurePaths(t *testing.T) {
	t.Parallel()

	if _, err := golease.New(nil); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	adapter, _ := golease.New(acquirerStub{})
	if err := adapter.WithClaim(context.Background(), "", time.Second, func(context.Context, golease.Ownership) error { return nil }); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("invalid input error = %v", err)
	}
	cause := errors.New("unavailable")
	adapter, _ = golease.New(acquirerStub{err: cause})
	if err := adapter.WithClaim(context.Background(), "key", time.Second, func(context.Context, golease.Ownership) error { return nil }); !errors.Is(err, cause) {
		t.Fatalf("acquire error = %v", err)
	}
	adapter, _ = golease.New(acquirerStub{handle: &handleStub{}})
	if err := adapter.WithClaim(context.Background(), "key", time.Second, func(context.Context, golease.Ownership) error { return nil }); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("invalid handle error = %v", err)
	}
	execution, release := errors.New("execution"), errors.New("release")
	adapter, _ = golease.New(acquirerStub{handle: &handleStub{owner: "owner", fencing: 1, releaseErr: release}})
	err := adapter.WithClaim(context.Background(), "key", time.Second, func(context.Context, golease.Ownership) error { return execution })
	if !errors.Is(err, execution) || !errors.Is(err, release) {
		t.Fatalf("joined error = %v", err)
	}
}

type acquirerStub struct {
	handle *handleStub
	err    error
}

func (stub acquirerStub) Acquire(context.Context, string, time.Duration) (golease.Handle, error) {
	return stub.handle, stub.err
}

type handleStub struct {
	owner      string
	fencing    uint64
	released   bool
	releaseErr error
}

func (handle *handleStub) Owner() string   { return handle.owner }
func (handle *handleStub) Fencing() uint64 { return handle.fencing }
func (handle *handleStub) Release(context.Context) error {
	handle.released = true
	return handle.releaseErr
}
