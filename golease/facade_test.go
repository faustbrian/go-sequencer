package golease_test

import (
	"context"
	"errors"
	"testing"
	"time"

	sequencerlease "github.com/faustbrian/go-sequencer/adapters/lease"
	"github.com/faustbrian/go-sequencer/golease"
)

func TestFacadeDelegatesToCanonicalAdapter(t *testing.T) {
	t.Parallel()

	if !errors.Is(golease.ErrInvalidAdapter, sequencerlease.ErrInvalidAdapter) {
		t.Fatal("legacy and canonical sentinels differ")
	}
	if golease.DefaultCleanupTimeout != sequencerlease.DefaultCleanupTimeout ||
		golease.MaxCleanupTimeout != sequencerlease.MaxCleanupTimeout {
		t.Fatal("legacy and canonical cleanup defaults differ")
	}
	if _, err := golease.New(nil); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("New(nil) error = %v", err)
	}
	if _, err := golease.NewWithCleanupTimeout(facadeAcquirer{}, 0); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("NewWithCleanupTimeout(0) error = %v", err)
	}
	adapter, err := golease.New(facadeAcquirer{})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	if err := adapter.WithClaim(context.Background(), "key", time.Second, nil); !errors.Is(err, golease.ErrInvalidAdapter) {
		t.Fatalf("WithClaim(nil) error = %v", err)
	}
	var got golease.Ownership
	if err := adapter.WithClaim(context.Background(), "key", time.Second, func(_ context.Context, ownership golease.Ownership) error {
		got = ownership
		return nil
	}); err != nil {
		t.Fatalf("WithClaim(): %v", err)
	}
	if got != (golease.Ownership{Owner: "owner", Fencing: 7}) {
		t.Fatalf("ownership = %+v", got)
	}
}

type facadeAcquirer struct{}

func (facadeAcquirer) Acquire(context.Context, string, time.Duration) (golease.Handle, error) {
	return facadeHandle{}, nil
}

type facadeHandle struct{}

func (facadeHandle) Owner() string                 { return "owner" }
func (facadeHandle) Fencing() uint64               { return 7 }
func (facadeHandle) Release(context.Context) error { return nil }
