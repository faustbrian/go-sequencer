// Package golease provides the retained Sequencer lease adapter.
//
// Deprecated: use github.com/faustbrian/go-sequencer/adapters/lease. This
// package remains supported for the longer of 180 days after successor public
// availability and two subsequently published stable minor releases.
package golease

import (
	"context"
	"time"

	adapter "github.com/faustbrian/go-sequencer/adapters/lease"
)

const (
	// DefaultCleanupTimeout bounds lease release when New is used.
	DefaultCleanupTimeout = adapter.DefaultCleanupTimeout
	// MaxCleanupTimeout is the largest configurable lease-release bound.
	MaxCleanupTimeout = adapter.MaxCleanupTimeout
)

// ErrInvalidAdapter reports missing lease dependencies or ownership proof.
var ErrInvalidAdapter = adapter.ErrInvalidAdapter

// Ownership is the explicit proof passed to protected resource writes.
type Ownership struct {
	Owner   string
	Fencing uint64
}

// Handle is the narrow fenced handle exposed by a lease wrapper.
type Handle interface {
	Owner() string
	Fencing() uint64
	Release(context.Context) error
}

// Acquirer obtains one bounded distributed lease.
type Acquirer interface {
	Acquire(context.Context, string, time.Duration) (Handle, error)
}

// Adapter scopes one callback to an explicitly fenced lease.
type Adapter struct{ inner *adapter.Adapter }

// New validates the lease acquirer.
func New(acquirer Acquirer) (*Adapter, error) {
	return NewWithCleanupTimeout(acquirer, DefaultCleanupTimeout)
}

// NewWithCleanupTimeout validates the acquirer and finite release bound.
func NewWithCleanupTimeout(acquirer Acquirer, cleanupTimeout time.Duration) (*Adapter, error) {
	if acquirer == nil {
		return nil, ErrInvalidAdapter
	}
	inner, err := adapter.NewWithCleanupTimeout(acquirerBridge{acquirer: acquirer}, cleanupTimeout)
	if err != nil {
		return nil, err
	}
	return &Adapter{inner: inner}, nil
}

// WithClaim acquires, proves, executes, and compare-releases one singleton.
func (facade *Adapter) WithClaim(ctx context.Context, key string, ttl time.Duration, execute func(context.Context, Ownership) error) error {
	if execute == nil {
		return ErrInvalidAdapter
	}
	return facade.inner.WithClaim(ctx, key, ttl, func(ctx context.Context, ownership adapter.Ownership) error {
		return execute(ctx, Ownership{Owner: ownership.Owner, Fencing: ownership.Fencing})
	})
}

type acquirerBridge struct{ acquirer Acquirer }

func (bridge acquirerBridge) Acquire(ctx context.Context, key string, ttl time.Duration) (adapter.Handle, error) {
	return bridge.acquirer.Acquire(ctx, key, ttl)
}
