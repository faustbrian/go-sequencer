// Package golease provides the retained Sequencer lease adapter.
// Named facade types remain distinct from canonical adapter types; values are
// bridged while sentinels, defaults and runtime semantics are preserved.
//
// Deprecated: use github.com/faustbrian/go-sequencer/v2/adapters/lease. This
// package remains supported for the longer of 180 days after successor public
// availability and two subsequently published stable minor releases.
package golease

import (
	"context"
	"time"

	adapter "github.com/faustbrian/go-sequencer/v2/adapters/lease"
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
	// Owner is the acquired nonempty identity, not an authenticated principal.
	Owner string
	// Fencing is the nonzero token that protected resource writes must enforce.
	Fencing uint64
}

// Handle is the narrow fenced handle exposed by a lease wrapper.
type Handle interface {
	// Owner returns the stable identity bound to the acquired handle.
	Owner() string
	// Fencing returns its stable nonzero generation token.
	Fencing() uint64
	// Release relinquishes this handle without releasing a successor owner.
	// It must honor the detached cleanup deadline.
	Release(context.Context) error
}

// Acquirer obtains one bounded distributed lease.
type Acquirer interface {
	// Acquire returns a fenced handle for the key and positive TTL.
	// The provider owns availability, cancellation and lease renewal semantics.
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
// Release is deferred even on malformed proof or callback panic, with a detached
// cleanup deadline. Release failure carries ErrUnknownResult. The adapter does
// not renew the lease or stop a callback at TTL expiry; protected writes must fence.
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
