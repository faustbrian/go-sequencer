// Package golease bridges operation-scoped singleton work to a fenced lease
// client such as lease without coupling the root package to one backend.
package golease

import (
	"context"
	"errors"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

const (
	// DefaultCleanupTimeout bounds lease release when New is used.
	DefaultCleanupTimeout = 5 * time.Second
	// MaxCleanupTimeout is the largest configurable lease-release bound.
	MaxCleanupTimeout = time.Minute
)

// ErrInvalidAdapter reports missing lease dependencies or ownership proof.
var ErrInvalidAdapter = errors.New("sequencer/golease: invalid adapter")

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
type Adapter struct {
	acquirer       Acquirer
	cleanupTimeout time.Duration
}

// New validates the lease acquirer.
func New(acquirer Acquirer) (*Adapter, error) {
	return NewWithCleanupTimeout(acquirer, DefaultCleanupTimeout)
}

// NewWithCleanupTimeout validates the acquirer and finite release bound.
func NewWithCleanupTimeout(acquirer Acquirer, cleanupTimeout time.Duration) (*Adapter, error) {
	if acquirer == nil || cleanupTimeout <= 0 || cleanupTimeout > MaxCleanupTimeout {
		return nil, ErrInvalidAdapter
	}
	return &Adapter{acquirer: acquirer, cleanupTimeout: cleanupTimeout}, nil
}

// WithClaim acquires, proves, executes, and compare-releases one singleton.
func (adapter *Adapter) WithClaim(ctx context.Context, key string, ttl time.Duration, execute func(context.Context, Ownership) error) (err error) {
	if key == "" || ttl <= 0 || execute == nil {
		return ErrInvalidAdapter
	}
	handle, err := adapter.acquirer.Acquire(ctx, key, ttl)
	if err != nil {
		return err
	}
	if handle == nil {
		return ErrInvalidAdapter
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adapter.cleanupTimeout)
		defer cancel()
		if releaseErr := handle.Release(cleanupCtx); releaseErr != nil {
			err = sequencer.UnknownResult(errors.Join(err, releaseErr))
		}
	}()
	if handle.Owner() == "" || handle.Fencing() == 0 {
		return ErrInvalidAdapter
	}
	return execute(ctx, Ownership{Owner: handle.Owner(), Fencing: handle.Fencing()})
}
