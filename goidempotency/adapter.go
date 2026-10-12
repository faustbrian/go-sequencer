// Package goidempotency provides the retained Sequencer idempotency adapter.
// Named facade types remain distinct from canonical adapter types; values are
// bridged while sentinels, defaults and runtime semantics are preserved.
//
// Deprecated: use github.com/faustbrian/go-sequencer/v2/adapters/idempotency.
// This package remains supported for the longer of 180 days after successor
// public availability and two subsequently published stable minor releases.
package goidempotency

import (
	"context"
	"time"

	adapter "github.com/faustbrian/go-sequencer/v2/adapters/idempotency"
)

const (
	// DefaultCleanupTimeout bounds a terminal gate update when New is used.
	DefaultCleanupTimeout = adapter.DefaultCleanupTimeout
	// MaxCleanupTimeout is the largest configurable terminal gate update bound.
	MaxCleanupTimeout = adapter.MaxCleanupTimeout
)

// ErrInvalidAdapter reports missing idempotency dependencies or keys.
var ErrInvalidAdapter = adapter.ErrInvalidAdapter

// Token is the opaque ownership proof returned by the application service.
type Token any

// Gate is the narrow seam implemented by a fail-closed idempotency wrapper.
type Gate interface {
	// Begin returns ownership and whether to execute; false with nil error skips work.
	// A true decision must supply a non-nil token and protect the logical effect durably.
	Begin(context.Context, string) (Token, bool, error)
	// Complete confirms the effect succeeded under the accepted token.
	// It must honor the detached cleanup deadline.
	Complete(context.Context, Token) error
	// Fail records the callback failure under the accepted token; it must not
	// authorize unsafe replay of an effect with an unknown outcome.
	Fail(context.Context, Token, error) error
}

// Adapter coordinates one explicitly idempotent callback.
type Adapter struct{ inner *adapter.Adapter }

// New validates the idempotency gate.
func New(gate Gate) (*Adapter, error) {
	return NewWithCleanupTimeout(gate, DefaultCleanupTimeout)
}

// NewWithCleanupTimeout validates the gate and finite terminal-update bound.
func NewWithCleanupTimeout(gate Gate, cleanupTimeout time.Duration) (*Adapter, error) {
	if gate == nil {
		return nil, ErrInvalidAdapter
	}
	inner, err := adapter.NewWithCleanupTimeout(gateBridge{gate: gate}, cleanupTimeout)
	if err != nil {
		return nil, err
	}
	return &Adapter{inner: inner}, nil
}

// Do runs only newly acquired work and records its terminal result.
// Cleanup uses a cancellation-detached deadline; cleanup error carries ErrUnknownResult.
// A callback panic is not recovered and does not call Complete or Fail.
func (adapter *Adapter) Do(ctx context.Context, key string, execute func(context.Context) error) error {
	return adapter.inner.Do(ctx, key, execute)
}

type gateBridge struct{ gate Gate }

func (bridge gateBridge) Begin(ctx context.Context, key string) (adapter.Token, bool, error) {
	return bridge.gate.Begin(ctx, key)
}

func (bridge gateBridge) Complete(ctx context.Context, token adapter.Token) error {
	return bridge.gate.Complete(ctx, token)
}

func (bridge gateBridge) Fail(ctx context.Context, token adapter.Token, err error) error {
	return bridge.gate.Fail(ctx, token, err)
}
