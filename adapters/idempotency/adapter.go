// Package sequenceridempotency bridges explicitly idempotent operations to a durable
// idempotency service without hiding availability or replay decisions.
//
// The application owns key scope, durable token semantics and the borrowed Gate.
// Calls require a non-nil context and cooperative providers; this adapter does
// not make arbitrary effects idempotent, recover callback panics or close clients.
package sequenceridempotency

import (
	"context"
	"errors"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
)

const (
	// DefaultCleanupTimeout bounds a terminal gate update when New is used.
	DefaultCleanupTimeout = 5 * time.Second
	// MaxCleanupTimeout is the largest configurable terminal gate update bound.
	MaxCleanupTimeout = time.Minute
)

// ErrInvalidAdapter reports missing idempotency dependencies or keys.
var ErrInvalidAdapter = errors.New("sequencer/goidempotency: invalid adapter")

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
type Adapter struct {
	gate           Gate
	cleanupTimeout time.Duration
}

// New validates the idempotency gate.
func New(gate Gate) (*Adapter, error) {
	return NewWithCleanupTimeout(gate, DefaultCleanupTimeout)
}

// NewWithCleanupTimeout validates the gate and finite terminal-update bound.
func NewWithCleanupTimeout(gate Gate, cleanupTimeout time.Duration) (*Adapter, error) {
	if gate == nil || cleanupTimeout <= 0 || cleanupTimeout > MaxCleanupTimeout {
		return nil, ErrInvalidAdapter
	}
	return &Adapter{gate: gate, cleanupTimeout: cleanupTimeout}, nil
}

// Do runs only newly acquired work and records its terminal result.
// Cleanup uses a cancellation-detached deadline; cleanup error carries ErrUnknownResult.
// A callback panic is not recovered and does not call Complete or Fail.
func (adapter *Adapter) Do(ctx context.Context, key string, execute func(context.Context) error) error {
	if key == "" || execute == nil {
		return ErrInvalidAdapter
	}
	// Terminal updates follow returned callback outcomes, not panic unwinding.
	// A caller recovering a panic must reconcile the accepted token itself.
	token, shouldExecute, err := adapter.gate.Begin(ctx, key)
	if err != nil || !shouldExecute {
		return err
	}
	if token == nil {
		return ErrInvalidAdapter
	}
	if err = execute(ctx); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adapter.cleanupTimeout)
		defer cancel()
		if cleanupErr := adapter.gate.Fail(cleanupCtx, token, err); cleanupErr != nil {
			return sequencer.UnknownResult(errors.Join(err, cleanupErr))
		}
		return err
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adapter.cleanupTimeout)
	defer cancel()
	if err := adapter.gate.Complete(cleanupCtx, token); err != nil {
		return sequencer.UnknownResult(err)
	}
	return nil
}
