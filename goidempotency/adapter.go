// Package goidempotency bridges explicitly idempotent operations to a durable
// idempotency service without hiding availability or replay decisions.
package goidempotency

import (
	"context"
	"errors"
	"time"
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
	Begin(context.Context, string) (Token, bool, error)
	Complete(context.Context, Token) error
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
func (adapter *Adapter) Do(ctx context.Context, key string, execute func(context.Context) error) error {
	if key == "" || execute == nil {
		return ErrInvalidAdapter
	}
	token, shouldExecute, err := adapter.gate.Begin(ctx, key)
	if err != nil || !shouldExecute {
		return err
	}
	if err = execute(ctx); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adapter.cleanupTimeout)
		defer cancel()
		return errors.Join(err, adapter.gate.Fail(cleanupCtx, token, err))
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adapter.cleanupTimeout)
	defer cancel()
	return adapter.gate.Complete(cleanupCtx, token)
}
