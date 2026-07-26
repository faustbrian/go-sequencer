// Package goidempotency bridges explicitly idempotent operations to a durable
// idempotency service without hiding availability or replay decisions.
package goidempotency

import (
	"context"
	"errors"
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
type Adapter struct{ gate Gate }

// New validates the idempotency gate.
func New(gate Gate) (*Adapter, error) {
	if gate == nil {
		return nil, ErrInvalidAdapter
	}
	return &Adapter{gate: gate}, nil
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
		return errors.Join(err, adapter.gate.Fail(context.WithoutCancel(ctx), token, err))
	}
	return adapter.gate.Complete(context.WithoutCancel(ctx), token)
}
