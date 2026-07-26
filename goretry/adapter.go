// Package goretry maps typed sequencer failures to a bounded retry policy such
// as retry while leaving durable attempt ownership with the sequencer.
package goretry

import (
	"context"
	"errors"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

// ErrInvalidAdapter reports a missing bounded retry policy.
var ErrInvalidAdapter = errors.New("sequencer/goretry: invalid adapter")

// Classification is the transport-neutral retry decision.
type Classification uint8

const (
	// Permanent indicates an error that must not be retried.
	Permanent Classification = iota + 1
	// Retryable indicates an explicitly transient error.
	Retryable
)

// Classifier maps the root package's typed errors.
type Classifier struct{}

// Classify returns retryable only for an explicit sequencer retry error.
func (Classifier) Classify(err error) Classification {
	if errors.Is(err, sequencer.ErrRetryable) {
		return Retryable
	}
	return Permanent
}

// Policy executes a callback under explicit attempt and time budgets.
type Policy interface {
	Do(context.Context, func(context.Context) error) error
}

// Adapter delegates in-attempt transient retries to an external policy.
type Adapter struct{ policy Policy }

// New validates the bounded retry policy.
func New(policy Policy) (*Adapter, error) {
	if policy == nil {
		return nil, ErrInvalidAdapter
	}
	return &Adapter{policy: policy}, nil
}

// Do executes through the configured policy.
func (adapter *Adapter) Do(ctx context.Context, operation func(context.Context) error) error {
	if operation == nil {
		return ErrInvalidAdapter
	}
	return adapter.policy.Do(ctx, operation)
}
