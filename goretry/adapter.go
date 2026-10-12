// Package goretry provides the retained Sequencer retry adapter.
// Named facade types remain distinct from canonical adapter types; values are
// bridged while sentinels, defaults and runtime semantics are preserved.
//
// Deprecated: use github.com/faustbrian/go-sequencer/v2/adapters/retry. This
// package remains supported for the longer of 180 days after successor public
// availability and two subsequently published stable minor releases.
package goretry

import (
	"context"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	adapter "github.com/faustbrian/go-sequencer/v2/adapters/retry"
)

// ErrInvalidAdapter reports a missing bounded retry policy.
var ErrInvalidAdapter = adapter.ErrInvalidAdapter

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
	return Classification(adapter.Classifier{}.Classify(err))
}

// Policy executes a callback under explicit attempt and time budgets.
type Policy interface {
	// Do owns the only retry loop and must bound time, honor cancellation and
	// apply the typed failure classifier. The adapter bounds callback reservations,
	// not how long a policy can block or how it schedules those callbacks.
	Do(context.Context, func(context.Context) error) error
}

// Adapter delegates in-attempt transient retries to an external policy.
type Adapter struct{ inner *adapter.Adapter }

// New validates the bounded retry policy.
func New(policy Policy) (*Adapter, error) {
	inner, err := adapter.New(policy)
	if err != nil {
		return nil, err
	}
	return &Adapter{inner: inner}, nil
}

// Do executes through the configured policy and shared execution budget.
func (adapter *Adapter) Do(ctx context.Context, budget *sequencer.ExecutionBudget, operation func(context.Context) error) error {
	return adapter.inner.Do(ctx, budget, operation)
}
