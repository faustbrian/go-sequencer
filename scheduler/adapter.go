// Package scheduler bridges explicit future eligibility to an application
// scheduler without starting an implicit scheduler or goroutine.
//
// The borrowed Destination owns admission, persistence and cancellation. Defer
// accepts a past nonzero instant as well as a future one; it never edits the ledger.
package scheduler

import (
	"context"
	"errors"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
)

// ErrInvalidAdapter reports an unavailable scheduler or invalid request.
var ErrInvalidAdapter = errors.New("sequencer/scheduler: invalid adapter")

// Request is a payload-free future eligibility command.
type Request struct {
	// OperationID is the valid operation identity.
	OperationID sequencer.OperationID
	// Version is its exact nonzero version.
	Version uint
	// EligibleAt is a nonzero absolute instant; past instants are permitted.
	EligibleAt time.Time
}

// Destination is implemented by a scheduler integration owned by the app.
type Destination interface {
	// Schedule owns persistence, admission and deduplication of the command.
	// The adapter does not update the ledger, start timers or make enqueue atomic.
	Schedule(context.Context, Request) error
}

// Adapter emits explicit scheduled eligibility commands.
type Adapter struct{ destination Destination }

// New validates the scheduler destination.
func New(destination Destination) (*Adapter, error) {
	if destination == nil {
		return nil, ErrInvalidAdapter
	}
	return &Adapter{destination: destination}, nil
}

// Defer schedules one operation version at an absolute instant.
func (adapter *Adapter) Defer(ctx context.Context, id sequencer.OperationID, version uint, eligibleAt time.Time) error {
	if !id.Valid() || version == 0 || eligibleAt.IsZero() {
		return ErrInvalidAdapter
	}
	return adapter.destination.Schedule(ctx, Request{OperationID: id, Version: version, EligibleAt: eligibleAt})
}
