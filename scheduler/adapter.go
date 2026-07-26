// Package scheduler bridges explicit future eligibility to an application
// scheduler without starting an implicit scheduler or goroutine.
package scheduler

import (
	"context"
	"errors"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

// ErrInvalidAdapter reports an unavailable scheduler or invalid request.
var ErrInvalidAdapter = errors.New("sequencer/scheduler: invalid adapter")

// Request is a payload-free future eligibility command.
type Request struct {
	OperationID sequencer.OperationID
	Version     uint
	EligibleAt  time.Time
}

// Destination is implemented by a scheduler integration owned by the app.
type Destination interface {
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
	if id == "" || version == 0 || eligibleAt.IsZero() {
		return ErrInvalidAdapter
	}
	return adapter.destination.Schedule(ctx, Request{OperationID: id, Version: version, EligibleAt: eligibleAt})
}
