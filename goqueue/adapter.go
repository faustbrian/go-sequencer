// Package goqueue bridges durable operation identities to asynchronous queues.
// It intentionally carries no handler payload, transaction, or secret data.
package goqueue

import (
	"context"
	"crypto/rand"
	"errors"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

var (
	// ErrInvalidAdapter reports incomplete asynchronous dependencies or messages.
	ErrInvalidAdapter = errors.New("sequencer/goqueue: invalid adapter")
	// ErrPublishOutcomeUnknown reports that queue admission could not be confirmed.
	// The returned Message retains its delivery identity for reconciliation.
	ErrPublishOutcomeUnknown = errors.New("sequencer/goqueue: publish outcome unknown")
)

// Request identifies the immutable definition to dispatch.
type Request struct {
	OperationID sequencer.OperationID `json:"operation_id"`
	Version     uint                  `json:"version"`
	Checksum    string                `json:"checksum"`
}

// Message is a payload-free durable queue command. Queue redelivery is safe
// because the worker delegates eligibility and ownership to the ledger.
type Message struct {
	OperationID sequencer.OperationID `json:"operation_id"`
	Version     uint                  `json:"version"`
	Checksum    string                `json:"checksum"`
	DeliveryID  string                `json:"delivery_id"`
}

// Publisher is the narrow seam implemented by a queue transport wrapper. Any
// returned error means queue admission is unknown rather than definitely absent.
type Publisher interface {
	Publish(context.Context, string, Message) error
}

// Dispatcher publishes bounded identity-only commands.
type Dispatcher struct {
	publisher Publisher
	topic     string
}

// NewDispatcher validates asynchronous transport dependencies.
func NewDispatcher(publisher Publisher, topic string) (*Dispatcher, error) {
	if publisher == nil || topic == "" || len(topic) > 255 {
		return nil, ErrInvalidAdapter
	}
	return &Dispatcher{publisher: publisher, topic: topic}, nil
}

// Dispatch publishes an operation command. It never claims cross-operation
// or enqueue-to-worker transaction atomicity.
func (dispatcher *Dispatcher) Dispatch(ctx context.Context, request Request) (Message, error) {
	if request.OperationID == "" || request.Version == 0 || request.Checksum == "" {
		return Message{}, ErrInvalidAdapter
	}
	message := Message{OperationID: request.OperationID, Version: request.Version, Checksum: request.Checksum, DeliveryID: rand.Text()}
	if err := dispatcher.publisher.Publish(ctx, dispatcher.topic, message); err != nil {
		return message, errors.Join(ErrPublishOutcomeUnknown, err)
	}
	return message, nil
}

// Executor performs a ledger-owned attempt for one redelivered message.
type Executor interface {
	ExecuteMessage(context.Context, Message) error
}

// Settlement controls one queue delivery after durable execution returns.
// Implementations bind these operations to the delivery being handled.
type Settlement interface {
	Acknowledge(context.Context) error
	Reject(context.Context, error) error
}

// Disposition reports whether a delivery was durably settled.
type Disposition uint8

const (
	// Acknowledged means durable completion and queue acknowledgement both succeeded.
	Acknowledged Disposition = iota + 1
	// Rejected means execution definitely failed and queue rejection succeeded.
	Rejected
	// Unsettled means execution or queue settlement remains unknown and redelivery is safe.
	Unsettled
)

// Worker validates queue input and invokes the durable executor.
type Worker struct{ executor Executor }

// NewWorker constructs an explicit worker handler; it starts no goroutines.
func NewWorker(executor Executor) (*Worker, error) {
	if executor == nil {
		return nil, ErrInvalidAdapter
	}
	return &Worker{executor: executor}, nil
}

// Handle processes one queue delivery under ledger-owned idempotency.
func (worker *Worker) Handle(ctx context.Context, message Message) error {
	if !validMessage(message) {
		return ErrInvalidAdapter
	}
	return worker.executor.ExecuteMessage(ctx, message)
}

// HandleDelivery executes and settles one delivery. Commit-unknown execution
// and unconfirmed settlement remain unsettled so the transport may redeliver.
func (worker *Worker) HandleDelivery(ctx context.Context, message Message, settlement Settlement) (Disposition, error) {
	if !validMessage(message) || settlement == nil {
		return Unsettled, ErrInvalidAdapter
	}
	executionErr := worker.executor.ExecuteMessage(ctx, message)
	if errors.Is(executionErr, sequencer.ErrUnknownResult) {
		return Unsettled, executionErr
	}
	if executionErr == nil {
		if err := settlement.Acknowledge(ctx); err != nil {
			return Unsettled, err
		}
		return Acknowledged, nil
	}
	if err := settlement.Reject(ctx, executionErr); err != nil {
		return Unsettled, errors.Join(executionErr, err)
	}
	return Rejected, executionErr
}

func validMessage(message Message) bool {
	return message.OperationID != "" && message.Version != 0 && message.Checksum != "" && message.DeliveryID != ""
}
