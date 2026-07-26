// Package goqueue bridges durable operation identities to asynchronous queues.
// It intentionally carries no handler payload, transaction, or secret data.
package goqueue

import (
	"context"
	"crypto/rand"
	"errors"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

// ErrInvalidAdapter reports incomplete asynchronous dependencies or messages.
var ErrInvalidAdapter = errors.New("sequencer/goqueue: invalid adapter")

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

// Publisher is the narrow seam implemented by a queue transport wrapper.
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
		return Message{}, err
	}
	return message, nil
}

// Executor performs a ledger-owned attempt for one redelivered message.
type Executor interface {
	ExecuteMessage(context.Context, Message) error
}

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
	if message.OperationID == "" || message.Version == 0 || message.Checksum == "" || message.DeliveryID == "" {
		return ErrInvalidAdapter
	}
	return worker.executor.ExecuteMessage(ctx, message)
}
