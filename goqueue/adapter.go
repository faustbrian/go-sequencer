// Package goqueue provides the retained Sequencer queue adapter.
//
// Deprecated: use github.com/faustbrian/go-sequencer/adapters/queue. This
// package remains supported for the longer of 180 days after successor public
// availability and two subsequently published stable minor releases.
package goqueue

import (
	"context"

	sequencer "github.com/faustbrian/go-sequencer"
	adapter "github.com/faustbrian/go-sequencer/adapters/queue"
)

var (
	// ErrInvalidAdapter reports incomplete asynchronous dependencies or messages.
	ErrInvalidAdapter = adapter.ErrInvalidAdapter
	// ErrPublishOutcomeUnknown reports that queue admission could not be confirmed.
	ErrPublishOutcomeUnknown = adapter.ErrPublishOutcomeUnknown
)

// Request identifies the immutable definition to dispatch.
type Request struct {
	OperationID sequencer.OperationID `json:"operation_id"`
	Version     uint                  `json:"version"`
	Checksum    string                `json:"checksum"`
	Channel     string                `json:"channel,omitempty"`
}

// Message is a payload-free durable queue command.
type Message struct {
	OperationID sequencer.OperationID `json:"operation_id"`
	Version     uint                  `json:"version"`
	Checksum    string                `json:"checksum"`
	Channel     string                `json:"channel,omitempty"`
	DeliveryID  string                `json:"delivery_id"`
}

// Publisher is the narrow seam implemented by a queue transport wrapper.
type Publisher interface {
	Publish(context.Context, string, Message) error
}

// Dispatcher publishes bounded identity-only commands.
type Dispatcher struct{ inner *adapter.Dispatcher }

// NewChannelDispatcher binds a semantic operation channel to one topic.
func NewChannelDispatcher(publisher Publisher, channel, topic string) (*Dispatcher, error) {
	if publisher == nil {
		return nil, ErrInvalidAdapter
	}
	inner, err := adapter.NewChannelDispatcher(publisherBridge{publisher: publisher}, channel, topic)
	if err != nil {
		return nil, err
	}
	return &Dispatcher{inner: inner}, nil
}

// NewDispatcher validates asynchronous transport dependencies.
func NewDispatcher(publisher Publisher, topic string) (*Dispatcher, error) {
	if publisher == nil {
		return nil, ErrInvalidAdapter
	}
	inner, err := adapter.NewDispatcher(publisherBridge{publisher: publisher}, topic)
	if err != nil {
		return nil, err
	}
	return &Dispatcher{inner: inner}, nil
}

// Dispatch publishes an operation command.
func (dispatcher *Dispatcher) Dispatch(ctx context.Context, request Request) (Message, error) {
	message, err := dispatcher.inner.Dispatch(ctx, adapter.Request{
		OperationID: request.OperationID,
		Version:     request.Version,
		Checksum:    request.Checksum,
		Channel:     request.Channel,
	})
	return fromCanonicalMessage(message), err
}

// Executor performs a ledger-owned attempt for one redelivered message.
type Executor interface {
	ExecuteMessage(context.Context, Message) error
}

// Settlement controls one queue delivery after durable execution returns.
type Settlement interface {
	Acknowledge(context.Context) error
	Reject(context.Context) error
}

// Disposition reports whether a delivery was durably settled.
type Disposition uint8

const (
	// Acknowledged means durable completion and acknowledgement succeeded.
	Acknowledged Disposition = iota + 1
	// Rejected means execution definitely failed and rejection succeeded.
	Rejected
	// Unsettled means execution or settlement remains unknown.
	Unsettled
)

// Worker validates queue input and invokes the durable executor.
type Worker struct{ inner *adapter.Worker }

// NewWorker constructs an explicit worker handler; it starts no goroutines.
func NewWorker(executor Executor) (*Worker, error) {
	if executor == nil {
		return nil, ErrInvalidAdapter
	}
	inner, err := adapter.NewWorker(executorBridge{executor: executor})
	return &Worker{inner: inner}, err
}

// NewChannelWorker binds a worker to exactly one semantic operation channel.
func NewChannelWorker(channel string, executor Executor) (*Worker, error) {
	if executor == nil {
		return nil, ErrInvalidAdapter
	}
	inner, err := adapter.NewChannelWorker(channel, executorBridge{executor: executor})
	if err != nil {
		return nil, err
	}
	return &Worker{inner: inner}, nil
}

// Handle processes one queue delivery under ledger-owned idempotency.
func (worker *Worker) Handle(ctx context.Context, message Message) error {
	return worker.inner.Handle(ctx, toCanonicalMessage(message))
}

// HandleDelivery executes and settles one delivery.
func (worker *Worker) HandleDelivery(ctx context.Context, message Message, settlement Settlement) (Disposition, error) {
	disposition, err := worker.inner.HandleDelivery(ctx, toCanonicalMessage(message), settlement)
	return Disposition(disposition), err
}

type publisherBridge struct{ publisher Publisher }

func (bridge publisherBridge) Publish(ctx context.Context, topic string, message adapter.Message) error {
	return bridge.publisher.Publish(ctx, topic, fromCanonicalMessage(message))
}

type executorBridge struct{ executor Executor }

func (bridge executorBridge) ExecuteMessage(ctx context.Context, message adapter.Message) error {
	return bridge.executor.ExecuteMessage(ctx, fromCanonicalMessage(message))
}

func toCanonicalMessage(message Message) adapter.Message {
	return adapter.Message{
		OperationID: message.OperationID,
		Version:     message.Version,
		Checksum:    message.Checksum,
		Channel:     message.Channel,
		DeliveryID:  message.DeliveryID,
	}
}

func fromCanonicalMessage(message adapter.Message) Message {
	return Message{
		OperationID: message.OperationID,
		Version:     message.Version,
		Checksum:    message.Checksum,
		Channel:     message.Channel,
		DeliveryID:  message.DeliveryID,
	}
}
