// Package goqueue provides the retained Sequencer queue adapter.
// Named facade types remain distinct from canonical adapter types; values are
// bridged while sentinels, defaults and runtime semantics are preserved.
//
// Deprecated: use github.com/faustbrian/go-sequencer/v2/adapters/queue. This
// package remains supported for the longer of 180 days after successor public
// availability and two subsequently published stable minor releases.
package goqueue

import (
	"context"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	adapter "github.com/faustbrian/go-sequencer/v2/adapters/queue"
)

var (
	// ErrInvalidAdapter reports incomplete asynchronous dependencies or messages.
	ErrInvalidAdapter = adapter.ErrInvalidAdapter
	// ErrPublishOutcomeUnknown reports that queue admission could not be confirmed.
	ErrPublishOutcomeUnknown = adapter.ErrPublishOutcomeUnknown
)

// Request identifies the immutable definition to dispatch.
type Request struct {
	// OperationID identifies the local definition to execute.
	OperationID sequencer.OperationID `json:"operation_id"`
	// Version is its exact nonzero version.
	Version uint `json:"version"`
	// Checksum is its canonical declared digest, not a message signature.
	Checksum string `json:"checksum"`
	// Channel is an optional valid group, or the exact configured channel binding.
	Channel string `json:"channel,omitempty"`
}

// Message is a payload-free durable queue command.
type Message struct {
	// OperationID identifies the local definition to execute.
	OperationID sequencer.OperationID `json:"operation_id"`
	// Version is its exact nonzero version.
	Version uint `json:"version"`
	// Checksum is its canonical declared digest, not a message signature.
	Checksum string `json:"checksum"`
	// Channel is an optional valid group, or the exact configured channel binding.
	Channel string `json:"channel,omitempty"`
	// DeliveryID is nonempty and at most 255 bytes; dispatch generates a fresh value
	// once per call. It correlates admission, not authorization or effect idempotency.
	DeliveryID string `json:"delivery_id"`
}

// Publisher is the narrow seam implemented by a queue transport wrapper.
type Publisher interface {
	// Publish sends one identity-only message to the configured nonempty topic.
	// Any error leaves admission unknown; a nil result confirms provider admission only.
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

// Dispatch delegates canonical validation and publishing through the facade.
// Each call generates a new DeliveryID; a publish error retains that Message
// with ErrPublishOutcomeUnknown for admission reconciliation.
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
	// ExecuteMessage must enforce ledger eligibility, checksum and fencing on redelivery.
	// Nil confirms durable completion; ErrUnknownResult leaves delivery unsettled;
	// other errors are treated as definite failure for rejection.
	ExecuteMessage(context.Context, Message) error
}

// Settlement controls one queue delivery after durable execution returns.
type Settlement interface {
	// Acknowledge confirms settlement of this delivery after durable completion.
	Acknowledge(context.Context) error
	// Reject confirms rejection of this delivery after a definite execution failure.
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

// Handle validates and delegates one message; it does not acknowledge or reject it.
// Safe redelivery depends on the injected executor enforcing the durable ledger.
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
