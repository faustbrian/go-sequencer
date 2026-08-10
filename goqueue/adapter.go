// Package goqueue bridges durable operation identities to asynchronous queues.
// It intentionally carries no handler payload, transaction, or secret data.
package goqueue

import (
	"context"
	"crypto/rand"
	"errors"
	"regexp"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

var channelPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,254}$`)

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
	Channel     string                `json:"channel,omitempty"`
}

// Message is a payload-free durable queue command. Queue redelivery is safe
// because the worker delegates eligibility and ownership to the ledger.
type Message struct {
	OperationID sequencer.OperationID `json:"operation_id"`
	Version     uint                  `json:"version"`
	Checksum    string                `json:"checksum"`
	Channel     string                `json:"channel,omitempty"`
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
	channel   string
}

// NewChannelDispatcher binds a semantic operation channel to one transport topic.
func NewChannelDispatcher(publisher Publisher, channel, topic string) (*Dispatcher, error) {
	if !channelPattern.MatchString(channel) {
		return nil, ErrInvalidAdapter
	}
	dispatcher, err := NewDispatcher(publisher, topic)
	if err != nil {
		return nil, err
	}
	dispatcher.channel = channel
	return dispatcher, nil
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
	if request.OperationID == "" || request.Version == 0 || request.Checksum == "" ||
		(request.Channel != "" && !channelPattern.MatchString(request.Channel)) ||
		(dispatcher.channel != "" && request.Channel != dispatcher.channel) {
		return Message{}, ErrInvalidAdapter
	}
	message := Message{OperationID: request.OperationID, Version: request.Version, Checksum: request.Checksum, Channel: request.Channel, DeliveryID: rand.Text()}
	if err := dispatcher.publisher.Publish(ctx, dispatcher.topic, message); err != nil {
		return message, errors.Join(ErrPublishOutcomeUnknown, err)
	}
	return message, nil
}

// Executor performs a ledger-owned attempt for one redelivered message. A nil
// error confirms durable completion, ErrUnknownResult leaves the result
// unsettled, and every other error is a definite failure.
type Executor interface {
	ExecuteMessage(context.Context, Message) error
}

// Settlement controls one queue delivery after durable execution returns.
// Implementations bind these operations to the delivery being handled.
type Settlement interface {
	Acknowledge(context.Context) error
	Reject(context.Context) error
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
type Worker struct {
	executor Executor
	channel  string
}

// NewWorker constructs an explicit worker handler; it starts no goroutines.
func NewWorker(executor Executor) (*Worker, error) {
	if executor == nil {
		return nil, ErrInvalidAdapter
	}
	return &Worker{executor: executor}, nil
}

// NewChannelWorker binds a worker to exactly one semantic operation channel.
func NewChannelWorker(channel string, executor Executor) (*Worker, error) {
	if !channelPattern.MatchString(channel) {
		return nil, ErrInvalidAdapter
	}
	worker, err := NewWorker(executor)
	if err != nil {
		return nil, err
	}
	worker.channel = channel
	return worker, nil
}

// Handle processes one queue delivery under ledger-owned idempotency.
func (worker *Worker) Handle(ctx context.Context, message Message) error {
	if !validMessage(message, worker.channel) {
		return ErrInvalidAdapter
	}
	return worker.executor.ExecuteMessage(ctx, message)
}

// HandleDelivery executes and settles one delivery. Commit-unknown execution
// and unconfirmed settlement remain unsettled so the transport may redeliver.
func (worker *Worker) HandleDelivery(ctx context.Context, message Message, settlement Settlement) (Disposition, error) {
	if !validMessage(message, worker.channel) || settlement == nil {
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
	if err := settlement.Reject(ctx); err != nil {
		return Unsettled, errors.Join(executionErr, err)
	}
	return Rejected, executionErr
}

func validMessage(message Message, expectedChannel string) bool {
	if message.OperationID == "" || message.Version == 0 || message.Checksum == "" || message.DeliveryID == "" {
		return false
	}
	if expectedChannel != "" {
		return message.Channel == expectedChannel
	}
	return message.Channel == "" || channelPattern.MatchString(message.Channel)
}
