package goqueue_test

import (
	"context"
	"errors"
	"testing"

	sequencerqueue "github.com/faustbrian/go-sequencer/adapters/queue"
	"github.com/faustbrian/go-sequencer/goqueue"
)

func TestFacadeDelegatesToCanonicalAdapter(t *testing.T) {
	t.Parallel()

	if !errors.Is(goqueue.ErrInvalidAdapter, sequencerqueue.ErrInvalidAdapter) ||
		!errors.Is(goqueue.ErrPublishOutcomeUnknown, sequencerqueue.ErrPublishOutcomeUnknown) {
		t.Fatal("legacy and canonical sentinels differ")
	}
	if uint8(goqueue.Acknowledged) != uint8(sequencerqueue.Acknowledged) ||
		uint8(goqueue.Rejected) != uint8(sequencerqueue.Rejected) ||
		uint8(goqueue.Unsettled) != uint8(sequencerqueue.Unsettled) {
		t.Fatal("legacy and canonical dispositions differ")
	}
	publisher := &facadePublisher{}
	if _, err := goqueue.NewDispatcher(nil, "topic"); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewDispatcher(nil) error = %v", err)
	}
	if _, err := goqueue.NewDispatcher(publisher, ""); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewDispatcher(empty) error = %v", err)
	}
	dispatcher, err := goqueue.NewDispatcher(publisher, "topic")
	if err != nil {
		t.Fatalf("NewDispatcher(): %v", err)
	}
	message, err := dispatcher.Dispatch(context.Background(), goqueue.Request{
		OperationID: "operation", Version: 1, Checksum: "sha256:operation",
	})
	if err != nil || publisher.message != message {
		t.Fatalf("Dispatch() = %+v, %v; published = %+v", message, err, publisher.message)
	}

	if _, err := goqueue.NewChannelDispatcher(nil, "channel", "topic"); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewChannelDispatcher(nil) error = %v", err)
	}
	if _, err := goqueue.NewChannelDispatcher(publisher, "", "topic"); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewChannelDispatcher(empty) error = %v", err)
	}
	if _, err := goqueue.NewChannelDispatcher(publisher, "channel", "topic"); err != nil {
		t.Fatalf("NewChannelDispatcher(): %v", err)
	}

	executor := &facadeExecutor{}
	if _, err := goqueue.NewWorker(nil); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewWorker(nil) error = %v", err)
	}
	worker, err := goqueue.NewWorker(executor)
	if err != nil {
		t.Fatalf("NewWorker(): %v", err)
	}
	if err := worker.Handle(context.Background(), message); err != nil || executor.message != message {
		t.Fatalf("Handle() = %v; executed = %+v", err, executor.message)
	}
	settlement := &facadeSettlement{}
	if disposition, err := worker.HandleDelivery(context.Background(), message, settlement); err != nil || disposition != goqueue.Acknowledged || !settlement.acknowledged {
		t.Fatalf("HandleDelivery() = %v, %v", disposition, err)
	}

	if _, err := goqueue.NewChannelWorker("channel", nil); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewChannelWorker(nil) error = %v", err)
	}
	if _, err := goqueue.NewChannelWorker("", executor); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewChannelWorker(empty) error = %v", err)
	}
	if _, err := goqueue.NewChannelWorker("channel", executor); err != nil {
		t.Fatalf("NewChannelWorker(): %v", err)
	}
	if goqueue.Rejected != 2 || goqueue.Unsettled != 3 || goqueue.ErrPublishOutcomeUnknown == nil {
		t.Fatal("legacy queue constants changed")
	}
}

type facadePublisher struct{ message goqueue.Message }

func (publisher *facadePublisher) Publish(_ context.Context, _ string, message goqueue.Message) error {
	publisher.message = message
	return nil
}

type facadeExecutor struct{ message goqueue.Message }

func (executor *facadeExecutor) ExecuteMessage(_ context.Context, message goqueue.Message) error {
	executor.message = message
	return nil
}

type facadeSettlement struct{ acknowledged bool }

func (settlement *facadeSettlement) Acknowledge(context.Context) error {
	settlement.acknowledged = true
	return nil
}

func (*facadeSettlement) Reject(context.Context) error { return nil }
