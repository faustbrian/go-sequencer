package goqueue_test

import (
	"context"
	"errors"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/goqueue"
)

func TestDispatcherPublishesIdentityOnlyMessage(t *testing.T) {
	t.Parallel()

	publisher := &publisherStub{}
	dispatcher, err := goqueue.NewDispatcher(publisher, "deployments")
	if err != nil {
		t.Fatal(err)
	}
	message, err := dispatcher.Dispatch(context.Background(), goqueue.Request{
		OperationID: "postal.backfill", Version: 2, Checksum: "sha256:abc",
	})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.topic != "deployments" || publisher.message != message || message.DeliveryID == "" {
		t.Fatalf("published = %q %+v", publisher.topic, publisher.message)
	}
}

func TestWorkerDelegatesRedeliveryToDurableExecutor(t *testing.T) {
	t.Parallel()

	executor := &executorStub{err: sequencer.ErrNoEligibleOperation}
	worker, err := goqueue.NewWorker(executor)
	if err != nil {
		t.Fatal(err)
	}
	message := goqueue.Message{OperationID: "a", Version: 1, Checksum: "sha256:a", DeliveryID: "delivery"}
	if err := worker.Handle(context.Background(), message); !errors.Is(err, sequencer.ErrNoEligibleOperation) {
		t.Fatalf("Handle() error = %v", err)
	}
	if executor.message != message {
		t.Fatalf("executor message = %+v", executor.message)
	}
}

func TestAdaptersRejectInvalidInputAndPropagateTransportErrors(t *testing.T) {
	t.Parallel()

	if _, err := goqueue.NewDispatcher(nil, "topic"); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewDispatcher(nil) error = %v", err)
	}
	publisher := &publisherStub{err: errors.New("publish")}
	dispatcher, _ := goqueue.NewDispatcher(publisher, "topic")
	if _, err := dispatcher.Dispatch(context.Background(), goqueue.Request{}); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("Dispatch(invalid) error = %v", err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), goqueue.Request{OperationID: "a", Version: 1, Checksum: "sum"}); !errors.Is(err, publisher.err) {
		t.Fatalf("Dispatch(publish) error = %v", err)
	}
	if _, err := goqueue.NewWorker(nil); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewWorker(nil) error = %v", err)
	}
	worker, _ := goqueue.NewWorker(&executorStub{})
	if err := worker.Handle(context.Background(), goqueue.Message{}); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("Handle(invalid) error = %v", err)
	}
}

type publisherStub struct {
	topic   string
	message goqueue.Message
	err     error
}

func (publisher *publisherStub) Publish(_ context.Context, topic string, message goqueue.Message) error {
	publisher.topic, publisher.message = topic, message
	return publisher.err
}

type executorStub struct {
	message goqueue.Message
	err     error
}

func (executor *executorStub) ExecuteMessage(_ context.Context, message goqueue.Message) error {
	executor.message = message
	return executor.err
}
