package goqueue_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/goqueue"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
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

func TestWorkerRedeliveryCannotRepeatCompletedLedgerOperation(t *testing.T) {
	t.Parallel()

	store := memory.New()
	calls := 0
	spec := sequencer.OperationSpec{
		ID: "queue.redelivery", Version: 1, Checksum: "sha256:queue-redelivery",
		Description: "prove redelivery fencing", Channel: "queue",
		Policy: sequencer.Policy{Mode: sequencer.OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Second},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			calls++
			return sequencer.Output{Summary: "done"}, nil
		}),
	}
	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := sequencer.NewRunner(plan, store, sequencer.RunnerOptions{Owner: "queue-worker"})
	if err != nil {
		t.Fatal(err)
	}
	message := goqueue.Message{OperationID: spec.ID, Version: spec.Version, Checksum: spec.Checksum, DeliveryID: "delivery-1"}
	worker, err := goqueue.NewWorker(&ledgerExecutor{runner: runner, expected: message})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Handle(context.Background(), message); err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	message.DeliveryID = "delivery-2"
	if err := worker.Handle(context.Background(), message); err != nil {
		t.Fatalf("redelivered Handle() error = %v", err)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || calls != 1 || len(history) != 1 || history[0].State != sequencer.Succeeded {
		t.Fatalf("calls = %d, history = %+v, error = %v", calls, history, err)
	}
}

func TestAdaptersRejectInvalidInputAndPropagateTransportErrors(t *testing.T) {
	t.Parallel()

	if _, err := goqueue.NewDispatcher(nil, "topic"); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewDispatcher(nil) error = %v", err)
	}
	if _, err := goqueue.NewDispatcher(&publisherStub{}, strings.Repeat("t", 255)); err != nil {
		t.Fatalf("NewDispatcher(exact topic bound) error = %v", err)
	}
	if _, err := goqueue.NewDispatcher(&publisherStub{}, strings.Repeat("t", 256)); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewDispatcher(topic overflow) error = %v", err)
	}
	publisher := &publisherStub{err: errors.New("publish")}
	dispatcher, _ := goqueue.NewDispatcher(publisher, "topic")
	for _, request := range []goqueue.Request{
		{Version: 1, Checksum: "sum"},
		{OperationID: "a", Checksum: "sum"},
		{OperationID: "a", Version: 1},
	} {
		if _, err := dispatcher.Dispatch(context.Background(), request); !errors.Is(err, goqueue.ErrInvalidAdapter) {
			t.Fatalf("Dispatch(%+v) error = %v", request, err)
		}
	}
	if _, err := dispatcher.Dispatch(context.Background(), goqueue.Request{OperationID: "a", Version: 1, Checksum: "sum"}); !errors.Is(err, publisher.err) {
		t.Fatalf("Dispatch(publish) error = %v", err)
	}
	if _, err := goqueue.NewWorker(nil); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewWorker(nil) error = %v", err)
	}
	worker, _ := goqueue.NewWorker(&executorStub{})
	for _, message := range []goqueue.Message{
		{Version: 1, Checksum: "sum", DeliveryID: "delivery"},
		{OperationID: "a", Checksum: "sum", DeliveryID: "delivery"},
		{OperationID: "a", Version: 1, DeliveryID: "delivery"},
		{OperationID: "a", Version: 1, Checksum: "sum"},
	} {
		if err := worker.Handle(context.Background(), message); !errors.Is(err, goqueue.ErrInvalidAdapter) {
			t.Fatalf("Handle(%+v) error = %v", message, err)
		}
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

type ledgerExecutor struct {
	runner   *sequencer.Runner
	expected goqueue.Message
}

func (executor *ledgerExecutor) ExecuteMessage(ctx context.Context, message goqueue.Message) error {
	if message.OperationID != executor.expected.OperationID || message.Version != executor.expected.Version || message.Checksum != executor.expected.Checksum {
		return goqueue.ErrInvalidAdapter
	}
	_, err := executor.runner.Execute(ctx)
	return err
}

func (executor *executorStub) ExecuteMessage(_ context.Context, message goqueue.Message) error {
	executor.message = message
	return executor.err
}
