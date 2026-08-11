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

func TestChannelBoundDispatcherAndWorkerRejectCrossChannelMessages(t *testing.T) {
	t.Parallel()

	publisher := &publisherStub{}
	dispatcher, err := goqueue.NewChannelDispatcher(publisher, "deploy", "deployments")
	if err != nil {
		t.Fatal(err)
	}
	request := goqueue.Request{OperationID: "a", Version: 1, Checksum: "sum", Channel: "maintenance"}
	if _, err := dispatcher.Dispatch(context.Background(), request); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("Dispatch(channel mismatch) error = %v", err)
	}
	request.Channel = "deploy"
	message, err := dispatcher.Dispatch(context.Background(), request)
	if err != nil || message.Channel != "deploy" {
		t.Fatalf("Dispatch() = %+v, %v", message, err)
	}
	executor := &executorStub{}
	worker, err := goqueue.NewChannelWorker("deploy", executor)
	if err != nil {
		t.Fatal(err)
	}
	message.Channel = "maintenance"
	if err := worker.Handle(context.Background(), message); !errors.Is(err, goqueue.ErrInvalidAdapter) || executor.message.OperationID != "" {
		t.Fatalf("Handle(cross-channel) error = %v, executor message = %+v", err, executor.message)
	}
}

func TestChannelBoundConstructorsRejectInvalidBindings(t *testing.T) {
	t.Parallel()

	publisher := &publisherStub{}
	for _, channel := range []string{"", "invalid channel"} {
		if _, err := goqueue.NewChannelDispatcher(publisher, channel, "deployments"); !errors.Is(err, goqueue.ErrInvalidAdapter) {
			t.Fatalf("NewChannelDispatcher(%q) error = %v", channel, err)
		}
		if _, err := goqueue.NewChannelWorker(channel, &executorStub{}); !errors.Is(err, goqueue.ErrInvalidAdapter) {
			t.Fatalf("NewChannelWorker(%q) error = %v", channel, err)
		}
	}
	if _, err := goqueue.NewChannelDispatcher(nil, "deploy", "deployments"); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewChannelDispatcher(nil) error = %v", err)
	}
	if _, err := goqueue.NewChannelWorker("deploy", nil); !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("NewChannelWorker(nil) error = %v", err)
	}
}

func TestDispatcherPreservesDeliveryIdentityWhenPublishOutcomeIsUnknown(t *testing.T) {
	t.Parallel()

	cause := errors.New("publish acknowledgement lost")
	publisher := &publisherStub{err: cause}
	dispatcher, err := goqueue.NewDispatcher(publisher, "deployments")
	if err != nil {
		t.Fatal(err)
	}
	message, err := dispatcher.Dispatch(context.Background(), goqueue.Request{
		OperationID: "postal.backfill", Version: 2, Checksum: "sha256:abc",
	})
	if !errors.Is(err, goqueue.ErrPublishOutcomeUnknown) || !errors.Is(err, cause) {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if message.DeliveryID == "" || message != publisher.message {
		t.Fatalf("Dispatch() message = %+v, published = %+v", message, publisher.message)
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

func TestWorkerSettlesOnlyConfirmedExecutionOutcomes(t *testing.T) {
	t.Parallel()

	message := goqueue.Message{OperationID: "a", Version: 1, Checksum: "sha256:a", DeliveryID: "delivery"}
	tests := []struct {
		name            string
		executionErr    error
		wantDisposition goqueue.Disposition
		wantAck         bool
		wantReject      bool
	}{
		{name: "confirmed completion", wantDisposition: goqueue.Acknowledged, wantAck: true},
		{name: "definite failure", executionErr: sequencer.Permanent(errors.New("invalid")), wantDisposition: goqueue.Rejected, wantReject: true},
		{name: "commit unknown", executionErr: sequencer.UnknownResult(errors.New("commit response lost")), wantDisposition: goqueue.Unsettled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &executorStub{err: test.executionErr}
			settlement := &settlementStub{}
			worker, err := goqueue.NewWorker(executor)
			if err != nil {
				t.Fatal(err)
			}
			disposition, err := worker.HandleDelivery(context.Background(), message, settlement)
			if disposition != test.wantDisposition || !errors.Is(err, test.executionErr) {
				t.Fatalf("HandleDelivery() = %v, %v", disposition, err)
			}
			if settlement.acknowledged != test.wantAck || settlement.rejected != test.wantReject {
				t.Fatalf("settlement = acknowledged:%t rejected:%t", settlement.acknowledged, settlement.rejected)
			}
		})
	}
}

func TestWorkerLeavesDeliveryUnsettledWhenSettlementCannotBeConfirmed(t *testing.T) {
	t.Parallel()

	message := goqueue.Message{OperationID: "a", Version: 1, Checksum: "sha256:a", DeliveryID: "delivery"}
	settlementErr := errors.New("settlement unavailable")
	tests := []struct {
		name         string
		executionErr error
		settlement   *settlementStub
	}{
		{name: "acknowledgement", settlement: &settlementStub{ackErr: settlementErr}},
		{name: "rejection", executionErr: sequencer.Permanent(errors.New("invalid")), settlement: &settlementStub{rejectErr: settlementErr}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker, err := goqueue.NewWorker(&executorStub{err: test.executionErr})
			if err != nil {
				t.Fatal(err)
			}
			disposition, err := worker.HandleDelivery(context.Background(), message, test.settlement)
			if disposition != goqueue.Unsettled || !errors.Is(err, settlementErr) ||
				(test.executionErr != nil && !errors.Is(err, test.executionErr)) {
				t.Fatalf("HandleDelivery() = %v, %v", disposition, err)
			}
		})
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

func TestWorkerLostAcknowledgementRedeliveryDoesNotRepeatCommittedOperation(t *testing.T) {
	t.Parallel()

	store := memory.New()
	calls := 0
	spec := sequencer.OperationSpec{
		ID: "queue.ack-lost", Version: 1, Checksum: "sha256:queue-ack-lost",
		Description: "prove queue settlement follows durable completion", Channel: "queue",
		Policy: sequencer.Policy{Mode: sequencer.OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Second},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			calls++
			return sequencer.Output{Summary: "committed"}, nil
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
	ackLost := errors.New("queue acknowledgement lost")
	disposition, err := worker.HandleDelivery(context.Background(), message, &settlementStub{ackErr: ackLost})
	if disposition != goqueue.Unsettled || !errors.Is(err, ackLost) || calls != 1 {
		t.Fatalf("first delivery = %v, %v; calls = %d", disposition, err, calls)
	}
	message.DeliveryID = "delivery-2"
	settlement := &settlementStub{}
	disposition, err = worker.HandleDelivery(context.Background(), message, settlement)
	if disposition != goqueue.Acknowledged || err != nil || !settlement.acknowledged || calls != 1 {
		t.Fatalf("redelivery = %v, %v; acknowledged = %t, calls = %d", disposition, err, settlement.acknowledged, calls)
	}
	history, err := store.History(context.Background(), spec.ID, spec.Version, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Succeeded {
		t.Fatalf("History() = %+v, %v", history, err)
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
	valid := goqueue.Message{OperationID: "a", Version: 1, Checksum: "sum", DeliveryID: "delivery"}
	if disposition, err := worker.HandleDelivery(context.Background(), valid, nil); disposition != goqueue.Unsettled || !errors.Is(err, goqueue.ErrInvalidAdapter) {
		t.Fatalf("HandleDelivery(nil settlement) = %v, %v", disposition, err)
	}
	for _, message := range []goqueue.Message{
		{Version: 1, Checksum: "sum", DeliveryID: "delivery"},
		{OperationID: "a", Checksum: "sum", DeliveryID: "delivery"},
		{OperationID: "a", Version: 1, DeliveryID: "delivery"},
		{OperationID: "a", Version: 1, Checksum: "sum"},
	} {
		if err := worker.Handle(context.Background(), message); !errors.Is(err, goqueue.ErrInvalidAdapter) {
			t.Fatalf("Handle(%+v) error = %v", message, err)
		}
		if disposition, err := worker.HandleDelivery(context.Background(), message, &settlementStub{}); disposition != goqueue.Unsettled || !errors.Is(err, goqueue.ErrInvalidAdapter) {
			t.Fatalf("HandleDelivery(%+v) = %v, %v", message, disposition, err)
		}
	}
}

func TestQueueCommandsEnforceFieldBounds(t *testing.T) {
	t.Parallel()

	const (
		maxOperationIDBytes = 255
		maxChecksumBytes    = 512
		maxDeliveryIDBytes  = 255
	)
	exactID := sequencer.OperationID(strings.Repeat("a", maxOperationIDBytes))
	exactChecksum := strings.Repeat("c", maxChecksumBytes)
	dispatcher, err := goqueue.NewDispatcher(&publisherStub{}, "topic")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = dispatcher.Dispatch(context.Background(), goqueue.Request{
		OperationID: exactID, Version: 1, Checksum: exactChecksum,
	}); err != nil {
		t.Fatalf("Dispatch(exact bounds) error = %v", err)
	}
	for name, request := range map[string]goqueue.Request{
		"operation id overflow": {OperationID: sequencer.OperationID(strings.Repeat("a", maxOperationIDBytes+1)), Version: 1, Checksum: "sum"},
		"invalid operation id":  {OperationID: "invalid operation", Version: 1, Checksum: "sum"},
		"checksum overflow":     {OperationID: "a", Version: 1, Checksum: strings.Repeat("c", maxChecksumBytes+1)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := dispatcher.Dispatch(context.Background(), request); !errors.Is(err, goqueue.ErrInvalidAdapter) {
				t.Fatalf("Dispatch() error = %v", err)
			}
		})
	}

	executor := &executorStub{}
	worker, err := goqueue.NewWorker(executor)
	if err != nil {
		t.Fatal(err)
	}
	exact := goqueue.Message{
		OperationID: exactID, Version: 1, Checksum: exactChecksum,
		DeliveryID: strings.Repeat("d", maxDeliveryIDBytes),
	}
	if err = worker.Handle(context.Background(), exact); err != nil || executor.message != exact {
		t.Fatalf("Handle(exact bounds) = %+v, %v", executor.message, err)
	}
	for name, message := range map[string]goqueue.Message{
		"operation id overflow": {OperationID: sequencer.OperationID(strings.Repeat("a", maxOperationIDBytes+1)), Version: 1, Checksum: "sum", DeliveryID: "delivery"},
		"checksum overflow":     {OperationID: "a", Version: 1, Checksum: strings.Repeat("c", maxChecksumBytes+1), DeliveryID: "delivery"},
		"delivery id overflow":  {OperationID: "a", Version: 1, Checksum: "sum", DeliveryID: strings.Repeat("d", maxDeliveryIDBytes+1)},
	} {
		t.Run(name, func(t *testing.T) {
			executor.message = goqueue.Message{}
			if err := worker.Handle(context.Background(), message); !errors.Is(err, goqueue.ErrInvalidAdapter) || executor.message.OperationID != "" {
				t.Fatalf("Handle() = %+v, %v", executor.message, err)
			}
		})
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

type settlementStub struct {
	acknowledged bool
	rejected     bool
	ackErr       error
	rejectErr    error
}

func (settlement *settlementStub) Acknowledge(context.Context) error {
	settlement.acknowledged = true
	return settlement.ackErr
}

func (settlement *settlementStub) Reject(context.Context) error {
	settlement.rejected = true
	return settlement.rejectErr
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
