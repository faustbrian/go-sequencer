package sequencerqueue_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	goqueue "github.com/faustbrian/go-sequencer/v2/adapters/queue"
)

func TestQueueChecksumCanonicalGrammar(t *testing.T) {
	for _, test := range []struct {
		name, checksum string
		valid          bool
	}{
		{"canonical", "sha256:" + strings.Repeat("a", 64), true},
		{"70 bytes", "sha256:" + strings.Repeat("a", 63), false},
		{"72 bytes", "sha256:" + strings.Repeat("a", 65), false},
		{"uppercase", "sha256:" + strings.Repeat("A", 64), false},
		{"nonhex", "sha256:" + strings.Repeat("g", 64), false},
		{"wrong prefix", "SHA256:" + strings.Repeat("a", 64), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			publisher := &publisherStub{}
			dispatcher, err := goqueue.NewDispatcher(publisher, "operations")
			if err != nil {
				t.Fatal(err)
			}
			_, err = dispatcher.Dispatch(context.Background(), goqueue.Request{OperationID: "audit", Version: 1, Checksum: test.checksum})
			if test.valid && err != nil || !test.valid && !errors.Is(err, goqueue.ErrInvalidAdapter) {
				t.Fatalf("dispatch error=%v", err)
			}
			if (publisher.message.DeliveryID != "") != test.valid {
				t.Fatal("invalid command reached publisher")
			}
			executor := &fuzzExecutor{}
			worker, err := goqueue.NewWorker(executor)
			if err != nil {
				t.Fatal(err)
			}
			err = worker.Handle(context.Background(), goqueue.Message{OperationID: "audit", Version: 1, Checksum: test.checksum, DeliveryID: "delivery"})
			if test.valid && err != nil || !test.valid && !errors.Is(err, goqueue.ErrInvalidAdapter) || executor.called != test.valid {
				t.Fatalf("handle error=%v called=%t", err, executor.called)
			}
		})
	}
}
