package goqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/faustbrian/golib/pkg/sequencer/goqueue"
)

var fuzzIdentifierPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/-]{0,254}$`)

func FuzzQueueMessageJSONValidation(fuzz *testing.F) {
	fuzz.Add([]byte(`{"operation_id":"postal","version":1,"checksum":"sha256:postal","delivery_id":"delivery"}`), "")
	fuzz.Add([]byte(`{"operation_id":"postal","version":1,"checksum":"sha256:postal","channel":"deploy","delivery_id":"delivery"}`), "deploy")
	fuzz.Add([]byte(`{"operation_id":"","version":0,"checksum":"","channel":"Deploy Queue","delivery_id":""}`), "deploy")
	fuzz.Add([]byte(`{"operation_id":"postal","version":1,"checksum":"sha256:postal","delivery_id":"`+strings.Repeat("d", 256)+`"}`), "")
	fuzz.Add([]byte(`{"operation_id":"postal","version":1,"checksum":"sha256:postal","delivery_id":"delivery","payload":"must-not-be-forwarded"}`), "")
	fuzz.Fuzz(func(t *testing.T, encoded []byte, expectedChannel string) {
		const maxQueueMessageBytes = 16 << 10
		if len(encoded) > maxQueueMessageBytes || len(expectedChannel) > 512 {
			return
		}
		var message goqueue.Message
		if json.Unmarshal(encoded, &message) != nil {
			return
		}

		executor := &fuzzExecutor{}
		var worker *goqueue.Worker
		var err error
		if expectedChannel == "" {
			worker, err = goqueue.NewWorker(executor)
		} else {
			worker, err = goqueue.NewChannelWorker(expectedChannel, executor)
			if !fuzzIdentifierPattern.MatchString(expectedChannel) {
				if !errors.Is(err, goqueue.ErrInvalidAdapter) {
					t.Fatalf("invalid channel constructor error = %v", err)
				}
				return
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		err = worker.Handle(context.Background(), message)
		valid := fuzzIdentifierPattern.MatchString(string(message.OperationID)) && message.Version != 0 &&
			message.Checksum != "" && len(message.Checksum) <= 512 &&
			message.DeliveryID != "" && len(message.DeliveryID) <= 255 &&
			((expectedChannel == "" && (message.Channel == "" || fuzzIdentifierPattern.MatchString(message.Channel))) || message.Channel == expectedChannel)
		if valid != executor.called {
			t.Fatalf("executor called = %t for message %+v and channel %q", executor.called, message, expectedChannel)
		}
		if valid && err != nil {
			t.Fatalf("valid message error = %v", err)
		}
		if !valid && !errors.Is(err, goqueue.ErrInvalidAdapter) {
			t.Fatalf("invalid message error = %v", err)
		}
	})
}

type fuzzExecutor struct{ called bool }

func (executor *fuzzExecutor) ExecuteMessage(context.Context, goqueue.Message) error {
	executor.called = true
	return nil
}

var _ goqueue.Executor = (*fuzzExecutor)(nil)
