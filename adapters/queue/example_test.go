package sequencerqueue_test

import (
	"context"
	"fmt"

	sequencerqueue "github.com/faustbrian/go-sequencer/adapters/queue"
)

func ExampleNewDispatcher() {
	dispatcher, err := sequencerqueue.NewDispatcher(examplePublisher{}, "operations")
	fmt.Println(dispatcher != nil, err)
	// Output: true <nil>
}

type examplePublisher struct{}

func (examplePublisher) Publish(context.Context, string, sequencerqueue.Message) error {
	return nil
}
