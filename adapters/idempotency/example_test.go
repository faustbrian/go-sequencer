package sequenceridempotency_test

import (
	"context"
	"fmt"

	sequenceridempotency "github.com/faustbrian/go-sequencer/adapters/idempotency"
)

func ExampleNew() {
	adapter, err := sequenceridempotency.New(exampleGate{})
	fmt.Println(adapter != nil, err)
	// Output: true <nil>
}

type exampleGate struct{}

func (exampleGate) Begin(context.Context, string) (sequenceridempotency.Token, bool, error) {
	return "token", true, nil
}

func (exampleGate) Complete(context.Context, sequenceridempotency.Token) error { return nil }
func (exampleGate) Fail(context.Context, sequenceridempotency.Token, error) error {
	return nil
}
