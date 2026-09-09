package sequencerretry_test

import (
	"context"
	"fmt"

	sequencerretry "github.com/faustbrian/go-sequencer/adapters/retry"
)

func ExampleNew() {
	adapter, err := sequencerretry.New(examplePolicy{})
	fmt.Println(adapter != nil, err)
	// Output: true <nil>
}

type examplePolicy struct{}

func (examplePolicy) Do(ctx context.Context, operation func(context.Context) error) error {
	return operation(ctx)
}
