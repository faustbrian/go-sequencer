package sequencerlease_test

import (
	"context"
	"fmt"
	"time"

	sequencerlease "github.com/faustbrian/go-sequencer/adapters/lease"
)

func ExampleNew() {
	adapter, err := sequencerlease.New(exampleAcquirer{})
	fmt.Println(adapter != nil, err)
	// Output: true <nil>
}

type exampleAcquirer struct{}

func (exampleAcquirer) Acquire(context.Context, string, time.Duration) (sequencerlease.Handle, error) {
	return exampleHandle{}, nil
}

type exampleHandle struct{}

func (exampleHandle) Owner() string                 { return "worker" }
func (exampleHandle) Fencing() uint64               { return 1 }
func (exampleHandle) Release(context.Context) error { return nil }
