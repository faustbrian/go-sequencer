package sequencer_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

func TestFleetReplicasClaimLeaderlesslyWithoutDuplicateCompletion(t *testing.T) {
	t.Parallel()

	const operationCount = 12
	var mu sync.Mutex
	executions := make(map[sequencer.OperationID]int, operationCount)
	completed := make(chan struct{}, operationCount)
	specs := make([]sequencer.OperationSpec, operationCount)
	for index := range specs {
		spec := validSpec(sequencer.OperationID(fmt.Sprintf("fleet.replica-%02d", index)))
		spec.Handler = sequencer.HandlerFunc(func(_ context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
			mu.Lock()
			executions[attempt.OperationID]++
			mu.Unlock()
			return sequencer.Output{}, nil
		})
		specs[index] = spec
	}
	plan, err := sequencer.CompilePlan(specs, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store := memory.New()
	observer := sequencer.ObserverFunc(func(event sequencer.Event) {
		if event.Type == sequencer.EventCompleted {
			if event.State != sequencer.Succeeded || event.Err != nil {
				t.Errorf("completion event = %+v", event)
			}
			completed <- struct{}{}
		}
	})
	newReplica := func(owner string) *sequencer.Fleet {
		fleet, fleetErr := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
			RunnerOptions: sequencer.RunnerOptions{Owner: owner, Observers: []sequencer.Observer{observer}},
			ClaimInterval: time.Millisecond, RenewInterval: 2 * time.Millisecond,
			MaxConcurrency: 3, ShutdownWait: time.Second,
		})
		if fleetErr != nil {
			t.Fatal(fleetErr)
		}
		return fleet
	}
	first, second := newReplica("pod-a"), newReplica("pod-b")
	ctx, cancel := context.WithCancel(context.Background())
	type fleetResult struct {
		owner string
		state sequencer.RunnerState
		err   error
	}
	done := make(chan fleetResult, 2)
	go func() { done <- fleetResult{owner: "pod-a", err: first.Run(ctx), state: first.State()} }()
	go func() { done <- fleetResult{owner: "pod-b", err: second.Run(ctx), state: second.State()} }()
	for range operationCount {
		select {
		case <-completed:
		case <-time.After(2 * time.Second):
			t.Fatal("replicas did not complete the shared plan")
		}
	}
	cancel()
	for range 2 {
		select {
		case result := <-done:
			if result.err != nil {
				t.Fatalf("%s Run() error = %v, state = %s", result.owner, result.err, result.state)
			}
		case <-time.After(time.Second):
			t.Fatal("replica did not stop within the test bound")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(executions) != operationCount {
		t.Fatalf("executed operations = %d, want %d", len(executions), operationCount)
	}
	for id, count := range executions {
		if count != 1 {
			t.Fatalf("operation %s executions = %d", id, count)
		}
	}
}
