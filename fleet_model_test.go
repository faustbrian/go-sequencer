package sequencer_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/memory"
)

func TestFleetReplicasClaimLeaderlesslyWithoutDuplicateCompletion(t *testing.T) {
	t.Parallel()

	const operationCount = 12
	var mu sync.Mutex
	executions := make(map[sequencer.OperationID]int, operationCount)
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
	defer cancel()
	firstDone := startFleet(ctx, t, first)
	secondDone := startFleet(ctx, t, second)
	// Observer delivery is best-effort with bounded overflow. Persisted terminal
	// state, not a telemetry count, proves every shared operation completed.
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
waitForPersistence:
	for {
		allSucceeded := true
		for _, spec := range specs {
			record, snapshotErr := store.Snapshot(context.Background(), spec.ID, spec.Version)
			if snapshotErr != nil || record.State != sequencer.Succeeded {
				allSucceeded = false
				break
			}
		}
		if allSucceeded {
			break waitForPersistence
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			var firstErr, secondErr error
			select {
			case firstErr = <-firstDone:
			default:
			}
			select {
			case secondErr = <-secondDone:
			default:
			}
			t.Fatalf("replicas did not complete the shared plan: first state=%v error=%v; second state=%v error=%v", first.State(), firstErr, second.State(), secondErr)
		}
	}
	cancel()
	for _, replica := range []struct {
		owner string
		fleet *sequencer.Fleet
		done  <-chan error
	}{
		{owner: "pod-a", fleet: first, done: firstDone},
		{owner: "pod-b", fleet: second, done: secondDone},
	} {
		if err := awaitFleetResult(t, replica.done); err != nil {
			t.Fatalf("%s Run() error = %v, state = %s", replica.owner, err, replica.fleet.State())
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
