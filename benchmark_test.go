package sequencer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

func BenchmarkCompilePlanThousandOperations(benchmark *testing.B) {
	specs := make([]sequencer.OperationSpec, 1_000)
	specs[0] = fuzzSpec(0)
	for index := 1; index < len(specs); index++ {
		specs[index] = fuzzSpec(index)
		specs[index].Dependencies = []sequencer.OperationID{specs[index-1].ID}
	}
	benchmark.ReportAllocs()
	for benchmark.Loop() {
		if _, err := sequencer.CompilePlan(specs, sequencer.PlanOptions{}); err != nil {
			benchmark.Fatal(err)
		}
	}
}

func BenchmarkMemoryClaimBacklog(benchmark *testing.B) {
	ctx := context.Background()
	for benchmark.Loop() {
		store := memory.New()
		registrations := make([]sequencer.Registration, 1_000)
		ids := make([]sequencer.OperationID, 1_000)
		for index := range registrations {
			ids[index] = sequencer.OperationID(fmt.Sprintf("operation-%03d", index))
			registrations[index] = sequencer.Registration{ID: ids[index], Version: 1, Checksum: "sha256:test"}
		}
		if err := store.Register(ctx, registrations, time.Now()); err != nil {
			benchmark.Fatal(err)
		}
		if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: ids, Owner: "benchmark", Now: time.Now(), LeaseDuration: time.Minute}); err != nil {
			benchmark.Fatal(err)
		}
	}
}
