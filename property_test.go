package sequencer_test

import (
	"math/rand/v2"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestPlanPropertyDependenciesAlwaysPrecedeDependents(t *testing.T) {
	t.Parallel()

	for seed := uint64(0); seed < 1_000; seed++ {
		// #nosec G404 -- deterministic property generation is not security-sensitive.
		random := rand.New(rand.NewPCG(seed, seed+1))
		count := 1 + random.IntN(100)
		specs := make([]sequencer.OperationSpec, count)
		for index := range count {
			specs[index] = fuzzSpec(index)
			if index > 0 && random.IntN(2) == 1 {
				dependency := specs[random.IntN(index)]
				specs[index].DependencyRefs = []sequencer.DependencyRef{{ID: dependency.ID, Version: dependency.Version, Checksum: dependency.Checksum}}
			}
		}
		plan, err := sequencer.CompilePlan(specs, sequencer.PlanOptions{})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		positions := make(map[sequencer.OperationID]int, count)
		for index, id := range plan.IDs() {
			positions[id] = index
		}
		for _, spec := range specs {
			for _, dependency := range spec.DependencyRefs {
				if positions[dependency.ID] >= positions[spec.ID] {
					t.Fatalf("seed %d: %s does not precede %s", seed, dependency.ID, spec.ID)
				}
			}
		}
	}
}
