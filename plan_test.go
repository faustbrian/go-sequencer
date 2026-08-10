package sequencer_test

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestCompilePlanUsesDeterministicTopologicalOrder(t *testing.T) {
	t.Parallel()

	postal := validSpec("postal")
	postal.Dependencies = []sequencer.OperationID{"locations"}
	locations := validSpec("locations")
	locations.Dependencies = []sequencer.OperationID{"countries"}
	countries := validSpec("countries")
	audit := validSpec("audit")

	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{postal, audit, locations, countries}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}
	if got, want := plan.IDs(), []sequencer.OperationID{"audit", "countries", "locations", "postal"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs() = %v, want %v", got, want)
	}
	ids := plan.IDs()
	ids[0] = "changed"
	if plan.IDs()[0] != "audit" {
		t.Fatal("plan IDs are mutable")
	}
}

func TestCompilePlanOrderIsInvariantAcrossEquivalentInputPermutations(t *testing.T) {
	t.Parallel()

	rootA := validSpec("root-a")
	rootB := validSpec("root-b")
	join := validSpec("join")
	join.Dependencies = []sequencer.OperationID{"root-b", "root-a"}
	side := validSpec("side")
	side.Dependencies = []sequencer.OperationID{"root-a"}
	tail := validSpec("tail")
	tail.Dependencies = []sequencer.OperationID{"root-b", "join"}
	want := []sequencer.OperationID{"root-a", "root-b", "join", "side", "tail"}

	for _, specs := range operationSpecPermutations([]sequencer.OperationSpec{tail, side, join, rootB, rootA}) {
		for dependencyOrder := 0; dependencyOrder < 4; dependencyOrder++ {
			candidate := slices.Clone(specs)
			for index := range candidate {
				candidate[index].Dependencies = slices.Clone(candidate[index].Dependencies)
				if (candidate[index].ID == "join" && dependencyOrder&1 != 0) ||
					(candidate[index].ID == "tail" && dependencyOrder&2 != 0) {
					slices.Reverse(candidate[index].Dependencies)
				}
			}
			plan, err := sequencer.CompilePlan(candidate, sequencer.PlanOptions{})
			if err != nil {
				t.Fatalf("CompilePlan() error = %v", err)
			}
			if got := plan.IDs(); !reflect.DeepEqual(got, want) {
				t.Fatalf("IDs() = %v, want %v", got, want)
			}
		}
	}
}

func operationSpecPermutations(specs []sequencer.OperationSpec) [][]sequencer.OperationSpec {
	var permutations [][]sequencer.OperationSpec
	var visit func(int)
	visit = func(index int) {
		if index == len(specs) {
			permutations = append(permutations, slices.Clone(specs))
			return
		}
		for candidate := index; candidate < len(specs); candidate++ {
			specs[index], specs[candidate] = specs[candidate], specs[index]
			visit(index + 1)
			specs[index], specs[candidate] = specs[candidate], specs[index]
		}
	}
	visit(0)
	return permutations
}

func TestCompilePlanRejectsBrokenGraphs(t *testing.T) {
	t.Parallel()

	t.Run("missing dependency", func(t *testing.T) {
		a := validSpec("a")
		a.Dependencies = []sequencer.OperationID{"missing"}
		_, err := sequencer.CompilePlan([]sequencer.OperationSpec{a}, sequencer.PlanOptions{})
		if !errors.Is(err, sequencer.ErrMissingDependency) {
			t.Fatalf("error = %v, want ErrMissingDependency", err)
		}
	})

	t.Run("cycle", func(t *testing.T) {
		a, b := validSpec("a"), validSpec("b")
		a.Dependencies = []sequencer.OperationID{"b"}
		b.Dependencies = []sequencer.OperationID{"a"}
		_, err := sequencer.CompilePlan([]sequencer.OperationSpec{a, b}, sequencer.PlanOptions{})
		if !errors.Is(err, sequencer.ErrDependencyCycle) {
			t.Fatalf("error = %v, want ErrDependencyCycle", err)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		_, err := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a"), validSpec("a")}, sequencer.PlanOptions{})
		if !errors.Is(err, sequencer.ErrDuplicateOperation) {
			t.Fatalf("error = %v, want ErrDuplicateOperation", err)
		}
	})
}

func TestCompilePlanRequiresExactDependencyIdentity(t *testing.T) {
	t.Parallel()

	dependency := validSpec("dependency")
	dependency.Version = 2
	dependency.Checksum = "sha256:dependency-v2"
	dependent := validSpec("dependent")
	dependent.DependencyRefs = []sequencer.DependencyRef{{
		ID: dependency.ID, Version: dependency.Version, Checksum: dependency.Checksum,
	}}
	if _, err := sequencer.CompilePlan([]sequencer.OperationSpec{dependent, dependency}, sequencer.PlanOptions{}); err != nil {
		t.Fatalf("CompilePlan(exact dependency) error = %v", err)
	}

	for _, mutate := range []func(*sequencer.DependencyRef){
		func(reference *sequencer.DependencyRef) { reference.Version++ },
		func(reference *sequencer.DependencyRef) { reference.Checksum = "sha256:wrong" },
	} {
		candidate := dependent
		candidate.DependencyRefs = slices.Clone(dependent.DependencyRefs)
		mutate(&candidate.DependencyRefs[0])
		if _, err := sequencer.CompilePlan([]sequencer.OperationSpec{candidate, dependency}, sequencer.PlanOptions{}); !errors.Is(err, sequencer.ErrDefinitionDrift) {
			t.Fatalf("CompilePlan(mismatched dependency) error = %v, want ErrDefinitionDrift", err)
		}
	}
}

func TestCompilePlanEnforcesBounds(t *testing.T) {
	t.Parallel()

	_, err := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a"), validSpec("b")}, sequencer.PlanOptions{MaxOperations: 1})
	if !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("error = %v, want ErrResourceLimit", err)
	}
	if _, err := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a")}, sequencer.PlanOptions{MaxOperations: 1}); err != nil {
		t.Fatalf("exact operation limit error = %v", err)
	}
	for _, options := range []sequencer.PlanOptions{{MaxOperations: -1}, {MaxDepth: -1}} {
		if _, err := sequencer.CompilePlan(nil, options); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Fatalf("negative options %+v error = %v", options, err)
		}
	}
	_, err = sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("")}, sequencer.PlanOptions{})
	if !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("invalid operation error = %v", err)
	}
	a, b := validSpec("a"), validSpec("b")
	b.Dependencies = []sequencer.OperationID{"a"}
	_, err = sequencer.CompilePlan([]sequencer.OperationSpec{a, b}, sequencer.PlanOptions{MaxDepth: 1})
	if !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("depth error = %v", err)
	}
	if _, err := sequencer.CompilePlan([]sequencer.OperationSpec{a, b}, sequencer.PlanOptions{MaxDepth: 2}); err != nil {
		t.Fatalf("exact depth limit error = %v", err)
	}
}

func TestPlanReturnsDefensiveOperationsAndLookup(t *testing.T) {
	t.Parallel()

	plan, err := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a")}, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	operations := plan.Operations()
	snapshot := operations[0].Spec()
	snapshot.Tags = []string{"mutated"}
	operation, ok := plan.Operation("a")
	if !ok || len(operation.Spec().Tags) != 0 {
		t.Fatalf("Operation(a) = %+v, %t", operation, ok)
	}
	if _, ok := plan.Operation("missing"); ok {
		t.Fatal("Operation(missing) found")
	}
}
