package sequencer_test

import (
	"errors"
	"reflect"
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

func TestCompilePlanEnforcesBounds(t *testing.T) {
	t.Parallel()

	_, err := sequencer.CompilePlan([]sequencer.OperationSpec{validSpec("a"), validSpec("b")}, sequencer.PlanOptions{MaxOperations: 1})
	if !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("error = %v, want ErrResourceLimit", err)
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
