package sequencer

import (
	"fmt"
	"slices"
)

// PlanOptions contains explicit graph resource bounds.
type PlanOptions struct {
	MaxOperations int
	MaxDepth      int
}

// Plan is an immutable, deterministic topological execution plan.
type Plan struct {
	operations []Operation
	byID       map[OperationID]Operation
}

// CompilePlan validates and freezes a complete operation graph.
func CompilePlan(specs []OperationSpec, options PlanOptions) (*Plan, error) {
	maximum := options.MaxOperations
	if maximum == 0 {
		maximum = DefaultMaxOperations
	}
	maxDepth := options.MaxDepth
	if maxDepth == 0 {
		maxDepth = DefaultMaxGraphDepth
	}
	if maximum < 1 || maxDepth < 1 || len(specs) > maximum {
		return nil, ErrResourceLimit
	}

	byID := make(map[OperationID]Operation, len(specs))
	for _, spec := range specs {
		operation, err := NewOperation(spec)
		if err != nil {
			return nil, fmt.Errorf("compile %q: %w", spec.ID, err)
		}
		if _, duplicate := byID[spec.ID]; duplicate {
			return nil, fmt.Errorf("%w: %s", ErrDuplicateOperation, spec.ID)
		}
		byID[spec.ID] = operation
	}
	for id, operation := range byID {
		for _, dependency := range operation.spec.Dependencies {
			if _, exists := byID[dependency]; !exists {
				return nil, fmt.Errorf("%w: %s requires %s", ErrMissingDependency, id, dependency)
			}
		}
	}

	ids := make([]OperationID, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	states := make(map[OperationID]uint8, len(ids))
	ordered := make([]Operation, 0, len(ids))
	var visit func(OperationID, int) error
	visit = func(id OperationID, depth int) error {
		if depth > maxDepth {
			return ErrResourceLimit
		}
		switch states[id] {
		case 1:
			return fmt.Errorf("%w: %s", ErrDependencyCycle, id)
		case 2:
			return nil
		}
		states[id] = 1
		dependencies := slices.Clone(byID[id].spec.Dependencies)
		slices.Sort(dependencies)
		for _, dependency := range dependencies {
			if err := visit(dependency, depth+1); err != nil {
				return err
			}
		}
		states[id] = 2
		ordered = append(ordered, byID[id])
		return nil
	}
	for _, id := range ids {
		if err := visit(id, 1); err != nil {
			return nil, err
		}
	}
	return &Plan{operations: ordered, byID: byID}, nil
}

// IDs returns operation identifiers in deterministic execution order.
func (plan *Plan) IDs() []OperationID {
	ids := make([]OperationID, len(plan.operations))
	for index, operation := range plan.operations {
		ids[index] = operation.spec.ID
	}
	return ids
}

// Operations returns defensive copies in deterministic execution order.
func (plan *Plan) Operations() []Operation {
	operations := make([]Operation, len(plan.operations))
	for index, operation := range plan.operations {
		operations[index] = Operation{spec: cloneSpec(operation.spec)}
	}
	return operations
}

// Operation returns a defensive copy by stable identifier.
func (plan *Plan) Operation(id OperationID) (Operation, bool) {
	operation, ok := plan.byID[id]
	return Operation{spec: cloneSpec(operation.spec)}, ok
}
