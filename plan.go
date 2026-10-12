package sequencer

import (
	"fmt"
	"slices"
	"strings"
)

// PlanOptions contains explicit graph resource bounds.
type PlanOptions struct {
	// MaxOperations limits input definitions; zero selects DefaultMaxOperations, negative is invalid.
	MaxOperations int
	// MaxDepth limits recursive dependency visits starting at depth one;
	// zero selects DefaultMaxGraphDepth, negative is invalid.
	MaxDepth int
}

// Plan is immutable dependency-first metadata; callback collaborators remain borrowed.
// Read methods may be used concurrently, but collaborators need their own synchronization.
// Use CompilePlan; a nil Plan is not supported.
type Plan struct {
	operations []Operation
	byID       map[OperationID]Operation
}

// CompilePlan copies and validates a complete graph, rejecting duplicate IDs,
// missing or mismatched exact dependencies, cycles and resource-limit violations.
// Sorted IDs and sorted dependency visits produce deterministic depth-first,
// dependency-first order, not a globally lexicographically smallest ready-node order.
// Empty input is valid; errors return no partial Plan.
func CompilePlan(specs []OperationSpec, options PlanOptions) (*Plan, error) {
	if options.MaxOperations < 0 || options.MaxDepth < 0 {
		return nil, ErrResourceLimit
	}
	maximum := options.MaxOperations
	if maximum == 0 {
		maximum = DefaultMaxOperations
	}
	maxDepth := options.MaxDepth
	if maxDepth == 0 {
		maxDepth = DefaultMaxGraphDepth
	}
	if len(specs) > maximum {
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
		for _, dependency := range operation.spec.DependencyRefs {
			target, exists := byID[dependency.ID]
			if !exists {
				return nil, fmt.Errorf("%w: %s requires %s", ErrMissingDependency, id, dependency.ID)
			}
			if target.spec.Version != dependency.Version || target.spec.Checksum != dependency.Checksum {
				return nil, fmt.Errorf("%w: %s requires %s version %d", ErrDefinitionDrift, id, dependency.ID, dependency.Version)
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
		dependencies := slices.Clone(byID[id].spec.DependencyRefs)
		slices.SortFunc(dependencies, func(left, right DependencyRef) int {
			return strings.Compare(string(left.ID), string(right.ID))
		})
		for _, dependency := range dependencies {
			if err := visit(dependency.ID, depth+1); err != nil {
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

// Operations copies definition metadata in deterministic execution order;
// handlers and conditions remain borrowed.
func (plan *Plan) Operations() []Operation {
	operations := make([]Operation, len(plan.operations))
	for index, operation := range plan.operations {
		operations[index] = Operation{spec: cloneSpec(operation.spec)}
	}
	return operations
}

// Operation copies definition metadata by ID; handlers and conditions remain borrowed.
// A missing ID returns the zero Operation and false.
func (plan *Plan) Operation(id OperationID) (Operation, bool) {
	operation, ok := plan.byID[id]
	return Operation{spec: cloneSpec(operation.spec)}, ok
}
