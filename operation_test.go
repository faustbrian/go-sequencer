package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func TestNewOperationValidatesAndFreezesMetadata(t *testing.T) {
	t.Parallel()

	tags := []string{"postal", "backfill"}
	dependencies := []sequencer.OperationID{"schema-ready"}
	op, err := sequencer.NewOperation(sequencer.OperationSpec{
		ID:           "postal.backfill-postcodes",
		Version:      2,
		Checksum:     "sha256:0123456789abcdef",
		Description:  "Backfill normalized postcodes",
		Tags:         tags,
		Channel:      "deploy",
		Dependencies: dependencies,
		Environments: []string{"production"},
		Policy: sequencer.Policy{
			Mode:          sequencer.OneTime,
			MaxAttempts:   3,
			MaxExceptions: 3,
			Timeout:       time.Minute,
		},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{Summary: "done"}, nil
		}),
	})
	if err != nil {
		t.Fatalf("NewOperation() error = %v", err)
	}

	tags[0] = "mutated"
	dependencies[0] = "mutated"
	if got := op.Spec().Tags[0]; got != "postal" {
		t.Fatalf("operation retained caller tags: %q", got)
	}
	if got := op.Spec().Dependencies[0]; got != "schema-ready" {
		t.Fatalf("operation retained caller dependencies: %q", got)
	}
	snapshot := op.Spec()
	snapshot.Tags[0] = "changed"
	if got := op.Spec().Tags[0]; got != "postal" {
		t.Fatalf("Spec returned mutable tags: %q", got)
	}
}

func TestNewOperationRequiresAndFreezesExactDependencyReferences(t *testing.T) {
	t.Parallel()

	legacy := validSpec("legacy-dependent")
	legacy.Dependencies = []sequencer.OperationID{"dependency"}
	if _, err := sequencer.NewOperation(legacy); !errors.Is(err, sequencer.ErrUnpinnedDependency) {
		t.Fatalf("NewOperation(legacy dependency) error = %v, want ErrUnpinnedDependency", err)
	}

	references := []sequencer.DependencyRef{{ID: "dependency", Version: 2, Checksum: "sha256:dependency-v2"}}
	exact := validSpec("exact-dependent")
	exact.DependencyRefs = references
	operation, err := sequencer.NewOperation(exact)
	if err != nil {
		t.Fatalf("NewOperation(exact dependency) error = %v", err)
	}
	references[0].Checksum = "mutated"
	if got := operation.Spec().DependencyRefs[0].Checksum; got != "sha256:dependency-v2" {
		t.Fatalf("operation retained caller dependency refs: %q", got)
	}
	snapshot := operation.Spec()
	snapshot.DependencyRefs[0].Version = 99
	if got := operation.Spec().DependencyRefs[0].Version; got != 2 {
		t.Fatalf("Spec returned mutable dependency refs: %d", got)
	}
}

func TestNewOperationAcceptsEveryBoundedModeAndExactCollectionLimit(t *testing.T) {
	t.Parallel()

	spec := validSpec("repeatable.bounds")
	spec.Policy.Mode = sequencer.Repeatable
	spec.Policy.MaxAttempts = 1
	spec.Policy.MaxExceptions = 2
	spec.Dependencies = make([]sequencer.OperationID, sequencer.DefaultMaxDependencies)
	for index := range spec.Dependencies {
		spec.Dependencies[index] = sequencer.OperationID(fmt.Sprintf("dependency-%d", index))
	}
	spec.Tags = make([]string, sequencer.DefaultMaxTags)
	for index := range spec.Tags {
		spec.Tags[index] = fmt.Sprintf("tag-%d", index)
	}
	if _, err := sequencer.NewOperation(spec); err != nil {
		t.Fatalf("NewOperation() exact limits error = %v", err)
	}
}

func TestNewOperationRejectsUnsafeDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec sequencer.OperationSpec
	}{
		{name: "missing id", spec: validSpec("")},
		{name: "missing checksum", spec: func() sequencer.OperationSpec { s := validSpec("a"); s.Checksum = ""; return s }()},
		{name: "missing handler", spec: func() sequencer.OperationSpec { s := validSpec("a"); s.Handler = nil; return s }()},
		{name: "unbounded attempts", spec: func() sequencer.OperationSpec { s := validSpec("a"); s.Policy.MaxAttempts = 0; return s }()},
		{name: "unbounded exceptions", spec: func() sequencer.OperationSpec { s := validSpec("a"); s.Policy.MaxExceptions = 0; return s }()},
		{name: "unbounded timeout", spec: func() sequencer.OperationSpec { s := validSpec("a"); s.Policy.Timeout = 0; return s }()},
		{name: "invalid mode", spec: func() sequencer.OperationSpec {
			s := validSpec("a")
			s.Policy.Mode = sequencer.ExecutionMode(255)
			return s
		}()},
		{name: "too many dependencies", spec: func() sequencer.OperationSpec {
			s := validSpec("a")
			s.Dependencies = make([]sequencer.OperationID, sequencer.DefaultMaxDependencies+1)
			for index := range s.Dependencies {
				s.Dependencies[index] = sequencer.OperationID(fmt.Sprintf("d-%d", index))
			}
			return s
		}()},
		{name: "too many tags", spec: func() sequencer.OperationSpec {
			s := validSpec("a")
			s.Tags = make([]string, sequencer.DefaultMaxTags+1)
			return s
		}()},
		{name: "self dependency", spec: func() sequencer.OperationSpec {
			s := validSpec("a")
			s.Dependencies = []sequencer.OperationID{"a"}
			return s
		}()},
		{name: "duplicate dependency", spec: func() sequencer.OperationSpec {
			s := validSpec("a")
			s.Dependencies = []sequencer.OperationID{"b", "b"}
			return s
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := sequencer.NewOperation(test.spec)
			if !errors.Is(err, sequencer.ErrInvalidOperation) {
				t.Fatalf("error = %v, want ErrInvalidOperation", err)
			}
		})
	}
}

func validSpec(id sequencer.OperationID) sequencer.OperationSpec {
	return sequencer.OperationSpec{
		ID: id, Version: 1, Checksum: "sha256:0123456789abcdef",
		Description: "test operation", Channel: "deploy",
		Policy: sequencer.Policy{Mode: sequencer.OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Minute},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{}, nil
		}),
	}
}
