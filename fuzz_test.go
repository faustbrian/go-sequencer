package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"
	"unicode/utf8"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

func FuzzCompilePlanDeterminism(fuzz *testing.F) {
	fuzz.Add([]byte{0, 3, 1, 0, 2}, []byte{3, 2, 1, 0})
	fuzz.Add([]byte{1, 2}, []byte{1}) // cycle
	fuzz.Add([]byte{2, 1}, []byte{0}) // missing dependency
	fuzz.Add([]byte{3, 1}, []byte{0}) // duplicate identifier
	fuzz.Add([]byte{4, 2}, []byte{0}) // pinned-definition drift
	fuzz.Fuzz(func(t *testing.T, graph, permutation []byte) {
		if len(graph) < 2 {
			return
		}
		const maxFuzzOperations = 32
		const maxFuzzPermutationBytes = 256
		if len(permutation) > maxFuzzPermutationBytes {
			permutation = permutation[:maxFuzzPermutationBytes]
		}
		mode := graph[0] % 5
		count := min(max(int(graph[1]%maxFuzzOperations), 1), len(graph)-1)
		specs := make([]sequencer.OperationSpec, count)
		for index := range count {
			specs[index] = fuzzSpec(index)
			if index > 0 {
				dependency := int(graph[1+index%count]) % index
				specs[index].DependencyRefs = []sequencer.DependencyRef{{ID: specs[dependency].ID, Version: specs[dependency].Version, Checksum: specs[dependency].Checksum}}
			}
		}

		var want error
		switch mode {
		case 1:
			if count < 2 {
				return
			}
			specs[0].DependencyRefs = []sequencer.DependencyRef{{ID: specs[count-1].ID, Version: 1, Checksum: specs[count-1].Checksum}}
			specs[count-1].DependencyRefs = []sequencer.DependencyRef{{ID: specs[0].ID, Version: 1, Checksum: specs[0].Checksum}}
			want = sequencer.ErrDependencyCycle
		case 2:
			specs[0].DependencyRefs = []sequencer.DependencyRef{{ID: "absent", Version: 1, Checksum: "sha256:absent"}}
			want = sequencer.ErrMissingDependency
		case 3:
			specs = append(specs, specs[0])
			want = sequencer.ErrDuplicateOperation
		case 4:
			if count < 2 {
				return
			}
			specs[1].DependencyRefs = []sequencer.DependencyRef{{ID: specs[0].ID, Version: 2, Checksum: specs[0].Checksum}}
			want = sequencer.ErrDefinitionDrift
		}

		permuted := slices.Clone(specs)
		for index, value := range permutation {
			if len(permuted) == 0 {
				break
			}
			other := int(value) % len(permuted)
			index %= len(permuted)
			permuted[index], permuted[other] = permuted[other], permuted[index]
		}
		first, firstErr := sequencer.CompilePlan(specs, sequencer.PlanOptions{MaxOperations: maxFuzzOperations + 1, MaxDepth: maxFuzzOperations})
		second, secondErr := sequencer.CompilePlan(permuted, sequencer.PlanOptions{MaxOperations: maxFuzzOperations + 1, MaxDepth: maxFuzzOperations})
		if want != nil && (!errors.Is(firstErr, want) || !errors.Is(secondErr, want)) {
			t.Fatalf("hostile graph errors = %v, %v, want %v", firstErr, secondErr, want)
		}
		if (firstErr == nil) != (secondErr == nil) || !samePlanError(firstErr, secondErr) {
			t.Fatalf("permutation changed result: %v, %v", firstErr, secondErr)
		}
		if firstErr == nil && !reflect.DeepEqual(first.IDs(), second.IDs()) {
			t.Fatalf("permutation changed plan: %v, %v", first.IDs(), second.IDs())
		}
	})
}

func FuzzOperationDefinitionBoundaries(fuzz *testing.F) {
	fuzz.Add("operation", "queue", "tag", "dependency", uint8(1))
	fuzz.Add("../operation", "Deploy Queue", "secret\x00tag", "missing ref", uint8(65))
	fuzz.Fuzz(func(t *testing.T, id, channel, tag, dependency string, tagCount uint8) {
		const maxFuzzTextBytes = 512
		id = truncateBytes(id, maxFuzzTextBytes)
		channel = truncateBytes(channel, maxFuzzTextBytes)
		tag = truncateBytes(tag, maxFuzzTextBytes)
		dependency = truncateBytes(dependency, maxFuzzTextBytes)
		spec := fuzzSpec(0)
		spec.ID = sequencer.OperationID(id)
		spec.Channel = channel
		spec.Tags = make([]string, min(int(tagCount), sequencer.DefaultMaxTags+1))
		for index := range spec.Tags {
			spec.Tags[index] = tag
		}
		if dependency != "" {
			spec.DependencyRefs = []sequencer.DependencyRef{{ID: sequencer.OperationID(dependency), Version: 1, Checksum: "sha256:dependency"}}
		}

		first, firstErr := sequencer.NewOperation(spec)
		second, secondErr := sequencer.NewOperation(spec)
		if (firstErr == nil) != (secondErr == nil) || !samePlanError(firstErr, secondErr) {
			t.Fatalf("definition validation changed result: %v, %v", firstErr, secondErr)
		}
		firstSpec, secondSpec := first.Spec(), second.Spec()
		firstSpec.Handler, secondSpec.Handler = nil, nil
		firstSpec.Condition, secondSpec.Condition = nil, nil
		if firstErr == nil && !reflect.DeepEqual(firstSpec, secondSpec) {
			t.Fatal("accepted definition is not deterministic")
		}
	})
}

func FuzzSanitizePersistenceText(fuzz *testing.F) {
	fuzz.Add("secret\x00value", uint16(8))
	fuzz.Add("é", uint16(1))
	fuzz.Fuzz(func(t *testing.T, value string, bound uint16) {
		const maxFuzzPersistenceInputBytes = 128 << 10
		if len(value) > maxFuzzPersistenceInputBytes {
			return
		}
		maximum := int(bound)
		got := sequencer.SanitizePersistenceText(value, maximum)
		if len(got) > maximum {
			t.Fatalf("length = %d, maximum = %d", len(got), maximum)
		}
		if !utf8.ValidString(got) {
			t.Fatalf("invalid UTF-8 output %q", got)
		}
	})
}

func FuzzMixedBinaryClaimsNeverCrossLocalVersion(fuzz *testing.F) {
	fuzz.Add(uint8(1))
	fuzz.Add(uint8(2))
	fuzz.Fuzz(func(t *testing.T, selector uint8) {
		version := uint(selector%2) + 1
		checksums := map[uint]string{1: "sha256:v1", 2: "sha256:v2"}
		store := memory.New()
		now := time.Date(2026, 8, 9, 13, 0, 0, 0, time.UTC)
		if err := store.Register(context.Background(), []sequencer.Registration{
			{ID: "rolling", Version: 1, Checksum: checksums[1]},
			{ID: "rolling", Version: 2, Checksum: checksums[2]},
		}, now); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: "rolling", Version: version, Checksum: checksums[version]}},
			Owner:      "local-binary", Now: now, LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if claim.Attempt.Version != version {
			t.Fatalf("claimed version = %d, local version = %d", claim.Attempt.Version, version)
		}
	})
}

func fuzzSpec(index int) sequencer.OperationSpec {
	return sequencer.OperationSpec{
		ID:      sequencer.OperationID(fmt.Sprintf("operation-%03d", index)),
		Version: 1, Checksum: fmt.Sprintf("sha256:%d", index),
		Description: "fuzz operation", Channel: "fuzz",
		Policy: sequencer.Policy{Mode: sequencer.OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Second},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
			return sequencer.Output{}, nil
		}),
	}
}

func samePlanError(left, right error) bool {
	sentinels := []error{
		sequencer.ErrInvalidOperation,
		sequencer.ErrDuplicateOperation,
		sequencer.ErrMissingDependency,
		sequencer.ErrDependencyCycle,
		sequencer.ErrDefinitionDrift,
		sequencer.ErrResourceLimit,
	}
	for _, sentinel := range sentinels {
		if errors.Is(left, sentinel) != errors.Is(right, sentinel) {
			return false
		}
	}
	return (left == nil) == (right == nil)
}

func truncateBytes(value string, maximum int) string {
	if len(value) > maximum {
		return value[:maximum]
	}
	return value
}
