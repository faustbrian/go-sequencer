package sequencer_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
	"unicode/utf8"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

func FuzzCompilePlanDeterminism(fuzz *testing.F) {
	fuzz.Add([]byte{3, 1, 0, 2})
	fuzz.Add([]byte{1})
	fuzz.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		count := min(len(data), 64)
		specs := make([]sequencer.OperationSpec, count)
		for index := range count {
			specs[index] = fuzzSpec(index)
			if index > 0 {
				dependency := int(data[index%len(data)]) % index
				specs[index].Dependencies = []sequencer.OperationID{specs[dependency].ID}
			}
		}
		first, firstErr := sequencer.CompilePlan(specs, sequencer.PlanOptions{})
		second, secondErr := sequencer.CompilePlan(specs, sequencer.PlanOptions{})
		if (firstErr == nil) != (secondErr == nil) {
			t.Fatalf("nondeterministic errors: %v, %v", firstErr, secondErr)
		}
		if firstErr == nil && !reflect.DeepEqual(first.IDs(), second.IDs()) {
			t.Fatalf("nondeterministic plans: %v, %v", first.IDs(), second.IDs())
		}
	})
}

func FuzzSanitizePersistenceText(fuzz *testing.F) {
	fuzz.Add("secret\x00value", uint16(8))
	fuzz.Add("é", uint16(1))
	fuzz.Fuzz(func(t *testing.T, value string, bound uint16) {
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
