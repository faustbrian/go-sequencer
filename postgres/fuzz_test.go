package postgres

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
)

func FuzzPersistedAttemptStateErrorAndOutput(fuzz *testing.F) {
	fuzz.Add("succeeded", "", []byte(`{"Summary":"done","Metadata":{"rows":"1"}}`))
	fuzz.Add("unknown", "secret\x00detail", []byte(`{`))
	fuzz.Add("running", "é", []byte(`{"Summary":"\ud800","Metadata":null}`))
	fuzz.Fuzz(func(t *testing.T, state, detail string, output []byte) {
		if len(state) > 64 || len(detail) > sequencer.DefaultMaxErrorBytes || len(output) > sequencer.DefaultMaxOutputBytes {
			return
		}
		now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
		row := []any{"operation", int64(1), int64(1), "owner", int64(1), state, now, now, detail, output}
		store := newStore(&fakeDatabase{rows: &fakeRows{values: [][]any{row}}})
		history, err := store.History(context.Background(), "operation", 1, 1)
		again, againErr := newStore(&fakeDatabase{rows: &fakeRows{values: [][]any{row}}}).History(context.Background(), "operation", 1, 1)
		if (err == nil) != (againErr == nil) || (err == nil && !reflect.DeepEqual(history, again)) {
			t.Fatalf("persisted attempt decoding changed result: first=%+v/%v second=%+v/%v", history, err, again, againErr)
		}
		if err != nil {
			return
		}
		if len(history) != 1 || history[0].State.String() != state || history[0].ErrorDetail != detail {
			t.Fatalf("decoded attempt = %+v", history)
		}
		if len(history[0].Output.Summary) > sequencer.DefaultMaxOutputBytes {
			t.Fatalf("decoded output summary has %d bytes", len(history[0].Output.Summary))
		}
	})
}

func FuzzPersistedDependencyRefs(fuzz *testing.F) {
	fuzz.Add([]byte(`[]`))
	fuzz.Add([]byte(`[{"id":"schema","version":1,"checksum":"sha256:schema"}]`))
	fuzz.Add([]byte(`[{"id":"b","version":1,"checksum":"b"},{"id":"a","version":1,"checksum":"a"}]`))
	fuzz.Add([]byte(`[{"id":"a","version":1,"checksum":"a"},{"id":"a","version":2,"checksum":"b"}]`))
	fuzz.Add([]byte(`{"not":"an-array"}`))
	fuzz.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxPersistedDefinitionBytes {
			return
		}
		dependencies, err := decodeDependencyRefs(encoded)
		if err != nil {
			if !errors.Is(err, sequencer.ErrDefinitionDrift) {
				t.Fatalf("decode error = %v", err)
			}
			return
		}
		if len(dependencies) > sequencer.DefaultMaxDependencies {
			t.Fatalf("decoded %d dependencies", len(dependencies))
		}
		roundTrip := encodeDependencyRefs(dependencies)
		again, err := decodeDependencyRefs(roundTrip)
		if err != nil || !slices.Equal(dependencies, again) {
			t.Fatalf("round trip = %+v, %v", again, err)
		}
	})
}

func FuzzPersistedCompensationRef(fuzz *testing.F) {
	fuzz.Add([]byte(`{"id":"forward","version":1,"checksum":"sha256:forward"}`))
	fuzz.Add([]byte(`{"id":"","version":0,"checksum":""}`))
	fuzz.Add([]byte(`null`))
	fuzz.Add([]byte(`{`))
	fuzz.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maxPersistedReferenceBytes {
			return
		}
		reference, err := decodeDependencyRef(encoded)
		if err != nil {
			if !errors.Is(err, sequencer.ErrDefinitionDrift) {
				t.Fatalf("decode error = %v", err)
			}
			return
		}
		roundTrip := encodeDependencyRef(reference)
		again, err := decodeDependencyRef(roundTrip)
		if err != nil || !equalDependencyRef(reference, again) {
			t.Fatalf("round trip = %+v, %v", again, err)
		}
	})
}
