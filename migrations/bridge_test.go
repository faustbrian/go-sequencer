package migrations_test

import (
	"context"
	"errors"
	"testing"

	"github.com/faustbrian/golib/pkg/sequencer/migrations"
)

func TestBridgeAssertsPrerequisiteWithoutOwningHistory(t *testing.T) {
	t.Parallel()

	bridge, err := migrations.New(readerStub{version: 42})
	if err != nil {
		t.Fatal(err)
	}
	if err := bridge.Assert(context.Background(), migrations.Prerequisite{MinimumVersion: 40}); err != nil {
		t.Fatalf("Assert() error = %v", err)
	}
	if err := bridge.Assert(context.Background(), migrations.Prerequisite{MinimumVersion: 43}); !errors.Is(err, migrations.ErrPrerequisiteMissing) {
		t.Fatalf("Assert() error = %v", err)
	}
}

func TestBridgeValidationAndReaderFailure(t *testing.T) {
	t.Parallel()

	if _, err := migrations.New(nil); !errors.Is(err, migrations.ErrInvalidBridge) {
		t.Fatalf("New(nil) error = %v", err)
	}
	bridge, _ := migrations.New(readerStub{})
	if err := bridge.Assert(context.Background(), migrations.Prerequisite{}); !errors.Is(err, migrations.ErrInvalidBridge) {
		t.Fatalf("Assert(empty) error = %v", err)
	}
	cause := errors.New("reader")
	bridge, _ = migrations.New(readerStub{err: cause})
	if err := bridge.Assert(context.Background(), migrations.Prerequisite{MinimumVersion: 1}); !errors.Is(err, cause) {
		t.Fatalf("Assert(reader) error = %v", err)
	}
}

type readerStub struct {
	version uint64
	err     error
}

func (reader readerStub) CurrentVersion(context.Context) (uint64, error) {
	return reader.version, reader.err
}
