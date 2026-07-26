// Package migrations asserts schema prerequisites without owning migration
// discovery, planning, execution, or history.
package migrations

import (
	"context"
	"errors"
	"fmt"
)

var (
	// ErrInvalidBridge reports a missing application migration reader.
	ErrInvalidBridge = errors.New("sequencer/migrations: invalid bridge")
	// ErrPrerequisiteMissing reports a schema version below the requirement.
	ErrPrerequisiteMissing = errors.New("sequencer/migrations: prerequisite missing")
)

// VersionReader exposes the application migration engine's current version.
type VersionReader interface {
	CurrentVersion(context.Context) (uint64, error)
}

// Prerequisite declares the minimum schema version an operation needs.
type Prerequisite struct{ MinimumVersion uint64 }

// Bridge checks prerequisites while leaving migration history untouched.
type Bridge struct{ reader VersionReader }

// New constructs a prerequisite bridge.
func New(reader VersionReader) (*Bridge, error) {
	if reader == nil {
		return nil, ErrInvalidBridge
	}
	return &Bridge{reader: reader}, nil
}

// Assert verifies the current schema version satisfies the prerequisite.
func (bridge *Bridge) Assert(ctx context.Context, prerequisite Prerequisite) error {
	if prerequisite.MinimumVersion == 0 {
		return ErrInvalidBridge
	}
	current, err := bridge.reader.CurrentVersion(ctx)
	if err != nil {
		return err
	}
	if current < prerequisite.MinimumVersion {
		return fmt.Errorf("%w: have %d, require %d", ErrPrerequisiteMissing, current, prerequisite.MinimumVersion)
	}
	return nil
}
