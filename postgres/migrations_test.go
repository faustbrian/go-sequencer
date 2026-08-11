package postgres_test

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/faustbrian/golib/pkg/sequencer/postgres"
)

func TestMigrationsExposeVersionedDurableLedger(t *testing.T) {
	t.Parallel()

	data, err := fs.ReadFile(postgres.Migrations(), "00001_create_sequencer_ledger.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	for _, required := range []string{
		"sequencer_operations", "sequencer_attempts", "sequencer_audit_events",
		"checksum", "fencing_token", "lease_expires_at", "SKIP LOCKED",
	} {
		if !strings.Contains(string(data), required) {
			t.Errorf("migration missing %q", required)
		}
	}
}

func TestMigrationsExposePinnedDependencyExpansion(t *testing.T) {
	t.Parallel()

	data, err := fs.ReadFile(postgres.Migrations(), "00002_pin_dependency_definitions.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	for _, required := range []string{
		"dependency_refs", "jsonb", "cardinality(dependencies) = 0",
		"unknown_outcome", "dead_letter", "compensates", "indeterminate", "dead_lettered",
	} {
		if !strings.Contains(string(data), required) {
			t.Errorf("migration missing %q", required)
		}
	}
}

func TestMigrationsFenceLegacyUnknownRecovery(t *testing.T) {
	t.Parallel()

	data, err := fs.ReadFile(postgres.Migrations(), "00003_block_legacy_unknown_recovery.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	for _, required := range []string{
		"OLD.unknown_outcome = 0", "OLD.state IN ('claimed', 'running')",
		"NEW.state = 'eligible'", "BEFORE UPDATE OF state",
	} {
		if !strings.Contains(string(data), required) {
			t.Errorf("migration missing %q", required)
		}
	}
}

func TestNewRequiresPool(t *testing.T) {
	t.Parallel()

	if _, err := postgres.New(nil); err == nil {
		t.Fatal("New(nil) error = nil")
	}
}
