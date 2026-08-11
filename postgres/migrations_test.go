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

func TestMigrationsFenceCompensationGenerations(t *testing.T) {
	t.Parallel()

	required := map[string][]string{
		"00004_prepare_compensation_generation_fence.sql": {
			"NO TRANSACTION", "DROP INDEX CONCURRENTLY IF EXISTS",
			"CREATE INDEX CONCURRENTLY", "active_compensation_preflight_idx",
		},
		"00005_fence_compensation_generations.sql": {
			"LOCK TABLE sequencer_operations IN SHARE ROW EXCLUSIVE MODE",
			"active compensation must be resolved before installing generation fencing",
			"active_compensations bigint NOT NULL DEFAULT 0", "compensation_fencing_token bigint",
			"historical compensation generation is unbound", "NOT VALID",
			"was_active IS DISTINCT FROM is_active", "RETURN NULL",
			"NEW.compensation_fencing_token = forward.fencing_token",
			"BEFORE UPDATE OF state, attempt_number, compensation_fencing_token",
		},
		"00006_validate_compensation_generation_fence.sql": {
			"VALIDATE CONSTRAINT sequencer_operations_active_compensations_nonnegative",
			"VALIDATE CONSTRAINT sequencer_operations_compensation_fencing_positive",
		},
		"00007_cleanup_compensation_generation_fence.sql": {
			"NO TRANSACTION", "DROP INDEX CONCURRENTLY IF EXISTS", "active_compensation_preflight_idx",
		},
	}
	for name, fragments := range required {
		data, err := fs.ReadFile(postgres.Migrations(), name)
		if err != nil {
			t.Fatalf("ReadFile(%s) error = %v", name, err)
		}
		for _, fragment := range fragments {
			if !strings.Contains(string(data), fragment) {
				t.Errorf("migration %s missing %q", name, fragment)
			}
		}
	}
}

func TestNewRequiresPool(t *testing.T) {
	t.Parallel()

	if _, err := postgres.New(nil); err == nil {
		t.Fatal("New(nil) error = nil")
	}
}
