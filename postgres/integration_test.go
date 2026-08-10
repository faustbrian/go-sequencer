//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	sequencerpostgres "github.com/faustbrian/golib/pkg/sequencer/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresStoreConcurrentClaimsRecoveryAndDrift(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("sequencer"),
		tcpostgres.WithUsername("sequencer"),
		tcpostgres.WithPassword("sequencer"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	connection, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	applyMigration(t, ctx, pool, "00001_create_sequencer_ledger.sql")
	if _, err := pool.Exec(ctx, `
INSERT INTO sequencer_operations (
    operation_id, version, checksum, dependencies, state, eligible_at,
    created_at, updated_at
) VALUES
    ('migration.empty', 1, 'sha256:empty', '{}', 'eligible', clock_timestamp(), clock_timestamp(), clock_timestamp()),
    ('migration.legacy', 1, 'sha256:legacy', ARRAY['schema'], 'eligible', clock_timestamp(), clock_timestamp(), clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	applyMigration(t, ctx, pool, "00002_pin_dependency_definitions.sql")
	var emptyRefs, legacyRefs *string
	if err := pool.QueryRow(ctx, `
SELECT dependency_refs::text FROM sequencer_operations WHERE operation_id = 'migration.empty'`).Scan(&emptyRefs); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT dependency_refs::text FROM sequencer_operations WHERE operation_id = 'migration.legacy'`).Scan(&legacyRefs); err != nil {
		t.Fatal(err)
	}
	if emptyRefs == nil || *emptyRefs != "[]" || legacyRefs != nil {
		t.Fatalf("expanded dependency refs: empty=%v legacy=%v", emptyRefs, legacyRefs)
	}
	store, err := sequencerpostgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	schemaV1 := sequencer.DependencyRef{ID: "schema", Version: 1, Checksum: "sha256:schema"}
	registration := sequencer.Registration{ID: "postal.backfill", Version: 1, Checksum: "sha256:postal", DependencyRefs: []sequencer.DependencyRef{schemaV1}}
	if err := store.Register(ctx, []sequencer.Registration{{ID: "schema", Version: 1, Checksum: "sha256:schema"}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	registrationAudit, err := store.Audit(ctx, "schema", 1, 10)
	if err != nil || len(registrationAudit) != 1 || registrationAudit[0].From != sequencer.Pending || registrationAudit[0].To != sequencer.Eligible {
		t.Fatalf("registration audit = %+v, %v", registrationAudit, err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates:    []sequencer.ClaimCandidate{{ID: "schema", Version: 1, Checksum: "sha256:wrong"}},
		Owner:         "wrong-binary",
		LeaseDuration: time.Minute,
	}); !errors.Is(err, sequencer.ErrChecksumDrift) {
		t.Fatalf("checksum-mismatched ClaimNext() error = %v", err)
	}
	schemaClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"schema"}, Owner: "schema", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, schemaClaim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: schemaClaim.Ownership(), State: sequencer.Succeeded}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "migration.legacy", Version: 1, Checksum: "sha256:legacy"}},
		Owner:      "legacy-before-resolution", LeaseDuration: time.Minute,
	}); !errors.Is(err, sequencer.ErrNoEligibleOperation) {
		t.Fatalf("unresolved legacy ClaimNext() error = %v", err)
	}
	if err := store.Register(ctx, []sequencer.Registration{{
		ID: "migration.legacy", Version: 1, Checksum: "sha256:legacy",
		DependencyRefs: []sequencer.DependencyRef{schemaV1},
	}}, time.Now()); err != nil {
		t.Fatalf("resolve legacy dependencies: %v", err)
	}
	legacyClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "migration.legacy", Version: 1, Checksum: "sha256:legacy"}},
		Owner:      "legacy-after-resolution", LeaseDuration: time.Minute,
	})
	if err != nil || legacyClaim.Attempt.OperationID != "migration.legacy" {
		t.Fatalf("resolved legacy claim = %+v, %v", legacyClaim, err)
	}
	if err := store.Register(ctx, []sequencer.Registration{registration}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Register(ctx, []sequencer.Registration{{
		ID: registration.ID, Version: registration.Version, Checksum: registration.Checksum,
		DependencyRefs: []sequencer.DependencyRef{{ID: "schema", Version: 2, Checksum: "sha256:schema-v2"}},
	}}, time.Now()); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("dependency drift error = %v", err)
	}
	if err := store.Register(ctx, []sequencer.Registration{{ID: registration.ID, Version: 1, Checksum: "sha256:drift"}}, time.Now()); !errors.Is(err, sequencer.ErrChecksumDrift) {
		t.Fatalf("checksum drift error = %v", err)
	}
	if err := store.Register(ctx, []sequencer.Registration{{ID: "schema", Version: 2, Checksum: "sha256:schema-v2"}}, time.Now()); err != nil {
		t.Fatal(err)
	}

	for _, source := range []sequencer.State{sequencer.Retryable, sequencer.Deferred} {
		id := sequencer.OperationID("claim." + source.String())
		if err := store.Register(ctx, []sequencer.Registration{{ID: id, Version: 1, Checksum: "sha256:" + source.String()}}, time.Now()); err != nil {
			t.Fatal(err)
		}
		first, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: "sha256:" + source.String()}},
			Owner:      "first", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(ctx, first.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := store.Complete(ctx, sequencer.Completion{Ownership: first.Ownership(), State: source, EligibleAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: "sha256:" + source.String()}},
			Owner:      "second", LeaseDuration: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
		events, err := store.Audit(ctx, id, 1, 10)
		if err != nil || len(events) < 6 {
			t.Fatalf("%s claim audit = %+v, %v", source, events, err)
		}
		last := events[len(events)-2:]
		if last[0].From != source || last[0].To != sequencer.Eligible ||
			last[1].From != sequencer.Eligible || last[1].To != sequencer.Claimed {
			t.Fatalf("%s claim edges = %+v", source, last)
		}
	}

	var wait sync.WaitGroup
	winners := make(chan sequencer.Claim, 32)
	for index := range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			claim, claimErr := store.ClaimNext(ctx, sequencer.ClaimRequest{
				OperationIDs: []sequencer.OperationID{registration.ID},
				Owner:        string(rune('a' + index)), LeaseDuration: 50 * time.Millisecond,
			})
			if claimErr == nil {
				winners <- claim
			} else if !errors.Is(claimErr, sequencer.ErrNoEligibleOperation) {
				t.Errorf("ClaimNext() error = %v", claimErr)
			}
		}()
	}
	wait.Wait()
	close(winners)
	if got := len(winners); got != 1 {
		t.Fatalf("claim winners = %d, want 1", got)
	}
	winner := <-winners
	time.Sleep(75 * time.Millisecond)
	if recovered, err := store.RecoverExpired(ctx, time.Now()); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	audit, err := store.Audit(ctx, registration.ID, 1, 20)
	if err != nil || len(audit) < 3 || audit[len(audit)-2].To != sequencer.Retryable || audit[len(audit)-1].To != sequencer.Eligible ||
		audit[len(audit)-2].Owner != winner.Attempt.Owner || audit[len(audit)-1].Owner != winner.Attempt.Owner {
		t.Fatalf("recovery audit = %+v, %v", audit, err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		OperationIDs: []sequencer.OperationID{registration.ID},
		Owner:        "recovery", LeaseDuration: time.Minute,
	})
	if err != nil || claim.Attempt.Number != 2 || claim.Attempt.Fencing != 2 {
		t.Fatalf("recovery claim = %+v, %v", claim, err)
	}

	failed := sequencer.Registration{ID: "postal.failed", Version: 1, Checksum: "sha256:failed"}
	if err := store.Register(ctx, []sequencer.Registration{failed}, time.Now()); err != nil {
		t.Fatal(err)
	}
	failedClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{failed.ID}, Owner: "operator", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, failedClaim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: failedClaim.Ownership(), State: sequencer.Failed}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx, failed.ID, 1)
	if err != nil || snapshot.State != sequencer.Failed || snapshot.AttemptNumber != 1 {
		t.Fatalf("Snapshot() = %+v, %v", snapshot, err)
	}
	history, err := store.History(ctx, failed.ID, 1, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Failed {
		t.Fatalf("History() = %+v, %v", history, err)
	}
	if _, err := store.Snapshot(ctx, "missing", 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("Snapshot(missing) error = %v", err)
	}
	if _, err := store.History(ctx, failed.ID, 1, 0); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("History(limit) error = %v", err)
	}
	if _, err := store.Audit(ctx, failed.ID, 1, 0); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Audit(limit) error = %v", err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: failed.ID, Version: 1, Actor: "operator", Reason: "approved retry"}); err != nil {
		t.Fatal(err)
	}
	audit, err = store.Audit(ctx, failed.ID, 1, 20)
	if err != nil || audit[len(audit)-1].From != sequencer.Failed || audit[len(audit)-1].To != sequencer.Eligible {
		t.Fatalf("reset audit = %+v, %v", audit, err)
	}

	rolling := []sequencer.Registration{
		{ID: "rolling", Version: 1, Checksum: "sha256:v1"},
		{ID: "rolling", Version: 2, Checksum: "sha256:v2"},
	}
	if err := store.Register(ctx, rolling, time.Now()); err != nil {
		t.Fatal(err)
	}
	oldClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "rolling", Version: 1, Checksum: "sha256:v1"}},
		Owner:      "old-binary", LeaseDuration: time.Minute,
	})
	if err != nil || oldClaim.Attempt.Version != 1 {
		t.Fatalf("old binary claim = %+v, %v", oldClaim, err)
	}
	if _, err := store.MarkRunning(ctx, oldClaim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	until, err := store.RenewLease(ctx, oldClaim.Ownership(), time.Now(), 2*time.Minute)
	if err != nil || !until.After(time.Now().Add(time.Minute)) {
		t.Fatalf("RenewLease() = %s, %v", until, err)
	}
	shorter, err := store.RenewLease(ctx, oldClaim.Ownership(), time.Now(), time.Minute)
	if err != nil || shorter.Before(until) {
		t.Fatalf("shorter RenewLease() = %s, %v; previous expiry %s", shorter, err, until)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: oldClaim.Ownership(), State: sequencer.Succeeded}); err != nil {
		t.Fatal(err)
	}
}

func applyMigration(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) {
	t.Helper()
	migration, err := fs.ReadFile(sequencerpostgres.Migrations(), name)
	if err != nil {
		t.Fatal(err)
	}
	up := strings.Split(string(migration), "-- +goose Down")[0]
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("apply migration %s: %v", name, err)
	}
}
