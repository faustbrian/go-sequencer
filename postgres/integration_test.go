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
	migration, err := fs.ReadFile(sequencerpostgres.Migrations(), "00001_create_sequencer_ledger.sql")
	if err != nil {
		t.Fatal(err)
	}
	up := strings.Split(string(migration), "-- +goose Down")[0]
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
	store, err := sequencerpostgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	registration := sequencer.Registration{ID: "postal.backfill", Version: 1, Checksum: "sha256:postal", Dependencies: []sequencer.OperationID{"schema"}}
	if err := store.Register(ctx, []sequencer.Registration{{ID: "schema", Version: 1, Checksum: "sha256:schema"}}, time.Now()); err != nil {
		t.Fatal(err)
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
	if err := store.Register(ctx, []sequencer.Registration{registration}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Register(ctx, []sequencer.Registration{{ID: registration.ID, Version: 1, Checksum: "sha256:drift"}}, time.Now()); !errors.Is(err, sequencer.ErrChecksumDrift) {
		t.Fatalf("checksum drift error = %v", err)
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
	time.Sleep(75 * time.Millisecond)
	if recovered, err := store.RecoverExpired(ctx, time.Now()); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	audit, err := store.Audit(ctx, registration.ID, 1, 20)
	if err != nil || len(audit) < 3 || audit[len(audit)-2].To != sequencer.Retryable || audit[len(audit)-1].To != sequencer.Eligible {
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
}
