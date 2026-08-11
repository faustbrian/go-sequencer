//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	sequencerpostgres "github.com/faustbrian/golib/pkg/sequencer/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const postgresIntegrationImage = "postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"

func TestPostgresStoreFailsClosedAndRecoversAfterServerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve PostgreSQL restart port: %v", err)
	}
	hostPort := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatalf("release PostgreSQL restart port: %v", err)
	}
	container, err := tcpostgres.Run(ctx, postgresIntegrationImage,
		tcpostgres.WithDatabase("sequencer"),
		tcpostgres.WithUsername("sequencer"),
		tcpostgres.WithPassword("sequencer"),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
			config.PortBindings = network.PortMap{
				network.MustParsePort("5432/tcp"): {
					{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort},
				},
			}
		}),
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
	applyMigration(t, ctx, pool, "00002_pin_dependency_definitions.sql")
	applyMigration(t, ctx, pool, "00003_block_legacy_unknown_recovery.sql")
	store, err := sequencerpostgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	registration := sequencer.Registration{
		ID: "failover.operation", Version: 1, Checksum: "sha256:failover", Channel: "deploy",
	}
	if err := store.Register(ctx, []sequencer.Registration{registration}, time.Now()); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: 1, Checksum: registration.Checksum, Channel: registration.Channel}},
		Owner:      "pod-before-failover", Now: time.Now(), LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), claim.Attempt.StartedAt); err != nil {
		t.Fatal(err)
	}

	stopTimeout := 10 * time.Second
	if err := container.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop PostgreSQL: %v", err)
	}
	renewContext, stopRenew := context.WithTimeout(ctx, time.Second)
	_, renewErr := store.RenewLease(renewContext, claim.Ownership(), time.Now(), time.Second)
	stopRenew()
	if renewErr == nil {
		t.Fatal("RenewLease() succeeded while PostgreSQL was stopped")
	}
	if err := container.Start(ctx); err != nil {
		t.Fatalf("restart PostgreSQL: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		err = pool.Ping(ctx)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool did not recover after PostgreSQL restart: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	expiryDeadline := time.Now().Add(30 * time.Second)
	for {
		var expired bool
		if err := pool.QueryRow(ctx, `
SELECT lease_expires_at <= clock_timestamp()
FROM sequencer_operations
WHERE operation_id = $1 AND version = $2`, registration.ID, registration.Version).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		if time.Now().After(expiryDeadline) {
			t.Fatal("lease did not expire after PostgreSQL restart")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if recovered, err := store.RecoverExpired(ctx, time.Now().Add(time.Minute)); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
	}
	record, err := store.Snapshot(ctx, registration.ID, registration.Version)
	if err != nil || record.State != sequencer.Indeterminate {
		t.Fatalf("Snapshot() = %+v, %v", record, err)
	}
	if err := store.Complete(ctx, sequencer.Completion{
		Ownership: claim.Ownership(), State: sequencer.Succeeded, At: time.Now(),
	}); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("stale Complete() error = %v", err)
	}
}

func TestPostgresStoreConcurrentClaimsRecoveryAndDrift(t *testing.T) {
	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, postgresIntegrationImage,
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
	applyMigration(t, ctx, pool, "00003_block_legacy_unknown_recovery.sql")
	if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET channel = 'deploy'
WHERE operation_id = 'migration.legacy' AND version = 1`); err != nil {
		t.Fatal(err)
	}
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
	const recoveryBatchSize = 32
	for index := recoveryBatchSize; index >= 0; index-- {
		id := sequencer.OperationID(fmt.Sprintf("recovery.batch-%02d", index))
		if err := store.Register(ctx, []sequencer.Registration{{ID: id, Version: 1, Checksum: "sha256:" + string(id)}}, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{id}, Owner: "recovery-owner",
			Now: time.Now(), LeaseDuration: time.Millisecond,
		}); err != nil {
			t.Fatal(err)
		}
	}
	expiredAt := time.Now().Add(-time.Second)
	if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations
SET lease_expires_at = $1
WHERE operation_id LIKE 'recovery.batch-%'`, expiredAt); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpired(ctx, time.Now()); err != nil || recovered != recoveryBatchSize {
		t.Fatalf("first bounded RecoverExpired() = %d, %v; want %d", recovered, err, recoveryBatchSize)
	}
	remaining, err := store.Snapshot(ctx, "recovery.batch-32", 1)
	if err != nil || remaining.State != sequencer.Claimed {
		t.Fatalf("remaining recovery Snapshot() = %+v, %v; want claimed", remaining, err)
	}
	if recovered, err := store.RecoverExpired(ctx, time.Now()); err != nil || recovered != 1 {
		t.Fatalf("second bounded RecoverExpired() = %d, %v; want 1", recovered, err)
	}
	oversizedDependencies := make([]sequencer.DependencyRef, sequencer.DefaultMaxDependencies)
	for index := range oversizedDependencies {
		oversizedDependencies[index] = sequencer.DependencyRef{
			ID:      sequencer.OperationID(fmt.Sprintf("dependency-%03d", index)),
			Version: 1, Checksum: strings.Repeat("x", sequencer.DefaultMaxChecksumBytes),
		}
	}
	if err := store.Register(ctx, []sequencer.Registration{{
		ID: "persisted.oversized-write", Version: 1, Checksum: "sum",
		DependencyRefs: oversizedDependencies,
	}}, time.Now()); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Register(oversized definition) error = %v", err)
	}
	boundedRegistration := sequencer.Registration{ID: "persisted.bounds", Version: 1, Checksum: "sha256:bounds"}
	if err := store.Register(ctx, []sequencer.Registration{boundedRegistration}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations
SET dependency_refs = jsonb_build_array(jsonb_build_object(
    'id', 'dependency', 'version', 1, 'checksum', repeat('x', $3)))
WHERE operation_id = $1 AND version = $2`, boundedRegistration.ID, boundedRegistration.Version, (64<<10)+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(ctx, boundedRegistration.ID, boundedRegistration.Version); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("Snapshot(oversized dependencies) error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations
SET dependency_refs = '[]'::jsonb,
    compensates = jsonb_build_object('id', 'dependency', 'version', 1, 'checksum', repeat('x', $3))
WHERE operation_id = $1 AND version = $2`, boundedRegistration.ID, boundedRegistration.Version, (4<<10)+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(ctx, boundedRegistration.ID, boundedRegistration.Version); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("Snapshot(oversized compensation) error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET compensates = NULL
WHERE operation_id = $1 AND version = $2`, boundedRegistration.ID, boundedRegistration.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO sequencer_attempts (
    operation_id, version, attempt_number, owner, fencing_token,
    state, started_at, completed_at, output
) VALUES ($1, $2, 1, 'owner', 1, 'succeeded', clock_timestamp(),
          clock_timestamp(), jsonb_build_object('Summary', repeat('x', $3)))`,
		boundedRegistration.ID, boundedRegistration.Version, sequencer.DefaultMaxOutputBytes+1); err != nil {
		t.Fatal(err)
	}
	if _, err := store.History(ctx, boundedRegistration.ID, boundedRegistration.Version, 1); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("History(oversized output) error = %v", err)
	}
	schemaV1 := sequencer.DependencyRef{ID: "schema", Version: 1, Checksum: "sha256:schema"}
	registration := sequencer.Registration{
		ID: "postal.backfill", Version: 1, Checksum: "sha256:postal",
		DependencyRefs: []sequencer.DependencyRef{schemaV1}, UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent,
	}
	if err := store.Register(ctx, []sequencer.Registration{{ID: "schema", Version: 1, Checksum: "sha256:schema"}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	channelRegistration := sequencer.Registration{ID: "channel.operation", Version: 1, Checksum: "sha256:channel", Channel: "deploy"}
	if err := store.Register(ctx, []sequencer.Registration{channelRegistration}, time.Now()); err != nil {
		t.Fatal(err)
	}
	channelDrift := channelRegistration
	channelDrift.Channel = "queue"
	if err := store.Register(ctx, []sequencer.Registration{channelDrift}, time.Now()); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("channel registration drift error = %v", err)
	}
	if _, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: channelRegistration.ID, Version: 1, Checksum: channelRegistration.Checksum, Channel: "queue"}},
		Owner:      "wrong-channel", LeaseDuration: time.Minute,
	}); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("channel claim drift error = %v", err)
	}
	channelSnapshot, err := store.Snapshot(ctx, channelRegistration.ID, channelRegistration.Version)
	if err != nil || channelSnapshot.Channel != "deploy" {
		t.Fatalf("channel snapshot = %+v, %v", channelSnapshot, err)
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
		ID: "migration.legacy", Version: 1, Checksum: "sha256:legacy", Channel: "deploy",
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
		if err := store.Complete(ctx, sequencer.Completion{
			Ownership: first.Ownership(), State: source, EligibleAt: time.Unix(1, 0),
			RetryException: source == sequencer.Retryable,
		}); err != nil {
			t.Fatal(err)
		}
		second, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: "sha256:" + source.String()}},
			Owner:      "second", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		wantExceptions := uint(0)
		if source == sequencer.Retryable {
			wantExceptions = 1
		}
		if second.Budget.Attempt != 2 || second.Budget.Exceptions != wantExceptions {
			t.Fatalf("%s second claim budget = %+v", source, second.Budget)
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
	if err != nil || len(audit) < 3 || audit[len(audit)-2].To != sequencer.Indeterminate || audit[len(audit)-1].To != sequencer.Eligible ||
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
	if err := store.Complete(ctx, sequencer.Completion{
		Ownership: failedClaim.Ownership(), From: sequencer.Claimed,
		State: sequencer.Failed, ErrorDetail: sequencer.ErrBudgetExhausted.Error(),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(ctx, failed.ID, 1)
	if err != nil || snapshot.State != sequencer.Failed || snapshot.AttemptNumber != 1 {
		t.Fatalf("Snapshot() = %+v, %v", snapshot, err)
	}
	history, err := store.History(ctx, failed.ID, 1, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Failed || history[0].ErrorDetail != sequencer.ErrBudgetExhausted.Error() {
		t.Fatalf("History() = %+v, %v", history, err)
	}
	audit, err = store.Audit(ctx, failed.ID, 1, 10)
	if err != nil || audit[len(audit)-1].From != sequencer.Claimed || audit[len(audit)-1].To != sequencer.Failed {
		t.Fatalf("direct claim settlement audit = %+v, %v", audit, err)
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

	unknowns := []sequencer.Registration{
		{ID: "unknown.block", Version: 1, Checksum: "sha256:unknown-block"},
		{ID: "unknown.replay", Version: 1, Checksum: "sha256:unknown-replay", UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent},
		{ID: "unknown.dead", Version: 1, Checksum: "sha256:unknown-dead", DeadLetter: true},
		{ID: "unknown.concurrent", Version: 1, Checksum: "sha256:unknown-concurrent"},
	}
	for _, unknown := range unknowns {
		if err := store.Register(ctx, []sequencer.Registration{unknown}, time.Now()); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: unknown.ID, Version: unknown.Version, Checksum: unknown.Checksum}},
			Owner:      "unknown-owner", LeaseDuration: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := pool.Exec(ctx, legacyBlockingRecoverySQL); err == nil {
		t.Fatal("legacy recovery replayed a blocked unknown outcome")
	}
	if recovered, err := store.RecoverExpired(ctx, time.Now()); err != nil || recovered != len(unknowns) {
		t.Fatalf("unknown RecoverExpired() = %d, %v", recovered, err)
	}
	blockedSnapshot, err := store.Snapshot(ctx, "unknown.block", 1)
	if err != nil || blockedSnapshot.State != sequencer.Indeterminate {
		t.Fatalf("blocked unknown snapshot = %+v, %v", blockedSnapshot, err)
	}
	replaySnapshot, err := store.Snapshot(ctx, "unknown.replay", 1)
	if err != nil || replaySnapshot.State != sequencer.Eligible {
		t.Fatalf("replay unknown snapshot = %+v, %v", replaySnapshot, err)
	}
	history, err = store.History(ctx, "unknown.block", 1, 10)
	if err != nil || len(history) != 1 || history[0].State != sequencer.Indeterminate || history[0].ErrorDetail != sequencer.ErrUnknownResult.Error() {
		t.Fatalf("unknown history = %+v, %v", history, err)
	}
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
		OperationID: "unknown.block", Version: 1, Attempt: blockedSnapshot.AttemptNumber + 1, Fencing: blockedSnapshot.Fencing,
		Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "wrong attempt", At: time.Now(),
	}); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("wrong-attempt ResolveUnknown() error = %v", err)
	}
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
		OperationID: "unknown.block", Version: 1, Attempt: blockedSnapshot.AttemptNumber, Fencing: blockedSnapshot.Fencing,
		Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "stale decision", At: blockedSnapshot.UpdatedAt.Add(-time.Nanosecond),
	}); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("stale-time ResolveUnknown() error = %v", err)
	}
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
		OperationID: "unknown.block", Version: 1, Attempt: blockedSnapshot.AttemptNumber, Fencing: blockedSnapshot.Fencing,
		Resolution: sequencer.ReconcileRetry,
		Actor:      "operator", Reason: "effect absent", At: time.Now(),
	}); err != nil {
		t.Fatalf("ResolveUnknown(retry) error = %v", err)
	}
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
		OperationID: "unknown.block", Version: 1, Attempt: blockedSnapshot.AttemptNumber, Fencing: blockedSnapshot.Fencing,
		Resolution: sequencer.ReconcileRetry,
		Actor:      "operator", Reason: "again", At: time.Now(),
	}); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("stale ResolveUnknown() error = %v", err)
	}
	deadUnknownSnapshot, err := store.Snapshot(ctx, "unknown.dead", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
		OperationID: "unknown.dead", Version: 1, Attempt: deadUnknownSnapshot.AttemptNumber, Fencing: deadUnknownSnapshot.Fencing,
		Resolution: sequencer.ReconcileFailed,
		Actor:      "operator", Reason: "effect failed", At: time.Now(),
	}); err != nil {
		t.Fatalf("ResolveUnknown(dead letter) error = %v", err)
	}
	deadSnapshot, err := store.Snapshot(ctx, "unknown.dead", 1)
	if err != nil || deadSnapshot.State != sequencer.DeadLettered || !deadSnapshot.DeadLetter {
		t.Fatalf("dead-letter snapshot = %+v, %v", deadSnapshot, err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: "unknown.dead", Version: 1, Actor: "operator", Reason: "approved replay"}); err != nil {
		t.Fatalf("Reset(dead letter) error = %v", err)
	}

	var reconcileWait sync.WaitGroup
	reconcileErrors := make(chan error, 2)
	concurrentUnknownSnapshot, err := store.Snapshot(ctx, "unknown.concurrent", 1)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		reconcileWait.Add(1)
		go func() {
			defer reconcileWait.Done()
			reconcileErrors <- store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
				OperationID: "unknown.concurrent", Version: 1,
				Attempt: concurrentUnknownSnapshot.AttemptNumber, Fencing: concurrentUnknownSnapshot.Fencing,
				Resolution: sequencer.ReconcileSucceeded,
				Actor:      "operator", Reason: "verified", At: time.Now(),
			})
		}()
	}
	reconcileWait.Wait()
	close(reconcileErrors)
	var resolved, rejected int
	for reconcileErr := range reconcileErrors {
		if reconcileErr == nil {
			resolved++
		} else if errors.Is(reconcileErr, sequencer.ErrReconcileForbidden) {
			rejected++
		} else {
			t.Fatalf("concurrent ResolveUnknown() error = %v", reconcileErr)
		}
	}
	if resolved != 1 || rejected != 1 {
		t.Fatalf("concurrent reconciliation resolved=%d rejected=%d", resolved, rejected)
	}

	compensates := schemaV1
	compensation := sequencer.Registration{
		ID: "schema.compensation", Version: 1, Checksum: "sha256:compensation",
		DependencyRefs: []sequencer.DependencyRef{schemaV1}, Compensates: &compensates,
	}
	if err := store.Register(ctx, []sequencer.Registration{compensation}, time.Now()); err != nil {
		t.Fatal(err)
	}
	compensationSnapshot, err := store.Snapshot(ctx, compensation.ID, compensation.Version)
	if err != nil || compensationSnapshot.Compensates == nil || *compensationSnapshot.Compensates != schemaV1 {
		t.Fatalf("compensation snapshot = %+v, %v", compensationSnapshot, err)
	}

	canceled := sequencer.Registration{ID: "reset.canceled", Version: 1, Checksum: "sha256:canceled"}
	if err := store.Register(ctx, []sequencer.Registration{canceled}, time.Now()); err != nil {
		t.Fatal(err)
	}
	canceledClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: canceled.ID, Version: canceled.Version, Checksum: canceled.Checksum}},
		Owner:      "cancel-owner", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, canceledClaim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{Ownership: canceledClaim.Ownership(), State: sequencer.Canceled}); err != nil {
		t.Fatal(err)
	}
	if err := store.Reset(ctx, sequencer.ResetRequest{OperationID: canceled.ID, Version: canceled.Version, Actor: "operator", Reason: "resume canceled"}); err != nil {
		t.Fatalf("Reset(canceled) error = %v", err)
	}

	corrupt := sequencer.Registration{ID: "recovery.corrupt", Version: 1, Checksum: "sha256:recovery-corrupt"}
	if err := store.Register(ctx, []sequencer.Registration{corrupt}, time.Now()); err != nil {
		t.Fatal(err)
	}
	corruptClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: corrupt.ID, Version: corrupt.Version, Checksum: corrupt.Checksum}},
		Owner:      "corrupt-owner", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, corruptClaim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	beforeCorruption, err := store.Snapshot(ctx, corrupt.ID, corrupt.Version)
	if err != nil {
		t.Fatal(err)
	}
	beforeAudit, err := store.Audit(ctx, corrupt.ID, corrupt.Version, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
DELETE FROM sequencer_attempts
WHERE operation_id = $1 AND version = $2 AND attempt_number = $3`,
		corrupt.ID, corrupt.Version, corruptClaim.Attempt.Number); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET lease_expires_at = clock_timestamp() - interval '1 second'
WHERE operation_id = $1 AND version = $2`, corrupt.ID, corrupt.Version); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverExpired(ctx, time.Now()); recovered != 0 || err == nil {
		t.Fatalf("RecoverExpired(corrupt) = %d, %v", recovered, err)
	}
	afterCorruption, err := store.Snapshot(ctx, corrupt.ID, corrupt.Version)
	if err != nil {
		t.Fatal(err)
	}
	afterAudit, err := store.Audit(ctx, corrupt.ID, corrupt.Version, 10)
	if err != nil {
		t.Fatal(err)
	}
	if afterCorruption.State != beforeCorruption.State || afterCorruption.Owner != beforeCorruption.Owner ||
		afterCorruption.Fencing != beforeCorruption.Fencing || !afterCorruption.UpdatedAt.Equal(beforeCorruption.UpdatedAt) ||
		len(afterAudit) != len(beforeAudit) {
		t.Fatalf("corrupt recovery mutated projection/audit: before=%+v/%d after=%+v/%d",
			beforeCorruption, len(beforeAudit), afterCorruption, len(afterAudit))
	}

	for _, boundary := range []string{"mark-running-missing", "mark-running-mismatch", "complete-missing", "complete-mismatch"} {
		registration := sequencer.Registration{ID: sequencer.OperationID("corrupt." + boundary), Version: 1, Checksum: "sha256:" + boundary}
		if err := store.Register(ctx, []sequencer.Registration{registration}, time.Now()); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: 1, Checksum: registration.Checksum}},
			Owner:      "corrupt-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(boundary, "complete") {
			if _, err := store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		before, err := store.Snapshot(ctx, registration.ID, registration.Version)
		if err != nil {
			t.Fatal(err)
		}
		auditBefore, err := store.Audit(ctx, registration.ID, registration.Version, 10)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(boundary, "missing") {
			if _, err := pool.Exec(ctx, `DELETE FROM sequencer_attempts WHERE operation_id = $1 AND version = $2`, registration.ID, registration.Version); err != nil {
				t.Fatal(err)
			}
		} else if _, err := pool.Exec(ctx, `UPDATE sequencer_attempts SET owner = 'wrong-owner' WHERE operation_id = $1 AND version = $2`, registration.ID, registration.Version); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(boundary, "mark-running") {
			_, err = store.MarkRunning(ctx, claim.Ownership(), time.Now())
		} else {
			err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded})
		}
		if !errors.Is(err, sequencer.ErrDefinitionDrift) {
			t.Fatalf("%s corrupt transition error = %v", boundary, err)
		}
		after, snapshotErr := store.Snapshot(ctx, registration.ID, registration.Version)
		auditAfter, auditErr := store.Audit(ctx, registration.ID, registration.Version, 10)
		if snapshotErr != nil || auditErr != nil || after.State != before.State || after.Owner != before.Owner ||
			after.Fencing != before.Fencing || !after.UpdatedAt.Equal(before.UpdatedAt) || len(auditAfter) != len(auditBefore) {
			t.Fatalf("%s corrupt transition mutated projection/audit: before=%+v/%d after=%+v/%d errors=%v/%v",
				boundary, before, len(auditBefore), after, len(auditAfter), snapshotErr, auditErr)
		}
	}
}

const legacyBlockingRecoverySQL = `
WITH candidates AS MATERIALIZED (
    SELECT operation_id, version, attempt_number, owner, fencing_token, state
    FROM sequencer_operations
    WHERE operation_id = 'unknown.block'
      AND state IN ('claimed', 'running')
      AND lease_expires_at <= clock_timestamp()
    FOR UPDATE SKIP LOCKED
), expired AS (
    UPDATE sequencer_operations operation SET
        state = 'eligible', owner = NULL, lease_expires_at = NULL,
        eligible_at = clock_timestamp(), updated_at = clock_timestamp()
    FROM candidates
    WHERE operation.operation_id = candidates.operation_id
      AND operation.version = candidates.version
    RETURNING operation.operation_id
)
SELECT count(*) FROM expired`

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
