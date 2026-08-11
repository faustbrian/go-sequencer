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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
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
	applyMigration(t, ctx, pool, "00004_prepare_compensation_generation_fence.sql")
	applyMigration(t, ctx, pool, "00004_prepare_compensation_generation_fence.sql")
	if _, err := pool.Exec(ctx, `
INSERT INTO sequencer_operations (
    operation_id, version, checksum, channel, dependencies, dependency_refs,
    compensates, state, attempt_number, owner, fencing_token,
    lease_expires_at, eligible_at, created_at, updated_at
) VALUES
    ('migration.active-forward', 1, 'sha256:active-forward', 'deploy', '{}', '[]',
     NULL, 'succeeded', 1, NULL, 1, NULL,
     clock_timestamp(), clock_timestamp(), clock_timestamp()),
    ('migration.active-reverse', 1, 'sha256:active-reverse', 'deploy',
     ARRAY['migration.active-forward'],
     '[{"id":"migration.active-forward","version":1,"checksum":"sha256:active-forward"}]',
     '{"id":"migration.active-forward","version":1,"checksum":"sha256:active-forward"}',
     'claimed', 1, 'legacy-compensator', 1,
     clock_timestamp() + interval '1 minute',
     clock_timestamp(), clock_timestamp(), clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	migration, err := fs.ReadFile(sequencerpostgres.Migrations(), "00005_fence_compensation_generations.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, strings.Split(string(migration), "-- +goose Down")[0]); err == nil {
		t.Fatal("generation-fence migration accepted an active legacy compensation")
	}
	if _, err = pool.Exec(ctx, `
DELETE FROM sequencer_operations
WHERE operation_id IN ('migration.active-reverse', 'migration.active-forward')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `
INSERT INTO sequencer_operations (
    operation_id, version, checksum, channel, dependencies, dependency_refs,
    compensates, state, attempt_number, fencing_token,
    eligible_at, created_at, updated_at
) VALUES
    ('migration.historical-forward', 1, 'sha256:historical-forward', 'deploy', '{}', '[]',
     NULL, 'succeeded', 7, 7, clock_timestamp(), clock_timestamp(), clock_timestamp()),
    ('migration.historical-reverse', 1, 'sha256:historical-reverse', 'deploy',
     ARRAY['migration.historical-forward'],
     '[{"id":"migration.historical-forward","version":1,"checksum":"sha256:historical-forward"}]',
     '{"id":"migration.historical-forward","version":1,"checksum":"sha256:historical-forward"}',
     'succeeded', 1, 1, clock_timestamp(), clock_timestamp(), clock_timestamp());
INSERT INTO sequencer_audit_events (
    operation_id, version, attempt_number, from_state, to_state,
    occurred_at, owner, fencing_token, actor, reason
) VALUES
    ('migration.historical-forward', 1, 7, 'running', 'succeeded',
     clock_timestamp() - interval '2 seconds', 'forward-owner', 7, 'forward-owner', 'completed'),
    ('migration.historical-reverse', 1, 1, 'eligible', 'claimed',
     clock_timestamp() - interval '1 second', 'reverse-owner', 1, 'reverse-owner', 'claimed')`); err != nil {
		t.Fatal(err)
	}
	applyMigration(t, ctx, pool, "00005_fence_compensation_generations.sql")
	var historicalFencing *int64
	if err = pool.QueryRow(ctx, `
SELECT compensation_fencing_token
FROM sequencer_operations
WHERE operation_id = 'migration.historical-reverse' AND version = 1`).Scan(&historicalFencing); err != nil {
		t.Fatal(err)
	}
	if historicalFencing != nil {
		t.Fatalf("historical compensation fencing = %d, want unbound", *historicalFencing)
	}
	if _, err = pool.Exec(ctx, `
UPDATE sequencer_operations
SET compensation_fencing_token = 7
WHERE operation_id = 'migration.historical-reverse' AND version = 1`); err == nil ||
		!strings.Contains(err.Error(), "compensation generation binding is immutable") {
		t.Fatalf("historical compensation binding error = %v", err)
	}
	applyMigration(t, ctx, pool, "00006_validate_compensation_generation_fence.sql")
	applyMigration(t, ctx, pool, "00007_cleanup_compensation_generation_fence.sql")
	applyMigration(t, ctx, pool, "00007_cleanup_compensation_generation_fence.sql")
	store, err := sequencerpostgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Reset(ctx, sequencer.ResetRequest{
		OperationID: "migration.historical-reverse", Version: 1, At: time.Now(),
		Actor: "operator", Reason: "unsafe historical replay",
	}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset(historical compensation) error = %v, want reset forbidden", err)
	}
	result, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET state = 'eligible', eligible_at = clock_timestamp(),
    owner = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
WHERE operation_id = 'migration.historical-reverse' AND version = 1 AND state = 'succeeded'`)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected() != 0 {
		t.Fatalf("legacy historical compensation reset rows = %d, want 0", result.RowsAffected())
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

func TestPostgresStorePromotesSynchronousStandbyAndReconnects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	network, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatalf("create PostgreSQL failover network: %v", err)
	}
	t.Cleanup(func() { _ = network.Remove(context.Background()) })
	primary, err := tcpostgres.Run(ctx, postgresIntegrationImage,
		tcpostgres.WithDatabase("sequencer"),
		tcpostgres.WithUsername("sequencer"),
		tcpostgres.WithPassword("sequencer"),
		tcpostgres.BasicWaitStrategies(),
		tcnetwork.WithNetwork([]string{"primary"}, network),
		testcontainers.WithCmd("postgres",
			"-c", "wal_level=replica",
			"-c", "max_wal_senders=4",
			"-c", "max_replication_slots=4",
			"-c", "hot_standby=on",
		),
	)
	if err != nil {
		t.Fatalf("start PostgreSQL primary: %v", err)
	}
	t.Cleanup(func() { _ = primary.Terminate(context.Background()) })
	primaryConnection, err := primary.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	primaryPool, err := pgxpool.New(ctx, primaryConnection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(primaryPool.Close)
	if _, err = primaryPool.Exec(ctx, `
CREATE ROLE sequencer_replica WITH REPLICATION LOGIN PASSWORD 'sequencer_replica'`); err != nil {
		t.Fatal(err)
	}
	exitCode, _, err := primary.Exec(ctx, []string{
		"sh", "-ceu", "printf '%s\n' 'host replication sequencer_replica all scram-sha-256' >> \"$PGDATA/pg_hba.conf\"",
	})
	if err != nil || exitCode != 0 {
		t.Fatalf("configure primary replication authentication: exit=%d error=%v", exitCode, err)
	}
	if _, err = primaryPool.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		t.Fatal(err)
	}
	standbyScript := `
mkdir -p "$PGDATA"
find "$PGDATA" -mindepth 1 -delete
pg_basebackup -h primary -p 5432 -D "$PGDATA" -U sequencer_replica -Fp -Xs -R -C -S sequencer_standby
printf "%s\n" "primary_conninfo = 'host=primary port=5432 user=sequencer_replica password=sequencer_replica application_name=sequencer_standby sslmode=disable'" >> "$PGDATA/postgresql.auto.conf"
chmod 0700 "$PGDATA"
exec postgres -c hot_standby=on -c listen_addresses='*'`
	standbyRequest := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        postgresIntegrationImage,
			Entrypoint:   []string{"sh", "-ceu"},
			Cmd:          []string{standbyScript},
			Env:          map[string]string{"PGDATA": "/var/lib/postgresql/data", "PGPASSWORD": "sequencer_replica"},
			ExposedPorts: []string{"5432/tcp"},
			User:         "postgres",
			WaitingFor: wait.ForLog("database system is ready to accept read-only connections").
				WithStartupTimeout(time.Minute),
		},
		Started: true,
	}
	if err = tcnetwork.WithNetwork([]string{"standby"}, network)(&standbyRequest); err != nil {
		t.Fatal(err)
	}
	standby, err := testcontainers.GenericContainer(ctx, standbyRequest)
	if err != nil {
		t.Fatalf("start PostgreSQL standby: %v", err)
	}
	t.Cleanup(func() { _ = standby.Terminate(context.Background()) })
	for _, statement := range []string{
		"ALTER SYSTEM SET synchronous_standby_names = '*'",
		"ALTER SYSTEM SET synchronous_commit = 'remote_apply'",
		"SELECT pg_reload_conf()",
	} {
		if _, err = primaryPool.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	replicationDeadline := time.Now().Add(30 * time.Second)
	for {
		var synchronous bool
		if err = primaryPool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_stat_replication
    WHERE application_name = 'sequencer_standby'
      AND state = 'streaming' AND sync_state = 'sync'
)`).Scan(&synchronous); err != nil {
			t.Fatal(err)
		}
		if synchronous {
			break
		}
		if time.Now().After(replicationDeadline) {
			t.Fatal("standby did not reach synchronous streaming state")
		}
		time.Sleep(50 * time.Millisecond)
	}
	primaryHost, err := primary.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	primaryPort, err := primary.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	standbyHost, err := standby.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	standbyPort, err := standby.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	haConnection := fmt.Sprintf(
		"host=%s,%s port=%s,%s user=sequencer password=sequencer dbname=sequencer sslmode=disable target_session_attrs=read-write connect_timeout=2",
		primaryHost, standbyHost, primaryPort.Port(), standbyPort.Port(),
	)
	haPool, err := pgxpool.New(ctx, haConnection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(haPool.Close)
	applyMigration(t, ctx, haPool, "00001_create_sequencer_ledger.sql")
	applyMigration(t, ctx, haPool, "00002_pin_dependency_definitions.sql")
	applyMigration(t, ctx, haPool, "00003_block_legacy_unknown_recovery.sql")
	applyMigration(t, ctx, haPool, "00004_prepare_compensation_generation_fence.sql")
	applyMigration(t, ctx, haPool, "00005_fence_compensation_generations.sql")
	applyMigration(t, ctx, haPool, "00006_validate_compensation_generation_fence.sql")
	applyMigration(t, ctx, haPool, "00007_cleanup_compensation_generation_fence.sql")
	store, err := sequencerpostgres.New(haPool)
	if err != nil {
		t.Fatal(err)
	}
	registration := sequencer.Registration{
		ID: "promoted.operation", Version: 1, Checksum: "sha256:promoted-operation",
	}
	if err = store.Register(ctx, []sequencer.Registration{registration}, time.Now()); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: registration.Version, Checksum: registration.Checksum}},
		Owner:      "primary-owner", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = primary.Terminate(ctx); err != nil {
		t.Fatalf("terminate PostgreSQL primary: %v", err)
	}
	exitCode, _, err = standby.Exec(ctx, []string{
		"pg_ctl", "promote", "-D", "/var/lib/postgresql/data", "-w",
	})
	if err != nil || exitCode != 0 {
		t.Fatalf("promote PostgreSQL standby: exit=%d error=%v", exitCode, err)
	}
	promotionDeadline := time.Now().Add(30 * time.Second)
	for {
		var recovery bool
		err = haPool.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&recovery)
		if err == nil && !recovery {
			break
		}
		if time.Now().After(promotionDeadline) {
			t.Fatalf("pool did not reconnect to promoted standby: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	expiryDeadline := time.Now().Add(30 * time.Second)
	for {
		var expired bool
		if err = haPool.QueryRow(ctx, `
SELECT lease_expires_at <= clock_timestamp()
FROM sequencer_operations
WHERE operation_id = $1 AND version = $2`, registration.ID, registration.Version).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		if time.Now().After(expiryDeadline) {
			t.Fatal("replicated lease did not expire after standby promotion")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if recovered, err := store.RecoverExpired(ctx, time.Now()); err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() after promotion = %d, %v", recovered, err)
	}
	record, err := store.Snapshot(ctx, registration.ID, registration.Version)
	if err != nil || record.State != sequencer.Indeterminate {
		t.Fatalf("promoted Snapshot() = %+v, %v", record, err)
	}
	if err = store.Complete(ctx, sequencer.Completion{
		Ownership: claim.Ownership(), State: sequencer.Succeeded, At: time.Now(),
	}); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("stale promoted Complete() error = %v", err)
	}
	var reconciliationTime time.Time
	if err = haPool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&reconciliationTime); err != nil {
		t.Fatal(err)
	}
	if err = store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
		OperationID: registration.ID, Version: registration.Version,
		Attempt: claim.Attempt.Number, Fencing: claim.Attempt.Fencing,
		Resolution: sequencer.ReconcileRetry, Actor: "operator",
		Reason: "primary lost after acknowledged claim", At: reconciliationTime,
	}); err != nil {
		t.Fatal(err)
	}
	reconciled, err := store.Snapshot(ctx, registration.ID, registration.Version)
	if err != nil || reconciled.State != sequencer.Eligible {
		t.Fatalf("reconciled promoted Snapshot() = %+v, %v", reconciled, err)
	}
	takeover, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: registration.Version, Checksum: registration.Checksum}},
		Owner:      "promoted-owner", LeaseDuration: time.Minute,
	})
	if err != nil {
		var due, hasChannel, hasDependencies bool
		if inspectErr := haPool.QueryRow(ctx, `
SELECT eligible_at <= clock_timestamp(), channel IS NOT NULL, dependency_refs IS NOT NULL
FROM sequencer_operations
WHERE operation_id = $1 AND version = $2`, registration.ID, registration.Version).Scan(
			&due, &hasChannel, &hasDependencies,
		); inspectErr != nil {
			t.Fatalf("promoted takeover error = %v; inspect = %v", err, inspectErr)
		}
		t.Fatalf("promoted takeover error = %v; due=%t channel=%t dependencies=%t", err, due, hasChannel, hasDependencies)
	}
	if takeover.Attempt.Fencing <= claim.Attempt.Fencing {
		t.Fatalf("promoted fence = %d, original = %d", takeover.Attempt.Fencing, claim.Attempt.Fencing)
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
	applyMigration(t, ctx, pool, "00004_prepare_compensation_generation_fence.sql")
	applyMigration(t, ctx, pool, "00005_fence_compensation_generations.sql")
	applyMigration(t, ctx, pool, "00006_validate_compensation_generation_fence.sql")
	applyMigration(t, ctx, pool, "00007_cleanup_compensation_generation_fence.sql")
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
	t.Run("compensation reset fence", func(t *testing.T) {
		forward := sequencer.DependencyRef{ID: "compensation.forward", Version: 1, Checksum: "sha256:compensation-forward"}
		compensation := sequencer.Registration{
			ID: "compensation.reverse", Version: 1, Checksum: "sha256:compensation-reverse",
			DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
		}
		if err := store.Register(ctx, []sequencer.Registration{
			{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}, compensation,
		}, time.Now()); err != nil {
			t.Fatal(err)
		}
		forwardClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
			Owner:      "forward-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(ctx, forwardClaim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: forwardClaim.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}
		compensationClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
			Owner:      "compensation-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		var compensationFencing int64
		if err = pool.QueryRow(ctx, `
SELECT compensation_fencing_token
FROM sequencer_operations
WHERE operation_id = $1 AND version = $2`, compensation.ID, compensation.Version).Scan(&compensationFencing); err != nil {
			t.Fatal(err)
		}
		if compensationFencing != int64(forwardClaim.Attempt.Fencing) {
			t.Fatalf("compensation generation fencing = %d, want %d", compensationFencing, forwardClaim.Attempt.Fencing)
		}
		if _, err = pool.Exec(ctx, `
UPDATE sequencer_operations
SET compensation_fencing_token = compensation_fencing_token + 1
WHERE operation_id = $1 AND version = $2`, compensation.ID, compensation.Version); err == nil ||
			!strings.Contains(err.Error(), "compensation generation binding is immutable") {
			t.Fatalf("compensation generation mutation error = %v", err)
		}
		if _, err = store.MarkRunning(ctx, compensationClaim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		reset := sequencer.ResetRequest{
			OperationID: forward.ID, Version: forward.Version, At: time.Now(),
			Actor: "operator", Reason: "new forward generation",
		}
		if err = store.Reset(ctx, reset); !errors.Is(err, sequencer.ErrResetForbidden) {
			t.Fatalf("Reset() during compensation error = %v, want reset forbidden", err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: compensationClaim.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}
		if err = store.Reset(ctx, sequencer.ResetRequest{
			OperationID: compensation.ID, Version: compensation.Version, At: time.Now(),
			Actor: "operator", Reason: "replay same forward generation",
		}); err != nil {
			t.Fatalf("same-generation compensation reset error = %v", err)
		}
		compensationClaim, err = store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
			Owner:      "compensation-replay-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(ctx, compensationClaim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: compensationClaim.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}
		if err = store.Reset(ctx, reset); err != nil {
			t.Fatalf("Reset() after compensation error = %v", err)
		}
		if err = store.Reset(ctx, sequencer.ResetRequest{
			OperationID: compensation.ID, Version: compensation.Version, At: time.Now(),
			Actor: "operator", Reason: "replay while forward is eligible",
		}); !errors.Is(err, sequencer.ErrResetForbidden) {
			t.Fatalf("compensation reset during forward reset error = %v, want reset forbidden", err)
		}
		secondForward, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
			Owner:      "forward-generation-two", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(ctx, secondForward.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: secondForward.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}
		if err = store.Reset(ctx, sequencer.ResetRequest{
			OperationID: compensation.ID, Version: compensation.Version, At: time.Now(),
			Actor: "operator", Reason: "replay stale compensation",
		}); !errors.Is(err, sequencer.ErrResetForbidden) {
			t.Fatalf("stale compensation Reset() error = %v, want reset forbidden", err)
		}
	})
	t.Run("compensation after skipped forward", func(t *testing.T) {
		forward := sequencer.DependencyRef{ID: "skipped-compensation.forward", Version: 1, Checksum: "sha256:skipped-compensation-forward"}
		compensation := sequencer.Registration{
			ID: "skipped-compensation.reverse", Version: 1, Checksum: "sha256:skipped-compensation-reverse",
			DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
		}
		if err := store.Register(ctx, []sequencer.Registration{
			{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}, compensation,
		}, time.Now()); err != nil {
			t.Fatal(err)
		}
		forwardClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
			Owner:      "skipped-forward-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(ctx, forwardClaim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: forwardClaim.Ownership(), State: sequencer.Skipped}); err != nil {
			t.Fatal(err)
		}
		compensationClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: compensation.ID, Version: compensation.Version, Checksum: compensation.Checksum}},
			Owner:      "skipped-compensation-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatalf("ClaimNext(compensation) error = %v", err)
		}
		if _, err = store.MarkRunning(ctx, compensationClaim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: compensationClaim.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("nested compensation reset does not leak counter", func(t *testing.T) {
		forward := sequencer.DependencyRef{ID: "nested-compensation.forward", Version: 1, Checksum: "sha256:nested-compensation-forward"}
		middle := sequencer.DependencyRef{ID: "nested-compensation.middle", Version: 1, Checksum: "sha256:nested-compensation-middle"}
		last := sequencer.Registration{
			ID: "nested-compensation.last", Version: 1, Checksum: "sha256:nested-compensation-last",
			DependencyRefs: []sequencer.DependencyRef{middle}, Compensates: &middle,
		}
		if err := store.Register(ctx, []sequencer.Registration{
			{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum},
			{ID: middle.ID, Version: middle.Version, Checksum: middle.Checksum, DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward},
			last,
		}, time.Now()); err != nil {
			t.Fatal(err)
		}
		for index, candidate := range []sequencer.DependencyRef{forward, middle} {
			claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
				Candidates: []sequencer.ClaimCandidate{{ID: candidate.ID, Version: candidate.Version, Checksum: candidate.Checksum}},
				Owner:      fmt.Sprintf("nested-owner-%d", index), LeaseDuration: time.Minute,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
				t.Fatal(err)
			}
			if err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded}); err != nil {
				t.Fatal(err)
			}
		}
		lastClaim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: last.ID, Version: last.Version, Checksum: last.Checksum}},
			Owner:      "nested-last-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET state = 'eligible', eligible_at = clock_timestamp(),
    owner = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND state = 'succeeded'`, middle.ID, middle.Version)
		if err != nil {
			t.Fatal(err)
		}
		if result.RowsAffected() != 0 {
			t.Fatalf("nested forward reset rows = %d, want 0", result.RowsAffected())
		}
		var forwardActive, middleActive int64
		if err = pool.QueryRow(ctx, `
SELECT forward.active_compensations, middle.active_compensations
FROM sequencer_operations forward
JOIN sequencer_operations middle ON middle.operation_id = $3 AND middle.version = $4
WHERE forward.operation_id = $1 AND forward.version = $2`,
			forward.ID, forward.Version, middle.ID, middle.Version,
		).Scan(&forwardActive, &middleActive); err != nil {
			t.Fatal(err)
		}
		if forwardActive != 0 || middleActive != 1 {
			t.Fatalf("nested compensation counters = %d/%d, want 0/1", forwardActive, middleActive)
		}
		if _, err = store.MarkRunning(ctx, lastClaim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: lastClaim.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("every active compensation state fences reset", func(t *testing.T) {
		states := []string{"claimed", "running", "retryable", "deferred", "indeterminate", "eligible"}
		for _, state := range states {
			state := state
			t.Run(state, func(t *testing.T) {
				forward, compensations := preparePostgresCompensations(t, ctx, store, "state-"+state, 1)
				owner := any(nil)
				lease := any(nil)
				if state == "claimed" || state == "running" {
					owner = "state-owner"
					lease = time.Now().Add(time.Minute)
				}
				if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET state = $3, attempt_number = 1,
    owner = $4, fencing_token = 1, lease_expires_at = $5,
    updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2`,
					compensations[0].ID, compensations[0].Version, state, owner, lease,
				); err != nil {
					t.Fatal(err)
				}
				if err := store.Reset(ctx, sequencer.ResetRequest{
					OperationID: forward.ID, Version: forward.Version, At: time.Now(),
					Actor: "operator", Reason: "active state fence",
				}); !errors.Is(err, sequencer.ErrResetForbidden) {
					t.Fatalf("Reset() error = %v, want reset forbidden", err)
				}
				var active int64
				if err := pool.QueryRow(ctx, `
SELECT active_compensations FROM sequencer_operations
WHERE operation_id = $1 AND version = $2`, forward.ID, forward.Version).Scan(&active); err != nil {
					t.Fatal(err)
				}
				if active != 1 {
					t.Fatalf("active_compensations = %d, want 1", active)
				}
				if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET state = 'failed', owner = NULL,
    lease_expires_at = NULL, updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2`, compensations[0].ID, compensations[0].Version); err != nil {
					t.Fatal(err)
				}
				if err := store.Reset(ctx, sequencer.ResetRequest{
					OperationID: forward.ID, Version: forward.Version, At: time.Now(),
					Actor: "operator", Reason: "terminal compensation",
				}); err != nil {
					t.Fatalf("Reset() after terminal compensation error = %v", err)
				}
			})
		}
	})
	t.Run("multiple active compensations retain reset fence", func(t *testing.T) {
		forward, compensations := preparePostgresCompensations(t, ctx, store, "counter", 2)
		for _, compensation := range compensations {
			if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET state = 'running', attempt_number = 1,
    owner = 'counter-owner', fencing_token = 1,
    lease_expires_at = clock_timestamp() + interval '1 minute',
    updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2`, compensation.ID, compensation.Version); err != nil {
				t.Fatal(err)
			}
		}
		reset := sequencer.ResetRequest{
			OperationID: forward.ID, Version: forward.Version, At: time.Now(),
			Actor: "operator", Reason: "counter fence",
		}
		for index, compensation := range compensations {
			if _, err := pool.Exec(ctx, `
UPDATE sequencer_operations SET state = 'succeeded', owner = NULL,
    lease_expires_at = NULL, updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2`, compensation.ID, compensation.Version); err != nil {
				t.Fatal(err)
			}
			err := store.Reset(ctx, reset)
			if index == 0 && !errors.Is(err, sequencer.ErrResetForbidden) {
				t.Fatalf("Reset() after first compensation error = %v, want reset forbidden", err)
			}
			if index == 1 && err != nil {
				t.Fatalf("Reset() after all compensations error = %v", err)
			}
		}
	})
	t.Run("legacy compensation claim cannot cross reset", func(t *testing.T) {
		forward := sequencer.DependencyRef{ID: "legacy-race.forward", Version: 1, Checksum: "sha256:legacy-race-forward"}
		compensation := sequencer.Registration{
			ID: "legacy-race.reverse", Version: 1, Checksum: "sha256:legacy-race-reverse",
			DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
		}
		if err := store.Register(ctx, []sequencer.Registration{
			{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}, compensation,
		}, time.Now()); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
			Owner:      "legacy-race-forward-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}

		resetTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resetTx.Rollback(ctx) }()
		if _, err = resetTx.Exec(ctx, `
SELECT 1 FROM sequencer_operations
WHERE operation_id = $1 AND version = $2
FOR UPDATE`, forward.ID, forward.Version); err != nil {
			t.Fatal(err)
		}
		claimConnection, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer claimConnection.Release()
		var claimPID int32
		if err = claimConnection.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&claimPID); err != nil {
			t.Fatal(err)
		}
		claimResult := make(chan error, 1)
		go func() {
			_, claimErr := claimConnection.Exec(ctx, `
UPDATE sequencer_operations SET
    state = 'claimed', owner = 'legacy-compensator',
    fencing_token = fencing_token + 1,
    attempt_number = attempt_number + 1,
    lease_expires_at = clock_timestamp() + interval '1 minute',
    updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND state = 'eligible'`, compensation.ID, compensation.Version)
			claimResult <- claimErr
		}()
		waitDeadline := time.Now().Add(5 * time.Second)
		for {
			var waiting bool
			if err = pool.QueryRow(ctx, `
	SELECT COALESCE(wait_event_type = 'Lock', false)
	FROM pg_stat_activity
WHERE pid = $1`, claimPID).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			if time.Now().After(waitDeadline) {
				t.Fatal("legacy compensation claim did not wait for forward reset")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if _, err = resetTx.Exec(ctx, `
UPDATE sequencer_operations SET state = 'eligible', eligible_at = clock_timestamp(),
    owner = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND state = 'succeeded'`, forward.ID, forward.Version); err != nil {
			t.Fatal(err)
		}
		if err = resetTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		claimErr := <-claimResult
		var postgresError *pgconn.PgError
		if !errors.As(claimErr, &postgresError) || postgresError.Code != "55000" ||
			postgresError.Message != "compensation generation is no longer current" {
			t.Fatalf("legacy compensation claim error = %v", claimErr)
		}
		var forwardState, compensationState string
		var activeCompensations int64
		if err = pool.QueryRow(ctx, `
SELECT forward.state, compensation.state, forward.active_compensations
FROM sequencer_operations forward
JOIN sequencer_operations compensation
  ON compensation.operation_id = $3 AND compensation.version = $4
WHERE forward.operation_id = $1 AND forward.version = $2`,
			forward.ID, forward.Version, compensation.ID, compensation.Version,
		).Scan(&forwardState, &compensationState, &activeCompensations); err != nil {
			t.Fatal(err)
		}
		if forwardState != "eligible" || compensationState != "eligible" || activeCompensations != 0 {
			t.Fatalf("post-race states = %s/%s active=%d", forwardState, compensationState, activeCompensations)
		}
	})
	t.Run("committed compensation claim blocks concurrent reset", func(t *testing.T) {
		forward := sequencer.DependencyRef{ID: "claim-first.forward", Version: 1, Checksum: "sha256:claim-first-forward"}
		compensation := sequencer.Registration{
			ID: "claim-first.reverse", Version: 1, Checksum: "sha256:claim-first-reverse",
			DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
		}
		if err := store.Register(ctx, []sequencer.Registration{
			{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}, compensation,
		}, time.Now()); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
			Owner:      "claim-first-forward-owner", LeaseDuration: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
			t.Fatal(err)
		}
		if err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded}); err != nil {
			t.Fatal(err)
		}

		claimTx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = claimTx.Rollback(ctx) }()
		if _, err = claimTx.Exec(ctx, `
UPDATE sequencer_operations SET
    state = 'claimed', owner = 'legacy-compensator',
    fencing_token = fencing_token + 1,
    attempt_number = attempt_number + 1,
    lease_expires_at = clock_timestamp() + interval '1 minute',
    updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND state = 'eligible'`, compensation.ID, compensation.Version); err != nil {
			t.Fatal(err)
		}
		resetConnection, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer resetConnection.Release()
		var resetPID int32
		if err = resetConnection.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&resetPID); err != nil {
			t.Fatal(err)
		}
		resetResult := make(chan struct {
			rows int64
			err  error
		}, 1)
		go func() {
			tag, resetErr := resetConnection.Exec(ctx, `
UPDATE sequencer_operations SET state = 'eligible', eligible_at = clock_timestamp(),
    owner = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND state = 'succeeded'`, forward.ID, forward.Version)
			resetResult <- struct {
				rows int64
				err  error
			}{rows: tag.RowsAffected(), err: resetErr}
		}()
		waitDeadline := time.Now().Add(5 * time.Second)
		for {
			var waiting bool
			if err = pool.QueryRow(ctx, `
SELECT COALESCE(wait_event_type = 'Lock', false)
FROM pg_stat_activity
WHERE pid = $1`, resetPID).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting {
				break
			}
			if time.Now().After(waitDeadline) {
				t.Fatal("concurrent reset did not wait for compensation claim")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err = claimTx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		result := <-resetResult
		if result.err != nil || result.rows != 0 {
			t.Fatalf("legacy Reset() result = %d rows, %v; want fenced no-op", result.rows, result.err)
		}
		if err = store.Reset(ctx, sequencer.ResetRequest{
			OperationID: forward.ID, Version: forward.Version, At: time.Now(),
			Actor: "operator", Reason: "new forward generation",
		}); !errors.Is(err, sequencer.ErrResetForbidden) {
			t.Fatalf("Store.Reset() error = %v, want reset forbidden", err)
		}
	})
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

func preparePostgresCompensations(
	t *testing.T,
	ctx context.Context,
	store *sequencerpostgres.Store,
	prefix string,
	count int,
) (sequencer.DependencyRef, []sequencer.Registration) {
	t.Helper()
	forward := sequencer.DependencyRef{
		ID: sequencer.OperationID(prefix + ".forward"), Version: 1,
		Checksum: "sha256:" + prefix + "-forward",
	}
	compensations := make([]sequencer.Registration, count)
	registrations := []sequencer.Registration{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}}
	for index := range compensations {
		compensations[index] = sequencer.Registration{
			ID: sequencer.OperationID(fmt.Sprintf("%s.reverse-%d", prefix, index)), Version: 1,
			Checksum:       "sha256:" + prefix + fmt.Sprintf("-reverse-%d", index),
			DependencyRefs: []sequencer.DependencyRef{forward}, Compensates: &forward,
		}
		registrations = append(registrations, compensations[index])
	}
	if err := store.Register(ctx, registrations, time.Now()); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: forward.ID, Version: forward.Version, Checksum: forward.Checksum}},
		Owner:      "forward-owner", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded}); err != nil {
		t.Fatal(err)
	}
	return forward, compensations
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
	if strings.Contains(up, "-- +goose NO TRANSACTION") {
		for statement := range strings.SplitSeq(up, ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, err := pool.Exec(ctx, statement); err != nil {
				t.Fatalf("apply migration %s: %v", name, err)
			}
		}
		return
	}
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatalf("apply migration %s: %v", name, err)
	}
}
