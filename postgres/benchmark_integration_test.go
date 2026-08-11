//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	sequencerpostgres "github.com/faustbrian/golib/pkg/sequencer/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	postgresBenchmarkCandidates = 1_000
	postgresBenchmarkHistory    = 1_000
	postgresBenchmarkImage      = "postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
)

func BenchmarkPostgresStore(benchmark *testing.B) {
	ctx := context.Background()
	pool := benchmarkPostgresPool(benchmark, ctx)
	store, err := sequencerpostgres.New(pool)
	if err != nil {
		benchmark.Fatal(err)
	}
	registration := sequencer.Registration{ID: "benchmark.claim", Version: 1, Checksum: "sha256:benchmark-claim"}
	if err := store.Register(ctx, []sequencer.Registration{registration}, time.Unix(1, 0)); err != nil {
		benchmark.Fatal(err)
	}
	candidates := make([]sequencer.ClaimCandidate, postgresBenchmarkCandidates)
	for index := range len(candidates) - 1 {
		candidates[index] = sequencer.ClaimCandidate{ID: sequencer.OperationID(fmt.Sprintf("missing-%04d", index)), Version: 1, Checksum: "sha256:missing"}
	}
	candidates[len(candidates)-1] = sequencer.ClaimCandidate{ID: registration.ID, Version: registration.Version, Checksum: registration.Checksum}
	request := sequencer.ClaimRequest{Candidates: candidates, Owner: "benchmark", LeaseDuration: time.Minute}

	benchmark.Run("claim_candidate_filtering_1000", func(benchmark *testing.B) {
		benchmark.ReportAllocs()
		benchmark.ReportMetric(postgresBenchmarkCandidates, "candidates/op")
		for benchmark.Loop() {
			claim, err := store.ClaimNext(ctx, request)
			if err != nil {
				benchmark.Fatal(err)
			}
			benchmark.StopTimer()
			completePostgresBenchmarkAttempt(benchmark, ctx, store, claim)
			benchmark.StartTimer()
		}
	})

	historyRegistration := sequencer.Registration{ID: "benchmark.history", Version: 1, Checksum: "sha256:benchmark-history"}
	if err := store.Register(ctx, []sequencer.Registration{historyRegistration}, time.Unix(1, 0)); err != nil {
		benchmark.Fatal(err)
	}
	for range postgresBenchmarkHistory {
		claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
			Candidates: []sequencer.ClaimCandidate{{ID: historyRegistration.ID, Version: historyRegistration.Version, Checksum: historyRegistration.Checksum}},
			Owner:      "benchmark", LeaseDuration: time.Minute,
		})
		if err != nil {
			benchmark.Fatal(err)
		}
		completePostgresBenchmarkAttempt(benchmark, ctx, store, claim)
	}
	benchmark.Run("bounded_history_1000", func(benchmark *testing.B) {
		benchmark.ReportAllocs()
		benchmark.ReportMetric(postgresBenchmarkHistory, "records/op")
		for benchmark.Loop() {
			history, err := store.History(ctx, historyRegistration.ID, historyRegistration.Version, postgresBenchmarkHistory)
			if err != nil || len(history) != postgresBenchmarkHistory {
				benchmark.Fatalf("History() records = %d, error = %v", len(history), err)
			}
		}
	})
}

func completePostgresBenchmarkAttempt(benchmark *testing.B, ctx context.Context, store *sequencerpostgres.Store, claim sequencer.Claim) {
	benchmark.Helper()
	now := time.Now()
	if _, err := store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
		benchmark.Fatal(err)
	}
	if err := store.Complete(ctx, sequencer.Completion{
		Ownership: claim.Ownership(), State: sequencer.Retryable, At: now,
		RetryException: true,
		EligibleAt:     time.Unix(1, 0), Output: sequencer.Output{Summary: "bounded deterministic attempt"},
	}); err != nil {
		benchmark.Fatal(err)
	}
}

func benchmarkPostgresPool(benchmark *testing.B, ctx context.Context) *pgxpool.Pool {
	benchmark.Helper()
	container, err := tcpostgres.Run(ctx, postgresBenchmarkImage,
		tcpostgres.WithDatabase("sequencer"),
		tcpostgres.WithUsername("sequencer"),
		tcpostgres.WithPassword("sequencer"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		benchmark.Fatalf("start PostgreSQL: %v", err)
	}
	benchmark.Cleanup(func() { _ = container.Terminate(context.Background()) })
	connection, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		benchmark.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, connection)
	if err != nil {
		benchmark.Fatal(err)
	}
	benchmark.Cleanup(pool.Close)
	for _, name := range []string{"00001_create_sequencer_ledger.sql", "00002_pin_dependency_definitions.sql"} {
		migration, err := fs.ReadFile(sequencerpostgres.Migrations(), name)
		if err != nil {
			benchmark.Fatal(err)
		}
		up := strings.Split(string(migration), "-- +goose Down")[0]
		if _, err := pool.Exec(ctx, up); err != nil {
			benchmark.Fatalf("apply migration %s: %v", name, err)
		}
	}
	return pool
}
