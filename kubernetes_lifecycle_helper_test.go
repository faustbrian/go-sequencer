//go:build kubernetes

package sequencer_test

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	sequencerpostgres "github.com/faustbrian/golib/pkg/sequencer/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const kubernetesHelperOperation = sequencer.OperationID("kubernetes.lifecycle")

func TestKubernetesLifecycleHelper(t *testing.T) {
	connection := requiredEnvironment(t, "DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if os.Getenv("SEQUENCER_HELPER_MODE") == "migrate" {
		applyKubernetesMigrations(t, ctx, pool)
		return
	}

	store, err := sequencerpostgres.New(pool)
	if err != nil {
		t.Fatal(err)
	}
	version := environmentUint(t, "SEQUENCER_VERSION", 1)
	count := environmentUint(t, "SEQUENCER_OPERATION_COUNT", 1)
	behavior := os.Getenv("SEQUENCER_BEHAVIOR")
	runContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	var started atomic.Uint64
	specs := make([]sequencer.OperationSpec, count)
	for index := range count {
		id := kubernetesHelperOperation
		if count > 1 {
			id = sequencer.OperationID(fmt.Sprintf("kubernetes.leaderless-%02d", index))
		}
		specs[index] = kubernetesOperationSpec(id, version, behavior, runContext, &started)
	}
	plan, err := sequencer.CompilePlan(specs, sequencer.PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
		RunnerOptions: sequencer.RunnerOptions{
			Owner:         requiredEnvironment(t, "POD_UID"),
			LeaseDuration: time.Duration(environmentUint(t, "SEQUENCER_LEASE_MILLISECONDS", 25_000)) * time.Millisecond,
		},
		ClaimInterval:  100 * time.Millisecond,
		RenewInterval:  time.Second,
		MaxConcurrency: 2,
		ShutdownWait:   5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	runDone := startFleet(runContext, t, fleet)

	server := &http.Server{Addr: ":8080", Handler: kubernetesProbeHandler(fleet, &started), ReadHeaderTimeout: time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()

	select {
	case err := <-runDone:
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
		_ = server.Shutdown(shutdownContext)
		shutdownCancel()
		if err != nil {
			t.Fatal(err)
		}
	case err := <-serverDone:
		if err != nil && err != http.ErrServerClosed {
			t.Fatal(err)
		}
		t.Fatal("probe server stopped before fleet")
	}
}

func kubernetesOperationSpec(id sequencer.OperationID, version uint, behavior string, termination context.Context, started *atomic.Uint64) sequencer.OperationSpec {
	return sequencer.OperationSpec{
		ID: id, Version: version, Checksum: fmt.Sprintf("sha256:kubernetes-v%d", version),
		Description: "Kubernetes lifecycle proof operation", Channel: "kubernetes",
		Policy: sequencer.Policy{
			Mode: sequencer.OneTime, MaxAttempts: 3, MaxExceptions: 3,
			Timeout: 8 * time.Second, Cancellation: sequencer.CancellationDrainOnly,
			UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent,
		},
		Handler: sequencer.HandlerFunc(func(ctx context.Context, attempt sequencer.Attempt) (sequencer.Output, error) {
			started.Add(1)
			switch behavior {
			case "drain":
				<-termination.Done()
				time.Sleep(2 * time.Second)
			case "leaderless":
				time.Sleep(2 * time.Second)
			case "crash-recover":
				if attempt.Number == 1 {
					select {
					case <-time.After(time.Minute):
					case <-ctx.Done():
					}
				}
			}
			return sequencer.Output{Summary: fmt.Sprintf("owner=%s attempt=%d", attempt.Owner, attempt.Number)}, nil
		}),
	}
}

func kubernetesProbeHandler(fleet *sequencer.Fleet, started *atomic.Uint64) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		switch request.URL.Path {
		case "/readyz":
			if !fleet.Ready() {
				response.WriteHeader(http.StatusServiceUnavailable)
			}
			_, _ = fmt.Fprintln(response, fleet.State())
		case "/started":
			_, _ = fmt.Fprintln(response, started.Load())
		default:
			response.WriteHeader(http.StatusNotFound)
		}
	})
}

func applyKubernetesMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	entries, err := fs.Glob(sequencerpostgres.Migrations(), "*.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		migration, readErr := fs.ReadFile(sequencerpostgres.Migrations(), entry)
		if readErr != nil {
			t.Fatal(readErr)
		}
		up := strings.Split(string(migration), "-- +goose Down")[0]
		if _, execErr := pool.Exec(ctx, up); execErr != nil {
			t.Fatalf("apply %s: %v", entry, execErr)
		}
	}
}

func requiredEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func environmentUint(t *testing.T, name string, fallback uint) uint {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 {
		t.Fatalf("invalid %s=%q", name, value)
	}
	return uint(parsed)
}
