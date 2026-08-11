package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/goidempotency"
	"github.com/faustbrian/golib/pkg/sequencer/goqueue"
	"github.com/faustbrian/golib/pkg/sequencer/goretry"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

const (
	benchmarkHistorySize    = 10_000
	benchmarkPlanSize       = 10_000
	benchmarkCandidateCount = 10_000
	benchmarkRecoverySize   = 1_000
	benchmarkContenders     = 32
)

var benchmarkNow = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func BenchmarkCompilePlan(benchmark *testing.B) {
	workloads := []struct {
		name  string
		specs []sequencer.OperationSpec
	}{
		{name: "linear_1000", specs: benchmarkLinearPlan(1_000)},
		{name: "layered_10000", specs: benchmarkLayeredPlan(benchmarkPlanSize, 40)},
	}
	for _, workload := range workloads {
		benchmark.Run(workload.name, func(benchmark *testing.B) {
			benchmark.ReportAllocs()
			benchmark.ReportMetric(float64(len(workload.specs)), "operations/op")
			for benchmark.Loop() {
				if _, err := sequencer.CompilePlan(workload.specs, sequencer.PlanOptions{}); err != nil {
					benchmark.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMemoryBoundedHistory(benchmark *testing.B) {
	store := memory.New()
	registration := sequencer.Registration{ID: "history", Version: 1, Checksum: "sha256:history"}
	if err := store.Register(context.Background(), []sequencer.Registration{registration}, benchmarkNow); err != nil {
		benchmark.Fatal(err)
	}
	for attempt := range benchmarkHistorySize {
		now := benchmarkNow.Add(time.Duration(attempt*4) * time.Nanosecond)
		claim, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
			OperationIDs: []sequencer.OperationID{registration.ID}, Owner: "history", Now: now,
			LeaseDuration: time.Minute,
		})
		if err != nil {
			benchmark.Fatal(err)
		}
		if _, err := store.MarkRunning(context.Background(), claim.Ownership(), now.Add(time.Nanosecond)); err != nil {
			benchmark.Fatal(err)
		}
		if err := store.Complete(context.Background(), sequencer.Completion{
			Ownership: claim.Ownership(), State: sequencer.Retryable,
			RetryException: true,
			At:             now.Add(2 * time.Nanosecond), EligibleAt: now.Add(3 * time.Nanosecond),
			Output: sequencer.Output{Summary: "bounded deterministic attempt"},
		}); err != nil {
			benchmark.Fatal(err)
		}
	}

	benchmark.Run("attempts_10000", func(benchmark *testing.B) {
		benchmark.ReportAllocs()
		benchmark.ReportMetric(benchmarkHistorySize, "records/op")
		for benchmark.Loop() {
			history, err := store.History(context.Background(), registration.ID, registration.Version, benchmarkHistorySize)
			if err != nil || len(history) != benchmarkHistorySize {
				benchmark.Fatalf("History() records = %d, error = %v", len(history), err)
			}
		}
	})
	benchmark.Run("audit_10000", func(benchmark *testing.B) {
		benchmark.ReportAllocs()
		benchmark.ReportMetric(benchmarkHistorySize, "events/op")
		for benchmark.Loop() {
			audit, err := store.Audit(context.Background(), registration.ID, registration.Version, benchmarkHistorySize)
			if err != nil || len(audit) != benchmarkHistorySize {
				benchmark.Fatalf("Audit() events = %d, error = %v", len(audit), err)
			}
		}
	})
}

func BenchmarkMemoryClaimCandidateFiltering(benchmark *testing.B) {
	candidates := make([]sequencer.ClaimCandidate, benchmarkCandidateCount)
	for index := range len(candidates) - 1 {
		candidates[index] = sequencer.ClaimCandidate{ID: sequencer.OperationID(fmt.Sprintf("missing-%05d", index)), Version: 1, Checksum: "sha256:missing"}
	}
	candidates[len(candidates)-1] = sequencer.ClaimCandidate{ID: "eligible", Version: 1, Checksum: "sha256:eligible"}
	request := sequencer.ClaimRequest{Candidates: candidates, Owner: "benchmark", Now: benchmarkNow, LeaseDuration: time.Minute}

	benchmark.ReportAllocs()
	benchmark.ReportMetric(benchmarkCandidateCount, "candidates/op")
	for benchmark.Loop() {
		benchmark.StopTimer()
		store := memory.New()
		if err := store.Register(context.Background(), []sequencer.Registration{{ID: "eligible", Version: 1, Checksum: "sha256:eligible"}}, benchmarkNow); err != nil {
			benchmark.Fatal(err)
		}
		benchmark.StartTimer()
		claim, err := store.ClaimNext(context.Background(), request)
		if err != nil || claim.Attempt.OperationID != "eligible" {
			benchmark.Fatalf("ClaimNext() = %+v, %v", claim, err)
		}
	}
}

func BenchmarkMemoryClaimContention(benchmark *testing.B) {
	benchmark.ReportAllocs()
	benchmark.ReportMetric(benchmarkContenders, "contenders/op")
	for benchmark.Loop() {
		benchmark.StopTimer()
		store := memory.New()
		if err := store.Register(context.Background(), []sequencer.Registration{{ID: "contended", Version: 1, Checksum: "sha256:contended"}}, benchmarkNow); err != nil {
			benchmark.Fatal(err)
		}
		benchmark.StartTimer()

		var wait sync.WaitGroup
		var winners atomic.Int64
		errorsFound := make(chan error, benchmarkContenders)
		for contender := range benchmarkContenders {
			wait.Add(1)
			go func() {
				defer wait.Done()
				_, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
					OperationIDs: []sequencer.OperationID{"contended"},
					Owner:        fmt.Sprintf("replica-%02d", contender), Now: benchmarkNow, LeaseDuration: time.Minute,
				})
				if err == nil {
					winners.Add(1)
					return
				}
				if !errors.Is(err, sequencer.ErrNoEligibleOperation) {
					errorsFound <- err
				}
			}()
		}
		wait.Wait()
		if len(errorsFound) != 0 || winners.Load() != 1 {
			benchmark.Fatalf("claim winners = %d, unexpected errors = %d", winners.Load(), len(errorsFound))
		}
	}
}

func BenchmarkFleetInitialPollCandidateFiltering(benchmark *testing.B) {
	specs := benchmarkLayeredPlan(benchmarkPlanSize, 40)
	wanted := 0
	for index := range specs {
		if index%10 == 0 {
			specs[index].Channel = "selected"
			wanted++
		} else {
			specs[index].Channel = "excluded"
		}
	}
	plan, err := sequencer.CompilePlan(specs, sequencer.PlanOptions{})
	if err != nil {
		benchmark.Fatal(err)
	}

	benchmark.ReportAllocs()
	benchmark.ReportMetric(benchmarkPlanSize, "registrations/op")
	benchmark.ReportMetric(float64(wanted), "candidates/op")
	for benchmark.Loop() {
		benchmark.StopTimer()
		ctx, cancel := context.WithCancel(context.Background())
		store := &cancelingPollStore{cancel: cancel}
		fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{
			RunnerOptions: sequencer.RunnerOptions{Owner: "benchmark", Channels: []string{"selected"}},
			ClaimInterval: time.Second,
		})
		if err != nil {
			benchmark.Fatal(err)
		}
		benchmark.StartTimer()
		if err := fleet.Run(ctx); err != nil {
			benchmark.Fatal(err)
		}
		benchmark.StopTimer()
		cancel()
		if store.registrations != benchmarkPlanSize || store.candidates != wanted {
			benchmark.Fatalf("initial poll registrations = %d, candidates = %d", store.registrations, store.candidates)
		}
		benchmark.StartTimer()
	}
}

func BenchmarkMemoryRecovery(benchmark *testing.B) {
	registrations := make([]sequencer.Registration, benchmarkRecoverySize)
	ids := make([]sequencer.OperationID, benchmarkRecoverySize)
	for index := range registrations {
		ids[index] = sequencer.OperationID(fmt.Sprintf("recover-%04d", index))
		registrations[index] = sequencer.Registration{
			ID: ids[index], Version: 1, Checksum: "sha256:recover",
			UnknownOutcome: sequencer.UnknownOutcomeReplayIdempotent,
		}
	}

	benchmark.ReportAllocs()
	benchmark.ReportMetric(benchmarkRecoverySize, "expired/op")
	for benchmark.Loop() {
		benchmark.StopTimer()
		store := memory.New()
		if err := store.Register(context.Background(), registrations, benchmarkNow); err != nil {
			benchmark.Fatal(err)
		}
		for _, id := range ids {
			if _, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
				OperationIDs: []sequencer.OperationID{id}, Owner: "expired", Now: benchmarkNow, LeaseDuration: time.Nanosecond,
			}); err != nil {
				benchmark.Fatal(err)
			}
		}
		benchmark.StartTimer()
		recovered, err := store.RecoverExpired(context.Background(), benchmarkNow.Add(time.Nanosecond))
		if err != nil || recovered != benchmarkRecoverySize {
			benchmark.Fatalf("RecoverExpired() = %d, %v", recovered, err)
		}
	}
}

func BenchmarkQueueSettlement(benchmark *testing.B) {
	worker, err := goqueue.NewWorker(benchmarkExecutor{})
	if err != nil {
		benchmark.Fatal(err)
	}
	message := goqueue.Message{OperationID: "queue", Version: 1, Checksum: "sha256:queue", DeliveryID: "delivery"}
	settlement := benchmarkSettlement{}
	benchmark.ReportAllocs()
	for benchmark.Loop() {
		disposition, err := worker.HandleDelivery(context.Background(), message, settlement)
		if err != nil || disposition != goqueue.Acknowledged {
			benchmark.Fatalf("HandleDelivery() = %v, %v", disposition, err)
		}
	}
}

func BenchmarkRetryAndIdempotencyAdapters(benchmark *testing.B) {
	benchmark.Run("retry_budget_8", func(benchmark *testing.B) {
		adapter, err := goretry.New(benchmarkRetryPolicy{attempts: 8})
		if err != nil {
			benchmark.Fatal(err)
		}
		benchmark.ReportAllocs()
		benchmark.ReportMetric(8, "attempts/op")
		for benchmark.Loop() {
			benchmark.StopTimer()
			budget, err := sequencer.NewExecutionBudget(8)
			if err != nil {
				benchmark.Fatal(err)
			}
			benchmark.StartTimer()
			if err := adapter.Do(context.Background(), budget, func(context.Context) error { return nil }); err != nil {
				benchmark.Fatal(err)
			}
		}
	})
	benchmark.Run("idempotency_acquire_complete", func(benchmark *testing.B) {
		adapter, err := goidempotency.New(benchmarkGate{})
		if err != nil {
			benchmark.Fatal(err)
		}
		benchmark.ReportAllocs()
		for benchmark.Loop() {
			if err := adapter.Do(context.Background(), "stable-key", func(context.Context) error { return nil }); err != nil {
				benchmark.Fatal(err)
			}
		}
	})
}

func benchmarkLinearPlan(size int) []sequencer.OperationSpec {
	specs := make([]sequencer.OperationSpec, size)
	for index := range specs {
		specs[index] = benchmarkSpec(index)
		if index > 0 {
			specs[index].DependencyRefs = []sequencer.DependencyRef{{
				ID: specs[index-1].ID, Version: specs[index-1].Version, Checksum: specs[index-1].Checksum,
			}}
		}
	}
	return specs
}

func benchmarkLayeredPlan(size, width int) []sequencer.OperationSpec {
	specs := make([]sequencer.OperationSpec, size)
	for index := range specs {
		specs[index] = benchmarkSpec(index)
		if index >= width {
			specs[index].DependencyRefs = []sequencer.DependencyRef{{
				ID: specs[index-width].ID, Version: specs[index-width].Version, Checksum: specs[index-width].Checksum,
			}}
		}
	}
	return specs
}

func benchmarkSpec(index int) sequencer.OperationSpec {
	id := sequencer.OperationID(fmt.Sprintf("operation-%05d", index))
	return sequencer.OperationSpec{
		ID: id, Version: 1, Checksum: "sha256:" + string(id), Description: "deterministic benchmark operation", Channel: "benchmark",
		Policy:  sequencer.Policy{Mode: sequencer.OneTime, MaxAttempts: 1, MaxExceptions: 1, Timeout: time.Second},
		Handler: sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) { return sequencer.Output{}, nil }),
	}
}

type cancelingPollStore struct {
	cancel        context.CancelFunc
	registrations int
	candidates    int
}

func (store *cancelingPollStore) Register(_ context.Context, registrations []sequencer.Registration, _ time.Time) error {
	store.registrations = len(registrations)
	return nil
}
func (store *cancelingPollStore) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	store.candidates = len(request.Candidates)
	store.cancel()
	return sequencer.Claim{}, ctx.Err()
}
func (*cancelingPollStore) MarkRunning(context.Context, sequencer.Ownership, time.Time) (sequencer.AttemptRecord, error) {
	return sequencer.AttemptRecord{}, errors.New("unexpected MarkRunning")
}
func (*cancelingPollStore) Complete(context.Context, sequencer.Completion) error {
	return errors.New("unexpected Complete")
}
func (*cancelingPollStore) RecoverExpired(context.Context, time.Time) (int, error) { return 0, nil }
func (*cancelingPollStore) Snapshot(context.Context, sequencer.OperationID, uint) (sequencer.Record, error) {
	return sequencer.Record{}, errors.New("unexpected Snapshot")
}
func (*cancelingPollStore) History(context.Context, sequencer.OperationID, uint, int) ([]sequencer.AttemptRecord, error) {
	return nil, errors.New("unexpected History")
}
func (*cancelingPollStore) Audit(context.Context, sequencer.OperationID, uint, int) ([]sequencer.AuditEvent, error) {
	return nil, errors.New("unexpected Audit")
}
func (*cancelingPollStore) Reset(context.Context, sequencer.ResetRequest) error {
	return errors.New("unexpected Reset")
}
func (*cancelingPollStore) RenewLease(context.Context, sequencer.Ownership, time.Time, time.Duration) (time.Time, error) {
	return time.Time{}, errors.New("unexpected RenewLease")
}

type benchmarkExecutor struct{}

func (benchmarkExecutor) ExecuteMessage(context.Context, goqueue.Message) error { return nil }

type benchmarkSettlement struct{}

func (benchmarkSettlement) Acknowledge(context.Context) error { return nil }
func (benchmarkSettlement) Reject(context.Context) error      { return nil }

type benchmarkRetryPolicy struct{ attempts int }

func (policy benchmarkRetryPolicy) Do(ctx context.Context, operation func(context.Context) error) error {
	var err error
	for range policy.attempts {
		err = operation(ctx)
	}
	return err
}

type benchmarkGate struct{}

func (benchmarkGate) Begin(context.Context, string) (goidempotency.Token, bool, error) {
	return "token", true, nil
}
func (benchmarkGate) Complete(context.Context, goidempotency.Token) error { return nil }
func (benchmarkGate) Fail(context.Context, goidempotency.Token, error) error {
	return nil
}
