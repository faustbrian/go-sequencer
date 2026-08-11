package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/memory"
)

type recoveryModelAction uint8

const (
	modelClaim recoveryModelAction = iota
	modelMarkRunning
	modelRenew
	modelExpire
	modelReconcileRetry
	modelComplete
	modelStaleComplete
)

func TestFleetRecoveryStateModelExploresCrashAndTakeoverTraces(t *testing.T) {
	t.Parallel()

	var traces [][]recoveryModelAction
	generateRecoveryTraces(nil, sequencer.Eligible, 0, 8, &traces)
	if len(traces) < 20 {
		t.Fatalf("generated traces = %d, want broad state exploration", len(traces))
	}
	for index, trace := range traces {
		t.Run(fmt.Sprintf("trace-%03d", index), func(t *testing.T) {
			assertRecoveryTrace(t, trace)
		})
	}
}

func generateRecoveryTraces(prefix []recoveryModelAction, state sequencer.State, attempts, remaining int, traces *[][]recoveryModelAction) {
	if remaining == 0 || state == sequencer.Succeeded {
		*traces = append(*traces, append([]recoveryModelAction(nil), prefix...))
		return
	}
	var actions []recoveryModelAction
	switch state {
	case sequencer.Eligible:
		actions = []recoveryModelAction{modelClaim}
	case sequencer.Claimed:
		actions = []recoveryModelAction{modelMarkRunning, modelRenew, modelExpire}
	case sequencer.Running:
		actions = []recoveryModelAction{modelRenew, modelExpire, modelComplete}
	case sequencer.Indeterminate:
		actions = []recoveryModelAction{modelReconcileRetry}
	case sequencer.Pending, sequencer.Succeeded, sequencer.Skipped, sequencer.Failed,
		sequencer.Retryable, sequencer.Deferred, sequencer.Canceled, sequencer.RolledBack,
		sequencer.Blocked, sequencer.DeadLettered:
		panic(fmt.Sprintf("unexpected recovery model state %s", state))
	}
	if attempts > 1 && (state == sequencer.Claimed || state == sequencer.Running) {
		actions = append(actions, modelStaleComplete)
	}
	for _, action := range actions {
		nextState, nextAttempts := state, attempts
		switch action {
		case modelClaim:
			nextState, nextAttempts = sequencer.Claimed, attempts+1
		case modelMarkRunning:
			nextState = sequencer.Running
		case modelExpire:
			nextState = sequencer.Indeterminate
		case modelReconcileRetry:
			nextState = sequencer.Eligible
		case modelComplete:
			nextState = sequencer.Succeeded
		case modelRenew, modelStaleComplete:
		}
		generateRecoveryTraces(append(prefix, action), nextState, nextAttempts, remaining-1, traces)
	}
}

func assertRecoveryTrace(t *testing.T, trace []recoveryModelAction) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	store := memory.New()
	registration := sequencer.Registration{ID: "model.operation", Version: 1, Checksum: "sha256:model", Channel: "deploy"}
	if err := store.Register(ctx, []sequencer.Registration{registration}, now); err != nil {
		t.Fatal(err)
	}
	state := sequencer.Eligible
	var current, first sequencer.Claim
	for step, action := range trace {
		now = now.Add(100 * time.Millisecond)
		switch action {
		case modelClaim:
			claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{
				Candidates: []sequencer.ClaimCandidate{{ID: registration.ID, Version: 1, Checksum: registration.Checksum, Channel: registration.Channel}},
				Owner:      fmt.Sprintf("pod-%d", step), Now: now, LeaseDuration: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			current = claim
			if first.Attempt.Number == 0 {
				first = claim
			}
			state = sequencer.Claimed
		case modelMarkRunning:
			if _, err := store.MarkRunning(ctx, current.Ownership(), now); err != nil {
				t.Fatal(err)
			}
			state = sequencer.Running
		case modelRenew:
			until, err := store.RenewLease(ctx, current.Ownership(), now, time.Second)
			if err != nil || !until.After(now) {
				t.Fatalf("RenewLease() = %s, %v", until, err)
			}
		case modelExpire:
			now = now.Add(2 * time.Second)
			if recovered, err := store.RecoverExpired(ctx, now); err != nil || recovered != 1 {
				t.Fatalf("RecoverExpired() = %d, %v", recovered, err)
			}
			state = sequencer.Indeterminate
		case modelReconcileRetry:
			if err := store.ResolveUnknown(ctx, sequencer.ReconcileRequest{
				OperationID: registration.ID, Version: 1, Attempt: current.Attempt.Number,
				Fencing: current.Attempt.Fencing, Resolution: sequencer.ReconcileRetry,
				Actor: "operator", Reason: "effect absent", At: now,
			}); err != nil {
				t.Fatal(err)
			}
			state = sequencer.Eligible
		case modelComplete:
			if err := store.Complete(ctx, sequencer.Completion{Ownership: current.Ownership(), State: sequencer.Succeeded, At: now}); err != nil {
				t.Fatal(err)
			}
			state = sequencer.Succeeded
		case modelStaleComplete:
			if err := store.Complete(ctx, sequencer.Completion{Ownership: first.Ownership(), State: sequencer.Succeeded, At: now}); !errors.Is(err, sequencer.ErrStaleOwner) {
				t.Fatalf("stale Complete() error = %v", err)
			}
		}
		record, err := store.Snapshot(ctx, registration.ID, registration.Version)
		if err != nil || record.State != state || record.AttemptNumber != current.Attempt.Number || record.Fencing != current.Attempt.Fencing {
			t.Fatalf("step %d action %d record = %+v, %v; model state=%s claim=%+v", step, action, record, err, state, current)
		}
	}
}
