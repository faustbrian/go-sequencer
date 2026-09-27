package sequencer_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/memory"
)

func TestDirectMemoryAuditTextIsRedactedAndAttributable(t *testing.T) {
	for _, action := range []string{"complete", "reset", "reconcile"} {
		for _, test := range []struct {
			name, actor, reason string
			invalid             bool
		}{
			{name: "redacted", actor: "operator password=synthetic-value", reason: "approved token=synthetic-value"},
			{name: "empty cleaned actor", actor: "\x00\t", reason: "approved", invalid: true},
			{name: "empty cleaned reason", actor: "operator", reason: "\x00\t", invalid: true},
		} {
			t.Run(action+"/"+test.name, func(t *testing.T) {
				ctx := context.Background()
				now := time.Now()
				store := memory.New()
				if err := store.Register(ctx, []sequencer.Registration{{ID: "audit", Version: 1, Checksum: sequencer.ChecksumBytes([]byte("audit"))}}, now); err != nil {
					t.Fatal(err)
				}
				claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"audit"}, Owner: "owner", Now: now, LeaseDuration: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = store.MarkRunning(ctx, claim.Ownership(), now); err != nil {
					t.Fatal(err)
				}
				if action != "complete" {
					state := sequencer.Succeeded
					if action == "reconcile" {
						state = sequencer.Indeterminate
					}
					if err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: state, At: now}); err != nil {
						t.Fatal(err)
					}
				}
				before, err := store.Snapshot(ctx, "audit", 1)
				if err != nil {
					t.Fatal(err)
				}
				auditBefore, err := store.Audit(ctx, "audit", 1, 10)
				if err != nil {
					t.Fatal(err)
				}
				historyBefore, err := store.History(ctx, "audit", 1, 10)
				if err != nil {
					t.Fatal(err)
				}
				switch action {
				case "complete":
					err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, At: now, Actor: test.actor, Reason: test.reason})
				case "reset":
					err = store.Reset(ctx, sequencer.ResetRequest{OperationID: "audit", Version: 1, At: now.Add(time.Second), Actor: test.actor, Reason: test.reason})
				case "reconcile":
					err = store.ResolveUnknown(ctx, sequencer.ReconcileRequest{OperationID: "audit", Version: 1, Attempt: claim.Attempt.Number, Fencing: claim.Attempt.Fencing, At: now.Add(time.Second), Resolution: sequencer.ReconcileSucceeded, Actor: test.actor, Reason: test.reason})
				}
				if test.invalid {
					if err == nil {
						t.Fatal("control-only attribution accepted")
					}
					after, snapshotErr := store.Snapshot(ctx, "audit", 1)
					auditAfter, auditErr := store.Audit(ctx, "audit", 1, 10)
					historyAfter, historyErr := store.History(ctx, "audit", 1, 10)
					if snapshotErr != nil || auditErr != nil || historyErr != nil || !reflect.DeepEqual(auditAfter, auditBefore) || !reflect.DeepEqual(after, before) || !reflect.DeepEqual(historyAfter, historyBefore) {
						t.Fatalf("invalid attribution changed state: %+v -> %+v", before, after)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				audit, err := store.Audit(ctx, "audit", 1, 10)
				if err != nil || len(audit) == 0 {
					t.Fatalf("audit error=%v", err)
				}
				last := audit[len(audit)-1]
				if last.Actor != "operator password=[REDACTED]" || last.Reason != "approved token=[REDACTED]" {
					t.Fatalf("unredacted audit actor=%q reason=%q", last.Actor, last.Reason)
				}
			})
		}
	}
}

func TestRunnerObserverClassifiesContextCancellation(t *testing.T) {
	for _, fleetMode := range []bool{false, true} {
		t.Run(map[bool]string{false: "runner", true: "fleet"}[fleetMode], func(t *testing.T) {
			spec := validSpec("observer.canceled")
			spec.Handler = sequencer.HandlerFunc(func(context.Context, sequencer.Attempt) (sequencer.Output, error) {
				return sequencer.Output{}, context.Canceled
			})
			plan, _ := sequencer.CompilePlan([]sequencer.OperationSpec{spec}, sequencer.PlanOptions{})
			events := make(chan sequencer.Event, 4)
			store := memory.New()
			options := sequencer.RunnerOptions{Owner: "owner", Observers: []sequencer.Observer{sequencer.ObserverFunc(func(event sequencer.Event) { events <- event })}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			if fleetMode {
				fleet, err := sequencer.NewFleet(plan, store, sequencer.FleetOptions{RunnerOptions: options, ClaimInterval: time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				go func() { done <- fleet.Run(ctx) }()
				defer func() {
					cancel()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("fleet did not stop")
					}
				}()
			} else {
				runner, err := sequencer.NewRunner(plan, store, options)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = runner.Execute(ctx)
			}
			for {
				select {
				case event := <-events:
					if event.Type == sequencer.EventCompleted {
						record, err := store.Snapshot(context.Background(), spec.ID, spec.Version)
						history, historyErr := store.History(context.Background(), spec.ID, spec.Version, 10)
						if err != nil || historyErr != nil || record.State != sequencer.Canceled || len(history) != 1 || history[0].State != sequencer.Canceled || history[0].ErrorDetail != sequencer.ErrCanceled.Error() {
							t.Fatalf("durable canceled classification: record=%+v history=%+v errors=%v/%v", record, history, err, historyErr)
						}
						if event.State != sequencer.Canceled || !errors.Is(event.Err, sequencer.ErrCanceled) {
							t.Fatalf("canceled event state=%v error=%v", event.State, event.Err)
						}
						return
					}
				case <-time.After(time.Second):
					t.Fatal("completion event missing")
				}
			}
		})
	}
}
