//go:build integration

package postgres_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	sequencerpostgres "github.com/faustbrian/go-sequencer/v2/postgres"
)

func verifyPostgresAuditSecurity(t *testing.T, ctx context.Context, store *sequencerpostgres.Store) {
	t.Helper()
	verifyPostgresOwnerIdentity(t, ctx, store)
	for _, action := range []string{"complete", "reset", "reconcile"} {
		for _, test := range []struct {
			name, actor, reason string
			invalid             bool
		}{
			{name: "redacted", actor: "operator password=synthetic-value", reason: "approved token=synthetic-value"},
			{name: "empty-actor", actor: "\x00\t", reason: "approved", invalid: true},
			{name: "empty-reason", actor: "operator", reason: "\x00\t", invalid: true},
		} {
			t.Run("audit/"+action+"/"+test.name, func(t *testing.T) {
				id := sequencer.OperationID("audit.security-" + action + "-" + test.name)
				checksum := testChecksum(string(id))
				if err := store.Register(ctx, []sequencer.Registration{{ID: id, Version: 1, Checksum: checksum}}, time.Now()); err != nil {
					t.Fatal(err)
				}
				claim, err := store.ClaimNext(ctx, sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: checksum}}, Owner: "owner", LeaseDuration: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
					t.Fatal(err)
				}
				if action != "complete" {
					state := sequencer.Succeeded
					if action == "reconcile" {
						state = sequencer.Indeterminate
					}
					if err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: state}); err != nil {
						t.Fatal(err)
					}
				}
				before, err := store.Snapshot(ctx, id, 1)
				if err != nil {
					t.Fatal(err)
				}
				auditBefore, err := store.Audit(ctx, id, 1, 10)
				if err != nil {
					t.Fatal(err)
				}
				historyBefore, err := store.History(ctx, id, 1, 10)
				if err != nil {
					t.Fatal(err)
				}
				switch action {
				case "complete":
					err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: sequencer.Succeeded, Actor: test.actor, Reason: test.reason})
				case "reset":
					err = store.Reset(ctx, sequencer.ResetRequest{OperationID: id, Version: 1, At: time.Now(), Actor: test.actor, Reason: test.reason})
				case "reconcile":
					err = store.ResolveUnknown(ctx, sequencer.ReconcileRequest{OperationID: id, Version: 1, Attempt: claim.Attempt.Number, Fencing: claim.Attempt.Fencing, At: time.Now().Add(time.Second), Resolution: sequencer.ReconcileSucceeded, Actor: test.actor, Reason: test.reason})
				}
				if test.invalid {
					if err == nil {
						t.Fatal("empty cleaned attribution accepted")
					}
					after, snapshotErr := store.Snapshot(ctx, id, 1)
					auditAfter, auditErr := store.Audit(ctx, id, 1, 10)
					historyAfter, historyErr := store.History(ctx, id, 1, 10)
					if snapshotErr != nil || auditErr != nil || historyErr != nil || !reflect.DeepEqual(auditAfter, auditBefore) || !reflect.DeepEqual(after, before) || !reflect.DeepEqual(historyAfter, historyBefore) {
						t.Fatal("invalid attribution mutated durable state")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				audit, err := store.Audit(ctx, id, 1, 10)
				if err != nil || len(audit) == 0 {
					t.Fatalf("audit error=%v", err)
				}
				last := audit[len(audit)-1]
				if last.Actor != "operator password=[REDACTED]" || last.Reason != "approved token=[REDACTED]" {
					t.Fatalf("persisted audit actor=%q reason=%q", last.Actor, last.Reason)
				}
			})
		}
	}
}

func verifyPostgresOwnerIdentity(t *testing.T, ctx context.Context, store *sequencerpostgres.Store) {
	t.Helper()
	id := sequencer.OperationID("owner.security.identity")
	checksum := testChecksum(string(id))
	if err := store.Register(ctx, []sequencer.Registration{{ID: id, Version: 1, Checksum: checksum}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	before, err := store.Snapshot(ctx, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	historyBefore, err := store.History(ctx, id, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	auditBefore, err := store.Audit(ctx, id, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	request := sequencer.ClaimRequest{Candidates: []sequencer.ClaimCandidate{{ID: id, Version: 1, Checksum: checksum}}, LeaseDuration: time.Minute}
	for _, owner := range []string{"worker password=synthetic-value", "worker\x00identity", "\x00\t"} {
		request.Owner = owner
		if _, err := store.ClaimNext(ctx, request); err == nil {
			t.Fatal("unsafe owner accepted")
		}
		after, err := store.Snapshot(ctx, id, 1)
		if err != nil {
			t.Fatal(err)
		}
		historyAfter, err := store.History(ctx, id, 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		auditAfter, err := store.Audit(ctx, id, 1, 10)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(historyBefore, historyAfter) || !reflect.DeepEqual(auditBefore, auditAfter) {
			t.Fatal("rejected owner changed persisted state")
		}
	}
	request.Owner = "safe-worker"
	claim, err := store.ClaimNext(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(ctx, claim.Ownership(), time.Now()); err != nil {
		t.Fatal(err)
	}
	record, err := store.Snapshot(ctx, id, 1)
	if err != nil {
		t.Fatal(err)
	}
	history, err := store.History(ctx, id, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := store.Audit(ctx, id, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Ownership().Owner != request.Owner || record.Owner != request.Owner || len(history) != 1 || history[0].Owner != request.Owner {
		t.Fatal("safe owner fencing identity changed")
	}
	for _, event := range audit {
		if event.To == sequencer.Claimed || event.To == sequencer.Running {
			if event.Owner != request.Owner || event.Actor != request.Owner {
				t.Fatal("safe audit owner identity changed")
			}
		}
	}
}
