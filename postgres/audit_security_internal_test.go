package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type auditCaptureTx struct {
	*fakeTx
	actor, reason string
}

type ownerCaptureTx struct {
	*fakeTx
	owners []string
}

func (tx *ownerCaptureTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	index := 2
	if strings.Contains(sql, "WITH requested AS") {
		index = 4
	}
	tx.owners = append(tx.owners, args[index].(string))
	return tx.fakeTx.QueryRow(ctx, sql, args...)
}

func (tx *ownerCaptureTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.Contains(sql, "INSERT INTO sequencer_attempts"):
		tx.owners = append(tx.owners, args[3].(string))
	case strings.Contains(sql, "INSERT INTO sequencer_audit_events"):
		tx.owners = append(tx.owners, args[6].(string), args[8].(string))
	case strings.Contains(sql, "UPDATE sequencer_attempts"):
		tx.owners = append(tx.owners, args[3].(string))
	}
	return tx.fakeTx.Exec(ctx, sql, args...)
}

func TestPostgresClaimAndStartPreserveOwnerParameters(t *testing.T) {
	tx := &ownerCaptureTx{fakeTx: &fakeTx{rows: []pgx.Row{claimRow(), runningRow()}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("INSERT 0 1"), pgconn.NewCommandTag("UPDATE 1")}}}
	store := newStore(&fakeDatabase{tx: tx})
	request := validClaimRequest()
	claim, err := store.ClaimNext(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.MarkRunning(context.Background(), claim.Ownership(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if claim.Ownership().Owner != request.Owner || record.Owner != request.Owner || len(tx.owners) != 8 {
		t.Fatalf("owner parameters/return identity mismatch: %+v", tx.owners)
	}
	for _, owner := range tx.owners {
		if owner != request.Owner {
			t.Fatal("safe owner changed before SQL persistence")
		}
	}
}

func (tx *auditCaptureTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "INSERT INTO sequencer_audit_events") {
		tx.actor, _ = args[8].(string)
		tx.reason, _ = args[9].(string)
	}
	return tx.fakeTx.Exec(ctx, sql, args...)
}

func TestDirectPostgresAuditParametersAreRedactedAndAttributable(t *testing.T) {
	for _, action := range []string{"complete", "reset", "reconcile"} {
		for _, test := range []struct {
			name, actor, reason string
			invalid             bool
		}{
			{name: "redacted", actor: "operator password=synthetic-value", reason: "approved token=synthetic-value"},
			{name: "empty actor", actor: "\x00\t", reason: "approved", invalid: true},
			{name: "empty reason", actor: "operator", reason: "\x00\t", invalid: true},
		} {
			t.Run(action+"/"+test.name, func(t *testing.T) {
				row := completionRow()
				if action == "reset" {
					row = resetRow("succeeded")
				}
				if action == "reconcile" {
					row = reconcileRow("indeterminate", "succeeded")
				}
				tx := &auditCaptureTx{fakeTx: &fakeTx{rows: []pgx.Row{row}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}}
				beforeDatabase := errors.New("database unexpectedly accessed")
				database := &fakeDatabase{tx: tx}
				if test.invalid {
					database.beginErr = beforeDatabase
				}
				store := newStore(database)
				var err error
				switch action {
				case "complete":
					request := validCompletion()
					request.Actor, request.Reason = test.actor, test.reason
					err = store.Complete(context.Background(), request)
				case "reset":
					err = store.Reset(context.Background(), sequencer.ResetRequest{OperationID: "a", Version: 1, Actor: test.actor, Reason: test.reason})
				case "reconcile":
					request := validReconcileRequest()
					request.Actor, request.Reason = test.actor, test.reason
					err = store.ResolveUnknown(context.Background(), request)
				}
				if test.invalid {
					if err == nil || errors.Is(err, beforeDatabase) {
						t.Fatalf("empty cleaned attribution reached database: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if tx.actor != "operator password=[REDACTED]" || tx.reason != "approved token=[REDACTED]" {
					t.Fatalf("unredacted audit parameters actor=%q reason=%q", tx.actor, tx.reason)
				}
			})
		}
	}
}

func TestUnsafePostgresClaimOwnerRejectedBeforeDatabase(t *testing.T) {
	for _, owner := range []string{"worker password=synthetic-value", "worker\x00identity", "\x00\t"} {
		databaseErr := errors.New("database unexpectedly accessed")
		store := newStore(&fakeDatabase{beginErr: databaseErr})
		request := validClaimRequest()
		request.Owner = owner
		if _, err := store.ClaimNext(context.Background(), request); err == nil || errors.Is(err, databaseErr) {
			t.Fatalf("unsafe owner reached database: %v", err)
		}
	}
}
