package sequencehttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	sequencer "github.com/faustbrian/go-sequencer/v2"
	"github.com/faustbrian/go-sequencer/v2/memory"
	"github.com/faustbrian/go-sequencer/v2/sequencehttp"
)

type auditStoreController struct {
	*controllerStub
	store *memory.Store
}

func (controller *auditStoreController) Reset(ctx context.Context, request sequencehttp.ResetRequest) error {
	return controller.store.Reset(ctx, sequencer.ResetRequest{OperationID: sequencer.OperationID(request.OperationID), Version: request.Version, Actor: request.Actor, Reason: request.Reason, At: time.Now()})
}

func (controller *auditStoreController) Reconcile(ctx context.Context, request sequencer.ReconcileRequest) error {
	return controller.store.ResolveUnknown(ctx, request)
}

func TestAdministrativeHTTPAuditUsesDefensiveStoreSanitization(t *testing.T) {
	for _, action := range []string{"reset", "reconcile"} {
		for _, emptyReason := range []bool{false, true} {
			t.Run(action+map[bool]string{false: "/redacted", true: "/empty reason"}[emptyReason], func(t *testing.T) {
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
				state := sequencer.Succeeded
				if action == "reconcile" {
					state = sequencer.Indeterminate
				}
				if err = store.Complete(ctx, sequencer.Completion{Ownership: claim.Ownership(), State: state, At: now}); err != nil {
					t.Fatal(err)
				}
				actor := "operator password=synthetic-value"
				before, err := store.Snapshot(ctx, "audit", 1)
				if err != nil {
					t.Fatal(err)
				}
				historyBefore, err := store.History(ctx, "audit", 1, 10)
				if err != nil {
					t.Fatal(err)
				}
				auditBefore, err := store.Audit(ctx, "audit", 1, 10)
				if err != nil {
					t.Fatal(err)
				}
				reason := "approved token=synthetic-value"
				if emptyReason {
					reason = "\x00\t"
				}
				body, err := json.Marshal(map[string]any{"version": 1, "actor": actor, "reason": reason})
				if action == "reconcile" {
					body, err = json.Marshal(map[string]any{"version": 1, "actor": actor, "reason": reason, "attempt": claim.Attempt.Number, "fencing": claim.Attempt.Fencing, "resolution": "succeeded"})
				}
				if err != nil {
					t.Fatal(err)
				}
				handler, err := sequencehttp.New(&auditStoreController{controllerStub: &controllerStub{}, store: store}, authorizerStub{principal: actor})
				if err != nil {
					t.Fatal(err)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequestWithContext(ctx, http.MethodPost, "/operations/audit/"+action, bytes.NewReader(body)))
				if emptyReason {
					if response.Code != http.StatusConflict {
						t.Fatalf("empty reason status=%d", response.Code)
					}
					record, snapshotErr := store.Snapshot(ctx, "audit", 1)
					auditAfter, auditErr := store.Audit(ctx, "audit", 1, 10)
					historyAfter, historyErr := store.History(ctx, "audit", 1, 10)
					if snapshotErr != nil || auditErr != nil || historyErr != nil || !reflect.DeepEqual(record, before) || !reflect.DeepEqual(auditAfter, auditBefore) || !reflect.DeepEqual(historyAfter, historyBefore) {
						t.Fatal("invalid HTTP attribution changed state")
					}
					return
				}
				if response.Code != http.StatusAccepted {
					t.Fatalf("status=%d", response.Code)
				}
				audit, err := store.Audit(ctx, "audit", 1, 10)
				if err != nil || len(audit) == 0 {
					t.Fatalf("audit error=%v", err)
				}
				last := audit[len(audit)-1]
				if last.Actor != "operator password=[REDACTED]" || last.Reason != "approved token=[REDACTED]" {
					t.Fatalf("HTTP audit actor=%q reason=%q", last.Actor, last.Reason)
				}
			})
		}
	}
}
