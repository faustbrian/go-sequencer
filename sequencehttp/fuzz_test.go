package sequencehttp_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/sequencehttp"
)

func FuzzAdministrativeCommands(fuzz *testing.F) {
	fuzz.Add(http.MethodPost, "/operations/postal/reset", []byte(`{"version":1,"actor":"operator","reason":"retry"}`))
	fuzz.Add(http.MethodPost, "/operations/postal/reset", []byte(`{"version":1,"actor":"attacker","reason":"retry"}`))
	fuzz.Add(http.MethodPost, "/operations/postal/reset", bytes.Repeat([]byte("x"), (8<<10)+1))
	fuzz.Add(http.MethodPost, "/operations/postal/reconcile", []byte(`{"version":1,"attempt":2,"fencing":3,"resolution":"retry","actor":"operator","reason":"effect absent"}`))
	fuzz.Add(http.MethodPost, "/operations/postal/reconcile", []byte(`{"version":1,"attempt":2,"fencing":3,"resolution":"retry","actor":"attacker","reason":"effect absent"}`))
	fuzz.Add(http.MethodGet, "/operations/postal?version=1", []byte{})
	fuzz.Add(http.MethodPost, "/execute", []byte{})
	fuzz.Fuzz(func(t *testing.T, method, target string, body []byte) {
		const maxFuzzRequestBytes = 16 << 10
		if len(method) > 32 || len(target) > 2<<10 || len(body) > maxFuzzRequestBytes {
			return
		}
		controller := &fuzzController{}
		handler, err := sequencehttp.New(controller, fuzzAuthorizer{})
		if err != nil {
			t.Fatal(err)
		}
		request, err := http.NewRequestWithContext(context.Background(), method, target, bytes.NewReader(body))
		if err != nil {
			return
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		if response.Code >= http.StatusInternalServerError {
			t.Fatalf("administrative request returned %d", response.Code)
		}
		if controller.resetCalled {
			if response.Code != http.StatusAccepted || controller.reset.Actor != "operator" || controller.reset.OperationID == "" {
				t.Fatalf("unsafe reset invocation: status=%d request=%+v", response.Code, controller.reset)
			}
		}
		if len(body) > 8<<10 && controller.resetCalled {
			t.Fatal("oversized reset body reached controller")
		}
		if controller.reconcileCalled {
			if response.Code != http.StatusAccepted || controller.reconcile.Actor != "operator" ||
				controller.reconcile.OperationID == "" || controller.reconcile.Attempt == 0 || controller.reconcile.Fencing == 0 {
				t.Fatalf("unsafe reconciliation invocation: status=%d request=%+v", response.Code, controller.reconcile)
			}
		}
		if len(body) > 8<<10 && controller.reconcileCalled {
			t.Fatal("oversized reconciliation body reached controller")
		}
	})
}

type fuzzController struct {
	resetCalled     bool
	reset           sequencehttp.ResetRequest
	reconcileCalled bool
	reconcile       sequencer.ReconcileRequest
}

func (*fuzzController) Inspect(_ context.Context, id string, version uint) (any, error) {
	return map[string]any{"id": id, "version": version}, nil
}

func (*fuzzController) Execute(context.Context) error { return nil }

func (controller *fuzzController) Reset(_ context.Context, request sequencehttp.ResetRequest) error {
	controller.resetCalled = true
	controller.reset = request
	return nil
}

func (controller *fuzzController) Reconcile(_ context.Context, request sequencer.ReconcileRequest) error {
	controller.reconcileCalled = true
	controller.reconcile = request
	return nil
}

type fuzzAuthorizer struct{}

func (fuzzAuthorizer) Authorize(context.Context, sequencehttp.Action, string) (string, error) {
	return "operator", nil
}
