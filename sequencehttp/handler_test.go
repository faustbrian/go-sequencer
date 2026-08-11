package sequencehttp_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/faustbrian/golib/pkg/sequencer"
	"github.com/faustbrian/golib/pkg/sequencer/sequencehttp"
)

func TestHandlerRequiresApplicationAuthorization(t *testing.T) {
	t.Parallel()

	controller := &controllerStub{}
	handler, err := sequencehttp.New(controller, authorizerStub{err: errors.New("denied")})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/execute", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || controller.executed {
		t.Fatalf("status = %d, executed = %t", response.Code, controller.executed)
	}
}

func TestHandlerBindsResetActorToAuthorizedPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authorizer authorizerStub
		actor      string
		status     int
	}{
		{name: "matching actor", authorizer: authorizerStub{principal: "operator"}, actor: "operator", status: http.StatusAccepted},
		{name: "mismatched actor", authorizer: authorizerStub{principal: "operator"}, actor: "attacker", status: http.StatusForbidden},
		{name: "empty principal", authorizer: authorizerStub{emptyPrincipal: true}, actor: "operator", status: http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			controller := &controllerStub{}
			authorizer := test.authorizer
			authorizer.observed = &authorizationCall{}
			handler, err := sequencehttp.New(controller, authorizer)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/operations/postal/reset",
				bytes.NewBufferString(`{"version":1,"actor":"`+test.actor+`","reason":"retry"}`),
			)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if authorizer.observed.action != sequencehttp.ActionReset || authorizer.observed.resource != "postal" {
				t.Fatalf("authorization = (%q, %q), want (%q, %q)", authorizer.observed.action, authorizer.observed.resource, sequencehttp.ActionReset, "postal")
			}
			if test.status == http.StatusAccepted && controller.reset.Actor != "operator" {
				t.Fatalf("reset actor = %q, want authorized principal", controller.reset.Actor)
			}
			if test.status != http.StatusAccepted && controller.reset.OperationID != "" {
				t.Fatalf("unauthorized reset reached controller: %+v", controller.reset)
			}
		})
	}
}

func TestHandlerReconcilesUnknownAttemptAsAuthorizedPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authorizer authorizerStub
		actor      string
		status     int
	}{
		{name: "matching actor", authorizer: authorizerStub{principal: "operator"}, actor: "operator", status: http.StatusAccepted},
		{name: "mismatched actor", authorizer: authorizerStub{principal: "operator"}, actor: "attacker", status: http.StatusForbidden},
		{name: "empty principal", authorizer: authorizerStub{emptyPrincipal: true}, actor: "operator", status: http.StatusForbidden},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			controller := &controllerStub{}
			authorizer := test.authorizer
			authorizer.observed = &authorizationCall{}
			handler, err := sequencehttp.New(controller, authorizer)
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"/operations/postal/reconcile",
				bytes.NewBufferString(`{"version":2,"attempt":3,"fencing":42,"resolution":"retry","actor":"`+test.actor+`","reason":"effect absent"}`),
			)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			if authorizer.observed.action != sequencehttp.ActionReconcile || authorizer.observed.resource != "postal" {
				t.Fatalf("authorization = (%q, %q), want (%q, %q)", authorizer.observed.action, authorizer.observed.resource, sequencehttp.ActionReconcile, "postal")
			}
			if test.status != http.StatusAccepted {
				if controller.reconcile.OperationID != "" {
					t.Fatalf("unauthorized reconcile reached controller: %+v", controller.reconcile)
				}
				return
			}
			if controller.reconcile.OperationID != "postal" || controller.reconcile.Version != 2 ||
				controller.reconcile.Attempt != 3 || controller.reconcile.Fencing != 42 ||
				controller.reconcile.Resolution != sequencer.ReconcileRetry || controller.reconcile.Actor != "operator" ||
				controller.reconcile.Reason != "effect absent" || controller.reconcile.At.IsZero() {
				t.Fatalf("reconcile request = %+v", controller.reconcile)
			}
		})
	}
}

func TestHandlerEnforcesAdministrativeIdentityAndReasonBounds(t *testing.T) {
	t.Parallel()

	exactActor := strings.Repeat("a", sequencer.DefaultMaxActorBytes)
	exactReason := strings.Repeat("r", sequencer.DefaultMaxReasonBytes)
	tests := []struct {
		name      string
		path      string
		principal string
		body      string
		status    int
	}{
		{
			name: "reset exact bounds", path: "/operations/a/reset", principal: exactActor,
			body:   `{"version":1,"actor":"` + exactActor + `","reason":"` + exactReason + `"}`,
			status: http.StatusAccepted,
		},
		{
			name: "reset actor overflow", path: "/operations/a/reset", principal: "operator",
			body:   `{"version":1,"actor":"` + strings.Repeat("a", sequencer.DefaultMaxActorBytes+1) + `","reason":"reason"}`,
			status: http.StatusBadRequest,
		},
		{
			name: "reset reason overflow", path: "/operations/a/reset", principal: "operator",
			body:   `{"version":1,"actor":"operator","reason":"` + strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1) + `"}`,
			status: http.StatusBadRequest,
		},
		{
			name: "reconcile exact bounds", path: "/operations/a/reconcile", principal: exactActor,
			body:   `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"` + exactActor + `","reason":"` + exactReason + `"}`,
			status: http.StatusAccepted,
		},
		{
			name: "reconcile actor overflow", path: "/operations/a/reconcile", principal: "operator",
			body:   `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"` + strings.Repeat("a", sequencer.DefaultMaxActorBytes+1) + `","reason":"reason"}`,
			status: http.StatusBadRequest,
		},
		{
			name: "reconcile reason overflow", path: "/operations/a/reconcile", principal: "operator",
			body:   `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"operator","reason":"` + strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1) + `"}`,
			status: http.StatusBadRequest,
		},
		{
			name: "authorized principal overflow", path: "/execute",
			principal: strings.Repeat("a", sequencer.DefaultMaxActorBytes+1), status: http.StatusForbidden,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controller := &controllerStub{}
			handler, err := sequencehttp.New(controller, authorizerStub{principal: test.principal})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, test.path, strings.NewReader(test.body))
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
		})
	}
}

func TestHandlerMapsReconcileResolutionsWithoutNarrowingFencing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resolution string
		want       sequencer.ReconcileResolution
	}{
		{name: "succeeded", resolution: "succeeded", want: sequencer.ReconcileSucceeded},
		{name: "retry", resolution: "retry", want: sequencer.ReconcileRetry},
		{name: "failed", resolution: "failed", want: sequencer.ReconcileFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			controller := &controllerStub{}
			handler, err := sequencehttp.New(controller, authorizerStub{})
			if err != nil {
				t.Fatal(err)
			}
			body := `{"version":1,"attempt":2,"fencing":18446744073709551615,"resolution":"` + test.resolution + `","actor":"op","reason":"verified"}`
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/operations/a/reconcile", bytes.NewBufferString(body)))

			if response.Code != http.StatusAccepted || controller.reconcile.Resolution != test.want || controller.reconcile.Fencing != ^uint64(0) {
				t.Fatalf("status = %d, reconcile = %+v", response.Code, controller.reconcile)
			}
		})
	}
}

func TestHandlerRejectsInvalidReconcileRequestsAndStoreRejection(t *testing.T) {
	t.Parallel()

	controller := &controllerStub{err: errors.New("store rejected reconcile")}
	handler, err := sequencehttp.New(controller, authorizerStub{})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		method string
		path   string
		body   string
		status int
	}{
		{http.MethodPut, "/operations/a/reconcile", "", http.StatusNotFound},
		{http.MethodPost, "/other/a/reconcile", "", http.StatusNotFound},
		{http.MethodPost, "/operations//reconcile", "", http.StatusNotFound},
		{http.MethodPost, "/operations/a/b/reconcile", "", http.StatusNotFound},
		{http.MethodPost, "/operations/a/reconcile", `{"unknown":true}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":0,"attempt":1,"fencing":1,"resolution":"retry","actor":"op","reason":"verified"}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":0,"fencing":1,"resolution":"retry","actor":"op","reason":"verified"}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":1,"fencing":0,"resolution":"retry","actor":"op","reason":"verified"}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":1,"fencing":1,"resolution":"unknown","actor":"op","reason":"verified"}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"","reason":"verified"}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"op","reason":""}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"op","reason":"verified"}{}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"op","reason":"` + strings.Repeat("x", 8<<10) + `"}`, http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reconcile", `{"version":1,"attempt":1,"fencing":1,"resolution":"retry","actor":"op","reason":"verified"}`, http.StatusConflict},
	}
	for _, test := range tests {
		response := httptest.NewRecorder()
		request := httptest.NewRequestWithContext(context.Background(), test.method, test.path, strings.NewReader(test.body))

		handler.ServeHTTP(response, request)

		if response.Code != test.status {
			t.Errorf("%s %s status = %d, want %d", test.method, test.path, response.Code, test.status)
		}
	}
}

func TestHandlerExposesBoundedInspectAndExecuteControls(t *testing.T) {
	t.Parallel()

	controller := &controllerStub{}
	handler, err := sequencehttp.New(controller, authorizerStub{})
	if err != nil {
		t.Fatal(err)
	}

	inspect := httptest.NewRecorder()
	handler.ServeHTTP(inspect, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/operations/postal?version=2", nil))
	if inspect.Code != http.StatusOK || controller.inspected != "postal" {
		t.Fatalf("inspect status = %d, id = %q", inspect.Code, controller.inspected)
	}

	execute := httptest.NewRecorder()
	handler.ServeHTTP(execute, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/execute", nil))
	if execute.Code != http.StatusAccepted || !controller.executed {
		t.Fatalf("execute status = %d, executed = %t", execute.Code, controller.executed)
	}
}

func TestHandlerBoundsAndValidatesOperationIDBeforeAuthorization(t *testing.T) {
	t.Parallel()

	exactID := "a" + strings.Repeat("b", 254)
	observed := &authorizationCall{}
	controller := &controllerStub{}
	handler, err := sequencehttp.New(controller, authorizerStub{observed: observed})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/operations/"+exactID+"?version=1", nil))
	if response.Code != http.StatusOK || controller.inspected != exactID || observed.resource != exactID {
		t.Fatalf("exact ID status = %d, inspected = %q, authorized = %q", response.Code, controller.inspected, observed.resource)
	}

	for _, id := range []string{"a" + strings.Repeat("b", 255), "Uppercase", "-leading"} {
		observed = &authorizationCall{}
		handler, err = sequencehttp.New(&controllerStub{}, authorizerStub{observed: observed})
		if err != nil {
			t.Fatal(err)
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/operations/"+id+"?version=1", nil))
		if response.Code != http.StatusNotFound || observed.action != "" {
			t.Fatalf("invalid ID %q status = %d, authorization = %+v", id, response.Code, observed)
		}
	}
}

func TestHandlerValidationAndFailureResponses(t *testing.T) {
	t.Parallel()

	if _, err := sequencehttp.New(nil, authorizerStub{}); !errors.Is(err, sequencehttp.ErrInvalidHandler) {
		t.Fatalf("New(nil) error = %v", err)
	}
	if _, err := sequencehttp.New(&controllerStub{}, nil); !errors.Is(err, sequencehttp.ErrInvalidHandler) {
		t.Fatalf("New(nil authorizer) error = %v", err)
	}
	controller := &controllerStub{err: errors.New("controller")}
	handler, _ := sequencehttp.New(controller, authorizerStub{})
	tests := []struct {
		method string
		path   string
		body   []byte
		status int
	}{
		{http.MethodGet, "/unknown", nil, http.StatusNotFound},
		{http.MethodPut, "/operations/a/reset", nil, http.StatusNotFound},
		{http.MethodPost, "/other/a/reset", nil, http.StatusNotFound},
		{http.MethodPost, "/operations/a", nil, http.StatusNotFound},
		{http.MethodGet, "/operations/?version=1", nil, http.StatusNotFound},
		{http.MethodGet, "/operations/a/b?version=1", nil, http.StatusNotFound},
		{http.MethodGet, "/operations/a?version=nope", nil, http.StatusBadRequest},
		{http.MethodGet, "/operations/a?version=0", nil, http.StatusBadRequest},
		{http.MethodGet, "/operations/a?version=1", nil, http.StatusNotFound},
		{http.MethodPost, "/execute", nil, http.StatusConflict},
		{http.MethodPost, "/operations//reset", nil, http.StatusNotFound},
		{http.MethodPost, "/operations/a/b/reset", nil, http.StatusNotFound},
		{http.MethodPost, "/operations/a/reset", []byte(`{"unknown":true}`), http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reset", []byte(`{"version":0,"actor":"op","reason":"retry"}`), http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reset", []byte(`{"version":1,"actor":"","reason":"retry"}`), http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reset", []byte(`{"version":1,"actor":"op","reason":""}`), http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reset", []byte(`{"version":1,"actor":"op","reason":"retry"}{}`), http.StatusBadRequest},
		{http.MethodPost, "/operations/a/reset", []byte(`{"version":1,"actor":"op","reason":"retry"}`), http.StatusConflict},
	}
	for _, test := range tests {
		request := httptest.NewRequestWithContext(context.Background(), test.method, test.path, bytes.NewReader(test.body))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Errorf("%s %s status = %d, want %d", test.method, test.path, response.Code, test.status)
		}
	}

	controller.err = nil
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/operations/a/reset", bytes.NewBufferString(`{"version":1,"actor":"op","reason":"retry"}`)))
	if response.Code != http.StatusAccepted || controller.reset.OperationID != "a" {
		t.Fatalf("reset status = %d, request = %+v", response.Code, controller.reset)
	}
	denied, _ := sequencehttp.New(controller, authorizerStub{err: errors.New("denied")})
	response = httptest.NewRecorder()
	denied.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/operations/a/reset", nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("denied reset status = %d", response.Code)
	}
	response = httptest.NewRecorder()
	denied.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/operations/a?version=1", nil))
	if response.Code != http.StatusForbidden {
		t.Fatalf("denied inspect status = %d", response.Code)
	}
	writer := &failingWriter{header: make(http.Header)}
	handler.ServeHTTP(writer, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/operations/a?version=1", nil))
}

func TestHandlerRejectsInspectVersionOutsidePlatformUint(t *testing.T) {
	if strconv.IntSize != 32 {
		t.Skip("the overflow boundary is specific to 32-bit uint")
	}

	controller := &controllerStub{}
	handler, err := sequencehttp.New(controller, authorizerStub{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/operations/a?version=4294967296",
		nil,
	)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

type controllerStub struct {
	inspected string
	executed  bool
	reset     sequencehttp.ResetRequest
	reconcile sequencer.ReconcileRequest
	err       error
}

func (controller *controllerStub) Inspect(_ context.Context, id string, version uint) (any, error) {
	controller.inspected = id
	return map[string]any{"id": id, "version": version}, controller.err
}

func (controller *controllerStub) Execute(context.Context) error {
	controller.executed = true
	return controller.err
}

func (controller *controllerStub) Reset(_ context.Context, request sequencehttp.ResetRequest) error {
	controller.reset = request
	return controller.err
}

func (controller *controllerStub) Reconcile(_ context.Context, request sequencer.ReconcileRequest) error {
	controller.reconcile = request
	return controller.err
}

type authorizerStub struct {
	principal      string
	emptyPrincipal bool
	err            error
	observed       *authorizationCall
}

type authorizationCall struct {
	action   sequencehttp.Action
	resource string
}

func (stub authorizerStub) Authorize(_ context.Context, action sequencehttp.Action, resource string) (string, error) {
	if stub.observed != nil {
		stub.observed.action = action
		stub.observed.resource = resource
	}
	if stub.emptyPrincipal {
		return "", stub.err
	}
	if stub.principal == "" {
		return "op", stub.err
	}
	return stub.principal, stub.err
}

type failingWriter struct{ header http.Header }

func (writer *failingWriter) Header() http.Header       { return writer.header }
func (writer *failingWriter) WriteHeader(int)           {}
func (writer *failingWriter) Write([]byte) (int, error) { return 0, errors.New("write") }
