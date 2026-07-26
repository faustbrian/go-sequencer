package sequencehttp_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

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
		{http.MethodGet, "/operations/a/b?version=1", nil, http.StatusNotFound},
		{http.MethodGet, "/operations/a?version=nope", nil, http.StatusBadRequest},
		{http.MethodGet, "/operations/a?version=1", nil, http.StatusNotFound},
		{http.MethodPost, "/execute", nil, http.StatusConflict},
		{http.MethodPost, "/operations/a/b/reset", nil, http.StatusNotFound},
		{http.MethodPost, "/operations/a/reset", []byte(`{"unknown":true}`), http.StatusBadRequest},
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

type authorizerStub struct{ err error }

func (stub authorizerStub) Authorize(context.Context, sequencehttp.Action, string) error {
	return stub.err
}

type failingWriter struct{ header http.Header }

func (writer *failingWriter) Header() http.Header       { return writer.header }
func (writer *failingWriter) WriteHeader(int)           {}
func (writer *failingWriter) Write([]byte) (int, error) { return 0, errors.New("write") }
