package sequencehttp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/faustbrian/go-sequencer/v2/sequencehttp"
)

func TestInspectionPreservesExactPreencodedBoundary(t *testing.T) {
	for _, test := range []struct {
		name, json string
		status     int
	}{
		{name: "exact boundary", json: `"` + strings.Repeat("x", sequencehttp.MaxResponseBytes-2) + `"`, status: http.StatusOK},
		{name: "HTML characters", json: `"<>&"`, status: http.StatusOK},
		{name: "oversize", json: `"` + strings.Repeat("x", sequencehttp.MaxResponseBytes-1) + `"`, status: http.StatusInternalServerError},
		{name: "invalid JSON", json: `{"unclosed":`, status: http.StatusInternalServerError},
		{name: "empty", status: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, err := sequencehttp.New(&controllerStub{inspection: sequencehttp.Inspection(test.json)}, authorizerStub{})
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/operations/a?version=1", nil))
			if response.Code != test.status {
				t.Fatalf("status=%d want=%d", response.Code, test.status)
			}
			if test.status == http.StatusOK && response.Body.String() != test.json {
				t.Fatal("inspection changed the validated bytes")
			}
			if test.status == http.StatusInternalServerError && response.Body.String() != `{"error":"request failed"}` {
				t.Fatal("inspection error was not generic")
			}
			if response.Body.Len() > sequencehttp.MaxResponseBytes {
				t.Fatal("response exceeded bound")
			}
		})
	}
}
