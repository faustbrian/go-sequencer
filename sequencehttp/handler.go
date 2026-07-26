// Package sequencehttp provides bounded administrative controls. Applications
// must supply authentication and authorization; this package provides none.
package sequencehttp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

const maxRequestBytes = 8 << 10

// ErrInvalidHandler reports missing administrative dependencies.
var ErrInvalidHandler = errors.New("sequencer/sequencehttp: invalid handler")

// Action identifies an authorization decision.
type Action string

const (
	// ActionInspect authorizes reading operation state.
	ActionInspect Action = "inspect"
	// ActionExecute authorizes starting synchronous execution.
	ActionExecute Action = "execute"
	// ActionReset authorizes an attributable replay reset.
	ActionReset Action = "reset"
)

// ResetRequest contains attributable administrative replay metadata.
type ResetRequest struct {
	OperationID string `json:"operation_id"`
	Version     uint   `json:"version"`
	Actor       string `json:"actor"`
	Reason      string `json:"reason"`
}

// Controller owns inspection and execution semantics.
type Controller interface {
	Inspect(context.Context, string, uint) (any, error)
	Execute(context.Context) error
	Reset(context.Context, ResetRequest) error
}

// Authorizer is implemented by the application security boundary.
type Authorizer interface {
	Authorize(context.Context, Action, string) error
}

// Handler exposes inspect, execute, and reset controls.
type Handler struct {
	controller Controller
	authorizer Authorizer
}

// New constructs a handler that denies no request implicitly: an explicit
// application authorizer is mandatory.
func New(controller Controller, authorizer Authorizer) (*Handler, error) {
	if controller == nil || authorizer == nil {
		return nil, ErrInvalidHandler
	}
	return &Handler{controller: controller, authorizer: authorizer}, nil
}

// ServeHTTP dispatches the small fixed administrative surface.
func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	switch {
	case request.Method == http.MethodPost && request.URL.Path == "/execute":
		handler.execute(response, request)
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/operations/"):
		handler.inspect(response, request)
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/reset") && strings.HasPrefix(request.URL.Path, "/operations/"):
		handler.reset(response, request)
	default:
		http.NotFound(response, request)
	}
}

func (handler *Handler) execute(response http.ResponseWriter, request *http.Request) {
	if !handler.authorized(response, request, ActionExecute, "") {
		return
	}
	if err := handler.controller.Execute(request.Context()); err != nil {
		writeError(response, http.StatusConflict)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}

func (handler *Handler) inspect(response http.ResponseWriter, request *http.Request) {
	id := strings.TrimPrefix(request.URL.Path, "/operations/")
	if id == "" || strings.Contains(id, "/") || !handler.authorized(response, request, ActionInspect, id) {
		if id == "" || strings.Contains(id, "/") {
			http.NotFound(response, request)
		}
		return
	}
	version, err := strconv.ParseUint(
		request.URL.Query().Get("version"),
		10,
		strconv.IntSize,
	)
	if err != nil || version == 0 {
		writeError(response, http.StatusBadRequest)
		return
	}
	result, err := handler.controller.Inspect(request.Context(), id, uint(version))
	if err != nil {
		writeError(response, http.StatusNotFound)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(response).Encode(result); err != nil {
		return
	}
}

func (handler *Handler) reset(response http.ResponseWriter, request *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/operations/"), "/reset")
	if id == "" || strings.Contains(id, "/") || !handler.authorized(response, request, ActionReset, id) {
		if id == "" || strings.Contains(id, "/") {
			http.NotFound(response, request)
		}
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, maxRequestBytes)
	var reset ResetRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&reset) != nil || reset.Version == 0 || reset.Actor == "" || reset.Reason == "" {
		writeError(response, http.StatusBadRequest)
		return
	}
	reset.OperationID = id
	if err := handler.controller.Reset(request.Context(), reset); err != nil {
		writeError(response, http.StatusConflict)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}

func (handler *Handler) authorized(response http.ResponseWriter, request *http.Request, action Action, id string) bool {
	if err := handler.authorizer.Authorize(request.Context(), action, id); err != nil {
		writeError(response, http.StatusForbidden)
		return false
	}
	return true
}

func writeError(response http.ResponseWriter, status int) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_, _ = response.Write([]byte(`{"error":"request failed"}`))
}
