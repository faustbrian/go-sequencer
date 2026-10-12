// Package sequencehttp provides bounded administrative controls. Applications
// must supply authentication and authorization; this package provides none.
//
// Only single-segment operation IDs are routable: slash is rejected even though
// the root OperationID grammar permits it. Bodies for reset/reconcile are limited
// to 8 KiB and one JSON value with unknown fields rejected; execution is synchronous.
// Controllers and authorizers are borrowed and must support concurrent requests.
package sequencehttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/faustbrian/go-sequencer/v2"
)

const maxRequestBytes = 8 << 10

// MaxResponseBytes bounds one pre-encoded administrative inspection response.
const MaxResponseBytes = 1 << 20

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
	// ActionReconcile authorizes resolving an indeterminate attempt.
	ActionReconcile Action = "reconcile"
)

// ResetRequest contains attributable administrative replay metadata.
type ResetRequest struct {
	// OperationID is replaced by the validated URL resource before controller dispatch.
	OperationID string `json:"operation_id"`
	// Version selects the nonzero exact definition.
	Version uint `json:"version"`
	// Actor must exactly match the authorized principal; the handler replaces it with that principal.
	Actor string `json:"actor"`
	// Reason is required and at most DefaultMaxReasonBytes; the controller owns audit persistence.
	Reason string `json:"reason"`
}

// Inspection is one pre-encoded JSON administrative response. Controllers
// retain ownership and must not mutate it until Inspect handling returns.
type Inspection []byte

// Controller owns inspection and execution semantics. Inspect must return
// pre-encoded UTF-8 JSON so the handler can reject oversized results before
// parsing or copying them.
type Controller interface {
	// Inspect returns nonempty, valid UTF-8 JSON at most MaxResponseBytes.
	// The controller must keep its bytes unchanged until request handling returns.
	Inspect(context.Context, string, uint) (Inspection, error)
	// Execute completes its synchronous action before the handler returns HTTP 202;
	// the status does not describe a separate asynchronous job.
	Execute(context.Context) error
	// Reset owns replay validation and attributable persistence for the URL-bound identity.
	Reset(context.Context, ResetRequest) error
	// Reconcile owns exact unknown-attempt resolution; the handler supplies wall-clock UTC time.
	Reconcile(context.Context, sequencer.ReconcileRequest) error
}

// Authorizer is implemented by the application security boundary. Authorize
// returns the stable, non-empty principal that was authenticated and
// authorized for the action and resource.
type Authorizer interface {
	// Authorize authenticates and authorizes this action/resource, returning a nonempty
	// principal of at most DefaultMaxActorBytes. Execute uses an empty resource.
	Authorize(context.Context, Action, string) (string, error)
}

// Handler exposes fixed inspect, execute, reset and reconciliation controls.
// It retains borrowed collaborators; the zero Handler is not usable.
type Handler struct {
	controller Controller
	authorizer Authorizer
}

// New requires non-nil controller and authorizer interfaces. Each supported
// action is authorized explicitly; typed-nil implementations are not detected.
// Construction installs no server, authentication middleware or background work.
func New(controller Controller, authorizer Authorizer) (*Handler, error) {
	if controller == nil || authorizer == nil {
		return nil, ErrInvalidHandler
	}
	return &Handler{controller: controller, authorizer: authorizer}, nil
}

// ServeHTTP authorizes and dispatches the fixed method/path surface. Controllers
// run synchronously on the request context, and error bodies are generic JSON.
// Valid Inspection bytes are written unchanged; write errors are not propagated.
// Unknown routes are 404. It supplies neither CSRF nor transport security.
func (handler *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	switch {
	case request.Method == http.MethodPost && request.URL.Path == "/execute":
		handler.execute(response, request)
	case request.Method == http.MethodGet && strings.HasPrefix(request.URL.Path, "/operations/"):
		handler.inspect(response, request)
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/reset") && strings.HasPrefix(request.URL.Path, "/operations/"):
		handler.reset(response, request)
	case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/reconcile") && strings.HasPrefix(request.URL.Path, "/operations/"):
		handler.reconcile(response, request)
	default:
		http.NotFound(response, request)
	}
}

func (handler *Handler) execute(response http.ResponseWriter, request *http.Request) {
	if _, ok := handler.authorized(response, request, ActionExecute, ""); !ok {
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
	if !validOperationID(id) {
		http.NotFound(response, request)
		return
	}
	if _, ok := handler.authorized(response, request, ActionInspect, id); !ok {
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
	if len(result) == 0 || len(result) > MaxResponseBytes || !utf8.Valid(result) || !json.Valid(result) {
		writeError(response, http.StatusInternalServerError)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("X-Content-Type-Options", "nosniff")
	// #nosec G705 -- UTF-8 JSON is validated and bounded above; nosniff prevents HTML interpretation while preserving controller-owned bytes.
	_, _ = response.Write(result)
}

func (handler *Handler) reset(response http.ResponseWriter, request *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/operations/"), "/reset")
	if !validOperationID(id) {
		http.NotFound(response, request)
		return
	}
	principal, ok := handler.authorized(response, request, ActionReset, id)
	if !ok {
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, maxRequestBytes)
	var reset ResetRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if decodeSingleJSON(decoder, &reset) != nil || reset.Version == 0 ||
		reset.Actor == "" || len(reset.Actor) > sequencer.DefaultMaxActorBytes ||
		reset.Reason == "" || len(reset.Reason) > sequencer.DefaultMaxReasonBytes {
		writeError(response, http.StatusBadRequest)
		return
	}
	if reset.Actor != principal {
		writeError(response, http.StatusForbidden)
		return
	}
	reset.OperationID = id
	reset.Actor = principal
	if err := handler.controller.Reset(request.Context(), reset); err != nil {
		writeError(response, http.StatusConflict)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}

type reconcileRequest struct {
	Version    uint   `json:"version"`
	Attempt    uint   `json:"attempt"`
	Fencing    uint64 `json:"fencing"`
	Resolution string `json:"resolution"`
	Actor      string `json:"actor"`
	Reason     string `json:"reason"`
}

func (handler *Handler) reconcile(response http.ResponseWriter, request *http.Request) {
	id := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, "/operations/"), "/reconcile")
	if !validOperationID(id) {
		http.NotFound(response, request)
		return
	}
	principal, ok := handler.authorized(response, request, ActionReconcile, id)
	if !ok {
		return
	}
	request.Body = http.MaxBytesReader(response, request.Body, maxRequestBytes)
	var body reconcileRequest
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if decodeSingleJSON(decoder, &body) != nil || body.Version == 0 || body.Attempt == 0 || body.Fencing == 0 ||
		body.Actor == "" || len(body.Actor) > sequencer.DefaultMaxActorBytes ||
		body.Reason == "" || len(body.Reason) > sequencer.DefaultMaxReasonBytes {
		writeError(response, http.StatusBadRequest)
		return
	}
	resolution, ok := reconcileResolution(body.Resolution)
	if !ok {
		writeError(response, http.StatusBadRequest)
		return
	}
	if body.Actor != principal {
		writeError(response, http.StatusForbidden)
		return
	}
	reconcile := sequencer.ReconcileRequest{
		OperationID: sequencer.OperationID(id),
		Version:     body.Version,
		Attempt:     body.Attempt,
		Fencing:     body.Fencing,
		Resolution:  resolution,
		Actor:       principal,
		Reason:      body.Reason,
		At:          time.Now().UTC(),
	}
	if err := handler.controller.Reconcile(request.Context(), reconcile); err != nil {
		writeError(response, http.StatusConflict)
		return
	}
	response.WriteHeader(http.StatusAccepted)
}

func validOperationID(id string) bool {
	return !strings.Contains(id, "/") && sequencer.OperationID(id).Valid()
}

func decodeSingleJSON(decoder *json.Decoder, target any) error {
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidHandler
	}
	return nil
}

func reconcileResolution(value string) (sequencer.ReconcileResolution, bool) {
	switch value {
	case "succeeded":
		return sequencer.ReconcileSucceeded, true
	case "retry":
		return sequencer.ReconcileRetry, true
	case "failed":
		return sequencer.ReconcileFailed, true
	default:
		return 0, false
	}
}

func (handler *Handler) authorized(response http.ResponseWriter, request *http.Request, action Action, id string) (string, bool) {
	principal, err := handler.authorizer.Authorize(request.Context(), action, id)
	if err != nil || principal == "" || len(principal) > sequencer.DefaultMaxActorBytes {
		writeError(response, http.StatusForbidden)
		return "", false
	}
	return principal, true
}

func writeError(response http.ResponseWriter, status int) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_, _ = response.Write([]byte(`{"error":"request failed"}`))
}
