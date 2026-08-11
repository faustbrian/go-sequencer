package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestIntegerConversionsRejectInvalidLedgerValues(t *testing.T) {
	t.Parallel()

	if _, err := toUint(-1); err == nil {
		t.Fatal("toUint(-1) error = nil")
	}
	if _, err := toUint64(-1); err == nil {
		t.Fatal("toUint64(-1) error = nil")
	}
	if _, err := toInt64(^uint(0)); err == nil {
		t.Fatal("toInt64(max uint) error = nil")
	}
	if value, err := toUint(1); err != nil || value != 1 {
		t.Fatalf("toUint(1) = %d, %v", value, err)
	}
	if value, err := toUint(0); err != nil || value != 0 {
		t.Fatalf("toUint(0) = %d, %v", value, err)
	}
	if value, err := toUint64(0); err != nil || value != 0 {
		t.Fatalf("toUint64(0) = %d, %v", value, err)
	}
	if value, err := toUint64(1); err != nil || value != 1 {
		t.Fatalf("toUint64(1) = %d, %v", value, err)
	}
	if value, err := toInt64(uint(math.MaxInt64)); err != nil || value != math.MaxInt64 {
		t.Fatalf("toInt64(MaxInt64) = %d, %v", value, err)
	}
}

func TestStoreImmediateDatabaseFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("database")
	ctx := context.Background()
	begin := newStore(&fakeDatabase{beginErr: cause})
	checks := []func() error{
		func() error {
			return begin.Register(ctx, []sequencer.Registration{{ID: "a", Version: 1, Checksum: "sum"}}, time.Now())
		},
		func() error { _, err := begin.ClaimNext(ctx, validClaimRequest()); return err },
		func() error { _, err := begin.MarkRunning(ctx, validOwnership(), time.Now()); return err },
		func() error { return begin.Complete(ctx, validCompletion()) },
		func() error { _, err := begin.RecoverExpired(ctx, time.Now()); return err },
		func() error {
			return begin.Reset(ctx, sequencer.ResetRequest{OperationID: "a", Version: 1, Actor: "op", Reason: "why"})
		},
	}
	for index, check := range checks {
		if err := check(); !errors.Is(err, cause) {
			t.Errorf("begin check %d error = %v", index, err)
		}
	}
	queries := newStore(&fakeDatabase{queryErr: cause, row: scriptedRow{err: cause}})
	if _, err := queries.Snapshot(ctx, "a", 1); !errors.Is(err, cause) {
		t.Errorf("Snapshot() error = %v", err)
	}
	if _, err := queries.History(ctx, "a", 1, 1); !errors.Is(err, cause) {
		t.Errorf("History() error = %v", err)
	}
	if _, err := queries.Audit(ctx, "a", 1, 1); !errors.Is(err, cause) {
		t.Errorf("Audit() error = %v", err)
	}
}

func TestStoreRecoverExpiredTransactionFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("failure")
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"scan", &fakeTx{rows: []pgx.Row{scriptedRow{err: cause}}}, cause},
		{"missing current attempt", &fakeTx{rows: []pgx.Row{recoveryRow(1, 0, 0, 0, 0, 0)}}, sequencer.ErrDefinitionDrift},
		{"missing projection update", &fakeTx{rows: []pgx.Row{recoveryRow(1, 1, 0, 0, 0, 0)}}, sequencer.ErrDefinitionDrift},
		{"missing unknown audit", &fakeTx{rows: []pgx.Row{recoveryRow(1, 1, 1, 0, 0, 0)}}, sequencer.ErrDefinitionDrift},
		{"missing replay audit", &fakeTx{rows: []pgx.Row{recoveryRow(1, 1, 1, 1, 1, 0)}}, sequencer.ErrDefinitionDrift},
		{"commit", &fakeTx{rows: []pgx.Row{recoveryRow(1, 1, 1, 1, 0, 0)}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			count, err := newStore(&fakeDatabase{tx: test.tx}).RecoverExpired(context.Background(), time.Now())
			if count != 0 || !errors.Is(err, test.want) {
				t.Fatalf("RecoverExpired() = %d, %v", count, err)
			}
		})
	}

	store := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{recoveryRow(1, 1, 1, 1, 0, 0)}}})
	if count, err := store.RecoverExpired(context.Background(), time.Now()); err != nil || count != 1 {
		t.Fatalf("RecoverExpired() = %d, %v", count, err)
	}
}

func TestStoreRegisterTransactionFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("failure")
	registration := []sequencer.Registration{{ID: "a", Version: 1, Checksum: "sum"}}
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"insert", &fakeTx{execErrs: []error{cause}}, cause},
		{"scan", &fakeTx{rows: []pgx.Row{scriptedRow{err: cause}}}, cause},
		{"drift", &fakeTx{rows: []pgx.Row{registrationRow("other", nil, []sequencer.DependencyRef{})}}, sequencer.ErrChecksumDrift},
		{"definition drift", &fakeTx{rows: []pgx.Row{registrationRow("sum", []string{"other"}, nil)}}, sequencer.ErrDefinitionDrift},
		{"commit", &fakeTx{rows: []pgx.Row{registrationRow("sum", nil, []sequencer.DependencyRef{})}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(&fakeDatabase{tx: test.tx})
			if err := store.Register(context.Background(), registration, time.Now()); !errors.Is(err, test.want) {
				t.Fatalf("Register() error = %v", err)
			}
		})
	}
}

func TestStoreRegisterWritesAuditForNewIdentity(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{
		rows:     []pgx.Row{registrationRow("sum", nil, []sequencer.DependencyRef{})},
		execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1")},
	}
	store := newStore(&fakeDatabase{tx: tx})
	if err := store.Register(context.Background(), []sequencer.Registration{{
		ID: "a", Version: 1, Checksum: "sum",
	}}, time.Now()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
}

func TestStoreRegisterComparesCanonicalDependencyOrder(t *testing.T) {
	t.Parallel()

	store := newStore(&fakeDatabase{tx: &fakeTx{
		rows: []pgx.Row{registrationRow("sum", []string{"b", "a"}, nil)},
	}})
	if err := store.Register(context.Background(), []sequencer.Registration{{
		ID: "operation", Version: 1, Checksum: "sum",
		DependencyRefs: []sequencer.DependencyRef{
			{ID: "a", Version: 1, Checksum: "a-sum"},
			{ID: "b", Version: 2, Checksum: "b-sum"},
		},
	}}, time.Now()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
}

func TestStoreRegisterRejectsInvalidAndDriftedDefinitions(t *testing.T) {
	t.Parallel()

	tooMany := make([]sequencer.DependencyRef, sequencer.DefaultMaxDependencies+1)
	for index := range tooMany {
		tooMany[index] = sequencer.DependencyRef{ID: sequencer.OperationID("dependency" + string(rune(index))), Version: 1, Checksum: "sum"}
	}
	dependency := sequencer.DependencyRef{ID: "dependency", Version: 1, Checksum: "sum"}
	invalid := []sequencer.Registration{
		{ID: "missing-version", Checksum: "sum"},
		{ID: "missing-checksum", Version: 1},
		{ID: "invalid-unknown-policy", Version: 1, Checksum: "sum", UnknownOutcome: sequencer.UnknownOutcomePolicy(255)},
		{ID: "legacy", Version: 1, Checksum: "sum", Dependencies: []sequencer.OperationID{"dependency"}},
		{ID: "large", Version: 1, Checksum: "sum", DependencyRefs: tooMany},
		{ID: "operation", Version: 1, Checksum: "sum", Compensates: &dependency},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency", Version: 1}}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{
			{ID: "dependency", Version: 1, Checksum: "a"},
			{ID: "dependency", Version: 2, Checksum: "b"},
		}},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{
			{ID: "dependency", Version: 1, Checksum: "a"},
			{ID: "dependency", Version: 1, Checksum: "b"},
		}},
	}
	for _, registration := range invalid {
		if err := newStore(&fakeDatabase{tx: &fakeTx{}}).Register(context.Background(), []sequencer.Registration{registration}, time.Now()); err == nil {
			t.Fatalf("Register(%+v) error = nil", registration)
		}
	}
	for _, registration := range []sequencer.Registration{
		{ID: "Invalid", Version: 1, Checksum: "sum"},
		{ID: "operation", Version: 1, Checksum: "sum", Channel: "Invalid Channel"},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "Invalid", Version: 1, Checksum: "sum"}}},
	} {
		if err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).Register(context.Background(), []sequencer.Registration{registration}, time.Now()); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Fatalf("Register(malformed identifier) error = %v", err)
		}
	}
	for _, registration := range []sequencer.Registration{
		{ID: "operation", Version: 1, Checksum: strings.Repeat("c", 513)},
		{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{{ID: "dependency", Version: 1, Checksum: strings.Repeat("c", 513)}}},
	} {
		if err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).Register(context.Background(), []sequencer.Registration{registration}, time.Now()); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Fatalf("Register(checksum overflow) error = %v", err)
		}
	}
	exactDependencies := make([]sequencer.DependencyRef, sequencer.DefaultMaxDependencies)
	dependencyIDs := make([]string, len(exactDependencies))
	for index := range exactDependencies {
		id := fmt.Sprintf("dependency-%03d", index)
		exactDependencies[index] = sequencer.DependencyRef{ID: sequencer.OperationID(id), Version: 1, Checksum: "x"}
		dependencyIDs[index] = id
	}
	remaining := maxPersistedDefinitionBytes - len(encodeDependencyRefs(exactDependencies))
	for index := range exactDependencies {
		addition := min(remaining, sequencer.DefaultMaxChecksumBytes-1)
		exactDependencies[index].Checksum += strings.Repeat("x", addition)
		remaining -= addition
	}
	if remaining != 0 || len(encodeDependencyRefs(exactDependencies)) != maxPersistedDefinitionBytes {
		t.Fatal("could not construct exact dependency definition boundary")
	}
	exactRegistrationChecksum := strings.Repeat("c", sequencer.DefaultMaxChecksumBytes)
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{
		registrationRow(exactRegistrationChecksum, dependencyIDs, exactDependencies),
	}}}).Register(context.Background(), []sequencer.Registration{{
		ID: "operation", Version: 1, Checksum: exactRegistrationChecksum, DependencyRefs: exactDependencies,
	}}, time.Now()); err != nil {
		t.Fatalf("Register(exact dependency definition bound) error = %v", err)
	}
	exactCompensation := sequencer.DependencyRef{ID: "dependency", Version: 1, Checksum: strings.Repeat("x", sequencer.DefaultMaxChecksumBytes)}
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{
		registrationRawRow("sum", []string{"dependency"}, encodeDependencyRefs([]sequencer.DependencyRef{exactCompensation}), encodeDependencyRef(&exactCompensation), 0, false),
	}}}).Register(context.Background(), []sequencer.Registration{{
		ID: "operation", Version: 1, Checksum: "sum",
		DependencyRefs: []sequencer.DependencyRef{exactCompensation}, Compensates: &exactCompensation,
	}}, time.Now()); err != nil {
		t.Fatalf("Register(exact compensation checksum bound) error = %v", err)
	}
	oversizedDependencies := append([]sequencer.DependencyRef(nil), exactDependencies...)
	for index := len(oversizedDependencies) - 1; index >= 0; index-- {
		if len(oversizedDependencies[index].Checksum) < sequencer.DefaultMaxChecksumBytes {
			oversizedDependencies[index].Checksum += "x"
			break
		}
	}
	if len(encodeDependencyRefs(oversizedDependencies)) != maxPersistedDefinitionBytes+1 {
		t.Fatal("could not construct oversized dependency definition")
	}
	if err := newStore(&fakeDatabase{tx: &fakeTx{}}).Register(context.Background(), []sequencer.Registration{{
		ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: oversizedDependencies,
	}}, time.Now()); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Register(oversized dependency definition) error = %v", err)
	}
	cause := errors.New("failure")
	registration := sequencer.Registration{ID: "operation", Version: 1, Checksum: "sum", DependencyRefs: []sequencer.DependencyRef{dependency}}
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"pin legacy refs", &fakeTx{rows: []pgx.Row{registrationRow("sum", []string{"dependency"}, nil)}, execErrs: []error{nil, cause}}, cause},
		{"malformed refs", &fakeTx{rows: []pgx.Row{registrationRawRow("sum", []string{"dependency"}, []byte(`{`), nil, 0, false)}}, sequencer.ErrDefinitionDrift},
		{"exact ref drift", &fakeTx{rows: []pgx.Row{registrationRow("sum", []string{"dependency"}, []sequencer.DependencyRef{{ID: "dependency", Version: 2, Checksum: "sum"}})}}, sequencer.ErrDefinitionDrift},
		{"malformed compensation", &fakeTx{rows: []pgx.Row{registrationRawRow("sum", []string{"dependency"}, encodeDependencyRefs([]sequencer.DependencyRef{dependency}), []byte(`{`), 0, false)}}, sequencer.ErrDefinitionDrift},
		{"policy drift", &fakeTx{rows: []pgx.Row{registrationRawRow("sum", []string{"dependency"}, encodeDependencyRefs([]sequencer.DependencyRef{dependency}), nil, 1, false)}}, sequencer.ErrDefinitionDrift},
		{"dead letter drift", &fakeTx{rows: []pgx.Row{registrationRawRow("sum", []string{"dependency"}, encodeDependencyRefs([]sequencer.DependencyRef{dependency}), nil, 0, true)}}, sequencer.ErrDefinitionDrift},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := newStore(&fakeDatabase{tx: test.tx}).Register(context.Background(), []sequencer.Registration{registration}, time.Now()); !errors.Is(err, test.want) {
				t.Fatalf("Register() error = %v", err)
			}
		})
	}

	newIdentity := []sequencer.Registration{{ID: "overflow", Version: ^uint(0), Checksum: "sum"}}
	overflowTx := &fakeTx{rows: []pgx.Row{registrationRow("sum", nil, []sequencer.DependencyRef{})}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1")}}
	if err := newStore(&fakeDatabase{tx: overflowTx}).Register(context.Background(), newIdentity, time.Now()); !errors.Is(err, errInvalidLedgerInteger) {
		t.Fatalf("Register(overflow audit) error = %v", err)
	}
	auditTx := &fakeTx{
		rows:     []pgx.Row{registrationRow("sum", nil, []sequencer.DependencyRef{})},
		execTags: []pgconn.CommandTag{pgconn.NewCommandTag("INSERT 0 1")},
		execErrs: []error{nil, cause},
	}
	if err := newStore(&fakeDatabase{tx: auditTx}).Register(context.Background(), []sequencer.Registration{{ID: "a", Version: 1, Checksum: "sum"}}, time.Now()); !errors.Is(err, cause) {
		t.Fatalf("Register(audit) error = %v", err)
	}
}

func TestStoreClaimTransactionFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("failure")
	success := claimRow()
	base := success.(scriptedRow).values
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"no rows", &fakeTx{rows: []pgx.Row{scriptedRow{err: pgx.ErrNoRows}}}, sequencer.ErrNoEligibleOperation},
		{"scan", &fakeTx{rows: []pgx.Row{scriptedRow{err: cause}}}, cause},
		{"checksum drift", &fakeTx{rows: []pgx.Row{checksumDriftRow()}}, sequencer.ErrChecksumDrift},
		{"negative version", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 2, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative attempt", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 3, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative fencing", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 4, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative run attempt", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 8, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative retry exceptions", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 9, int64(-1))}}}, errInvalidLedgerInteger},
		{"attempt insert", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"invalid source state", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 7, "invalid")}}}, cause},
		{"illegal eligibility transition", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 7, "running")}}}, sequencer.ErrInvalidTransition},
		{"eligibility audit", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 7, "retryable")}}, execErrs: []error{nil, cause}}, cause},
		{"audit insert", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, cause}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, nil}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(&fakeDatabase{tx: test.tx})
			_, err := store.ClaimNext(context.Background(), validClaimRequest())
			if test.name == "invalid source state" {
				if err == nil {
					t.Fatal("ClaimNext() error = nil")
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("ClaimNext() error = %v", err)
			}
		})
	}
	store := newStore(&fakeDatabase{})
	for _, request := range []sequencer.ClaimRequest{
		{OperationIDs: []sequencer.OperationID{"a"}, LeaseDuration: time.Minute},
		{OperationIDs: []sequencer.OperationID{"a"}, Owner: "owner"},
		{OperationIDs: []sequencer.OperationID{"a"}, Owner: "owner", LeaseDuration: -time.Second},
		{Owner: "owner", LeaseDuration: time.Minute},
	} {
		if _, err := store.ClaimNext(context.Background(), request); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Fatalf("ClaimNext(%+v) error = %v", request, err)
		}
	}
	if _, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "a", Version: ^uint(0), Checksum: "sum"}},
		Owner:      "owner", LeaseDuration: time.Minute,
	}); !errors.Is(err, errInvalidLedgerInteger) {
		t.Fatalf("ClaimNext(overflow) error = %v", err)
	}
	if _, err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).ClaimNext(context.Background(), sequencer.ClaimRequest{
		Candidates: []sequencer.ClaimCandidate{{ID: "a", Version: 1, Checksum: "sum"}},
		Owner:      "owner", LeaseDuration: time.Nanosecond,
	}); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("ClaimNext(sub-millisecond lease) error = %v", err)
	}
	overflowChecksum := validClaimRequest()
	overflowChecksum.Candidates = []sequencer.ClaimCandidate{{ID: "a", Version: 1, Checksum: strings.Repeat("c", 513)}}
	overflowChecksum.OperationIDs = nil
	if _, err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).ClaimNext(context.Background(), overflowChecksum); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("ClaimNext(checksum overflow) error = %v", err)
	}
	invalidChannel := validClaimRequest()
	invalidChannel.Candidates = []sequencer.ClaimCandidate{{ID: "a", Version: 1, Channel: "Invalid Channel"}}
	invalidChannel.OperationIDs = nil
	if _, err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).ClaimNext(context.Background(), invalidChannel); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("ClaimNext(invalid channel) error = %v", err)
	}
	invalidID := validClaimRequest()
	invalidID.Candidates = []sequencer.ClaimCandidate{{ID: "Invalid"}}
	invalidID.OperationIDs = nil
	if _, err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).ClaimNext(context.Background(), invalidID); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("ClaimNext(invalid operation ID) error = %v", err)
	}
	tooMany := validClaimRequest()
	tooMany.OperationIDs = make([]sequencer.OperationID, sequencer.DefaultMaxOperations+1)
	for index := range tooMany.OperationIDs {
		tooMany.OperationIDs[index] = "a"
	}
	if _, err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).ClaimNext(context.Background(), tooMany); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("ClaimNext(candidate overflow) error = %v", err)
	}
	overflowOwner := validClaimRequest()
	overflowOwner.Owner = strings.Repeat("o", sequencer.DefaultMaxActorBytes+1)
	if _, err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).ClaimNext(context.Background(), overflowOwner); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("ClaimNext(owner overflow) error = %v", err)
	}
	exactBounds := validClaimRequest()
	exactBounds.Owner = strings.Repeat("o", sequencer.DefaultMaxActorBytes)
	exactBounds.OperationIDs = nil
	exactBounds.Candidates = make([]sequencer.ClaimCandidate, sequencer.DefaultMaxOperations)
	for index := range exactBounds.Candidates {
		exactBounds.Candidates[index] = sequencer.ClaimCandidate{ID: "missing"}
	}
	exactBounds.Candidates[0] = sequencer.ClaimCandidate{
		ID: "a", Version: 1, Checksum: strings.Repeat("c", sequencer.DefaultMaxChecksumBytes),
	}
	exactClaim, err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{claimRow()}}}).ClaimNext(context.Background(), exactBounds)
	if err != nil {
		t.Fatalf("ClaimNext(exact bounds) error = %v", err)
	}
	if exactClaim.Budget.Attempt != 1 || exactClaim.Budget.Exceptions != 0 {
		t.Fatalf("ClaimNext() budget = %+v", exactClaim.Budget)
	}
}

func TestStoreMarkRunningTransactionFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("failure")
	success := runningRow()
	base := success.(scriptedRow).values
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"stale", &fakeTx{rows: []pgx.Row{scriptedRow{err: pgx.ErrNoRows}}}, sequencer.ErrStaleOwner},
		{"scan", &fakeTx{rows: []pgx.Row{scriptedRow{err: cause}}}, cause},
		{"negative version", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 1, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative attempt", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 2, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative fencing", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 4, int64(-1))}}}, errInvalidLedgerInteger},
		{"missing attempt", &fakeTx{rows: []pgx.Row{success}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}, sequencer.ErrDefinitionDrift},
		{"attempt update", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"audit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, cause}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, nil}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(&fakeDatabase{tx: test.tx})
			if _, err := store.MarkRunning(context.Background(), validOwnership(), time.Now()); !errors.Is(err, test.want) {
				t.Fatalf("MarkRunning() error = %v", err)
			}
		})
	}
	exactOwnership := validOwnership()
	exactOwnership.Owner = strings.Repeat("o", sequencer.DefaultMaxActorBytes)
	if _, err := newStore(&fakeDatabase{tx: &fakeTx{
		rows: []pgx.Row{runningRow()}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")},
	}}).MarkRunning(context.Background(), exactOwnership, time.Now()); err != nil {
		t.Fatalf("MarkRunning(exact owner bound) error = %v", err)
	}
}

func TestStoreRejectsInvalidOwnershipBeforeDatabaseAccess(t *testing.T) {
	t.Parallel()

	invalid := validOwnership()
	invalid.Owner = strings.Repeat("o", sequencer.DefaultMaxActorBytes+1)
	cause := errors.New("unexpected database call")
	store := newStore(&fakeDatabase{beginErr: cause, row: scriptedRow{err: cause}})
	if _, err := store.MarkRunning(context.Background(), invalid, time.Now()); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("MarkRunning() error = %v", err)
	}
	if _, err := store.RenewLease(context.Background(), invalid, time.Now(), time.Minute); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("RenewLease() error = %v", err)
	}
	completion := validCompletion()
	completion.Ownership = invalid
	if err := store.Complete(context.Background(), completion); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Complete() error = %v", err)
	}
}

func TestStoreRenewLeaseUsesFencingAndDatabaseTime(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	until := time.Date(2026, 8, 9, 10, 5, 0, 0, time.UTC)
	store := newStore(&fakeDatabase{row: scriptedRow{values: []any{until}}})
	got, err := store.RenewLease(ctx, validOwnership(), time.Now(), time.Minute)
	if err != nil || !got.Equal(until) {
		t.Fatalf("RenewLease() = %s, %v; want %s", got, err, until)
	}
	stale := newStore(&fakeDatabase{row: scriptedRow{err: pgx.ErrNoRows}})
	if _, err := stale.RenewLease(ctx, validOwnership(), time.Now(), time.Minute); !errors.Is(err, sequencer.ErrStaleOwner) {
		t.Fatalf("stale RenewLease() error = %v", err)
	}
	cause := errors.New("database")
	failed := newStore(&fakeDatabase{row: scriptedRow{err: cause}})
	if _, err := failed.RenewLease(ctx, validOwnership(), time.Now(), time.Minute); !errors.Is(err, cause) {
		t.Fatalf("failed RenewLease() error = %v", err)
	}
	if _, err := store.RenewLease(ctx, validOwnership(), time.Now(), 0); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("invalid RenewLease() error = %v", err)
	}
	if _, err := store.RenewLease(ctx, validOwnership(), time.Now(), time.Nanosecond); !errors.Is(err, sequencer.ErrInvalidLease) {
		t.Fatalf("sub-millisecond RenewLease() error = %v", err)
	}
}

func TestStoreCompleteTransactionFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("failure")
	success := completionRow()
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"stale", &fakeTx{rows: []pgx.Row{scriptedRow{err: pgx.ErrNoRows}}}, sequencer.ErrStaleOwner},
		{"scan", &fakeTx{rows: []pgx.Row{scriptedRow{err: cause}}}, cause},
		{"missing attempt", &fakeTx{rows: []pgx.Row{success}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 0")}}, sequencer.ErrDefinitionDrift},
		{"attempt update", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"audit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, cause}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, nil}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(&fakeDatabase{tx: test.tx})
			if err := store.Complete(context.Background(), validCompletion()); !errors.Is(err, test.want) {
				t.Fatalf("Complete() error = %v", err)
			}
		})
	}
	store := newStore(&fakeDatabase{})
	invalid := validCompletion()
	invalid.State = sequencer.Eligible
	if err := store.Complete(context.Background(), invalid); !errors.Is(err, sequencer.ErrInvalidTransition) {
		t.Fatalf("Complete(state) error = %v", err)
	}
	invalidRetryException := validCompletion()
	invalidRetryException.RetryException = true
	if err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).Complete(context.Background(), invalidRetryException); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Complete(invalid retry exception) error = %v", err)
	}
	missingRetryException := validCompletion()
	missingRetryException.State = sequencer.Retryable
	if err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).Complete(context.Background(), missingRetryException); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("Complete(missing retry exception) error = %v", err)
	}
	large := validCompletion()
	large.Output.Summary = string(make([]byte, sequencer.DefaultMaxOutputBytes+1))
	if err := store.Complete(context.Background(), large); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Complete(output) error = %v", err)
	}
	for _, completion := range []sequencer.Completion{
		func() sequencer.Completion {
			completion := validCompletion()
			completion.Actor = strings.Repeat("a", sequencer.DefaultMaxActorBytes+1)
			return completion
		}(),
		func() sequencer.Completion {
			completion := validCompletion()
			completion.Reason = strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1)
			return completion
		}(),
	} {
		if err := newStore(&fakeDatabase{beginErr: errors.New("unexpected database call")}).Complete(context.Background(), completion); !errors.Is(err, sequencer.ErrResourceLimit) {
			t.Fatalf("Complete(audit overflow) error = %v", err)
		}
	}
	exact := validCompletion()
	exact.Actor = strings.Repeat("a", sequencer.DefaultMaxActorBytes)
	exact.Reason = strings.Repeat("r", sequencer.DefaultMaxReasonBytes)
	emptyJSON, err := json.Marshal(exact.Output)
	if err != nil {
		t.Fatal(err)
	}
	exact.Output.Summary = strings.Repeat("x", sequencer.DefaultMaxOutputBytes-len(emptyJSON))
	exactStore := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{completionRow()}, execErrs: []error{nil, nil}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}})
	if err := exactStore.Complete(context.Background(), exact); err != nil {
		t.Fatalf("Complete(exact output bound) error = %v", err)
	}
	overflow := validCompletion()
	overflow.Version = ^uint(0)
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{completionRow()}, execErrs: []error{nil}, execTags: []pgconn.CommandTag{pgconn.NewCommandTag("UPDATE 1")}}}).Complete(context.Background(), overflow); !errors.Is(err, errInvalidLedgerInteger) {
		t.Fatalf("Complete(version) error = %v", err)
	}
}

func TestStoreBoundsDetachedTransactionRollback(t *testing.T) {
	t.Parallel()

	tx := &fakeTx{rows: []pgx.Row{scriptedRow{err: errors.New("scan")}}}
	_, _ = newStore(&fakeDatabase{tx: tx}).ClaimNext(context.Background(), validClaimRequest())
	if !tx.rollbackHasDeadline {
		t.Fatal("Rollback() context has no deadline")
	}
	remaining := time.Until(tx.rollbackDeadline)
	if remaining < 4*time.Second || remaining > defaultRollbackTimeout {
		t.Fatalf("Rollback() deadline remaining = %s, want about %s", remaining, defaultRollbackTimeout)
	}
}

func TestStoreReadDecodingFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("scan")
	ctx := context.Background()
	if _, err := newStore(&fakeDatabase{row: scriptedRow{err: pgx.ErrNoRows}}).Snapshot(ctx, "a", 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("Snapshot(missing) error = %v", err)
	}
	invalidSnapshot := []any{
		"a", int64(1), "sum", stringPointer(""), []sequencer.OperationID{}, []byte(`[]`), []byte(nil),
		int16(0), false, "invalid", int64(0), "", int64(0), time.Time{}, time.Now(), time.Now(), int64(0), int64(0),
	}
	if _, err := newStore(&fakeDatabase{row: scriptedRow{values: invalidSnapshot}}).Snapshot(ctx, "a", 1); err == nil {
		t.Fatal("Snapshot(invalid state) error = nil")
	}
	for _, index := range []int{1, 10, 12, 16, 17} {
		if _, err := newStore(&fakeDatabase{row: scriptedRow{values: replace(invalidSnapshot, index, int64(-1))}}).Snapshot(ctx, "a", 1); !errors.Is(err, errInvalidLedgerInteger) {
			t.Fatalf("Snapshot(integer %d) error = %v", index, err)
		}
	}
	for _, test := range []struct {
		name  string
		index int
		value any
	}{
		{"missing dependency refs", 5, []byte(nil)},
		{"missing channel", 3, (*string)(nil)},
		{"malformed dependency refs", 5, []byte(`{`)},
		{"oversized dependency refs", 5, []byte(`[]` + strings.Repeat(" ", (64<<10)+1))},
		{"malformed compensation", 6, []byte(`{`)},
		{"oversized compensation", 6, append(encodeDependencyRef(&sequencer.DependencyRef{ID: "a", Version: 1, Checksum: "sum"}), []byte(strings.Repeat(" ", (4<<10)+1))...)},
		{"invalid unknown policy", 7, int16(2)},
	} {
		t.Run("snapshot "+test.name, func(t *testing.T) {
			values := replace(invalidSnapshot, 9, "eligible")
			values = replace(values, test.index, test.value)
			if _, err := newStore(&fakeDatabase{row: scriptedRow{values: values}}).Snapshot(ctx, "a", 1); !errors.Is(err, sequencer.ErrDefinitionDrift) {
				t.Fatalf("Snapshot() error = %v", err)
			}
		})
	}

	historyBase := []any{"a", int64(1), int64(1), "owner", int64(1), "succeeded", time.Now(), time.Now(), "", []byte(`{}`)}
	historyCases := []struct {
		name string
		rows *fakeRows
	}{
		{"scan", &fakeRows{values: [][]any{historyBase}, scanErr: cause}},
		{"version", &fakeRows{values: [][]any{replace(historyBase, 1, int64(-1))}}},
		{"attempt", &fakeRows{values: [][]any{replace(historyBase, 2, int64(-1))}}},
		{"fencing", &fakeRows{values: [][]any{replace(historyBase, 4, int64(-1))}}},
		{"state", &fakeRows{values: [][]any{replace(historyBase, 5, "invalid")}}},
		{"json", &fakeRows{values: [][]any{replace(historyBase, 9, []byte(`{`))}}},
		{"oversized output", &fakeRows{values: [][]any{replace(historyBase, 9, []byte(`{}`+strings.Repeat(" ", (64<<10)+1)))}}},
		{"rows", &fakeRows{err: cause}},
	}
	for _, test := range historyCases {
		t.Run("history "+test.name, func(t *testing.T) {
			if _, err := newStore(&fakeDatabase{rows: test.rows}).History(ctx, "a", 1, 1); err == nil {
				t.Fatal("History() error = nil")
			}
		})
	}
	exactOutput := []byte(`{}` + strings.Repeat(" ", sequencer.DefaultMaxOutputBytes-2))
	if _, err := newStore(&fakeDatabase{rows: &fakeRows{values: [][]any{
		replace(historyBase, 9, exactOutput),
	}}}).History(ctx, "a", 1, 1); err != nil {
		t.Fatalf("History(exact output bound) error = %v", err)
	}

	auditBase := []any{"a", int64(1), int64(1), "eligible", "claimed", time.Now(), "owner", int64(1), "actor", "reason"}
	auditCases := []struct {
		name string
		rows *fakeRows
	}{
		{"scan", &fakeRows{values: [][]any{auditBase}, scanErr: cause}},
		{"version", &fakeRows{values: [][]any{replace(auditBase, 1, int64(-1))}}},
		{"attempt", &fakeRows{values: [][]any{replace(auditBase, 2, int64(-1))}}},
		{"fencing", &fakeRows{values: [][]any{replace(auditBase, 7, int64(-1))}}},
		{"from", &fakeRows{values: [][]any{replace(auditBase, 3, "invalid")}}},
		{"to", &fakeRows{values: [][]any{replace(auditBase, 4, "invalid")}}},
		{"rows", &fakeRows{err: cause}},
	}
	for _, test := range auditCases {
		t.Run("audit "+test.name, func(t *testing.T) {
			if _, err := newStore(&fakeDatabase{rows: test.rows}).Audit(ctx, "a", 1, 1); err == nil {
				t.Fatal("Audit() error = nil")
			}
		})
	}
	store := newStore(&fakeDatabase{})
	if _, err := store.History(ctx, "a", 1, 0); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("History(limit) error = %v", err)
	}
	if _, err := store.Audit(ctx, "a", 1, 0); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Audit(limit) error = %v", err)
	}
	bounded := newStore(&fakeDatabase{rows: &fakeRows{}})
	if _, err := bounded.History(ctx, "a", 1, sequencer.DefaultMaxHistory); err != nil {
		t.Fatalf("History(exact maximum) error = %v", err)
	}
	bounded = newStore(&fakeDatabase{rows: &fakeRows{}})
	if _, err := bounded.Audit(ctx, "a", 1, sequencer.DefaultMaxHistory); err != nil {
		t.Fatalf("Audit(exact maximum) error = %v", err)
	}
}

func TestStoreResetTransactionFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("failure")
	success := resetRow("failed")
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"forbidden", &fakeTx{rows: []pgx.Row{scriptedRow{err: pgx.ErrNoRows}}}, sequencer.ErrResetForbidden},
		{"scan", &fakeTx{rows: []pgx.Row{scriptedRow{err: cause}}}, cause},
		{"state", &fakeTx{rows: []pgx.Row{resetRow("invalid")}}, cause},
		{"audit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(&fakeDatabase{tx: test.tx})
			err := store.Reset(context.Background(), sequencer.ResetRequest{OperationID: "a", Version: 1, Actor: "op", Reason: "why"})
			if test.name == "state" {
				if err == nil {
					t.Fatal("Reset() error = nil")
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("Reset() error = %v", err)
			}
		})
	}
	for _, request := range []sequencer.ResetRequest{
		{Reason: "why"},
		{Actor: "op"},
		{OperationID: "a", Actor: "op", Reason: "why"},
		{Version: 1, Actor: "op", Reason: "why"},
		{OperationID: "a", Version: 1, Actor: strings.Repeat("a", sequencer.DefaultMaxActorBytes+1), Reason: "why"},
		{OperationID: "a", Version: 1, Actor: "op", Reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes+1)},
	} {
		if err := newStore(&fakeDatabase{}).Reset(context.Background(), request); !errors.Is(err, sequencer.ErrResetForbidden) {
			t.Fatalf("Reset(%+v) error = %v", request, err)
		}
	}
	if err := newStore(&fakeDatabase{beginErr: cause}).Reset(context.Background(), sequencer.ResetRequest{
		OperationID: "Invalid", Version: 1, Actor: "op", Reason: "why",
	}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset(invalid operation ID) error = %v", err)
	}
	exact := sequencer.ResetRequest{
		OperationID: "a", Version: 1,
		Actor:  strings.Repeat("a", sequencer.DefaultMaxActorBytes),
		Reason: strings.Repeat("r", sequencer.DefaultMaxReasonBytes),
	}
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{success}}}).Reset(context.Background(), exact); err != nil {
		t.Fatalf("Reset(exact bounds) error = %v", err)
	}
	overflow := sequencer.ResetRequest{OperationID: "a", Version: ^uint(0), Actor: "op", Reason: "why"}
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{success}}}).Reset(context.Background(), overflow); !errors.Is(err, errInvalidLedgerInteger) {
		t.Fatalf("Reset(version) error = %v", err)
	}
}

func TestStoreResolveUnknownTransactionFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("failure")
	success := reconcileRow("indeterminate", "eligible")
	tests := []struct {
		name string
		tx   *fakeTx
		want error
	}{
		{"forbidden", &fakeTx{rows: []pgx.Row{scriptedRow{err: pgx.ErrNoRows}}}, sequencer.ErrReconcileForbidden},
		{"scan", &fakeTx{rows: []pgx.Row{scriptedRow{err: cause}}}, cause},
		{"from state", &fakeTx{rows: []pgx.Row{reconcileRow("invalid", "eligible")}}, cause},
		{"to state", &fakeTx{rows: []pgx.Row{reconcileRow("indeterminate", "invalid")}}, cause},
		{"audit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := newStore(&fakeDatabase{tx: test.tx}).ResolveUnknown(context.Background(), validReconcileRequest())
			if test.name == "from state" || test.name == "to state" {
				if err == nil {
					t.Fatal("ResolveUnknown() error = nil")
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("ResolveUnknown() error = %v", err)
			}
		})
	}
	if err := newStore(&fakeDatabase{beginErr: cause}).ResolveUnknown(context.Background(), validReconcileRequest()); !errors.Is(err, cause) {
		t.Fatalf("ResolveUnknown(begin) error = %v", err)
	}
	exact := validReconcileRequest()
	exact.Actor = strings.Repeat("a", sequencer.DefaultMaxActorBytes)
	exact.Reason = strings.Repeat("r", sequencer.DefaultMaxReasonBytes)
	exact.Fencing = math.MaxInt64
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{success}}}).ResolveUnknown(context.Background(), exact); err != nil {
		t.Fatalf("ResolveUnknown(exact bounds) error = %v", err)
	}
	invalid := validReconcileRequest()
	invalid.Attempt = 0
	if err := newStore(&fakeDatabase{}).ResolveUnknown(context.Background(), invalid); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("ResolveUnknown(invalid) error = %v", err)
	}
	invalidID := validReconcileRequest()
	invalidID.OperationID = "Invalid"
	if err := newStore(&fakeDatabase{beginErr: cause}).ResolveUnknown(context.Background(), invalidID); !errors.Is(err, sequencer.ErrReconcileForbidden) {
		t.Fatalf("ResolveUnknown(invalid operation ID) error = %v", err)
	}
	for _, request := range []sequencer.ReconcileRequest{
		func() sequencer.ReconcileRequest {
			value := validReconcileRequest()
			value.Version = ^uint(0)
			return value
		}(),
		func() sequencer.ReconcileRequest {
			value := validReconcileRequest()
			value.Attempt = ^uint(0)
			return value
		}(),
		func() sequencer.ReconcileRequest {
			value := validReconcileRequest()
			value.Fencing = math.MaxUint64
			return value
		}(),
	} {
		if err := newStore(&fakeDatabase{}).ResolveUnknown(context.Background(), request); !errors.Is(err, sequencer.ErrReconcileForbidden) {
			t.Fatalf("ResolveUnknown(overflow) error = %v", err)
		}
	}
}

func TestSmallPostgresHelpers(t *testing.T) {
	t.Parallel()

	if got := firstNonEmpty("", ""); got != "" {
		t.Fatalf("firstNonEmpty() = %q", got)
	}
	exactDependencies := []byte(`[]` + strings.Repeat(" ", maxPersistedDefinitionBytes-2))
	if _, err := decodeDependencyRefs(exactDependencies); err != nil {
		t.Fatalf("decodeDependencyRefs(exact bound) error = %v", err)
	}
	exactReference := encodeDependencyRef(&sequencer.DependencyRef{ID: "a", Version: 1, Checksum: "sum"})
	exactReference = append(exactReference, []byte(strings.Repeat(" ", maxPersistedReferenceBytes-len(exactReference)))...)
	if _, err := decodeDependencyRef(exactReference); err != nil {
		t.Fatalf("decodeDependencyRef(exact bound) error = %v", err)
	}
	overflowChecksum := strings.Repeat("x", sequencer.DefaultMaxChecksumBytes+1)
	if _, err := decodeDependencyRefs(encodeDependencyRefs([]sequencer.DependencyRef{{
		ID: "a", Version: 1, Checksum: overflowChecksum,
	}})); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("decodeDependencyRefs(checksum overflow) error = %v", err)
	}
	overflowReference := sequencer.DependencyRef{ID: "a", Version: 1, Checksum: overflowChecksum}
	if _, err := decodeDependencyRef(encodeDependencyRef(&overflowReference)); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("decodeDependencyRef(checksum overflow) error = %v", err)
	}
	if _, err := decodeDependencyRefs([]byte("null")); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("decodeDependencyRefs(null) error = %v", err)
	}
	unsorted := []sequencer.DependencyRef{
		{ID: "b", Version: 1, Checksum: "b"},
		{ID: "a", Version: 1, Checksum: "a"},
	}
	if _, err := decodeDependencyRefs(encodeDependencyRefs(unsorted)); !errors.Is(err, sequencer.ErrDefinitionDrift) {
		t.Fatalf("decodeDependencyRefs(unsorted) error = %v", err)
	}
	tooMany := make([]sequencer.DependencyRef, sequencer.DefaultMaxDependencies+1)
	if _, err := canonicalDependencyRefs(sequencer.Registration{DependencyRefs: tooMany}); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("canonicalDependencyRefs(dependency overflow) error = %v", err)
	}
	for name, registration := range map[string]sequencer.Registration{
		"invalid id": {
			DependencyRefs: []sequencer.DependencyRef{{ID: "Invalid", Version: 1, Checksum: "sum"}},
		},
		"self dependency": {
			ID: "a", DependencyRefs: []sequencer.DependencyRef{{ID: "a", Version: 1, Checksum: "sum"}},
		},
		"missing version": {
			DependencyRefs: []sequencer.DependencyRef{{ID: "a", Checksum: "sum"}},
		},
		"missing checksum": {
			DependencyRefs: []sequencer.DependencyRef{{ID: "a", Version: 1}},
		},
		"duplicate id": {
			DependencyRefs: []sequencer.DependencyRef{
				{ID: "a", Version: 1, Checksum: "one"},
				{ID: "a", Version: 2, Checksum: "two"},
			},
		},
	} {
		if _, err := canonicalDependencyRefs(registration); !errors.Is(err, sequencer.ErrInvalidOperation) {
			t.Errorf("canonicalDependencyRefs(%s) error = %v", name, err)
		}
	}
	if got := firstNonEmpty("", "second"); got != "second" {
		t.Fatalf("firstNonEmpty(second) = %q", got)
	}
	if got := firstNonEmpty("first", "second"); got != "first" {
		t.Fatalf("firstNonEmpty(first) = %q", got)
	}
	if got, err := parseState(sequencer.Blocked.String()); err != nil || got != sequencer.Blocked {
		t.Fatalf("parseState(blocked) = %s, %v", got, err)
	}
	reference := &sequencer.DependencyRef{ID: "a", Version: 1, Checksum: "sum"}
	equal := *reference
	different := &sequencer.DependencyRef{ID: "b", Version: 1, Checksum: "sum"}
	for _, test := range []struct {
		name        string
		left, right *sequencer.DependencyRef
		want        bool
	}{
		{"both nil", nil, nil, true},
		{"left nil", nil, reference, false},
		{"right nil", reference, nil, false},
		{"equal", reference, &equal, true},
		{"different", reference, different, false},
	} {
		if got := equalDependencyRef(test.left, test.right); got != test.want {
			t.Errorf("equalDependencyRef(%s) = %t, want %t", test.name, got, test.want)
		}
	}
}

func validClaimRequest() sequencer.ClaimRequest {
	return sequencer.ClaimRequest{OperationIDs: []sequencer.OperationID{"a"}, Owner: "owner", LeaseDuration: time.Minute}
}

func validOwnership() sequencer.Ownership {
	return sequencer.Ownership{OperationID: "a", Version: 1, Owner: "owner", Fencing: 1}
}

func validCompletion() sequencer.Completion {
	return sequencer.Completion{Ownership: validOwnership(), State: sequencer.Succeeded}
}

func validReconcileRequest() sequencer.ReconcileRequest {
	return sequencer.ReconcileRequest{
		OperationID: "a", Version: 1, Attempt: 1, Fencing: 1,
		Resolution: sequencer.ReconcileRetry, Actor: "operator", Reason: "verified", At: time.Now(),
	}
}

func claimRow() pgx.Row {
	now := time.Now()
	return scriptedRow{values: []any{"claimed", "a", int64(1), int64(1), int64(1), now, now.Add(time.Minute), "eligible", int64(1), int64(0)}}
}

func checksumDriftRow() pgx.Row {
	return scriptedRow{values: []any{"checksum_drift", "a", int64(1), int64(0), int64(0), time.Time{}, time.Time{}, "eligible", int64(0), int64(0)}}
}

func registrationRow(checksum string, dependencies []string, refs []sequencer.DependencyRef) pgx.Row {
	var encoded []byte
	if refs != nil {
		encoded = encodeDependencyRefs(refs)
	}
	return scriptedRow{values: []any{checksum, stringPointer(""), dependencies, encoded, []byte(nil), int16(0), false, time.Now()}}
}

func registrationRawRow(checksum string, dependencies []string, refs, compensation []byte, unknown int16, deadLetter bool) pgx.Row {
	return scriptedRow{values: []any{checksum, stringPointer(""), dependencies, refs, compensation, unknown, deadLetter, time.Now()}}
}

func stringPointer(value string) *string { return &value }

func runningRow() pgx.Row {
	return scriptedRow{values: []any{"a", int64(1), int64(1), "owner", int64(1), time.Now()}}
}

func completionRow() pgx.Row {
	return scriptedRow{values: []any{int64(1), int64(1), time.Now()}}
}

func resetRow(state string) pgx.Row {
	return scriptedRow{values: []any{state, int64(1), int64(1), time.Now()}}
}

func reconcileRow(from, to string) pgx.Row {
	return scriptedRow{values: []any{from, to, int64(1), int64(1), time.Now()}}
}

func recoveryRow(candidates, attempts, projections, unknownAudits, replayable, replayAudits int64) pgx.Row {
	return scriptedRow{values: []any{candidates, attempts, projections, unknownAudits, replayable, replayAudits}}
}

func replace(values []any, index int, value any) []any {
	result := append([]any(nil), values...)
	result[index] = value
	return result
}

type fakeDatabase struct {
	tx       pgx.Tx
	beginErr error
	rows     pgx.Rows
	queryErr error
	row      pgx.Row
}

func (database *fakeDatabase) Begin(context.Context) (pgx.Tx, error) {
	return database.tx, database.beginErr
}
func (database *fakeDatabase) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return database.rows, database.queryErr
}
func (database *fakeDatabase) QueryRow(context.Context, string, ...any) pgx.Row { return database.row }

type scriptedRow struct {
	values []any
	err    error
}

func (row scriptedRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	return assign(destinations, row.values)
}

type fakeRows struct {
	values  [][]any
	index   int
	scanErr error
	err     error
}

func (rows *fakeRows) Close()                                       {}
func (rows *fakeRows) Err() error                                   { return rows.err }
func (rows *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (rows *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (rows *fakeRows) Next() bool {
	if rows.index >= len(rows.values) {
		return false
	}
	rows.index++
	return rows.index <= len(rows.values)
}
func (rows *fakeRows) Scan(destinations ...any) error {
	if rows.scanErr != nil {
		return rows.scanErr
	}
	return assign(destinations, rows.values[rows.index-1])
}
func (rows *fakeRows) Values() ([]any, error) { return rows.values[rows.index-1], nil }
func (rows *fakeRows) RawValues() [][]byte    { return nil }
func (rows *fakeRows) Conn() *pgx.Conn        { return nil }

type fakeTx struct {
	rows                []pgx.Row
	execErrs            []error
	execTags            []pgconn.CommandTag
	commitErr           error
	rollbackHasDeadline bool
	rollbackDeadline    time.Time
}

func (tx *fakeTx) Begin(context.Context) (pgx.Tx, error) { return tx, nil }
func (tx *fakeTx) Commit(context.Context) error          { return tx.commitErr }
func (tx *fakeTx) Rollback(ctx context.Context) error {
	tx.rollbackDeadline, tx.rollbackHasDeadline = ctx.Deadline()
	return nil
}
func (tx *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (tx *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx *fakeTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (tx *fakeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tag := pgconn.CommandTag{}
	if len(tx.execTags) > 0 {
		tag = tx.execTags[0]
		tx.execTags = tx.execTags[1:]
	}
	if len(tx.execErrs) == 0 {
		return tag, nil
	}
	err := tx.execErrs[0]
	tx.execErrs = tx.execErrs[1:]
	return tag, err
}
func (tx *fakeTx) Query(context.Context, string, ...any) (pgx.Rows, error) { return nil, nil }
func (tx *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row {
	row := tx.rows[0]
	tx.rows = tx.rows[1:]
	return row
}
func (tx *fakeTx) Conn() *pgx.Conn { return nil }

func assign(destinations, values []any) error {
	if len(destinations) != len(values) {
		return errors.New("scan arity")
	}
	for index, destination := range destinations {
		target := reflect.ValueOf(destination).Elem()
		value := reflect.ValueOf(values[index])
		switch {
		case value.Type().AssignableTo(target.Type()):
			target.Set(value)
		case value.Type().ConvertibleTo(target.Type()):
			target.Set(value.Convert(target.Type()))
		default:
			return errors.New("scan type")
		}
	}
	return nil
}
