package postgres

import (
	"context"
	"errors"
	"math"
	"reflect"
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
	if _, err := queries.RecoverExpired(ctx, time.Now()); !errors.Is(err, cause) {
		t.Errorf("RecoverExpired() error = %v", err)
	}
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
		{"drift", &fakeTx{rows: []pgx.Row{scriptedRow{values: []any{"other"}}}}, sequencer.ErrChecksumDrift},
		{"commit", &fakeTx{rows: []pgx.Row{scriptedRow{values: []any{"sum"}}}, commitErr: cause}, cause},
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
		{"negative version", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 1, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative attempt", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 2, int64(-1))}}}, errInvalidLedgerInteger},
		{"negative fencing", &fakeTx{rows: []pgx.Row{scriptedRow{values: replace(base, 3, int64(-1))}}}, errInvalidLedgerInteger},
		{"attempt insert", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"audit insert", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, cause}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, nil}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(&fakeDatabase{tx: test.tx})
			if _, err := store.ClaimNext(context.Background(), validClaimRequest()); !errors.Is(err, test.want) {
				t.Fatalf("ClaimNext() error = %v", err)
			}
		})
	}
	store := newStore(&fakeDatabase{})
	if _, err := store.ClaimNext(context.Background(), sequencer.ClaimRequest{}); !errors.Is(err, sequencer.ErrInvalidOperation) {
		t.Fatalf("ClaimNext(invalid) error = %v", err)
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
		{"attempt update", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"audit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, cause}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, nil}, commitErr: cause}, sequencer.ErrUnknownResult},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newStore(&fakeDatabase{tx: test.tx})
			if _, err := store.MarkRunning(context.Background(), validOwnership(), time.Now()); !errors.Is(err, test.want) {
				t.Fatalf("MarkRunning() error = %v", err)
			}
		})
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
		{"attempt update", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{cause}}, cause},
		{"audit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, cause}}, cause},
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil, nil}, commitErr: cause}, sequencer.ErrUnknownResult},
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
	large := validCompletion()
	large.Output.Summary = string(make([]byte, sequencer.DefaultMaxOutputBytes+1))
	if err := store.Complete(context.Background(), large); !errors.Is(err, sequencer.ErrResourceLimit) {
		t.Fatalf("Complete(output) error = %v", err)
	}
	overflow := validCompletion()
	overflow.Version = ^uint(0)
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{completionRow()}, execErrs: []error{nil}}}).Complete(context.Background(), overflow); !errors.Is(err, errInvalidLedgerInteger) {
		t.Fatalf("Complete(version) error = %v", err)
	}
}

func TestStoreReadDecodingFailures(t *testing.T) {
	t.Parallel()

	cause := errors.New("scan")
	ctx := context.Background()
	if _, err := newStore(&fakeDatabase{row: scriptedRow{err: pgx.ErrNoRows}}).Snapshot(ctx, "a", 1); !errors.Is(err, sequencer.ErrNotFound) {
		t.Fatalf("Snapshot(missing) error = %v", err)
	}
	invalidSnapshot := []any{"a", int64(1), "sum", []sequencer.OperationID{}, "invalid", int64(0), "", int64(0), time.Time{}, time.Now(), time.Now()}
	if _, err := newStore(&fakeDatabase{row: scriptedRow{values: invalidSnapshot}}).Snapshot(ctx, "a", 1); err == nil {
		t.Fatal("Snapshot(invalid state) error = nil")
	}
	for _, index := range []int{1, 5, 7} {
		if _, err := newStore(&fakeDatabase{row: scriptedRow{values: replace(invalidSnapshot, index, int64(-1))}}).Snapshot(ctx, "a", 1); !errors.Is(err, errInvalidLedgerInteger) {
			t.Fatalf("Snapshot(integer %d) error = %v", index, err)
		}
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
		{"rows", &fakeRows{err: cause}},
	}
	for _, test := range historyCases {
		t.Run("history "+test.name, func(t *testing.T) {
			if _, err := newStore(&fakeDatabase{rows: test.rows}).History(ctx, "a", 1, 1); err == nil {
				t.Fatal("History() error = nil")
			}
		})
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
		{"commit", &fakeTx{rows: []pgx.Row{success}, execErrs: []error{nil}, commitErr: cause}, cause},
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
	if err := newStore(&fakeDatabase{}).Reset(context.Background(), sequencer.ResetRequest{}); !errors.Is(err, sequencer.ErrResetForbidden) {
		t.Fatalf("Reset(invalid) error = %v", err)
	}
	overflow := sequencer.ResetRequest{OperationID: "a", Version: ^uint(0), Actor: "op", Reason: "why"}
	if err := newStore(&fakeDatabase{tx: &fakeTx{rows: []pgx.Row{success}}}).Reset(context.Background(), overflow); !errors.Is(err, errInvalidLedgerInteger) {
		t.Fatalf("Reset(version) error = %v", err)
	}
}

func TestSmallPostgresHelpers(t *testing.T) {
	t.Parallel()

	if got := firstNonEmpty("", ""); got != "" {
		t.Fatalf("firstNonEmpty() = %q", got)
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

func claimRow() pgx.Row {
	now := time.Now()
	return scriptedRow{values: []any{"a", int64(1), int64(1), int64(1), now, now.Add(time.Minute)}}
}

func runningRow() pgx.Row {
	return scriptedRow{values: []any{"a", int64(1), int64(1), "owner", int64(1), time.Now()}}
}

func completionRow() pgx.Row {
	return scriptedRow{values: []any{int64(1), int64(1), time.Now()}}
}

func resetRow(state string) pgx.Row {
	return scriptedRow{values: []any{state, int64(1), int64(1), time.Now()}}
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
	rows      []pgx.Row
	execErrs  []error
	commitErr error
}

func (tx *fakeTx) Begin(context.Context) (pgx.Tx, error) { return tx, nil }
func (tx *fakeTx) Commit(context.Context) error          { return tx.commitErr }
func (tx *fakeTx) Rollback(context.Context) error        { return nil }
func (tx *fakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}
func (tx *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx *fakeTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}
func (tx *fakeTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	if len(tx.execErrs) == 0 {
		return pgconn.CommandTag{}, nil
	}
	err := tx.execErrs[0]
	tx.execErrs = tx.execErrs[1:]
	return pgconn.CommandTag{}, err
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
