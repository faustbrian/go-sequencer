package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrPoolRequired reports a missing PostgreSQL connection pool.
var ErrPoolRequired = errors.New("sequencer/postgres: pool is required")

var errInvalidLedgerInteger = errors.New("sequencer/postgres: invalid ledger integer")

// Store persists operation projections, attempts, and audit events.
type Store struct{ database database }

type database interface {
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// New constructs a PostgreSQL store. Schema installation remains owned by the
// application's migration process through Migrations.
func New(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, ErrPoolRequired
	}
	return newStore(pool), nil
}

func newStore(database database) *Store { return &Store{database: database} }

// Register inserts immutable identities and fails closed on checksum drift.
func (store *Store) Register(ctx context.Context, registrations []sequencer.Registration, _ time.Time) error {
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	for _, registration := range registrations {
		dependencies := make([]string, len(registration.Dependencies))
		for index, dependency := range registration.Dependencies {
			dependencies[index] = string(dependency)
		}
		if _, err = tx.Exec(ctx, `
INSERT INTO sequencer_operations (
    operation_id, version, checksum, dependencies, state, eligible_at,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, 'eligible', clock_timestamp(),
          clock_timestamp(), clock_timestamp())
ON CONFLICT (operation_id, version) DO NOTHING`,
			registration.ID, registration.Version, registration.Checksum, dependencies,
		); err != nil {
			return err
		}
		var checksum string
		if err = tx.QueryRow(ctx, `
SELECT checksum FROM sequencer_operations
WHERE operation_id = $1 AND version = $2`, registration.ID, registration.Version).Scan(&checksum); err != nil {
			return err
		}
		if checksum != registration.Checksum {
			return fmt.Errorf("%w: %s version %d", sequencer.ErrChecksumDrift, registration.ID, registration.Version)
		}
	}
	return tx.Commit(ctx)
}

// ClaimNext transactionally claims the first dependency-ready plan candidate.
func (store *Store) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	if request.Owner == "" || request.LeaseDuration <= 0 || len(request.OperationIDs) == 0 {
		return sequencer.Claim{}, sequencer.ErrInvalidOperation
	}
	ids := make([]string, len(request.OperationIDs))
	for index, id := range request.OperationIDs {
		ids[index] = string(id)
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return sequencer.Claim{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var claim sequencer.Claim
	var version, number, fencing int64
	err = tx.QueryRow(ctx, `
WITH candidate AS (
    SELECT operation.operation_id, operation.version
    FROM unnest($1::text[]) WITH ORDINALITY requested(operation_id, ordinal)
    JOIN LATERAL (
        SELECT * FROM sequencer_operations
        WHERE operation_id = requested.operation_id
        ORDER BY version DESC LIMIT 1
    ) operation ON true
    WHERE operation.state IN ('eligible', 'retryable', 'deferred')
      AND operation.eligible_at <= clock_timestamp()
      AND NOT EXISTS (
          SELECT 1 FROM unnest(operation.dependencies) dependency(operation_id)
          WHERE NOT EXISTS (
              SELECT 1 FROM LATERAL (
                  SELECT state FROM sequencer_operations
                  WHERE operation_id = dependency.operation_id
                  ORDER BY version DESC LIMIT 1
              ) dependency_state
              WHERE dependency_state.state IN ('succeeded', 'skipped')
          )
      )
    ORDER BY requested.ordinal
    FOR UPDATE OF operation SKIP LOCKED
    LIMIT 1
), claimed AS (
    UPDATE sequencer_operations operation SET
        state = 'claimed', owner = $2,
        fencing_token = operation.fencing_token + 1,
        attempt_number = operation.attempt_number + 1,
        lease_expires_at = clock_timestamp() + ($3 * interval '1 millisecond'),
        updated_at = clock_timestamp()
    FROM candidate
    WHERE operation.operation_id = candidate.operation_id
      AND operation.version = candidate.version
    RETURNING operation.operation_id, operation.version,
              operation.attempt_number, operation.fencing_token,
              operation.updated_at, operation.lease_expires_at
)
SELECT * FROM claimed`, ids, request.Owner, request.LeaseDuration.Milliseconds()).Scan(
		&claim.Attempt.OperationID, &version, &number, &fencing,
		&claim.Attempt.StartedAt, &claim.Until,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.Claim{}, sequencer.ErrNoEligibleOperation
	}
	if err != nil {
		return sequencer.Claim{}, err
	}
	if claim.Attempt.Version, err = toUint(version); err != nil {
		return sequencer.Claim{}, err
	}
	if claim.Attempt.Number, err = toUint(number); err != nil {
		return sequencer.Claim{}, err
	}
	claim.Attempt.Owner = request.Owner
	if claim.Attempt.Fencing, err = toUint64(fencing); err != nil {
		return sequencer.Claim{}, err
	}
	if _, err = tx.Exec(ctx, `
INSERT INTO sequencer_attempts (
    operation_id, version, attempt_number, owner, fencing_token, state,
    started_at
) VALUES ($1, $2, $3, $4, $5, 'claimed', $6)`,
		claim.Attempt.OperationID, version, number, request.Owner, fencing,
		claim.Attempt.StartedAt,
	); err != nil {
		return sequencer.Claim{}, err
	}
	if err = insertAudit(ctx, tx, claim.Attempt.OperationID, version, number,
		sequencer.Eligible, sequencer.Claimed, claim.Attempt.StartedAt,
		request.Owner, fencing, request.Owner, "claimed"); err != nil {
		return sequencer.Claim{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return sequencer.Claim{}, sequencer.UnknownResult(err)
	}
	return claim, nil
}

// MarkRunning records the start boundary under the current fencing proof.
func (store *Store) MarkRunning(ctx context.Context, ownership sequencer.Ownership, _ time.Time) (sequencer.AttemptRecord, error) {
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return sequencer.AttemptRecord{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var record sequencer.AttemptRecord
	var version, number, fencing int64
	err = tx.QueryRow(ctx, `
UPDATE sequencer_operations SET state = 'running', updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND owner = $3
  AND fencing_token = $4 AND state = 'claimed'
  AND lease_expires_at > clock_timestamp()
RETURNING operation_id, version, attempt_number, owner, fencing_token,
          updated_at`, ownership.OperationID, ownership.Version,
		ownership.Owner, ownership.Fencing).Scan(
		&record.OperationID, &version, &number, &record.Owner, &fencing,
		&record.StartedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.AttemptRecord{}, sequencer.ErrStaleOwner
	}
	if err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if record.Version, err = toUint(version); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if record.Number, err = toUint(number); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if record.Fencing, err = toUint64(fencing); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	record.State = sequencer.Running
	if _, err = tx.Exec(ctx, `
UPDATE sequencer_attempts SET state = 'running'
WHERE operation_id = $1 AND version = $2 AND attempt_number = $3`,
		ownership.OperationID, version, number); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if err = insertAudit(ctx, tx, ownership.OperationID, version, number,
		sequencer.Claimed, sequencer.Running, record.StartedAt,
		ownership.Owner, fencing, ownership.Owner, "started"); err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return sequencer.AttemptRecord{}, sequencer.UnknownResult(err)
	}
	return record, nil
}

// Complete atomically persists the attempt outcome and current projection.
func (store *Store) Complete(ctx context.Context, completion sequencer.Completion) error {
	if err := sequencer.ValidateTransition(sequencer.Running, completion.State); err != nil {
		return err
	}
	output, err := json.Marshal(completion.Output)
	if err != nil || len(output) > sequencer.DefaultMaxOutputBytes {
		return sequencer.ErrResourceLimit
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var number, fencing int64
	var completedAt time.Time
	err = tx.QueryRow(ctx, `
UPDATE sequencer_operations SET
    state = $5, owner = NULL, lease_expires_at = NULL,
    eligible_at = CASE WHEN $5 IN ('retryable', 'deferred') THEN $6 ELSE eligible_at END,
    updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND owner = $3
  AND fencing_token = $4 AND state = 'running'
  AND lease_expires_at > clock_timestamp()
RETURNING attempt_number, fencing_token, updated_at`,
		completion.OperationID, completion.Version, completion.Owner,
		completion.Fencing, completion.State.String(), completion.EligibleAt,
	).Scan(&number, &fencing, &completedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.ErrStaleOwner
	}
	if err != nil {
		return err
	}
	detail := sequencer.SanitizePersistenceText(completion.ErrorDetail, sequencer.DefaultMaxErrorBytes)
	if _, err = tx.Exec(ctx, `
UPDATE sequencer_attempts SET state = $4, completed_at = $5,
    error_detail = NULLIF($6, ''), output = $7
WHERE operation_id = $1 AND version = $2 AND attempt_number = $3`,
		completion.OperationID, completion.Version, number,
		completion.State.String(), completedAt, detail, output); err != nil {
		return err
	}
	version, err := toInt64(completion.Version)
	if err != nil {
		return err
	}
	if err = insertAudit(ctx, tx, completion.OperationID, version,
		number, sequencer.Running, completion.State, completedAt,
		completion.Owner, fencing, firstNonEmpty(completion.Actor, completion.Owner),
		firstNonEmpty(completion.Reason, "completed")); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return sequencer.UnknownResult(err)
	}
	return nil
}

// RecoverExpired makes expired unknown-result attempts explicitly retryable.
func (store *Store) RecoverExpired(ctx context.Context, _ time.Time) (int, error) {
	var count int
	err := store.database.QueryRow(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT operation_id, version, attempt_number, fencing_token, state
    FROM sequencer_operations
    WHERE state IN ('claimed', 'running')
      AND lease_expires_at <= clock_timestamp()
    FOR UPDATE SKIP LOCKED
), expired AS (
    UPDATE sequencer_operations operation SET
        state = 'eligible', owner = NULL, lease_expires_at = NULL,
        eligible_at = clock_timestamp(), updated_at = clock_timestamp()
    FROM candidates
    WHERE operation.operation_id = candidates.operation_id
      AND operation.version = candidates.version
    RETURNING operation.operation_id, operation.version,
              operation.attempt_number, operation.fencing_token,
              operation.updated_at, candidates.state AS from_state
), attempts AS (
    UPDATE sequencer_attempts attempt SET
        state = 'retryable', completed_at = expired.updated_at,
        error_detail = 'sequencer: unknown result'
    FROM expired
    WHERE attempt.operation_id = expired.operation_id
      AND attempt.version = expired.version
      AND attempt.attempt_number = expired.attempt_number
    RETURNING expired.operation_id, expired.version, expired.attempt_number,
              expired.fencing_token, expired.updated_at, expired.from_state
), retry_events AS (
    INSERT INTO sequencer_audit_events (
        operation_id, version, attempt_number, from_state, to_state,
        occurred_at, fencing_token, actor, reason
    ) SELECT operation_id, version, attempt_number, from_state, 'retryable',
             updated_at, fencing_token, 'system',
             'lease expired; outcome unknown'
      FROM attempts
    RETURNING operation_id, version, attempt_number, occurred_at, fencing_token
), eligible_events AS (
    INSERT INTO sequencer_audit_events (
        operation_id, version, attempt_number, from_state, to_state,
        occurred_at, fencing_token, actor, reason
    ) SELECT operation_id, version, attempt_number, 'retryable', 'eligible',
             occurred_at, fencing_token, 'system', 'recovered'
      FROM retry_events
    RETURNING operation_id
)
SELECT count(*) FROM eligible_events`).Scan(&count)
	return count, err
}

// Snapshot returns one current projection without payload history.
func (store *Store) Snapshot(ctx context.Context, id sequencer.OperationID, version uint) (sequencer.Record, error) {
	var record sequencer.Record
	var state string
	var storedVersion, attempt, fencing int64
	err := store.database.QueryRow(ctx, `
SELECT operation_id, version, checksum, dependencies, state,
       attempt_number, COALESCE(owner, ''), fencing_token,
       COALESCE(lease_expires_at, 'epoch'), eligible_at, updated_at
FROM sequencer_operations WHERE operation_id = $1 AND version = $2`, id, version).Scan(
		&record.ID, &storedVersion, &record.Checksum, &record.Dependencies,
		&state, &attempt, &record.Owner, &fencing, &record.LeaseExpiresAt,
		&record.EligibleAt, &record.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.Record{}, sequencer.ErrNotFound
	}
	if err != nil {
		return sequencer.Record{}, err
	}
	if record.Version, err = toUint(storedVersion); err != nil {
		return sequencer.Record{}, err
	}
	if record.AttemptNumber, err = toUint(attempt); err != nil {
		return sequencer.Record{}, err
	}
	if record.Fencing, err = toUint64(fencing); err != nil {
		return sequencer.Record{}, err
	}
	record.State, err = parseState(state)
	return record, err
}

// History returns a bounded attempt history in attempt order.
func (store *Store) History(ctx context.Context, id sequencer.OperationID, version uint, limit int) ([]sequencer.AttemptRecord, error) {
	if limit < 1 || limit > sequencer.DefaultMaxHistory {
		return nil, sequencer.ErrResourceLimit
	}
	rows, err := store.database.Query(ctx, `
SELECT operation_id, version, attempt_number, owner, fencing_token, state,
       started_at, COALESCE(completed_at, 'epoch'), COALESCE(error_detail, ''),
       COALESCE(output, '{}'::jsonb)
FROM sequencer_attempts WHERE operation_id = $1 AND version = $2
ORDER BY attempt_number DESC LIMIT $3`, id, version, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var history []sequencer.AttemptRecord
	for rows.Next() {
		var record sequencer.AttemptRecord
		var storedVersion, attempt, fencing int64
		var state string
		var output []byte
		if err = rows.Scan(&record.OperationID, &storedVersion, &attempt,
			&record.Owner, &fencing, &state, &record.StartedAt,
			&record.CompletedAt, &record.ErrorDetail, &output); err != nil {
			return nil, err
		}
		if record.Version, err = toUint(storedVersion); err != nil {
			return nil, err
		}
		if record.Number, err = toUint(attempt); err != nil {
			return nil, err
		}
		if record.Fencing, err = toUint64(fencing); err != nil {
			return nil, err
		}
		if record.State, err = parseState(state); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(output, &record.Output); err != nil {
			return nil, err
		}
		history = append(history, record)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	slices.Reverse(history)
	return history, nil
}

// Audit returns bounded append-only state and administration events.
func (store *Store) Audit(ctx context.Context, id sequencer.OperationID, version uint, limit int) ([]sequencer.AuditEvent, error) {
	if limit < 1 || limit > sequencer.DefaultMaxHistory {
		return nil, sequencer.ErrResourceLimit
	}
	rows, err := store.database.Query(ctx, `
SELECT operation_id, version, attempt_number, from_state, to_state,
       occurred_at, COALESCE(owner, ''), fencing_token,
       COALESCE(actor, ''), reason
FROM sequencer_audit_events WHERE operation_id = $1 AND version = $2
ORDER BY event_id DESC LIMIT $3`, id, version, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var audit []sequencer.AuditEvent
	for rows.Next() {
		var event sequencer.AuditEvent
		var versionNumber, attempt, fencing int64
		var from, to string
		if err = rows.Scan(&event.OperationID, &versionNumber, &attempt,
			&from, &to, &event.At, &event.Owner, &fencing,
			&event.Actor, &event.Reason); err != nil {
			return nil, err
		}
		if event.Version, err = toUint(versionNumber); err != nil {
			return nil, err
		}
		if event.Attempt, err = toUint(attempt); err != nil {
			return nil, err
		}
		if event.Fencing, err = toUint64(fencing); err != nil {
			return nil, err
		}
		if event.From, err = parseState(from); err != nil {
			return nil, err
		}
		if event.To, err = parseState(to); err != nil {
			return nil, err
		}
		audit = append(audit, event)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	slices.Reverse(audit)
	return audit, nil
}

// Reset performs an explicit attributable replay authorization.
func (store *Store) Reset(ctx context.Context, request sequencer.ResetRequest) error {
	if request.Actor == "" || request.Reason == "" {
		return sequencer.ErrResetForbidden
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	var from string
	var attempt, fencing int64
	var at time.Time
	err = tx.QueryRow(ctx, `
WITH current AS MATERIALIZED (
    SELECT state FROM sequencer_operations
    WHERE operation_id = $1 AND version = $2
      AND state IN ('succeeded', 'failed', 'blocked')
    FOR UPDATE
), updated AS (
    UPDATE sequencer_operations operation SET
        state = 'eligible', eligible_at = clock_timestamp(),
        owner = NULL, lease_expires_at = NULL,
        updated_at = clock_timestamp()
    FROM current
    WHERE operation.operation_id = $1 AND operation.version = $2
    RETURNING current.state AS from_state, operation.attempt_number,
              operation.fencing_token, operation.updated_at
)
SELECT from_state, attempt_number, fencing_token, updated_at FROM updated`,
		request.OperationID, request.Version).Scan(&from, &attempt, &fencing, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.ErrResetForbidden
	}
	if err != nil {
		return err
	}
	fromState, err := parseState(from)
	if err != nil {
		return err
	}
	version, err := toInt64(request.Version)
	if err != nil {
		return err
	}
	if err = insertAudit(ctx, tx, request.OperationID, version,
		attempt, fromState, sequencer.Eligible, at, "", fencing,
		request.Actor, request.Reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func insertAudit(ctx context.Context, tx pgx.Tx, id sequencer.OperationID,
	version, attempt int64, from, to sequencer.State, at time.Time,
	owner string, fencing int64, actor, reason string,
) error {
	_, err := tx.Exec(ctx, `
INSERT INTO sequencer_audit_events (
    operation_id, version, attempt_number, from_state, to_state,
    occurred_at, owner, fencing_token, actor, reason
) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, NULLIF($9, ''), $10)`,
		id, version, attempt, from.String(), to.String(), at, owner, fencing,
		actor, reason)
	return err
}

func parseState(value string) (sequencer.State, error) {
	for state := sequencer.Pending; state <= sequencer.Blocked; state++ {
		if state.String() == value {
			return state, nil
		}
	}
	return 0, fmt.Errorf("sequencer/postgres: unknown state %q", value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func toUint(value int64) (uint, error) {
	if value < 0 || uint64(value) > uint64(^uint(0)) {
		return 0, fmt.Errorf("%w: %d", errInvalidLedgerInteger, value)
	}
	return uint(value), nil
}

func toUint64(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: %d", errInvalidLedgerInteger, value)
	}
	return uint64(value), nil
}

func toInt64(value uint) (int64, error) {
	if uint64(value) > math.MaxInt64 {
		return 0, fmt.Errorf("%w: %d exceeds int64", errInvalidLedgerInteger, value)
	}
	return int64(value), nil
}

var _ sequencer.Store = (*Store)(nil)
