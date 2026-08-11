package postgres

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	sequencer "github.com/faustbrian/golib/pkg/sequencer"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrPoolRequired reports a missing PostgreSQL connection pool.
var ErrPoolRequired = errors.New("sequencer/postgres: pool is required")

const (
	maxPersistedDefinitionBytes = 64 << 10
	maxPersistedReferenceBytes  = 4 << 10
	defaultRollbackTimeout      = 5 * time.Second
)

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

func rollbackDetached(parent context.Context, tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), defaultRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func ownershipValid(ownership sequencer.Ownership) bool {
	return ownership.OperationID.Valid() &&
		len(ownership.Owner) <= sequencer.DefaultMaxActorBytes
}

// Register inserts immutable identities and fails closed on checksum drift.
func (store *Store) Register(ctx context.Context, registrations []sequencer.Registration, _ time.Time) error {
	for _, registration := range registrations {
		if !registration.ID.Valid() {
			return sequencer.ErrInvalidOperation
		}
		if registration.Channel != "" && !sequencer.OperationID(registration.Channel).Valid() {
			return sequencer.ErrInvalidOperation
		}
		if len(registration.Checksum) > sequencer.DefaultMaxChecksumBytes {
			return sequencer.ErrResourceLimit
		}
		for _, dependency := range registration.DependencyRefs {
			if !dependency.ID.Valid() {
				return sequencer.ErrInvalidOperation
			}
			if len(dependency.Checksum) > sequencer.DefaultMaxChecksumBytes {
				return sequencer.ErrResourceLimit
			}
		}
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackDetached(ctx, tx)
	for _, registration := range registrations {
		dependencyRefs, err := canonicalDependencyRefs(registration)
		if err != nil {
			return err
		}
		if !registration.ID.Valid() || registration.Version == 0 || registration.Checksum == "" ||
			registration.UnknownOutcome > sequencer.UnknownOutcomeReplayIdempotent ||
			(registration.Compensates != nil && !slices.Contains(dependencyRefs, *registration.Compensates)) {
			return sequencer.ErrInvalidOperation
		}
		dependencies := make([]string, len(dependencyRefs))
		for index, dependency := range dependencyRefs {
			dependencies[index] = string(dependency.ID)
		}
		encodedDependencyRefs := encodeDependencyRefs(dependencyRefs)
		encodedCompensates := encodeDependencyRef(registration.Compensates)
		if len(encodedDependencyRefs) > maxPersistedDefinitionBytes {
			return sequencer.ErrResourceLimit
		}
		result, execErr := tx.Exec(ctx, `
INSERT INTO sequencer_operations (
    operation_id, version, checksum, channel, dependencies, dependency_refs,
    compensates, unknown_outcome, dead_letter, state, eligible_at,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9,
          'eligible', clock_timestamp(),
          clock_timestamp(), clock_timestamp())
ON CONFLICT (operation_id, version) DO NOTHING`,
			registration.ID, registration.Version, registration.Checksum, registration.Channel, dependencies,
			encodedDependencyRefs, encodedCompensates, int16(registration.UnknownOutcome), registration.DeadLetter,
		)
		if execErr != nil {
			return execErr
		}
		var checksum string
		var storedChannel *string
		var storedDependencies []string
		var storedDependencyRefs []byte
		var storedCompensates []byte
		var storedUnknownOutcome int16
		var storedDeadLetter bool
		var registeredAt time.Time
		if err = tx.QueryRow(ctx, `
SELECT checksum, channel, dependencies, dependency_refs, compensates,
       unknown_outcome, dead_letter, updated_at
FROM sequencer_operations
WHERE operation_id = $1 AND version = $2
FOR UPDATE`, registration.ID, registration.Version).Scan(
			&checksum, &storedChannel, &storedDependencies, &storedDependencyRefs, &storedCompensates,
			&storedUnknownOutcome, &storedDeadLetter, &registeredAt,
		); err != nil {
			return err
		}
		if checksum != registration.Checksum {
			return fmt.Errorf("%w: %s version %d", sequencer.ErrChecksumDrift, registration.ID, registration.Version)
		}
		if storedChannel == nil || *storedChannel != registration.Channel {
			return fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, registration.ID, registration.Version)
		}
		slices.Sort(storedDependencies)
		if !slices.Equal(storedDependencies, dependencies) {
			return fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, registration.ID, registration.Version)
		}
		if storedDependencyRefs == nil {
			if _, err = tx.Exec(ctx, `
UPDATE sequencer_operations SET dependency_refs = $3::jsonb
WHERE operation_id = $1 AND version = $2 AND dependency_refs IS NULL`,
				registration.ID, registration.Version, encodedDependencyRefs); err != nil {
				return err
			}
		} else {
			storedRefs, decodeErr := decodeDependencyRefs(storedDependencyRefs)
			if decodeErr != nil {
				return decodeErr
			}
			if !slices.Equal(storedRefs, dependencyRefs) {
				return fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, registration.ID, registration.Version)
			}
		}
		storedCompensation, decodeErr := decodeDependencyRef(storedCompensates)
		if decodeErr != nil || !equalDependencyRef(storedCompensation, registration.Compensates) ||
			storedUnknownOutcome != int16(registration.UnknownOutcome) || storedDeadLetter != registration.DeadLetter {
			return fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, registration.ID, registration.Version)
		}
		if result.RowsAffected() == 1 {
			version, conversionErr := toInt64(registration.Version)
			if conversionErr != nil {
				return conversionErr
			}
			if err = insertAudit(ctx, tx, registration.ID, version, 0,
				sequencer.Pending, sequencer.Eligible, registeredAt, "", 0,
				"system", "registered"); err != nil {
				return err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return sequencer.UnknownResult(err)
	}
	return nil
}

// ClaimNext transactionally claims the first dependency-ready plan candidate.
func (store *Store) ClaimNext(ctx context.Context, request sequencer.ClaimRequest) (sequencer.Claim, error) {
	if request.Owner == "" || len(request.Owner) > sequencer.DefaultMaxActorBytes || request.LeaseDuration <= 0 || (len(request.Candidates) == 0 && len(request.OperationIDs) == 0) {
		return sequencer.Claim{}, sequencer.ErrInvalidOperation
	}
	selectedCount := len(request.Candidates)
	if selectedCount == 0 {
		selectedCount = len(request.OperationIDs)
	}
	if selectedCount > sequencer.DefaultMaxOperations {
		return sequencer.Claim{}, sequencer.ErrResourceLimit
	}
	if request.LeaseDuration.Milliseconds() <= 0 {
		return sequencer.Claim{}, sequencer.ErrInvalidLease
	}
	candidates := request.Candidates
	if len(candidates) == 0 {
		candidates = make([]sequencer.ClaimCandidate, len(request.OperationIDs))
		for index, id := range request.OperationIDs {
			candidates[index] = sequencer.ClaimCandidate{ID: id}
		}
	}
	ids := make([]string, len(candidates))
	versions := make([]int64, len(candidates))
	checksums := make([]string, len(candidates))
	channels := make([]string, len(candidates))
	for index, candidate := range candidates {
		if !candidate.ID.Valid() {
			return sequencer.Claim{}, sequencer.ErrInvalidOperation
		}
		if len(candidate.Checksum) > sequencer.DefaultMaxChecksumBytes {
			return sequencer.Claim{}, sequencer.ErrResourceLimit
		}
		if candidate.Channel != "" && !sequencer.OperationID(candidate.Channel).Valid() {
			return sequencer.Claim{}, sequencer.ErrInvalidOperation
		}
		ids[index] = string(candidate.ID)
		var err error
		if versions[index], err = toInt64(candidate.Version); err != nil {
			return sequencer.Claim{}, err
		}
		checksums[index] = candidate.Checksum
		channels[index] = candidate.Channel
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return sequencer.Claim{}, err
	}
	defer rollbackDetached(ctx, tx)
	var claim sequencer.Claim
	var status, from string
	var version, number, fencing, runAttempt, retryExceptions int64
	err = tx.QueryRow(ctx, `
WITH requested AS MATERIALIZED (
    SELECT * FROM unnest($1::text[], $2::bigint[], $3::text[], $4::text[])
         WITH ORDINALITY input(operation_id, version, checksum, channel, ordinal)
), drift AS MATERIALIZED (
    SELECT operation.operation_id, operation.version,
           operation.checksum <> requested.checksum AS checksum_drift
    FROM requested
    JOIN sequencer_operations operation
      ON operation.operation_id = requested.operation_id
     AND operation.version = requested.version
    WHERE requested.version <> 0
	      AND ((requested.checksum <> '' AND operation.checksum <> requested.checksum)
	           OR (requested.channel <> '' AND operation.channel IS DISTINCT FROM requested.channel))
    ORDER BY requested.ordinal
    LIMIT 1
), candidate AS (
    SELECT operation.operation_id, operation.version, operation.state,
           budget_epoch.epoch_start_attempt,
           budget_usage.retry_exception_count
    FROM requested
    JOIN LATERAL (
        SELECT * FROM sequencer_operations
        WHERE operation_id = requested.operation_id
          AND (requested.version = 0 OR sequencer_operations.version = requested.version)
          AND (requested.checksum = '' OR sequencer_operations.checksum = requested.checksum)
          AND (requested.channel = '' OR sequencer_operations.channel = requested.channel)
        ORDER BY sequencer_operations.version DESC LIMIT 1
    ) operation ON true
	JOIN LATERAL (
	    SELECT COALESCE(MAX(event.attempt_number), 0) AS epoch_start_attempt
	    FROM sequencer_audit_events event
	    WHERE event.operation_id = operation.operation_id
	      AND event.version = operation.version
	      AND event.from_state IN ('succeeded', 'failed', 'blocked', 'canceled', 'dead_lettered')
	      AND event.to_state = 'eligible'
	) budget_epoch ON true
	JOIN LATERAL (
	    SELECT count(*) AS retry_exception_count
	    FROM sequencer_attempts history
	    WHERE history.operation_id = operation.operation_id
	      AND history.version = operation.version
	      AND history.attempt_number > budget_epoch.epoch_start_attempt
	      AND history.state = 'retryable'
	) budget_usage ON true
	    WHERE NOT EXISTS (SELECT 1 FROM drift)
	      AND operation.channel IS NOT NULL
      AND operation.dependency_refs IS NOT NULL
      AND operation.state IN ('eligible', 'retryable', 'deferred')
      AND operation.eligible_at <= clock_timestamp()
      AND NOT EXISTS (
          SELECT 1
          FROM jsonb_to_recordset(operation.dependency_refs)
               dependency(id text, version bigint, checksum text)
          WHERE NOT EXISTS (
              SELECT 1 FROM sequencer_operations dependency_state
              WHERE dependency_state.operation_id = dependency.id
                AND dependency_state.version = dependency.version
                AND dependency_state.checksum = dependency.checksum
                AND dependency_state.state IN ('succeeded', 'skipped')
          )
      )
    ORDER BY requested.ordinal
    FOR UPDATE OF operation SKIP LOCKED
    LIMIT 1
), claimed AS (
    UPDATE sequencer_operations operation SET
        state = 'claimed', owner = $5,
        fencing_token = operation.fencing_token + 1,
        attempt_number = operation.attempt_number + 1,
        lease_expires_at = clock_timestamp() + ($6 * interval '1 millisecond'),
        updated_at = clock_timestamp()
    FROM candidate
    WHERE operation.operation_id = candidate.operation_id
      AND operation.version = candidate.version
    RETURNING operation.operation_id, operation.version,
              operation.attempt_number, operation.fencing_token,
              operation.updated_at, operation.lease_expires_at,
              candidate.state AS from_state,
              operation.attempt_number - candidate.epoch_start_attempt AS run_attempt_number,
              candidate.retry_exception_count
)
SELECT CASE WHEN checksum_drift THEN 'checksum_drift' ELSE 'definition_drift' END,
       operation_id, version, 0::bigint, 0::bigint,
       'epoch'::timestamptz, 'epoch'::timestamptz, 'eligible',
       0::bigint, 0::bigint
FROM drift
UNION ALL
SELECT 'claimed', operation_id, version, attempt_number, fencing_token,
       updated_at, lease_expires_at, from_state, run_attempt_number,
       retry_exception_count
FROM claimed`, ids, versions, checksums, channels, request.Owner, request.LeaseDuration.Milliseconds()).Scan(
		&status, &claim.Attempt.OperationID, &version, &number, &fencing,
		&claim.Attempt.StartedAt, &claim.Until, &from, &runAttempt, &retryExceptions,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.Claim{}, sequencer.ErrNoEligibleOperation
	}
	if err != nil {
		return sequencer.Claim{}, err
	}
	if status == "checksum_drift" {
		return sequencer.Claim{}, fmt.Errorf("%w: %s version %d", sequencer.ErrChecksumDrift, claim.Attempt.OperationID, version)
	}
	if status == "definition_drift" {
		return sequencer.Claim{}, fmt.Errorf("%w: %s version %d", sequencer.ErrDefinitionDrift, claim.Attempt.OperationID, version)
	}
	if claim.Attempt.Version, err = toUint(version); err != nil {
		return sequencer.Claim{}, err
	}
	if claim.Attempt.Number, err = toUint(number); err != nil {
		return sequencer.Claim{}, err
	}
	if claim.Budget.Attempt, err = toUint(runAttempt); err != nil {
		return sequencer.Claim{}, err
	}
	if claim.Budget.Exceptions, err = toUint(retryExceptions); err != nil {
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
	fromState, err := parseState(from)
	if err != nil {
		return sequencer.Claim{}, err
	}
	if fromState != sequencer.Eligible {
		if err = sequencer.ValidateTransition(fromState, sequencer.Eligible); err != nil {
			return sequencer.Claim{}, err
		}
		if err = insertAudit(ctx, tx, claim.Attempt.OperationID, version, number,
			fromState, sequencer.Eligible, claim.Attempt.StartedAt,
			request.Owner, fencing, request.Owner, "became eligible"); err != nil {
			return sequencer.Claim{}, err
		}
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
	if !ownershipValid(ownership) {
		return sequencer.AttemptRecord{}, sequencer.ErrInvalidOperation
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return sequencer.AttemptRecord{}, err
	}
	defer rollbackDetached(ctx, tx)
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
	attemptUpdate, err := tx.Exec(ctx, `
UPDATE sequencer_attempts SET state = 'running'
WHERE operation_id = $1 AND version = $2 AND attempt_number = $3
  AND owner = $4 AND fencing_token = $5
  AND state = 'claimed' AND completed_at IS NULL`,
		ownership.OperationID, version, number, ownership.Owner, fencing)
	if err != nil {
		return sequencer.AttemptRecord{}, err
	}
	if attemptUpdate.RowsAffected() != 1 {
		return sequencer.AttemptRecord{}, fmt.Errorf("%w: running attempt mismatch", sequencer.ErrDefinitionDrift)
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

// RenewLease extends a claimed or running attempt under its current fencing proof.
// PostgreSQL time is authoritative so pod clock skew cannot shorten ownership.
func (store *Store) RenewLease(ctx context.Context, ownership sequencer.Ownership, _ time.Time, duration time.Duration) (time.Time, error) {
	if !ownershipValid(ownership) {
		return time.Time{}, sequencer.ErrInvalidOperation
	}
	if duration.Milliseconds() <= 0 {
		return time.Time{}, sequencer.ErrInvalidLease
	}
	var until time.Time
	err := store.database.QueryRow(ctx, `
UPDATE sequencer_operations SET
    lease_expires_at = GREATEST(
        lease_expires_at,
        clock_timestamp() + ($5 * interval '1 millisecond')
    ),
    updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND owner = $3
  AND fencing_token = $4 AND state IN ('claimed', 'running')
  AND lease_expires_at > clock_timestamp()
RETURNING lease_expires_at`, ownership.OperationID, ownership.Version,
		ownership.Owner, ownership.Fencing, duration.Milliseconds()).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, sequencer.ErrStaleOwner
	}
	return until, err
}

// Complete atomically persists the attempt outcome and current projection.
func (store *Store) Complete(ctx context.Context, completion sequencer.Completion) error {
	if !ownershipValid(completion.Ownership) {
		return sequencer.ErrInvalidOperation
	}
	from := completion.From
	if from == 0 {
		from = sequencer.Running
	}
	if err := sequencer.ValidateTransition(from, completion.State); err != nil {
		return err
	}
	if completion.State == sequencer.Retryable && !completion.RetryException ||
		completion.RetryException && completion.State != sequencer.Retryable && completion.State != sequencer.Failed && completion.State != sequencer.DeadLettered {
		return sequencer.ErrInvalidOperation
	}
	actor := firstNonEmpty(completion.Actor, completion.Owner)
	reason := firstNonEmpty(completion.Reason, "completed")
	if len(actor) > sequencer.DefaultMaxActorBytes || len(reason) > sequencer.DefaultMaxReasonBytes {
		return sequencer.ErrResourceLimit
	}
	output, err := json.Marshal(completion.Output)
	if err != nil || len(output) > sequencer.DefaultMaxOutputBytes {
		return sequencer.ErrResourceLimit
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackDetached(ctx, tx)
	var number, fencing int64
	var completedAt time.Time
	err = tx.QueryRow(ctx, `
UPDATE sequencer_operations SET
    state = $5, owner = NULL, lease_expires_at = NULL,
    eligible_at = CASE WHEN $5 IN ('retryable', 'deferred') THEN $6 ELSE eligible_at END,
    updated_at = clock_timestamp()
WHERE operation_id = $1 AND version = $2 AND owner = $3
  AND fencing_token = $4 AND state = $7
  AND lease_expires_at > clock_timestamp()
RETURNING attempt_number, fencing_token, updated_at`,
		completion.OperationID, completion.Version, completion.Owner,
		completion.Fencing, completion.State.String(), completion.EligibleAt,
		from.String(),
	).Scan(&number, &fencing, &completedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.ErrStaleOwner
	}
	if err != nil {
		return err
	}
	detail := sequencer.SanitizePersistenceText(completion.ErrorDetail, sequencer.DefaultMaxErrorBytes)
	attemptUpdate, err := tx.Exec(ctx, `
UPDATE sequencer_attempts SET state = $4, completed_at = $5,
    error_detail = NULLIF($6, ''), output = $7
WHERE operation_id = $1 AND version = $2 AND attempt_number = $3
  AND owner = $8 AND fencing_token = $9
  AND state = $10 AND completed_at IS NULL`,
		completion.OperationID, completion.Version, number,
		completion.State.String(), completedAt, detail, output,
		completion.Owner, fencing, from.String())
	if err != nil {
		return err
	}
	if attemptUpdate.RowsAffected() != 1 {
		return fmt.Errorf("%w: completion attempt mismatch", sequencer.ErrDefinitionDrift)
	}
	version, err := toInt64(completion.Version)
	if err != nil {
		return err
	}
	if err = insertAudit(ctx, tx, completion.OperationID, version,
		number, from, completion.State, completedAt,
		completion.Owner, fencing, actor, reason); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return sequencer.UnknownResult(err)
	}
	return nil
}

// RecoverExpired records expired attempts as indeterminate and authorizes
// automatic replay only for explicitly idempotent definitions.
func (store *Store) RecoverExpired(ctx context.Context, _ time.Time) (int, error) {
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer rollbackDetached(ctx, tx)
	var candidates, attempts, projections, unknownAudits, replayable, replayAudits int64
	err = tx.QueryRow(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT operation_id, version, attempt_number, owner, fencing_token, state,
           unknown_outcome
    FROM sequencer_operations
    WHERE state IN ('claimed', 'running')
      AND lease_expires_at <= clock_timestamp()
    ORDER BY lease_expires_at, operation_id, version
    LIMIT $1
    FOR UPDATE SKIP LOCKED
), attempts AS (
    UPDATE sequencer_attempts attempt SET
        state = 'indeterminate', completed_at = clock_timestamp(),
        error_detail = 'sequencer: unknown result'
    FROM candidates
    WHERE attempt.operation_id = candidates.operation_id
      AND attempt.version = candidates.version
      AND attempt.attempt_number = candidates.attempt_number
      AND attempt.owner = candidates.owner
      AND attempt.fencing_token = candidates.fencing_token
      AND attempt.state = candidates.state
      AND attempt.completed_at IS NULL
    RETURNING candidates.operation_id, candidates.version,
              candidates.attempt_number, candidates.owner,
              candidates.fencing_token, candidates.state AS from_state,
              candidates.unknown_outcome, attempt.completed_at
), expired AS (
    UPDATE sequencer_operations operation SET
        state = CASE attempts.unknown_outcome
                    WHEN 1 THEN 'eligible'
                    ELSE 'indeterminate'
                END,
        owner = NULL, lease_expires_at = NULL,
        eligible_at = CASE attempts.unknown_outcome
                          WHEN 1 THEN attempts.completed_at
                          ELSE operation.eligible_at
                      END,
        updated_at = attempts.completed_at
    FROM attempts
    WHERE operation.operation_id = attempts.operation_id
      AND operation.version = attempts.version
      AND operation.attempt_number = attempts.attempt_number
      AND operation.owner = attempts.owner
      AND operation.fencing_token = attempts.fencing_token
      AND operation.state = attempts.from_state
    RETURNING operation.operation_id, operation.version,
              operation.attempt_number, attempts.owner,
              operation.fencing_token, operation.updated_at,
              attempts.from_state, attempts.unknown_outcome
), unknown_events AS (
    INSERT INTO sequencer_audit_events (
        operation_id, version, attempt_number, from_state, to_state,
        occurred_at, owner, fencing_token, actor, reason
    ) SELECT operation_id, version, attempt_number, from_state, 'indeterminate',
             updated_at, owner, fencing_token, 'system',
             'lease expired; outcome unknown'
      FROM expired
    RETURNING operation_id, version, attempt_number, occurred_at, owner,
              fencing_token
), eligible_events AS (
    INSERT INTO sequencer_audit_events (
        operation_id, version, attempt_number, from_state, to_state,
        occurred_at, owner, fencing_token, actor, reason
    ) SELECT unknown_events.operation_id, unknown_events.version,
             unknown_events.attempt_number, 'indeterminate', 'eligible',
             unknown_events.occurred_at, unknown_events.owner,
             unknown_events.fencing_token, 'system',
             'idempotent replay authorized'
      FROM unknown_events
      JOIN expired USING (operation_id, version, attempt_number)
      WHERE expired.unknown_outcome = 1
    RETURNING operation_id
)
SELECT (SELECT count(*) FROM candidates),
       (SELECT count(*) FROM attempts),
       (SELECT count(*) FROM expired),
       (SELECT count(*) FROM unknown_events),
       (SELECT count(*) FROM attempts WHERE unknown_outcome = 1),
	   (SELECT count(*) FROM eligible_events)`, sequencer.DefaultRecoveryBatchSize).Scan(
		&candidates, &attempts, &projections, &unknownAudits, &replayable, &replayAudits,
	)
	if err != nil {
		return 0, err
	}
	if candidates != attempts || attempts != projections || projections != unknownAudits || replayable != replayAudits {
		return 0, fmt.Errorf("%w: expired operation/attempt/audit mismatch", sequencer.ErrDefinitionDrift)
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, sequencer.UnknownResult(err)
	}
	return int(candidates), nil
}

// Snapshot returns one current projection without payload history.
func (store *Store) Snapshot(ctx context.Context, id sequencer.OperationID, version uint) (sequencer.Record, error) {
	var record sequencer.Record
	var state string
	var storedVersion, attempt, fencing, runAttempt, retryExceptions int64
	var dependencyRefs []byte
	var compensates []byte
	var unknownOutcome int16
	var channel *string
	err := store.database.QueryRow(ctx, `
WITH budget_epoch AS (
    SELECT COALESCE(MAX(event.attempt_number), 0) AS epoch_start_attempt
    FROM sequencer_audit_events event
    WHERE event.operation_id = $1 AND event.version = $2
      AND event.from_state IN ('succeeded', 'failed', 'blocked', 'canceled', 'dead_lettered')
      AND event.to_state = 'eligible'
)
SELECT operation_id, version, checksum, channel, dependencies, dependency_refs,
       compensates, unknown_outcome, dead_letter, state,
       attempt_number, COALESCE(owner, ''), fencing_token,
       COALESCE(lease_expires_at, 'epoch'), eligible_at, updated_at,
       attempt_number - budget_epoch.epoch_start_attempt,
       (SELECT count(*) FROM sequencer_attempts history
        WHERE history.operation_id = $1 AND history.version = $2
          AND history.attempt_number > budget_epoch.epoch_start_attempt
          AND history.state = 'retryable')
FROM sequencer_operations
CROSS JOIN budget_epoch
WHERE operation_id = $1 AND version = $2`, id, version).Scan(
		&record.ID, &storedVersion, &record.Checksum, &channel, &record.Dependencies, &dependencyRefs,
		&compensates, &unknownOutcome, &record.DeadLetter, &state,
		&attempt, &record.Owner, &fencing, &record.LeaseExpiresAt,
		&record.EligibleAt, &record.UpdatedAt, &runAttempt, &retryExceptions,
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
	if record.RunAttempt, err = toUint(runAttempt); err != nil {
		return sequencer.Record{}, err
	}
	if record.RetryExceptions, err = toUint(retryExceptions); err != nil {
		return sequencer.Record{}, err
	}
	if record.Fencing, err = toUint64(fencing); err != nil {
		return sequencer.Record{}, err
	}
	if dependencyRefs == nil {
		return sequencer.Record{}, sequencer.ErrDefinitionDrift
	}
	if channel == nil {
		return sequencer.Record{}, sequencer.ErrDefinitionDrift
	}
	record.Channel = *channel
	if record.DependencyRefs, err = decodeDependencyRefs(dependencyRefs); err != nil {
		return sequencer.Record{}, err
	}
	if record.Compensates, err = decodeDependencyRef(compensates); err != nil {
		return sequencer.Record{}, err
	}
	if unknownOutcome < 0 || unknownOutcome > int16(sequencer.UnknownOutcomeReplayIdempotent) {
		return sequencer.Record{}, sequencer.ErrDefinitionDrift
	}
	record.UnknownOutcome = sequencer.UnknownOutcomePolicy(unknownOutcome)
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
		if len(output) > sequencer.DefaultMaxOutputBytes {
			return nil, sequencer.ErrDefinitionDrift
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
	if !request.OperationID.Valid() || request.Version == 0 ||
		request.Actor == "" || len(request.Actor) > sequencer.DefaultMaxActorBytes ||
		request.Reason == "" || len(request.Reason) > sequencer.DefaultMaxReasonBytes {
		return sequencer.ErrResetForbidden
	}
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackDetached(ctx, tx)
	var from string
	var attempt, fencing int64
	var at time.Time
	err = tx.QueryRow(ctx, `
WITH current AS MATERIALIZED (
    SELECT state FROM sequencer_operations
    WHERE operation_id = $1 AND version = $2
	      AND state IN ('succeeded', 'failed', 'blocked', 'canceled', 'dead_lettered')
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
	if err = tx.Commit(ctx); err != nil {
		return sequencer.UnknownResult(err)
	}
	return nil
}

// ResolveUnknown atomically applies one attributable decision to an
// indeterminate operation. Concurrent and stale decisions fail closed.
func (store *Store) ResolveUnknown(ctx context.Context, request sequencer.ReconcileRequest) error {
	if !request.OperationID.Valid() || request.Version == 0 || request.Attempt == 0 || request.Fencing == 0 || request.At.IsZero() ||
		request.Actor == "" || len(request.Actor) > sequencer.DefaultMaxActorBytes ||
		request.Reason == "" || len(request.Reason) > sequencer.DefaultMaxReasonBytes ||
		request.Resolution < sequencer.ReconcileSucceeded || request.Resolution > sequencer.ReconcileFailed {
		return sequencer.ErrReconcileForbidden
	}
	version, err := toInt64(request.Version)
	if err != nil {
		return sequencer.ErrReconcileForbidden
	}
	attemptProof, err := toInt64(request.Attempt)
	if err != nil || request.Fencing > math.MaxInt64 {
		return sequencer.ErrReconcileForbidden
	}
	fencingProof := int64(request.Fencing)
	tx, err := store.database.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackDetached(ctx, tx)
	var from, to string
	var attempt, fencing int64
	var at time.Time
	err = tx.QueryRow(ctx, `
UPDATE sequencer_operations SET
    state = CASE $5::smallint
                WHEN 1 THEN 'succeeded'
                WHEN 2 THEN 'eligible'
                WHEN 3 THEN CASE WHEN dead_letter THEN 'dead_lettered' ELSE 'failed' END
            END,
    eligible_at = CASE WHEN $5::smallint = 2 THEN $6 ELSE eligible_at END,
    updated_at = $6
WHERE operation_id = $1 AND version = $2
  AND attempt_number = $3 AND fencing_token = $4
  AND state = 'indeterminate' AND updated_at <= $6
RETURNING 'indeterminate', state, attempt_number, fencing_token, updated_at`,
		request.OperationID, version, attemptProof, fencingProof,
		int16(request.Resolution), request.At).Scan(
		&from, &to, &attempt, &fencing, &at,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return sequencer.ErrReconcileForbidden
	}
	if err != nil {
		return err
	}
	fromState, err := parseState(from)
	if err != nil {
		return err
	}
	toState, err := parseState(to)
	if err != nil {
		return err
	}
	if err = insertAudit(ctx, tx, request.OperationID, version, attempt,
		fromState, toState, at, "", fencing, request.Actor, request.Reason); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return sequencer.UnknownResult(err)
	}
	return nil
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
	for state := sequencer.Pending; state <= sequencer.DeadLettered; state++ {
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

type persistedDependencyRef struct {
	ID       string `json:"id"`
	Version  uint   `json:"version"`
	Checksum string `json:"checksum"`
}

func canonicalDependencyRefs(registration sequencer.Registration) ([]sequencer.DependencyRef, error) {
	if len(registration.Dependencies) > 0 {
		return nil, sequencer.ErrUnpinnedDependency
	}
	if len(registration.DependencyRefs) > sequencer.DefaultMaxDependencies {
		return nil, sequencer.ErrResourceLimit
	}
	dependencies := slices.Clone(registration.DependencyRefs)
	slices.SortFunc(dependencies, func(left, right sequencer.DependencyRef) int {
		return cmp.Compare(left.ID, right.ID)
	})
	for index, dependency := range dependencies {
		if len(dependency.Checksum) > sequencer.DefaultMaxChecksumBytes {
			return nil, sequencer.ErrResourceLimit
		}
		if !dependency.ID.Valid() || dependency.ID == registration.ID || dependency.Version == 0 || dependency.Checksum == "" ||
			(index > 0 && dependency.ID == dependencies[index-1].ID) {
			return nil, sequencer.ErrInvalidOperation
		}
	}
	return dependencies, nil
}

func encodeDependencyRefs(dependencies []sequencer.DependencyRef) []byte {
	persisted := make([]persistedDependencyRef, len(dependencies))
	for index, dependency := range dependencies {
		persisted[index] = persistedDependencyRef{ID: string(dependency.ID), Version: dependency.Version, Checksum: dependency.Checksum}
	}
	encoded, _ := json.Marshal(persisted)
	return encoded
}

func encodeDependencyRef(reference *sequencer.DependencyRef) []byte {
	if reference == nil {
		return nil
	}
	encoded, _ := json.Marshal(persistedDependencyRef{ID: string(reference.ID), Version: reference.Version, Checksum: reference.Checksum})
	return encoded
}

func decodeDependencyRefs(encoded []byte) ([]sequencer.DependencyRef, error) {
	if len(encoded) > maxPersistedDefinitionBytes {
		return nil, sequencer.ErrDefinitionDrift
	}
	var persisted []persistedDependencyRef
	if err := json.Unmarshal(encoded, &persisted); err != nil || persisted == nil {
		return nil, sequencer.ErrDefinitionDrift
	}
	dependencies := make([]sequencer.DependencyRef, len(persisted))
	for index, dependency := range persisted {
		dependencies[index] = sequencer.DependencyRef{ID: sequencer.OperationID(dependency.ID), Version: dependency.Version, Checksum: dependency.Checksum}
	}
	canonical, err := canonicalDependencyRefs(sequencer.Registration{DependencyRefs: dependencies})
	if err != nil || !slices.Equal(canonical, dependencies) {
		return nil, sequencer.ErrDefinitionDrift
	}
	return dependencies, nil
}

func decodeDependencyRef(encoded []byte) (*sequencer.DependencyRef, error) {
	if encoded == nil {
		return nil, nil
	}
	if len(encoded) > maxPersistedReferenceBytes {
		return nil, sequencer.ErrDefinitionDrift
	}
	var persisted persistedDependencyRef
	if err := json.Unmarshal(encoded, &persisted); err != nil {
		return nil, sequencer.ErrDefinitionDrift
	}
	reference := &sequencer.DependencyRef{ID: sequencer.OperationID(persisted.ID), Version: persisted.Version, Checksum: persisted.Checksum}
	_, err := canonicalDependencyRefs(sequencer.Registration{DependencyRefs: []sequencer.DependencyRef{*reference}})
	if err != nil {
		return nil, sequencer.ErrDefinitionDrift
	}
	return reference, nil
}

func equalDependencyRef(left, right *sequencer.DependencyRef) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func toUint(value int64) (uint, error) {
	parsed, err := strconv.ParseUint(strconv.FormatInt(value, 10), 10, strconv.IntSize)
	if err != nil {
		return 0, fmt.Errorf("%w: %d", errInvalidLedgerInteger, value)
	}
	return uint(parsed), nil //nolint:gosec // ParseUint limits parsed to the platform uint width.
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
var _ sequencer.LeaseStore = (*Store)(nil)
var _ sequencer.ReconciliationStore = (*Store)(nil)
