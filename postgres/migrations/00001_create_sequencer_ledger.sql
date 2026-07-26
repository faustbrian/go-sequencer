-- +goose Up
CREATE TABLE sequencer_operations (
    operation_id text NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    checksum text NOT NULL,
    dependencies text[] NOT NULL DEFAULT '{}',
    state text NOT NULL CHECK (state IN (
        'pending', 'eligible', 'claimed', 'running', 'succeeded', 'skipped',
        'failed', 'retryable', 'deferred', 'canceled', 'rolled_back', 'blocked'
    )),
    attempt_number bigint NOT NULL DEFAULT 0 CHECK (attempt_number >= 0),
    owner text,
    fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_expires_at timestamptz,
    eligible_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (operation_id, version),
    CHECK ((state IN ('claimed', 'running')) =
           (owner IS NOT NULL AND lease_expires_at IS NOT NULL))
);

CREATE TABLE sequencer_attempts (
    operation_id text NOT NULL,
    version bigint NOT NULL,
    attempt_number bigint NOT NULL CHECK (attempt_number > 0),
    owner text NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    state text NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    error_detail text,
    output jsonb,
    PRIMARY KEY (operation_id, version, attempt_number),
    FOREIGN KEY (operation_id, version)
        REFERENCES sequencer_operations (operation_id, version)
);

CREATE TABLE sequencer_audit_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation_id text NOT NULL,
    version bigint NOT NULL,
    attempt_number bigint NOT NULL,
    from_state text NOT NULL,
    to_state text NOT NULL,
    occurred_at timestamptz NOT NULL,
    owner text,
    fencing_token bigint NOT NULL,
    actor text,
    reason text NOT NULL,
    FOREIGN KEY (operation_id, version)
        REFERENCES sequencer_operations (operation_id, version)
);

CREATE INDEX sequencer_operations_claim_idx
    ON sequencer_operations (eligible_at, operation_id, version)
    WHERE state IN ('eligible', 'retryable', 'deferred');
CREATE INDEX sequencer_operations_recovery_idx
    ON sequencer_operations (lease_expires_at)
    WHERE state IN ('claimed', 'running');
CREATE INDEX sequencer_audit_lookup_idx
    ON sequencer_audit_events (operation_id, version, event_id DESC);

-- Claim implementations select candidates FOR UPDATE SKIP LOCKED before
-- incrementing the fencing token in the same transaction.

-- +goose Down
DROP TABLE sequencer_audit_events;
DROP TABLE sequencer_attempts;
DROP TABLE sequencer_operations;
