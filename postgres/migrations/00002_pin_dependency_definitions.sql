-- +goose Up
ALTER TABLE sequencer_operations
    ADD COLUMN dependency_refs jsonb,
    ADD COLUMN compensates jsonb,
    ADD COLUMN channel text,
    ADD COLUMN unknown_outcome smallint NOT NULL DEFAULT 0,
    ADD COLUMN dead_letter boolean NOT NULL DEFAULT false;

-- Empty legacy dependency sets are unambiguous. Non-empty legacy arrays stay
-- NULL until an exact current registration supplies ID, version, and checksum.
UPDATE sequencer_operations
SET dependency_refs = '[]'::jsonb
WHERE cardinality(dependencies) = 0;

ALTER TABLE sequencer_operations
    ADD CONSTRAINT sequencer_operations_dependency_refs_array
        CHECK (dependency_refs IS NULL OR jsonb_typeof(dependency_refs) = 'array'),
    ADD CONSTRAINT sequencer_operations_compensates_object
        CHECK (compensates IS NULL OR jsonb_typeof(compensates) = 'object'),
    ADD CONSTRAINT sequencer_operations_unknown_outcome
        CHECK (unknown_outcome IN (0, 1));

ALTER TABLE sequencer_operations
    DROP CONSTRAINT sequencer_operations_state_check,
    ADD CONSTRAINT sequencer_operations_state_check CHECK (state IN (
        'pending', 'eligible', 'claimed', 'running', 'succeeded', 'skipped',
        'failed', 'retryable', 'deferred', 'canceled', 'rolled_back', 'blocked',
        'indeterminate', 'dead_lettered'
    ));

-- A later enforcement migration may make dependency_refs NOT NULL only after
-- deployment has proved that no unresolved NULL rows remain.
