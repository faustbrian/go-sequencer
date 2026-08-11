-- +goose NO TRANSACTION
-- +goose Up
DROP INDEX CONCURRENTLY IF EXISTS sequencer_operations_active_compensation_preflight_idx;

CREATE INDEX CONCURRENTLY sequencer_operations_active_compensation_preflight_idx
    ON sequencer_operations (operation_id, version)
    WHERE compensates IS NOT NULL
      AND (state IN ('claimed', 'running', 'retryable', 'deferred', 'indeterminate')
           OR (state = 'eligible' AND attempt_number > 0));
