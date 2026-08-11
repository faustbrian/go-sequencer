-- +goose NO TRANSACTION
-- +goose Up
DROP INDEX CONCURRENTLY IF EXISTS sequencer_operations_active_compensation_preflight_idx;
