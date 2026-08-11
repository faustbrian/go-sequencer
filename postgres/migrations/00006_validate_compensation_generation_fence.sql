-- +goose Up
ALTER TABLE sequencer_operations
    VALIDATE CONSTRAINT sequencer_operations_active_compensations_nonnegative,
    VALIDATE CONSTRAINT sequencer_operations_compensation_fencing_positive;
