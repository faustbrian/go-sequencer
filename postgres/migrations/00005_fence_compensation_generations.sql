-- +goose Up
LOCK TABLE sequencer_operations IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sequencer_operations
        WHERE compensates IS NOT NULL
          AND (state IN ('claimed', 'running', 'retryable', 'deferred', 'indeterminate')
               OR (state = 'eligible' AND attempt_number > 0))
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'active compensation must be resolved before installing generation fencing';
    END IF;
END;
$$;

ALTER TABLE sequencer_operations
    ADD COLUMN active_compensations bigint NOT NULL DEFAULT 0,
    ADD COLUMN compensation_fencing_token bigint;

ALTER TABLE sequencer_operations
    ADD CONSTRAINT sequencer_operations_active_compensations_nonnegative
        CHECK (active_compensations >= 0) NOT VALID,
    ADD CONSTRAINT sequencer_operations_compensation_fencing_positive
        CHECK (compensation_fencing_token IS NULL
               OR (compensates IS NOT NULL AND compensation_fencing_token > 0)) NOT VALID;

CREATE FUNCTION sequencer_fence_compensation_generation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    was_active boolean;
    is_active boolean;
    forward_locked boolean;
    forward_fencing bigint;
BEGIN
    was_active := OLD.compensates IS NOT NULL
        AND (OLD.state IN ('claimed', 'running', 'retryable', 'deferred', 'indeterminate')
             OR (OLD.state = 'eligible' AND OLD.attempt_number > 0));
    is_active := NEW.compensates IS NOT NULL
        AND (NEW.state IN ('claimed', 'running', 'retryable', 'deferred', 'indeterminate')
             OR (NEW.state = 'eligible' AND NEW.attempt_number > 0));

    IF OLD.state IN ('succeeded', 'failed', 'blocked', 'canceled', 'dead_lettered')
       AND NEW.state = 'eligible'
       AND OLD.active_compensations > 0 THEN
        RETURN NULL;
    END IF;

    IF NEW.compensation_fencing_token IS DISTINCT FROM OLD.compensation_fencing_token THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'compensation generation binding is immutable';
    END IF;

    IF was_active IS DISTINCT FROM is_active AND is_active THEN
        IF OLD.attempt_number > 0 AND OLD.compensation_fencing_token IS NULL THEN
            IF OLD.state IN ('succeeded', 'failed', 'blocked', 'canceled', 'dead_lettered')
               AND NEW.state = 'eligible' THEN
                RETURN NULL;
            END IF;
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'historical compensation generation is unbound';
        END IF;

        UPDATE sequencer_operations forward
        SET active_compensations = active_compensations + 1
        WHERE forward.operation_id = NEW.compensates->>'id'
          AND forward.version = (NEW.compensates->>'version')::bigint
          AND forward.checksum = NEW.compensates->>'checksum'
          AND forward.state IN ('succeeded', 'skipped')
          AND (NEW.compensation_fencing_token IS NULL
               OR NEW.compensation_fencing_token = forward.fencing_token)
        RETURNING true, forward.fencing_token INTO forward_locked, forward_fencing;

        IF forward_locked IS DISTINCT FROM true THEN
            IF OLD.state IN ('succeeded', 'failed', 'blocked', 'canceled', 'dead_lettered')
               AND NEW.state = 'eligible' THEN
                RETURN NULL;
            END IF;
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'compensation generation is no longer current';
        END IF;

        NEW.compensation_fencing_token := forward_fencing;
    ELSIF was_active IS DISTINCT FROM is_active THEN
        UPDATE sequencer_operations forward
        SET active_compensations = active_compensations - 1
        WHERE forward.operation_id = NEW.compensates->>'id'
          AND forward.version = (NEW.compensates->>'version')::bigint
          AND forward.checksum = NEW.compensates->>'checksum'
          AND forward.active_compensations > 0
        RETURNING true INTO forward_locked;

        IF forward_locked IS DISTINCT FROM true THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                MESSAGE = 'compensation generation is no longer current';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER sequencer_fence_compensation_generations
BEFORE UPDATE OF state, attempt_number, compensation_fencing_token ON sequencer_operations
FOR EACH ROW
EXECUTE FUNCTION sequencer_fence_compensation_generation();
