-- +goose Up
CREATE FUNCTION sequencer_reject_legacy_blocked_unknown_recovery()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.unknown_outcome = 0
       AND OLD.state IN ('claimed', 'running')
       AND NEW.state = 'eligible' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            MESSAGE = 'blocked unknown outcome requires fenced sequencer recovery';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sequencer_block_legacy_unknown_recovery
BEFORE UPDATE OF state ON sequencer_operations
FOR EACH ROW
EXECUTE FUNCTION sequencer_reject_legacy_blocked_unknown_recovery();
