-- #188: a customer-claimed warm member is never re-pooled. Migration 0030's
-- warm_pool_members_claimed_not_ready check only holds while the claim is
-- recorded; this trigger makes the claim and the lifecycle one-way.
--
-- * A member is inserted only as an unclaimed 'preparing' reservation.
-- * claimed_allocation_id, once set, is never cleared or changed.
-- * A member enters 'ready' only from 'preparing' (a prepare receipt);
--   claiming, assigned, retiring and retired members never return to ready.
CREATE FUNCTION warm_pool_members_never_repooled() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'preparing' OR NEW.claimed_allocation_id IS NOT NULL THEN
            RAISE EXCEPTION 'warm member must start as an unclaimed preparing reservation'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.claimed_allocation_id IS NOT NULL
        AND NEW.claimed_allocation_id IS DISTINCT FROM OLD.claimed_allocation_id THEN
        RAISE EXCEPTION 'warm member claimed_allocation_id is immutable once set'
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.state = 'ready' AND OLD.state NOT IN ('preparing', 'ready') THEN
        RAISE EXCEPTION 'warm member may enter ready only from preparing'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER warm_pool_members_never_repooled BEFORE INSERT OR UPDATE ON warm_pool_members
    FOR EACH ROW EXECUTE FUNCTION warm_pool_members_never_repooled();
