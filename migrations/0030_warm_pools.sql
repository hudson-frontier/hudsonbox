-- #188 warm pools. 0029 is reserved for the concurrent #197 Kubernetes adapter.
-- Specified by docs/data-models.md ("Accepted v1 fleet and warm-pool model");
-- runtime notes and unfinished gates are in docs/warm-pools.md.
--
-- Warm supervisor capacity (ready hosts) is not a warm VM. A member row is one
-- prepared, unused microVM reservation on one host at one supervisor epoch.

-- Immutable template version: image plus runtime/resource/boot configuration.
-- Rows are never updated; a change is a new version (new id).
CREATE TABLE sandbox_templates (
    id               uuid        PRIMARY KEY,
    name             text        NOT NULL
        CHECK (name ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    version          integer     NOT NULL CHECK (version > 0),
    owner_project_id uuid        REFERENCES projects (id),
    -- 'shared' is explicit publication; a NULL owner is never an access bypass.
    publication      text        NOT NULL CHECK (publication IN ('private', 'shared')),
    image_digest     text        NOT NULL CHECK (image_digest ~ '^sha256:[0-9a-f]{64}$'),
    vcpu             integer     NOT NULL CHECK (vcpu > 0),
    memory_mib       bigint      NOT NULL CHECK (memory_mib > 0),
    disk_mib         bigint      NOT NULL CHECK (disk_mib > 0),
    boot_config      jsonb       NOT NULL CHECK (jsonb_typeof(boot_config) = 'object'),
    compatibility    jsonb       NOT NULL CHECK (jsonb_typeof(compatibility) = 'object'),
    -- Snapshot-backed preparation needs durable, published, compatible
    -- snapshots (#178). Until then only ordinary immutable boots are accepted.
    snapshot_id      uuid        CHECK (snapshot_id IS NULL),
    created_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT sandbox_templates_name_version UNIQUE (owner_project_id, name, version),
    CONSTRAINT sandbox_templates_owner_or_shared
        CHECK (owner_project_id IS NOT NULL OR publication = 'shared')
);
CREATE UNIQUE INDEX sandbox_templates_shared_name_version
    ON sandbox_templates (name, version) WHERE owner_project_id IS NULL;

CREATE FUNCTION sandbox_templates_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'sandbox template versions are immutable';
END $$;
CREATE TRIGGER sandbox_templates_immutable BEFORE UPDATE OR DELETE ON sandbox_templates
    FOR EACH ROW EXECUTE FUNCTION sandbox_templates_immutable();

-- Target stock of unused VMs for one template version in one host pool.
CREATE TABLE warm_pool_policies (
    id               uuid        PRIMARY KEY,
    template_id      uuid        NOT NULL REFERENCES sandbox_templates (id),
    host_pool_id     uuid        NOT NULL REFERENCES host_pools (id),
    -- Dedicated policy: only this project may claim. NULL: shared policy.
    owner_project_id uuid        REFERENCES projects (id),
    min_ready        integer     NOT NULL CHECK (min_ready >= 0),
    max_total        integer     NOT NULL,
    -- Operator budget per host: bounds platform reservations on any one host.
    max_per_host     integer     NOT NULL CHECK (max_per_host BETWEEN 1 AND 64),
    idle_ttl_seconds integer     NOT NULL CHECK (idle_ttl_seconds BETWEEN 60 AND 86400),
    enabled          boolean     NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT warm_pool_policies_bounds CHECK (max_total >= min_ready AND max_total <= 256),
    CONSTRAINT warm_pool_policies_template_pool UNIQUE (template_id, host_pool_id, owner_project_id)
);
CREATE INDEX warm_pool_policies_pool ON warm_pool_policies (host_pool_id);

-- One prepared VM reservation. States: preparing -> ready -> claiming ->
-- assigned; unused/failed/uncertain members go retiring -> retired only with
-- cleanup evidence. Nothing ever returns to ready from claiming or retiring.
CREATE TABLE warm_pool_members (
    id                    uuid        PRIMARY KEY,
    pool_id               uuid        NOT NULL REFERENCES warm_pool_policies (id),
    host_id               uuid        NOT NULL REFERENCES hosts (id),
    supervisor_epoch      bigint      NOT NULL CHECK (supervisor_epoch > 0),
    template_id           uuid        NOT NULL REFERENCES sandbox_templates (id),
    vm_ref                text        CHECK (vm_ref IS NULL OR vm_ref ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
    vcpu                  integer     NOT NULL CHECK (vcpu > 0),
    memory_mib            bigint      NOT NULL CHECK (memory_mib > 0),
    disk_mib              bigint      NOT NULL CHECK (disk_mib > 0),
    state                 text        NOT NULL
        CHECK (state IN ('preparing', 'ready', 'claiming', 'assigned', 'retiring', 'retired')),
    lease_expires_at      timestamptz,
    claim_revision        bigint      NOT NULL DEFAULT 0 CHECK (claim_revision >= 0),
    claimed_allocation_id uuid        UNIQUE REFERENCES allocations (id),
    -- Durable handoff intent and supervisor receipt; never cleared.
    handoff_intent        jsonb       CHECK (handoff_intent IS NULL OR jsonb_typeof(handoff_intent) = 'object'),
    handoff_receipt       jsonb       CHECK (handoff_receipt IS NULL OR jsonb_typeof(handoff_receipt) = 'object'),
    prepare_receipt       jsonb       CHECK (prepare_receipt IS NULL OR jsonb_typeof(prepare_receipt) = 'object'),
    cleanup_evidence      jsonb       CHECK (cleanup_evidence IS NULL OR jsonb_typeof(cleanup_evidence) = 'object'),
    retire_reason         text        CHECK (retire_reason IS NULL OR length(retire_reason) <= 64),
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT warm_pool_members_ready_complete CHECK (state <> 'ready'
        OR (vm_ref IS NOT NULL AND prepare_receipt IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CONSTRAINT warm_pool_members_claim_bound CHECK ((state IN ('claiming', 'assigned'))
        <= (claimed_allocation_id IS NOT NULL AND handoff_intent IS NOT NULL)),
    CONSTRAINT warm_pool_members_assigned_receipt CHECK (state <> 'assigned' OR handoff_receipt IS NOT NULL),
    CONSTRAINT warm_pool_members_retired_evidence CHECK ((state = 'retired') = (cleanup_evidence IS NOT NULL)),
    -- A customer-claimed member can never be offered again.
    CONSTRAINT warm_pool_members_claimed_not_ready CHECK (
        claimed_allocation_id IS NULL OR state IN ('claiming', 'assigned', 'retiring', 'retired'))
);
CREATE INDEX warm_pool_members_ready ON warm_pool_members (pool_id, created_at, id) WHERE state = 'ready';
CREATE INDEX warm_pool_members_host ON warm_pool_members (host_id) WHERE state <> 'retired';
CREATE INDEX warm_pool_members_pool_live ON warm_pool_members (pool_id) WHERE state <> 'retired';

-- Extend #196 durable capacity intent with warm-pool work. Generic standalone
-- claimers ignore rows with a warm_pool_id.
ALTER TABLE capacity_operations DROP CONSTRAINT capacity_operations_warm_pool_id_check,
    ADD CONSTRAINT capacity_operations_warm_pool FOREIGN KEY (warm_pool_id) REFERENCES warm_pool_policies (id),
    DROP CONSTRAINT capacity_operations_kind_check,
    ADD CONSTRAINT capacity_operations_kind_check
        CHECK (kind IN ('provision', 'status', 'drain', 'retire', 'warm_prepare', 'warm_retire')),
    ADD CONSTRAINT capacity_operations_warm_kind CHECK (
        (warm_pool_id IS NOT NULL) = (kind IN ('warm_prepare', 'warm_retire')));
CREATE INDEX capacity_operations_warm_claim ON capacity_operations (warm_pool_id, available_at, created_at, id)
    WHERE status IN ('queued', 'running') AND warm_pool_id IS NOT NULL;
