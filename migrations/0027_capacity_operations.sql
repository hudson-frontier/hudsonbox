-- #196; 0026 reserved for concurrent #195 fencing work.
-- These are infrastructure intents, never project/sandbox lifecycle authority.
ALTER TABLE hosts ADD COLUMN capacity_retired_at timestamptz,
    ADD CONSTRAINT hosts_retired_closed CHECK (capacity_retired_at IS NULL OR status='draining');
-- Retain historical identities without reserving an active machine address.
ALTER TABLE hosts DROP CONSTRAINT hosts_endpoint_unique,
    DROP CONSTRAINT hosts_transport_certificate_unique,
    DROP CONSTRAINT hosts_provider_ref_unique;
CREATE UNIQUE INDEX hosts_endpoint_unique ON hosts(endpoint) WHERE capacity_retired_at IS NULL;
CREATE UNIQUE INDEX hosts_transport_certificate_unique ON hosts(transport_certificate_sha256) WHERE capacity_retired_at IS NULL;
CREATE UNIQUE INDEX hosts_provider_ref_unique ON hosts(host_pool_id,provider_ref) WHERE capacity_retired_at IS NULL;
CREATE TABLE capacity_operations (
    id uuid PRIMARY KEY,
    host_pool_id uuid NOT NULL REFERENCES host_pools(id),
    -- Reserved seam; warm pools are not implemented and cannot be requested yet.
    warm_pool_id uuid CHECK (warm_pool_id IS NULL),
    kind text NOT NULL CHECK (kind IN ('provision','status','drain','retire')),
    target_ref text NOT NULL CHECK (length(target_ref) BETWEEN 1 AND 253),
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    request jsonb NOT NULL CHECK (jsonb_typeof(request)='object'),
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded')),
    claim_revision bigint NOT NULL DEFAULT 0 CHECK (claim_revision>=0),
    lease_expires_at timestamptz,
    provider_receipt jsonb CHECK (provider_receipt IS NULL OR jsonb_typeof(provider_receipt)='object'),
    error text CHECK (error IS NULL OR length(error)<=128),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(host_pool_id,idempotency_key),
    CHECK ((status='running') = (lease_expires_at IS NOT NULL)),
    CHECK ((status='succeeded') = (provider_receipt IS NOT NULL))
);
CREATE INDEX capacity_operations_claim ON capacity_operations(host_pool_id,created_at,id)
    WHERE status <> 'succeeded';
