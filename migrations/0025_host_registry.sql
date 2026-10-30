-- Multi-host registry (#192): host pools, project pool access and per-host
-- endpoint/transport identity. Specified by docs/data-models.md ("Accepted v1
-- fleet and warm-pool model"); implementation notes in docs/host-maintenance.md
-- ("Host registry and pools").
--
-- `type` selects a deployment adapter in application code. `provider_config`
-- and `placement_policy` are validated against `type` by the store before any
-- write; the database checks only their shape. Credentials are references,
-- never secret values.
CREATE TABLE host_pools (
    id                    uuid        PRIMARY KEY,
    name                  text        NOT NULL UNIQUE
        CHECK (name ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    type                  text        NOT NULL CHECK (type IN ('k8s', 'standalone')),
    -- Counts supervisor workers (standalone host services or worker Pods),
    -- not physical Kubernetes nodes.
    min_ready_hosts       integer     NOT NULL CHECK (min_ready_hosts >= 0),
    max_hosts             integer     NOT NULL,
    placement_policy      jsonb       NOT NULL,
    provider_config       jsonb       NOT NULL,
    credential_secret_ref text
        CHECK (credential_secret_ref IS NULL
            OR credential_secret_ref ~ '^[A-Za-z0-9][A-Za-z0-9._/:-]{0,252}$'),
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT host_pools_host_bounds
        CHECK (max_hosts >= min_ready_hosts AND max_hosts <= 100000),
    CONSTRAINT host_pools_placement_policy_is_object
        CHECK (jsonb_typeof(placement_policy) = 'object'),
    CONSTRAINT host_pools_provider_config_is_object
        CHECK (jsonb_typeof(provider_config) = 'object')
);

-- Backfill: every existing host joins one explicit shared standalone pool. Its
-- bound admits all existing rows; the store refuses new enrollment beyond it.
-- No project receives access implicitly; placement does not consult access yet.
INSERT INTO host_pools (id, name, type, min_ready_hosts, max_hosts, placement_policy, provider_config)
SELECT '00000000-0000-4000-8000-000000000001', 'legacy-standalone', 'standalone', 0,
       GREATEST(1000, (SELECT count(*) FROM hosts))::integer,
       '{"version":1,"tenancy":"shared"}', '{"version":1}';

CREATE TABLE project_host_pool_access (
    id           uuid        PRIMARY KEY,
    project_id   uuid        NOT NULL REFERENCES projects (id),
    host_pool_id uuid        NOT NULL REFERENCES host_pools (id),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT project_host_pool_access_pair UNIQUE (project_id, host_pool_id)
);
CREATE INDEX project_host_pool_access_pool ON project_host_pool_access (host_pool_id);

-- The default keeps legacy inserts in the backfilled pool; a host never moves
-- pools. Transport identity is all-or-nothing: NULL means the single-host
-- controller configuration still supplies the endpoint (legacy mode).
ALTER TABLE hosts
    ADD COLUMN host_pool_id uuid NOT NULL
        DEFAULT '00000000-0000-4000-8000-000000000001' REFERENCES host_pools (id),
    ADD COLUMN endpoint text,
    -- Exactly the host-ID-bound DNS subjectAltName the controller verifies.
    ADD COLUMN transport_server_name text,
    -- SHA-256 of the host server leaf certificate DER, lowercase hex.
    ADD COLUMN transport_certificate_sha256 text,
    -- External resource reference (machine name, Pod UID); never authority.
    ADD COLUMN provider_ref text,
    ADD CONSTRAINT hosts_transport_complete CHECK (
        (endpoint IS NULL) = (transport_server_name IS NULL)
        AND (endpoint IS NULL) = (transport_certificate_sha256 IS NULL)),
    ADD CONSTRAINT hosts_endpoint_https CHECK (endpoint IS NULL OR (
        char_length(endpoint) <= 270
        AND endpoint ~ '^https://([a-z0-9.-]+|\[[0-9a-f:.]+\]):[0-9]{1,5}$')),
    ADD CONSTRAINT hosts_transport_server_name CHECK (transport_server_name IS NULL
        OR transport_server_name = 'host-' || id::text || '.sandbox.internal'),
    ADD CONSTRAINT hosts_transport_certificate_sha256 CHECK (
        transport_certificate_sha256 IS NULL OR transport_certificate_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT hosts_provider_ref CHECK (provider_ref IS NULL
        OR provider_ref ~ '^[A-Za-z0-9][A-Za-z0-9._/:-]{0,252}$'),
    -- Two identities can never share an address or a server certificate.
    ADD CONSTRAINT hosts_endpoint_unique UNIQUE (endpoint),
    ADD CONSTRAINT hosts_transport_certificate_unique UNIQUE (transport_certificate_sha256),
    ADD CONSTRAINT hosts_provider_ref_unique UNIQUE (host_pool_id, provider_ref);
CREATE INDEX hosts_host_pool ON hosts (host_pool_id);
