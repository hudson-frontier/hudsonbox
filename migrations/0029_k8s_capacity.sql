-- Kubernetes infrastructure receipts; no sandbox lifecycle objects or authority.
CREATE TABLE k8s_workers (
    host_id uuid PRIMARY KEY REFERENCES hosts(id),
    host_pool_id uuid NOT NULL REFERENCES host_pools(id),
    provision_operation_id uuid NOT NULL UNIQUE REFERENCES capacity_operations(id),
    frozen jsonb NOT NULL CHECK (jsonb_typeof(frozen)='object'),
    pod_uid text CHECK (length(pod_uid) BETWEEN 1 AND 128),
    retired boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX k8s_workers_pool ON k8s_workers(host_pool_id);
