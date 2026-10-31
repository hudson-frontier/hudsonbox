-- One fixed-size observation record per enrolled identity, no request/error log.
ALTER TABLE hosts ADD COLUMN rpc_failures jsonb NOT NULL DEFAULT
    '{"unavailable":0,"deadline_exceeded":0,"unknown":0}'::jsonb,
    ADD COLUMN rpc_last_failure_at timestamptz,
    ADD CONSTRAINT hosts_rpc_failures_shape CHECK (
        jsonb_typeof(rpc_failures)='object'
        AND rpc_failures ?& ARRAY['unavailable','deadline_exceeded','unknown']
        AND rpc_failures - ARRAY['unavailable','deadline_exceeded','unknown'] = '{}'::jsonb
        AND (rpc_failures->>'unavailable')::bigint >= 0
        AND (rpc_failures->>'deadline_exceeded')::bigint >= 0
        AND (rpc_failures->>'unknown')::bigint >= 0);
