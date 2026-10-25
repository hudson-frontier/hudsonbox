-- Filter candidate projects before expanding their token metadata. Authentication
-- reads take priority over batching these infrequent credential-index writes.
CREATE INDEX projects_api_token_key_lookup
    ON projects USING gin (api_tokens) WITH (fastupdate = off);
