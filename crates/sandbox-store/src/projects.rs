//! Project rows and their API token metadata.

use sandbox_protocol::{Id, ProjectId, TokenHash, TokenKeyId};
use sqlx::Row as _;
use time::OffsetDateTime;

use crate::{Store, StoreError};

/// A project's lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectStatus {
    /// Ordinary operation.
    Active,
    /// Access blocked; the project and its data still exist.
    Suspended,
    /// Being torn down. No new work is admitted.
    Deleting,
}

impl ProjectStatus {
    fn from_column(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "suspended" => Some(Self::Suspended),
            "deleting" => Some(Self::Deleting),
            _ => None,
        }
    }
}

/// One token's stored metadata. Never the secret — only its hash.
#[derive(Debug, Clone)]
pub struct TokenRecord {
    /// The project the token belongs to.
    pub project_id: ProjectId,
    /// That project's current status, read in the same query so a suspended
    /// project cannot be missed by checking the token alone.
    pub project_status: ProjectStatus,
    /// Which credential this is.
    pub key_id: TokenKeyId,
    /// SHA-256 of the secret.
    pub hash: TokenHash,
    /// When the token stops being valid, if ever.
    pub expires_at: Option<OffsetDateTime>,
    /// When the token was revoked, if it was.
    pub revoked_at: Option<OffsetDateTime>,
}

const TOKEN_LOOKUP: &str = r"
    SELECT p.id AS project_id,
           p.status AS project_status,
           t.value->>'key_id' AS key_id,
           decode(t.value->>'hash', 'hex') AS hash,
           (t.value->>'expires_at')::timestamptz AS expires_at,
           (t.value->>'revoked_at')::timestamptz AS revoked_at
      FROM projects p
      CROSS JOIN LATERAL jsonb_array_elements(p.api_tokens) AS t(value)
     WHERE p.api_tokens @> jsonb_build_array(jsonb_build_object('key_id', $1::text))
       AND t.value->>'key_id' = $1
";

impl Store {
    /// Find the token metadata for a key identifier.
    ///
    /// Returns `Ok(None)` when no such key exists. The caller must still
    /// verify the secret: this lookup proves nothing on its own, and the key
    /// identifier is not a credential.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Query`] if the query fails, and
    /// [`StoreError::Corrupt`] if a stored row cannot be interpreted — a
    /// hash of the wrong length or an unknown status is a bug or tampering,
    /// never something to treat as "no match".
    pub async fn token_by_key_id(
        &self,
        key_id: &TokenKeyId,
    ) -> Result<Option<TokenRecord>, StoreError> {
        // The GIN containment predicate selects candidate projects before the
        // lateral expansion. An unknown key must not walk every project's array.
        // Read the token and current project status together; the index is not
        // cached authorization and does not replace secret/expiry/revocation checks.
        let row = sqlx::query(TOKEN_LOOKUP)
            .bind(key_id.as_str())
            .fetch_optional(self.pool())
            .await
            .map_err(StoreError::Query)?;

        let Some(row) = row else { return Ok(None) };

        token_record(&row).map(Some)
    }
}

pub(crate) fn token_record(row: &sqlx::postgres::PgRow) -> Result<TokenRecord, StoreError> {
    let status: String = row.try_get("project_status").map_err(StoreError::Query)?;
    let project_status = ProjectStatus::from_column(&status)
        .ok_or_else(|| StoreError::Corrupt(format!("unknown project status {status:?}")))?;

    let hash: Vec<u8> = row.try_get("hash").map_err(StoreError::Query)?;
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| StoreError::Corrupt("stored token hash is not 32 bytes".to_owned()))?;

    let stored_key_id: String = row.try_get("key_id").map_err(StoreError::Query)?;

    Ok(TokenRecord {
        project_id: ProjectId::from_uuid(row.try_get("project_id").map_err(StoreError::Query)?),
        project_status,
        key_id: TokenKeyId::from_stored(stored_key_id),
        hash: TokenHash::from_bytes(hash),
        expires_at: row.try_get("expires_at").map_err(StoreError::Query)?,
        revoked_at: row.try_get("revoked_at").map_err(StoreError::Query)?,
    })
}

#[cfg(test)]
mod lookup_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use sandbox_protocol::ProjectToken;
    use serde_json::json;

    #[sqlx::test(migrator = "crate::MIGRATOR")]
    async fn unknown_tokens_use_an_index_before_project_token_expansion(pool: sqlx::PgPool) {
        let mut rows = Vec::with_capacity(4096);
        let mut known = None;
        for _ in 0..4096 {
            let project = ProjectId::generate();
            let first = ProjectToken::generate().unwrap();
            let second = ProjectToken::generate().unwrap();
            rows.push(json!({"id":project.uuid(),"api_tokens":[
                {"key_id":first.key_id().as_str(),"hash":hex::encode(first.hash().as_bytes())},
                {"key_id":second.key_id().as_str(),"hash":hex::encode(second.hash().as_bytes())}
            ]}));
            known = Some((project, second.key_id().clone(), second.hash()));
        }
        sqlx::query("INSERT INTO projects(id,name,status,limits,api_tokens) SELECT p.id,'lookup-fixture','active','{}'::jsonb,p.api_tokens FROM jsonb_to_recordset($1) AS p(id uuid,api_tokens jsonb)")
            .bind(serde_json::Value::Array(rows)).execute(&pool).await.unwrap();
        sqlx::query("ANALYZE projects")
            .execute(&pool)
            .await
            .unwrap();
        let unknown = ProjectToken::generate().unwrap();
        let query = format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {TOKEN_LOOKUP}");
        let plan: serde_json::Value = sqlx::query_scalar(&query)
            .bind(unknown.key_id().as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
        let mut pending = vec![&plan[0]["Plan"]];
        let mut indexes = Vec::new();
        let mut expansions = 0;
        let mut project_rows = 0;
        while let Some(node) = pending.pop() {
            if let Some(index) = node["Index Name"].as_str() {
                indexes.push(index.to_owned());
            }
            if node["Node Type"] == "Function Scan" {
                expansions += node["Actual Loops"].as_u64().unwrap_or(0);
            }
            if node["Node Type"] == "Seq Scan" && node["Relation Name"] == "projects" {
                project_rows += node["Actual Rows"].as_u64().unwrap_or(0);
            }
            if let Some(children) = node["Plans"].as_array() {
                pending.extend(children);
            }
        }
        eprintln!(
            "token lookup fixture: {}",
            json!({"projects":4096,"tokens_per_project":2,
            "execution_ms":plan[0]["Execution Time"],"shared_hit_blocks":plan[0]["Plan"]["Shared Hit Blocks"],
            "sequential_project_rows":project_rows,"array_expansion_loops":expansions,"indexes":indexes})
        );
        assert!(
            indexes
                .iter()
                .any(|index| index == "projects_api_token_key_lookup"),
            "unknown-token lookup scanned project data instead of using its token-key index"
        );
        assert_eq!(
            expansions, 0,
            "unknown-token lookup expanded unrelated token arrays"
        );
        let store = Store::from_pool(pool);
        assert!(
            store
                .token_by_key_id(unknown.key_id())
                .await
                .unwrap()
                .is_none()
        );
        let (project, key, hash) = known.unwrap();
        let record = store.token_by_key_id(&key).await.unwrap().unwrap();
        assert_eq!(record.project_id, project);
        assert!(record.hash.verify(&hash));
    }
}
