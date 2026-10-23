//! Read models for the status routes.
//!
//! Every read is scoped by project in the query itself rather than filtered
//! afterwards. A resource belonging to another project must be indistinguishable
//! from one that does not exist, and the way to guarantee that is for the query
//! never to return it.

use sandbox_protocol::{Id, OperationId, ProjectId, SandboxId};
use sqlx::Row as _;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Store, StoreError};

// Share one database-time observation across expiry flags and bodies. Materialize
// only the clock, keeping operation filters/indexes available to the planner.
// Do not fetch expired result/error bodies into API memory.
// The wire layer also rechecks time before serialization.
pub(crate) const OPERATION_READ: &str = "WITH response_clock AS MATERIALIZED (SELECT clock_timestamp() AS read_at)
    SELECT id,sandbox_id,target_operation_id,kind,status,phase,created_at,completed_at,
    CASE WHEN status IN ('succeeded','failed','cancelled') THEN response_expires_at END AS response_expires_at,
    COALESCE(status IN ('succeeded','failed','cancelled') AND response_expires_at<=response_clock.read_at,false) AS response_expired,
    CASE WHEN status IN ('succeeded','failed','cancelled') AND response_expires_at<=response_clock.read_at THEN NULL ELSE result END AS result,
    CASE WHEN status IN ('succeeded','failed','cancelled') AND response_expires_at<=response_clock.read_at THEN NULL ELSE error END AS error,
    CASE WHEN output_status<>'none' AND (output_expires_at<=response_clock.read_at OR response_expires_at<=response_clock.read_at)
        THEN 'expired' ELSE output_status END AS output_status
    FROM operations CROSS JOIN response_clock";

/// An operation as a caller sees it.
#[derive(Debug, Clone)]
pub struct OperationView {
    /// Public response retention only; unresolved execution remains inspectable.
    pub response_expires_at: Option<OffsetDateTime>,
    /// Database-time decision, preserved even if the API clock is behind.
    pub response_expired: bool,
    /// The operation.
    pub id: OperationId,
    /// The sandbox it acts on.
    pub sandbox_id: SandboxId,
    /// The original command for a cancellation operation.
    pub target_operation_id: Option<OperationId>,
    /// What was requested.
    pub kind: String,
    /// Progress: queued, running, succeeded, failed, cancelled, unknown.
    pub status: String,
    /// Finer-grained progress within a status, when there is any.
    pub phase: Option<String>,
    /// Separate from process outcome; populated only for execute operations.
    pub output_status: Option<String>,
    /// Bounded result metadata, once there is a result.
    pub result: Option<serde_json::Value>,
    /// Why it failed, when it did.
    pub error: Option<serde_json::Value>,
    /// When it was admitted.
    pub created_at: OffsetDateTime,
    /// When it reached a terminal status.
    pub completed_at: Option<OffsetDateTime>,
}

impl OperationView {
    pub fn response_expired(&self) -> bool {
        self.response_expired
            || self
                .response_expires_at
                .is_some_and(|at| at <= OffsetDateTime::now_utc())
    }
}

/// A sandbox as a caller sees it.
#[derive(Debug, Clone)]
pub struct SandboxView {
    /// True for explicitly enabled fake-host observations; absent before confirmation.
    pub observation_simulated: Option<bool>,
    /// The sandbox.
    pub id: SandboxId,
    /// Optional display name.
    pub name: Option<String>,
    /// What the service is trying to reach.
    pub desired_state: String,
    /// What was last confirmed, which is not the same thing.
    pub observed_state: String,
    /// When that observation was made. Absent means nothing has been
    /// confirmed yet, which a caller must be able to tell apart from "just
    /// observed".
    pub observed_at: Option<OffsetDateTime>,
    /// The immutable image digest selected at admission.
    pub image_digest: String,
    /// Requested size.
    pub resources: serde_json::Value,
    /// Latest allocation generation.
    pub generation: i64,
    /// The transition currently in progress, if any.
    pub active_transition_operation_id: Option<OperationId>,
    /// When it was created.
    pub created_at: OffsetDateTime,
}

impl Store {
    /// Read one operation owned by this project.
    ///
    /// Returns `Ok(None)` both when the operation does not exist and when it
    /// belongs to someone else — the caller turns either into the same `404`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Query`] if the query fails.
    pub async fn operation_for_project(
        &self,
        project_id: ProjectId,
        operation_id: OperationId,
    ) -> Result<Option<OperationView>, StoreError> {
        let row = sqlx::query(&format!("{OPERATION_READ} WHERE id=$1 AND project_id=$2"))
            .bind(operation_id.uuid())
            .bind(project_id.uuid())
            .fetch_optional(self.pool())
            .await
            .map_err(StoreError::Query)?;

        let Some(row) = row else { return Ok(None) };

        Ok(Some(operation_view(&row)?))
    }

    /// Read one sandbox owned by this project.
    ///
    /// Returns `Ok(None)` for absent and for another project's, identically.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Query`] if the query fails.
    pub async fn sandbox_for_project(
        &self,
        project_id: ProjectId,
        sandbox_id: SandboxId,
    ) -> Result<Option<SandboxView>, StoreError> {
        let row = sqlx::query(
            r"
            SELECT id, name, desired_state, observed_state, observed_at, observation_simulated, image_digest,
                   resources, generation, active_transition_operation_id, created_at
              FROM sandboxes
             WHERE id = $1 AND project_id = $2
            ",
        )
        .bind(sandbox_id.uuid())
        .bind(project_id.uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(StoreError::Query)?;

        let Some(row) = row else { return Ok(None) };

        Ok(Some(sandbox_view(&row)?))
    }
}

pub(crate) fn operation_view(row: &sqlx::postgres::PgRow) -> Result<OperationView, StoreError> {
    Ok(OperationView {
        response_expires_at: row
            .try_get("response_expires_at")
            .map_err(StoreError::Query)?,
        response_expired: row.try_get("response_expired").map_err(StoreError::Query)?,
        id: OperationId::from_uuid(row.try_get::<Uuid, _>("id").map_err(StoreError::Query)?),
        sandbox_id: SandboxId::from_uuid(
            row.try_get::<Uuid, _>("sandbox_id")
                .map_err(StoreError::Query)?,
        ),
        target_operation_id: row
            .try_get::<Option<Uuid>, _>("target_operation_id")
            .map_err(StoreError::Query)?
            .map(OperationId::from_uuid),
        kind: row.try_get("kind").map_err(StoreError::Query)?,
        status: row.try_get("status").map_err(StoreError::Query)?,
        phase: row.try_get("phase").map_err(StoreError::Query)?,
        output_status: if row
            .try_get::<String, _>("kind")
            .map_err(StoreError::Query)?
            == "execute"
        {
            Some(row.try_get("output_status").map_err(StoreError::Query)?)
        } else {
            None
        },
        result: row.try_get("result").map_err(StoreError::Query)?,
        error: row.try_get("error").map_err(StoreError::Query)?,
        created_at: row.try_get("created_at").map_err(StoreError::Query)?,
        completed_at: row.try_get("completed_at").map_err(StoreError::Query)?,
    })
}

pub(crate) fn sandbox_view(row: &sqlx::postgres::PgRow) -> Result<SandboxView, StoreError> {
    Ok(SandboxView {
        observation_simulated: row
            .try_get("observation_simulated")
            .map_err(StoreError::Query)?,
        id: SandboxId::from_uuid(row.try_get::<Uuid, _>("id").map_err(StoreError::Query)?),
        name: row.try_get("name").map_err(StoreError::Query)?,
        desired_state: row.try_get("desired_state").map_err(StoreError::Query)?,
        observed_state: row.try_get("observed_state").map_err(StoreError::Query)?,
        observed_at: row.try_get("observed_at").map_err(StoreError::Query)?,
        image_digest: row.try_get("image_digest").map_err(StoreError::Query)?,
        resources: row.try_get("resources").map_err(StoreError::Query)?,
        generation: row.try_get("generation").map_err(StoreError::Query)?,
        active_transition_operation_id: row
            .try_get::<Option<Uuid>, _>("active_transition_operation_id")
            .map_err(StoreError::Query)?
            .map(OperationId::from_uuid),
        created_at: row.try_get("created_at").map_err(StoreError::Query)?,
    })
}
