//! Tenant-scoped bounded collection reads. Cursors supply positions, never authority.
use crate::{
    Store, StoreError,
    reads::{OPERATION_READ, OperationView, SandboxView, operation_view, sandbox_view},
};
use sandbox_protocol::{Id, ProjectId, SandboxId};
use sqlx::{Postgres, QueryBuilder};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Copy)]
pub struct PageLimit(u16);
impl PageLimit {
    pub fn new(limit: u16) -> Option<Self> {
        (1..=100).contains(&limit).then_some(Self(limit))
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Position {
    pub created_at: OffsetDateTime,
    pub id: Uuid,
}
#[derive(Debug)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<Position>,
}

fn after<'a>(query: &mut QueryBuilder<'a, Postgres>, before: Option<Position>, limit: PageLimit) {
    if let Some(before) = before {
        query
            .push(" AND (created_at,id)<(")
            .push_bind(before.created_at)
            .push(",")
            .push_bind(before.id)
            .push(")");
    }
    query
        .push(" ORDER BY created_at DESC,id DESC LIMIT ")
        .push_bind(i64::from(limit.0) + 1);
}
fn page<T>(mut items: Vec<T>, limit: PageLimit, position: impl FnOnce(&T) -> Position) -> Page<T> {
    let next = if items.len() > usize::from(limit.0) {
        items.pop();
        items.last().map(position)
    } else {
        None
    };
    Page { items, next }
}
impl Store {
    pub async fn list_sandboxes(
        &self,
        project: ProjectId,
        before: Option<Position>,
        limit: PageLimit,
    ) -> Result<Page<SandboxView>, StoreError> {
        let mut query=QueryBuilder::new("SELECT id,name,desired_state,observed_state,observed_at,observation_simulated,image_digest,
            resources,generation,active_transition_operation_id,created_at FROM sandboxes WHERE project_id=");
        query.push_bind(project.uuid());
        after(&mut query, before, limit);
        let rows = query
            .build()
            .fetch_all(self.pool())
            .await
            .map_err(StoreError::Query)?;
        let items = rows
            .iter()
            .map(sandbox_view)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(page(items, limit, |s| Position {
            created_at: s.created_at,
            id: s.id.uuid(),
        }))
    }
    pub async fn list_operations(
        &self,
        project: ProjectId,
        sandbox: Option<SandboxId>,
        before: Option<Position>,
        limit: PageLimit,
    ) -> Result<Page<OperationView>, StoreError> {
        let mut query = QueryBuilder::new(format!("{OPERATION_READ} WHERE project_id="));
        query.push_bind(project.uuid());
        if let Some(sandbox) = sandbox {
            query.push(" AND sandbox_id=").push_bind(sandbox.uuid());
        }
        after(&mut query, before, limit);
        let rows = query
            .build()
            .fetch_all(self.pool())
            .await
            .map_err(StoreError::Query)?;
        let items = rows
            .iter()
            .map(operation_view)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(page(items, limit, |o| Position {
            created_at: o.created_at,
            id: o.id.uuid(),
        }))
    }
}
