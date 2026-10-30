//! Advisory owner lookup for routing one operation to its allocation's host.
//!
//! The answer only selects a connection. Every dispatch transaction still
//! re-reads and fences the owner, and the controller refuses to send an action
//! whose ownership differs from the host and epoch it connected to.

use sandbox_protocol::{HostId, Id, OperationId};

use crate::{Store, StoreError};

impl Store {
    /// Host of the operation's sandbox's current allocation, if any. A released
    /// allocation still names the only host that may report on its work.
    pub async fn operation_owner_host(
        &self,
        operation: OperationId,
    ) -> Result<Option<HostId>, StoreError> {
        let host: Option<uuid::Uuid> = sqlx::query_scalar(
            "SELECT a.host_id FROM operations o JOIN sandboxes s ON s.id=o.sandbox_id
            JOIN allocations a ON a.id=s.current_allocation_id
            WHERE o.id=$1",
        )
        .bind(operation.uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(StoreError::Query)?;
        Ok(host.map(HostId::from_uuid))
    }
}
