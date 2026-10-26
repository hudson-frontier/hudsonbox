#![allow(clippy::unwrap_used)]
use super::*;
use sandbox_protocol::Id;

#[sqlx::test(migrator = "crate::MIGRATOR")]
async fn storage_audit_generic_plans_bound_the_scan_by_the_cursor(pool: sqlx::PgPool) {
    let mut transaction = pool.begin().await.unwrap();
    for setting in [
        "SET LOCAL plan_cache_mode=force_generic_plan",
        "SET LOCAL enable_seqscan=off",
        "SET LOCAL enable_bitmapscan=off",
    ] {
        sqlx::query(setting)
            .execute(&mut *transaction)
            .await
            .unwrap();
    }
    for sources in [false, true] {
        let query = format!("EXPLAIN (COSTS OFF) {}", page_query(sources, true));
        let rows = sqlx::query(&query)
            .bind(sandbox_protocol::OperationId::generate().uuid())
            .bind(PAGE_ROWS)
            .fetch_all(&mut *transaction)
            .await
            .unwrap();
        let condition = if sources {
            "Index Cond: (operation_id >"
        } else {
            "Index Cond: (id >"
        };
        assert!(
            rows.iter()
                .any(|row| row.get::<String, _>(0).contains(condition)),
            "cursor must constrain the index scan"
        );
    }
}
