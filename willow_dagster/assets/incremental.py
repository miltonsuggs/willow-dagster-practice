"""
assets/incremental.py — INCREMENTAL / PARTITIONED loading (section 3.6).

Concept map:
  * PARTITIONS: we split the NAV snapshot by month. Dagster tracks which
    partitions have materialized, so you can BACKFILL a range or process only
    new months — the orchestration-level version of the watermark pattern from
    your Python repo.
  * IDEMPOTENT UPSERT: each partition run DELETEs then INSERTs its own month, so
    re-running a partition never double-counts (safe re-runs / late data).
  * In the Dagster UI this asset shows a partition grid; you click a month (or a
    range) to materialize just those.
"""

from dagster import (
    asset, AssetExecutionContext, MonthlyPartitionsDefinition,
    MaterializeResult, MetadataValue,
)

from ..resources import DuckDBWarehouse
from .raw import raw_nav_monthly

# The dataset's NAV months span 2021->2025; define monthly partitions over it.
monthly = MonthlyPartitionsDefinition(start_date="2021-01-01", end_date="2025-07-01")


@asset(group_name="incremental", kinds={"duckdb"}, partitions_def=monthly,
       deps=[raw_nav_monthly])
def nav_by_month(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    """Load ONE month of NAV per run, idempotently (delete-then-insert that month).

    context.partition_key is the month being processed, e.g. '2023-05-01'.
    """
    month = context.partition_key  # 'YYYY-MM-01'
    con = warehouse.connect()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS nav_by_month (
                fund_id INTEGER, as_of_month DATE, nav_per_unit DOUBLE
            )
        """)
        # idempotent: clear this month first, then insert -> re-runs are safe
        con.execute("DELETE FROM nav_by_month WHERE as_of_month = ?", [month])
        con.execute("""
            INSERT INTO nav_by_month
            SELECT fund_id, CAST(as_of_month AS DATE), nav_per_unit
            FROM raw_nav_monthly
            WHERE CAST(as_of_month AS DATE) = ?
        """, [month])
        n = con.execute("SELECT COUNT(*) FROM nav_by_month WHERE as_of_month = ?",
                        [month]).fetchone()[0]
    finally:
        con.close()
    context.log.info(f"nav_by_month[{month}]: {n} rows")
    return MaterializeResult(metadata={"month": MetadataValue.text(month),
                                       "rows": MetadataValue.int(n)})
