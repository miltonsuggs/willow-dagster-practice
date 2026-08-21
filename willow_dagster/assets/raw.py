"""
assets/raw.py — INGESTION assets (the 'EL' in ELT).

Concept map for your interview:
  * SOFTWARE-DEFINED ASSET: each @asset function DEFINES a table you want to
    exist. You describe the desired data, not a sequence of tasks — that's the
    core difference from Airflow's task-centric DAGs.
  * IDEMPOTENT LOAD: CREATE OR REPLACE means re-running an asset re-creates it
    cleanly — no duplicates, safe to re-run (ties to 3.6 pipeline fundamentals).
  * These raw assets have NO upstream deps, so they sit at the left of the
    lineage graph in the Dagster UI.
"""

from pathlib import Path

from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue

from ..resources import DuckDBWarehouse, DATA_DIR


def _load_csv(context: AssetExecutionContext, wh: DuckDBWarehouse,
              table: str, csv_name: str) -> MaterializeResult:
    """Idempotently (re)load a CSV into a raw table and emit row-count metadata."""
    csv_path = Path(DATA_DIR) / csv_name
    con = wh.connect()
    try:
        con.execute(
            f"CREATE OR REPLACE TABLE raw_{table} AS "
            f"SELECT * FROM read_csv_auto('{csv_path}', header=true)"
        )
        n = con.execute(f"SELECT COUNT(*) FROM raw_{table}").fetchone()[0]
    finally:
        con.close()
    context.log.info(f"loaded raw_{table}: {n} rows")
    # Metadata shows up in the Dagster UI on the asset — great for observability.
    return MaterializeResult(metadata={"rows": MetadataValue.int(n),
                                       "source": MetadataValue.text(csv_name)})


@asset(group_name="raw", kinds={"duckdb"})
def raw_transactions(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    return _load_csv(context, warehouse, "transactions", "transactions.csv")


@asset(group_name="raw", kinds={"duckdb"})
def raw_funds(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    return _load_csv(context, warehouse, "funds", "funds.csv")


@asset(group_name="raw", kinds={"duckdb"})
def raw_investors(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    return _load_csv(context, warehouse, "investors", "investors.csv")


@asset(group_name="raw", kinds={"duckdb"})
def raw_nav_monthly(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    return _load_csv(context, warehouse, "nav_monthly", "nav_monthly.csv")
