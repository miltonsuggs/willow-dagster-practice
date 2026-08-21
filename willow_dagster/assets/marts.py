"""
assets/marts.py — TRANSFORMATION assets (the 'T' in ELT).

Concept map:
  * ASSET DEPENDENCIES via deps=[...]: these assets read/write the DuckDB
    warehouse directly (a shared external store), so we declare dependencies for
    ORDERING but do NOT pass data through Dagster's IO manager. (If an asset
    instead RETURNED a DataFrame, a downstream asset would take it as an argument
    and Dagster's IO manager would hand it over -- that's the other pattern. For
    warehouse SQL, deps=[] is the right, idiomatic choice.)
  * Dagster still draws the full LINEAGE GRAPH from these deps and runs upstream
    assets first -- the asset-centric equivalent of dbt's ref().
"""
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue

from ..resources import DuckDBWarehouse
from .raw import raw_funds, raw_transactions


@asset(group_name="marts", kinds={"duckdb"}, deps=[raw_funds])
def dim_fund(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    """One row per fund. deps=[raw_funds] -> Dagster builds raw_funds first."""
    con = warehouse.connect()
    try:
        con.execute("""
            CREATE OR REPLACE TABLE dim_fund AS
            SELECT
                md5(CAST(fund_id AS VARCHAR)) AS fund_key,
                fund_id,
                fund_name, asset_class, structure, manager, vintage_year
            FROM raw_funds
        """)
        n = con.execute("SELECT COUNT(*) FROM dim_fund").fetchone()[0]
    finally:
        con.close()
    return MaterializeResult(metadata={"rows": MetadataValue.int(n),
                                       "grain": MetadataValue.text("one row per fund")})


@asset(group_name="marts", kinds={"duckdb"}, deps=[raw_transactions, dim_fund])
def fct_cash_flow(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    """Transactional fact. Grain: one row per transaction. Depends on TWO upstream assets."""
    con = warehouse.connect()
    try:
        con.execute("""
            CREATE OR REPLACE TABLE fct_cash_flow AS
            SELECT
                t.transaction_id, t.investor_id, df.fund_key,
                t.txn_date, t.txn_type, t.amount,
                CASE
                    WHEN t.txn_type IN ('capital_call','fee')        THEN -t.amount
                    WHEN t.txn_type IN ('distribution','redemption')  THEN  t.amount
                    ELSE 0
                END AS cash_flow
            FROM raw_transactions t
            LEFT JOIN dim_fund df ON df.fund_id = t.fund_id
        """)
        n = con.execute("SELECT COUNT(*) FROM fct_cash_flow").fetchone()[0]
        net = con.execute("SELECT SUM(cash_flow) FROM fct_cash_flow").fetchone()[0]
    finally:
        con.close()
    return MaterializeResult(metadata={
        "rows": MetadataValue.int(n),
        "net_cash_flow": MetadataValue.float(float(net)),
        "grain": MetadataValue.text("one row per transaction (additive measures)"),
    })
