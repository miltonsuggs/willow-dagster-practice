"""
assets/checks.py — DATA QUALITY & OBSERVABILITY (section 3.7).

Dagster ASSET CHECKS attach validations directly to an asset. They show up in
the UI next to the asset with pass/fail status, so quality is visible, not
buried in a separate test job.

We demonstrate the three layers from your prep:
  * validation (not-null / accepted-values on a column),
  * reconciliation (source-to-target: do the fact rows tie back to raw?),
  * a business rule (grain: one row per transaction).
"""

from dagster import asset_check, AssetCheckResult, AssetCheckSeverity, MetadataValue

from ..resources import DuckDBWarehouse


@asset_check(asset="fct_cash_flow", description="Grain: transaction_id is unique")
def fct_cash_flow_grain(warehouse: DuckDBWarehouse) -> AssetCheckResult:
    dupes = warehouse.execute("""
        SELECT COUNT(*) FROM (
            SELECT transaction_id FROM fct_cash_flow
            GROUP BY transaction_id HAVING COUNT(*) > 1
        )
    """)[0][0]
    return AssetCheckResult(
        passed=(dupes == 0),
        severity=AssetCheckSeverity.ERROR,
        metadata={"duplicate_ids": MetadataValue.int(dupes)},
    )


@asset_check(asset="fct_cash_flow", description="Validation: txn_type in allowed set")
def fct_cash_flow_valid_types(warehouse: DuckDBWarehouse) -> AssetCheckResult:
    bad = warehouse.execute("""
        SELECT COUNT(*) FROM fct_cash_flow
        WHERE txn_type NOT IN ('subscription','capital_call','distribution','redemption','fee')
    """)[0][0]
    return AssetCheckResult(passed=(bad == 0),
                            metadata={"invalid_rows": MetadataValue.int(bad)})


@asset_check(asset="fct_cash_flow",
             description="Reconciliation: fact row count ties to raw_transactions")
def fct_cash_flow_reconciles_to_source(warehouse: DuckDBWarehouse) -> AssetCheckResult:
    """Source-to-target row/amount reconciliation — the check financial teams live
    by. If these don't tie out, something dropped or duplicated rows silently."""
    src_rows, src_amt = warehouse.execute(
        "SELECT COUNT(*), SUM(amount) FROM raw_transactions")[0]
    tgt_rows, tgt_amt = warehouse.execute(
        "SELECT COUNT(*), SUM(amount) FROM fct_cash_flow")[0]
    ok = (src_rows == tgt_rows) and (src_amt == tgt_amt)
    return AssetCheckResult(
        passed=ok,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "source_rows": MetadataValue.int(src_rows),
            "target_rows": MetadataValue.int(tgt_rows),
            "source_amount": MetadataValue.float(float(src_amt)),
            "target_amount": MetadataValue.float(float(tgt_amt)),
        },
    )
