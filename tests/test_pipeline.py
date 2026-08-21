"""
Tests that the Dagster assets and checks execute correctly, using an in-memory
DuckDB warehouse resource. Run:  uv run pytest -q

Pattern: materialize_to_memory runs assets in-process with test resources — the
Dagster equivalent of unit-testing a pipeline without a live scheduler.
"""
import duckdb
import pytest
from dagster import materialize_to_memory

from willow_dagster.assets.raw import raw_transactions, raw_funds
from willow_dagster.assets.marts import dim_fund, fct_cash_flow
from willow_dagster.resources import DuckDBWarehouse


@pytest.fixture
def wh(tmp_path):
    return DuckDBWarehouse(db_path=str(tmp_path / "t.duckdb"))


def test_pipeline_builds_and_reconciles(wh):
    result = materialize_to_memory(
        [raw_transactions, raw_funds, dim_fund, fct_cash_flow],
        resources={"warehouse": wh},
    )
    assert result.success

    con = duckdb.connect(wh.db_path)
    src = con.execute("SELECT COUNT(*), SUM(amount) FROM raw_transactions").fetchone()
    tgt = con.execute("SELECT COUNT(*), SUM(amount) FROM fct_cash_flow").fetchone()
    con.close()
    # source-to-target reconciliation: rows AND amounts tie out
    assert src == tgt


def test_grain_is_one_row_per_transaction(wh):
    materialize_to_memory(
        [raw_transactions, raw_funds, dim_fund, fct_cash_flow],
        resources={"warehouse": wh},
    )
    con = duckdb.connect(wh.db_path)
    dupes = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT transaction_id FROM fct_cash_flow
            GROUP BY transaction_id HAVING COUNT(*) > 1)
    """).fetchone()[0]
    con.close()
    assert dupes == 0
