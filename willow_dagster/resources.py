"""
resources.py — shared infrastructure Dagster injects into assets.

A RESOURCE is Dagster's way of handling external systems (warehouses, APIs, S3)
so assets stay testable: assets declare "I need the warehouse", Dagster supplies
it, and in tests you can swap in a different one. Here the warehouse is a local
DuckDB file (same idea maps to a Snowflake resource in production).
"""

from pathlib import Path

import duckdb
from dagster import ConfigurableResource

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
WAREHOUSE = ROOT / "willow_warehouse.duckdb"


class DuckDBWarehouse(ConfigurableResource):
    """A tiny warehouse resource (Snowflake-resource analog)."""
    db_path: str = str(WAREHOUSE)

    def connect(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path, read_only=read_only)

    def execute(self, sql: str):
        con = self.connect(read_only=True)
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()

    def query_df(self, sql: str):
        con = self.connect(read_only=True)
        try:
            return con.execute(sql).fetchdf()
        finally:
            con.close()