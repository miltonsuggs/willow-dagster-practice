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
    """A tiny warehouse resource. In prod this would be a Snowflake resource
    holding an account/credentials read from environment variables."""
    db_path: str = str(WAREHOUSE)

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path)

    def execute(self, sql: str):
        con = self.connect()
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()

    def query_df(self, sql: str):
        con = self.connect()
        try:
            return con.execute(sql).fetchdf()
        finally:
            con.close()
