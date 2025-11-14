from typing import Any

import duckdb
import pandas as pd
from dagster import ConfigurableIOManager, InputContext, OutputContext


class DuckDBIOManager(ConfigurableIOManager):
    """IO manager that persists pandas DataFrames to DuckDB."""

    database_path: str = "data/warehouse.duckdb"

    def _get_conn(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.database_path)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Persist DataFrame to DuckDB table named after asset key."""
        table_name = "_".join(context.asset_key.path)
        if not isinstance(obj, pd.DataFrame):
            msg = f"Expected pandas DataFrame, got {type(obj)}"
            raise TypeError(msg)

        conn = self._get_conn()
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
            conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
            conn.register("tmp_df", obj)
            conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} "
                "AS SELECT * FROM tmp_df"
            )
        finally:
            conn.close()

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load DataFrame from DuckDB table named after asset key."""
        table_name = "_".join(context.asset_key.path)
        conn = self._get_conn()
        try:
            query = f"SELECT * FROM {table_name}"
            return conn.execute(query).df()
        finally:
            conn.close()
