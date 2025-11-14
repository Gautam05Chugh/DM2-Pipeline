from dagster import Definitions, load_assets_from_modules

from dagster_jaffle_restaurant.assets import bronze_assets, silver_assets, gold_assets
from dagster_jaffle_restaurant.resources.duckdb_io_manager import DuckDBIOManager
from dagster_jaffle_restaurant.resources.http_client import HttpClient

all_assets = load_assets_from_modules(
    [bronze_assets, silver_assets, gold_assets],
)


defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb_io_manager": DuckDBIOManager(database_path="data/warehouse.duckdb"),
        "http_client": HttpClient(timeout_seconds=30),
    },
)
