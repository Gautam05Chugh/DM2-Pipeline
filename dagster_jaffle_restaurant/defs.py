from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from dagster_jaffle_restaurant.assets import bronze_assets, silver_assets, gold_assets
from dagster_jaffle_restaurant.resources.duckdb_io_manager import DuckDBIOManager
from dagster_jaffle_restaurant.resources.http_client import HttpClient

all_assets = load_assets_from_modules(
    [bronze_assets, silver_assets, gold_assets],
)

# Use full asset keys including the "gold" prefix
gold_selection = AssetSelection.keys(
    ("gold", "gold_avg_order_value"),
    ("gold", "gold_tickets_per_order"),
)

gold_refresh_job = define_asset_job(
    name="gold_refresh_job",
    selection=gold_selection,
)

gold_refresh_schedule = ScheduleDefinition(
    name="gold_refresh_every_hour",
    cron_schedule="0 * * * *",  # top of every hour
    job=gold_refresh_job,
)

defs = Definitions(
    assets=all_assets,
    jobs=[gold_refresh_job],
    schedules=[gold_refresh_schedule],
    resources={
        "duckdb_io_manager": DuckDBIOManager(database_path="data/warehouse.duckdb"),
        "http_client": HttpClient(timeout_seconds=30),
    },
)
