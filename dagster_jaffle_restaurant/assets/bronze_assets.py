from typing import List
import json

import pandas as pd
from dagster import asset, AssetExecutionContext

from dagster_jaffle_restaurant.resources.http_client import HttpClient

# The base Url which is same for every CSV File
JAFFLE_BASE = (
    "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-data/"
    "main/jaffle-data"
)

CUSTOMERS_URL = f"{JAFFLE_BASE}/raw_customers.csv"
ORDERS_URL = f"{JAFFLE_BASE}/raw_orders.csv"
ITEMS_URL = f"{JAFFLE_BASE}/raw_items.csv"
PRODUCTS_URL = f"{JAFFLE_BASE}/raw_products.csv"
STORES_URL = f"{JAFFLE_BASE}/raw_stores.csv"
SUPPLIES_URL = f"{JAFFLE_BASE}/raw_supplies.csv"

# Azure Blob Storage URL
TICKETS_URL = (
    "https://jafshop.blob.core.windows.net/jafshop-tickets-jsonl/support_tickets.jsonl?sp=rl&st=2025-10-31T10:19:26Z&se=2025-11-15T18:34:26Z&sv=2024-11-04&sr=c&sig=EuOUuV8x5p6iSHZP3wDvbgw1tWHScn2eBLKdBDB0b0w%3D"
)

# Loading Customers.csv to Pandas DF
@asset(key_prefix=["bronze"], io_manager_key="duckdb_io_manager")
def bronze_raw_customers(context: AssetExecutionContext) -> pd.DataFrame:
    """Raw customers table from CSV."""
    df = pd.read_csv(CUSTOMERS_URL)
    context.log.info("Loaded customers rows: %s", len(df))
    return df


@asset(key_prefix=["bronze"], io_manager_key="duckdb_io_manager")
def bronze_raw_orders(context: AssetExecutionContext) -> pd.DataFrame:
    """Raw orders table from CSV."""
    df = pd.read_csv(ORDERS_URL)
    context.log.info("Loaded orders rows: %s", len(df))
    return df


@asset(key_prefix=["bronze"], io_manager_key="duckdb_io_manager")
def bronze_raw_items(context: AssetExecutionContext) -> pd.DataFrame:
    """Raw items table from CSV."""
    df = pd.read_csv(ITEMS_URL)
    context.log.info("Loaded items rows: %s", len(df))
    return df


@asset(key_prefix=["bronze"], io_manager_key="duckdb_io_manager")
def bronze_raw_products(context: AssetExecutionContext) -> pd.DataFrame:
    """Raw products table from CSV."""
    df = pd.read_csv(PRODUCTS_URL)
    context.log.info("Loaded products rows: %s", len(df))
    return df


@asset(key_prefix=["bronze"], io_manager_key="duckdb_io_manager")
def bronze_raw_stores(context: AssetExecutionContext) -> pd.DataFrame:
    """Raw stores table from CSV."""
    df = pd.read_csv(STORES_URL)
    context.log.info("Loaded stores rows: %s", len(df))
    return df


@asset(key_prefix=["bronze"], io_manager_key="duckdb_io_manager")
def bronze_raw_supplies(context: AssetExecutionContext) -> pd.DataFrame:
    """Raw supplies table from CSV."""
    df = pd.read_csv(SUPPLIES_URL)
    context.log.info("Loaded supplies rows: %s", len(df))
    return df


@asset(
    key_prefix=["bronze"],
    io_manager_key="duckdb_io_manager",
    required_resource_keys={"http_client"},
)
def bronze_raw_tickets(context: AssetExecutionContext) -> pd.DataFrame:
    """Raw support tickets from Azure Blob JSONL."""
    client: HttpClient = context.resources.http_client

    context.log.info("Downloading tickets from Azure Blob")
    lines: List[str] = client.get_jsonl_lines(TICKETS_URL)
    if not lines:
        msg = "Tickets JSONL returned no lines"
        context.log.error(msg)
        raise ValueError(msg)

    records = []
    for idx, line in enumerate(lines, start=1):
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            context.log.error("Failed to parse JSONL line %s: %s", idx, exc)
            raise

    df = pd.DataFrame.from_records(records)
    context.log.info("Loaded tickets rows: %s; columns: %s", len(df), list(df.columns))
    return df
