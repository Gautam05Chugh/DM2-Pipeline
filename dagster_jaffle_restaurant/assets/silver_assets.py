import pandas as pd
from dagster import asset, AssetExecutionContext


@asset(
    key_prefix=["silver"],
    io_manager_key="duckdb_io_manager",
    deps=["bronze_raw_customers"],
)
def dim_customers(
    context: AssetExecutionContext,
    bronze_raw_customers: pd.DataFrame,
) -> pd.DataFrame:
    """Customers dimension table."""
    df = bronze_raw_customers.copy()
    df = df.rename(columns={"id": "customer_id"})
    df["first_name"] = df["first_name"].str.title()
    df["last_name"] = df["last_name"].str.title()
    context.log.info("Customers dim rows: %s", len(df))
    return df


@asset(
    key_prefix=["silver"],
    io_manager_key="duckdb_io_manager",
    deps=["bronze_raw_orders"],
)
def dim_orders(
    context: AssetExecutionContext,
    bronze_raw_orders: pd.DataFrame,
) -> pd.DataFrame:
    """Orders dimension table."""
    df = bronze_raw_orders.copy()
    df = df.rename(columns={"id": "order_id", "user_id": "customer_id"})
    df["order_date"] = pd.to_datetime(df["order_date"])
    context.log.info("Orders dim rows: %s", len(df))
    return df


@asset(
    key_prefix=["silver"],
    io_manager_key="duckdb_io_manager",
    deps=["bronze_raw_items", "bronze_raw_products"],
)
def fact_order_items(
    context: AssetExecutionContext,
    bronze_raw_items: pd.DataFrame,
    bronze_raw_products: pd.DataFrame,
) -> pd.DataFrame:
    """Order items fact table with product attributes."""
    items = bronze_raw_items.copy()
    products = bronze_raw_products.copy()

    items = items.rename(columns={"id": "order_item_id"})
    products = products.rename(columns={"id": "product_id"})

    joined = items.merge(products, on="product_id", how="left")
    joined["amount"] = joined["amount"].astype(float)
    joined["quantity"] = joined["quantity"].astype(int)
    joined["line_total"] = joined["amount"] * joined["quantity"]
    context.log.info("Order items fact rows: %s", len(joined))
    return joined


@asset(
    key_prefix=["silver"],
    io_manager_key="duckdb_io_manager",
    deps=["bronze_raw_tickets"],
)
def tickets(
    context: AssetExecutionContext,
    bronze_raw_tickets: pd.DataFrame,
) -> pd.DataFrame:
    """Tickets table keyed by order."""
    df = bronze_raw_tickets.copy()
    rename_map = {
        "id": "ticket_id",
        "created_at": "ticket_created_at",
        "status": "ticket_status",
        "channel": "ticket_channel",
    }
    df = df.rename(columns=rename_map)
    if "ticket_created_at" in df.columns:
        df["ticket_created_at"] = pd.to_datetime(
            df["ticket_created_at"],
            errors="coerce",
        )
    context.log.info("Tickets rows: %s", len(df))
    return df
