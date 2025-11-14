import pandas as pd
from dagster import asset, AssetExecutionContext

# Customer Dimensions Definition
@asset(
    key_prefix=["silver"],
    io_manager_key="duckdb_io_manager",
    deps=["bronze_raw_customers"],
)
def dim_customers(
    context: AssetExecutionContext,
    bronze_raw_customers: pd.DataFrame,
) -> pd.DataFrame:
    """
    Customers dimension table built from bronze_raw_customers.

    Expected bronze columns (Jaffle): id, first_name, last_name, email, etc.
    """
    df = bronze_raw_customers.copy()

    # Align primary key name
    if "id" in df.columns and "customer_id" not in df.columns:
        df = df.rename(columns={"id": "customer_id"})

    # Simple, readable name formatting
    if "first_name" in df.columns:
        df["first_name"] = df["first_name"].astype(str).str.title()
    if "last_name" in df.columns:
        df["last_name"] = df["last_name"].astype(str).str.title()

    context.log.info("Customers dim rows: %s; columns: %s", len(df), list(df.columns))
    return df

# Order Dimension Definition
@asset(
    key_prefix=["silver"],
    io_manager_key="duckdb_io_manager",
    deps=["bronze_raw_orders"],
)
def dim_orders(
    context: AssetExecutionContext,
    bronze_raw_orders: pd.DataFrame,
) -> pd.DataFrame:
    """
    Orders dimension table built from bronze_raw_orders.

    Expected bronze columns (Jaffle): id, user_id, order_date, status, etc.
    """
    df = bronze_raw_orders.copy()

    rename_map = {}

    if "id" in df.columns and "order_id" not in df.columns:
        rename_map["id"] = "order_id"

    if "user_id" in df.columns and "customer_id" not in df.columns:
        rename_map["user_id"] = "customer_id"

    df = df.rename(columns=rename_map)

    if "order_date" in df.columns:
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

    context.log.info("Orders dim rows: %s; columns: %s", len(df), list(df.columns))
    return df

# Total Quantity
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
    """
    Order items fact table, joining items to products.

    bronze_raw_items  (jaffle): id, order_id, product_id, quantity, amount
    bronze_raw_products (jaffle): id, name, price, etc.
    """
    items = bronze_raw_items.copy()
    products = bronze_raw_products.copy()

    context.log.info("Items columns: %s", list(items.columns))
    context.log.info("Products columns: %s", list(products.columns))

    # Normalize id column names
    if "id" in items.columns and "order_item_id" not in items.columns:
        items = items.rename(columns={"id": "order_item_id"})
    if "id" in products.columns and "product_id" not in products.columns:
        products = products.rename(columns={"id": "product_id"})

    # Determine product key in items
    product_key_col = None
    if "product_id" in items.columns:
        product_key_col = "product_id"
    elif "product" in items.columns:
        product_key_col = "product"
        items = items.rename(columns={"product": "product_id"})
    elif "sku" in items.columns:
        product_key_col = "sku"
        items = items.rename(columns={"sku": "product_id"})

    if product_key_col is None:
        msg = "Could not find product key in bronze_raw_items (expected product_id/product/sku)"
        context.log.error(msg)
        raise KeyError(msg)

    # Determine product key in products
    product_key_prod_col = None
    if "product_id" in products.columns:
        product_key_prod_col = "product_id"
    elif "sku" in products.columns:
        product_key_prod_col = "sku"
        products = products.rename(columns={"sku": "product_id"})

    if product_key_prod_col is None:
        msg = "Could not find product key in bronze_raw_products (expected id/product_id/sku)"
        context.log.error(msg)
        raise KeyError(msg)

    # Ensure aligned product_id
    if "product_id" not in items.columns or "product_id" not in products.columns:
        msg = "Expected product_id column on both items and products after renames."
        context.log.error(msg)
        raise KeyError(msg)

    # Ensure order_id exists
    if "order_id" not in items.columns:
        msg = "bronze_raw_items must contain order_id"
        context.log.error(msg)
        raise KeyError(msg)

    # Join items to products
    joined = items.merge(products, on="product_id", how="left")

    # Amount / price
    if "amount" in joined.columns:
        amount_col = "amount"
    elif "price" in joined.columns:
        amount_col = "price"
    else:
        amount_col = None

    if amount_col is not None:
        joined[amount_col] = joined[amount_col].astype(float)
    else:
        joined["amount"] = 0.0
        amount_col = "amount"

    # Quantity
    if "quantity" in joined.columns:
        joined["quantity"] = joined["quantity"].astype(int)
    else:
        joined["quantity"] = 1

    joined["line_total"] = joined[amount_col] * joined["quantity"]

    context.log.info(
        "Order items fact rows: %s; columns: %s",
        len(joined),
        list(joined.columns),
    )
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
    """
    Tickets table keyed by order, built from bronze_raw_tickets.
    Flexible to handle slightly different JSONL schemas.
    """
    df = bronze_raw_tickets.copy()
    context.log.info("Bronze tickets columns: %s", list(df.columns))

    rename_map = {}

    # Ticket id
    if "id" in df.columns:
        rename_map["id"] = "ticket_id"
    elif "ticket_id" in df.columns:
        rename_map["ticket_id"] = "ticket_id"

    # Order id
    if "order_id" in df.columns:
        rename_map["order_id"] = "order_id"
    elif "orderID" in df.columns:
        rename_map["orderID"] = "order_id"

    # Created at
    if "created_at" in df.columns:
        rename_map["created_at"] = "ticket_created_at"
    elif "ticket_created_at" in df.columns:
        rename_map["ticket_created_at"] = "ticket_created_at"

    # Status
    if "status" in df.columns:
        rename_map["status"] = "ticket_status"
    elif "ticket_status" in df.columns:
        rename_map["ticket_status"] = "ticket_status"

    # Channel
    if "channel" in df.columns:
        rename_map["channel"] = "ticket_channel"
    elif "ticket_channel" in df.columns:
        rename_map["ticket_channel"] = "ticket_channel"

    df = df.rename(columns=rename_map)

    # Validate minimal schema
    required_cols = ["ticket_id", "order_id"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        msg = f"Missing required columns in tickets table: {missing}"
        context.log.error(msg)
        raise KeyError(msg)

    if "ticket_created_at" in df.columns:
        df["ticket_created_at"] = pd.to_datetime(
            df["ticket_created_at"],
            errors="coerce",
        )

    context.log.info("Tickets rows after transform: %s", len(df))
    return df
