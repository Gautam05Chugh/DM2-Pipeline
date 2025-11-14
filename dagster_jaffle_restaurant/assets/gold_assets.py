import pandas as pd
from dagster import asset, AssetExecutionContext

# Defining the Fact_order_items for getting AOV
@asset(
    key_prefix=["gold"],
    io_manager_key="duckdb_io_manager",
    deps=["dim_orders", "fact_order_items"],
)
def gold_avg_order_value(
    context: AssetExecutionContext,
    dim_orders: pd.DataFrame,
    fact_order_items: pd.DataFrame,
) -> pd.DataFrame:
    """
    Average order value per order, plus overall average AOV.
    Depends on dim_orders and fact_order_items.
    """

    # Expect fact_order_items to have order_id and line_total
    if "order_id" not in fact_order_items.columns:
        raise KeyError("fact_order_items must contain 'order_id'")
    if "line_total" not in fact_order_items.columns:
        raise KeyError("fact_order_items must contain 'line_total'")

    # Expect dim_orders to have order_id and customer_id
    if "order_id" not in dim_orders.columns:
        raise KeyError("dim_orders must contain 'order_id'")

    # Compute order totals from fact table
    order_totals = (
        fact_order_items.groupby("order_id", as_index=False)["line_total"]
        .sum()
        .rename(columns={"line_total": "order_total"})
    )

    # Join totals onto dim_orders
    orders_with_total = dim_orders.merge(order_totals, on="order_id", how="left")
    orders_with_total["order_total"] = orders_with_total["order_total"].fillna(0.0)

    # Overall average order value
    overall_avg = float(orders_with_total["order_total"].mean())
    context.log.info("Overall AOV: %s", overall_avg)

    # Return one row per order with the per‑order total and the overall AOV
    result = orders_with_total.copy()
    result["overall_avg_order_value"] = overall_avg
    return result

# Defining Tickets per order
@asset(
    key_prefix=["gold"],
    io_manager_key="duckdb_io_manager",
    deps=["dim_orders", "tickets"],
)
def gold_tickets_per_order(
    context: AssetExecutionContext,
    dim_orders: pd.DataFrame,
    tickets: pd.DataFrame,
) -> pd.DataFrame:
    """
    Number of support tickets per order.
    Depends on dim_orders and tickets.
    """

    if "order_id" not in tickets.columns:
        raise KeyError("tickets table must contain 'order_id'")
    if "ticket_id" not in tickets.columns:
        raise KeyError("tickets table must contain 'ticket_id'")
    if "order_id" not in dim_orders.columns:
        raise KeyError("dim_orders must contain 'order_id'")

    # To count tickets per order
    ticket_counts = (
        tickets.dropna(subset=["order_id"])
        .groupby("order_id", as_index=False)["ticket_id"]
        .count()
        .rename(columns={"ticket_id": "ticket_count"})
    )

    # Join counts to orders
    result = dim_orders.merge(ticket_counts, on="order_id", how="left")
    result["ticket_count"] = result["ticket_count"].fillna(0).astype(int)

    context.log.info(
        "Orders with at least one ticket: %s",
        int((result["ticket_count"] > 0).sum()),
    )
    return result
