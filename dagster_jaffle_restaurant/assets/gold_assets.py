import pandas as pd
from dagster import asset, AssetExecutionContext


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
    """Average order value per order with overall average."""
    order_totals = (
        fact_order_items.groupby("order_id", as_index=False)["line_total"]
        .sum()
        .rename(columns={"line_total": "order_total"})
    )

    orders_with_total = dim_orders.merge(order_totals, on="order_id", how="left")
    orders_with_total["order_total"] = orders_with_total["order_total"].fillna(0.0)

    overall_avg = orders_with_total["order_total"].mean()
    context.log.info("Overall average order value: %s", overall_avg)

    result = orders_with_total[["order_id", "customer_id", "order_date", "order_total"]].copy()
    result["overall_avg_order_value"] = overall_avg
    return result


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
    """Number of support tickets associated with each order."""
    if "order_id" not in tickets.columns:
        msg = "tickets table must contain order_id"
        raise KeyError(msg)

    ticket_counts = (
        tickets.groupby("order_id", as_index=False)["ticket_id"]
        .count()
        .rename(columns={"ticket_id": "ticket_count"})
    )

    result = dim_orders.merge(ticket_counts, on="order_id", how="left")
    result["ticket_count"] = result["ticket_count"].fillna(0).astype(int)
    context.log.info(
        "Orders with at least one ticket: %s",
        (result["ticket_count"] > 0).sum(),
    )
    return result
