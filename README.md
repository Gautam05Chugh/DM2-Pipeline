# Dagster Jaffle Restaurant

This project is a local data platform for a fictional restaurant (Jaffle Shop) built with Dagster and DuckDB.

It ingests:
- 6 CSV files from the public Jaffle Shop dataset (customers, orders, items, products, stores, supplies)
- A JSONL dump of support tickets stored in Azure Blob Storage

The data is modeled in a bronze–silver–gold layered architecture and exposes two analytics models for non‑technical users:

1. `gold_avg_order_value` – Average Order Value (AOV) per order and overall.
2. `gold_tickets_per_order` – Number of support tickets per order.

---

## 1. Project structure

dagster_jaffle_restaurant/
README.md
pyproject.toml
.gitignore
.pylintrc
data/
warehouse.duckdb # DuckDB file created at runtime
dagster_jaffle_restaurant/
init.py
defs.py # Dagster Definitions entrypoint
assets/
init.py
bronze_assets.py # Raw ingestion from CSV + Azure Blob JSONL
silver_assets.py # Domain models (dimensions, facts)
gold_assets.py # Analytics models (AOV, tickets per order)
resources/
init.py
duckdb_io_manager.py # IO manager writing/reading to DuckDB
http_client.py # Simple HTTP client for Azure Blob JSONL


---

## 2. Data sources

### 2.1 Jaffle Shop CSV files

The following raw CSVs are read directly from the public Jaffle Shop GitHub repository:

- `raw_customers.csv`
- `raw_orders.csv`
- `raw_items.csv`
- `raw_products.csv`
- `raw_stores.csv`
- `raw_supplies.csv`

They are ingested by the bronze assets:

- `bronze_raw_customers`
- `bronze_raw_orders`
- `bronze_raw_items`
- `bronze_raw_products`
- `bronze_raw_stores`
- `bronze_raw_supplies`

### 2.2 Azure Blob JSONL support tickets

Support tickets are stored as a JSON Lines file in Azure Blob Storage, accessed via a read‑only SAS URL.

The bronze asset:

- `bronze_raw_tickets`

downloads the JSONL, parses each line as a JSON record, and loads it into DuckDB.

---

## 3. Medallion architecture

The project follows a bronze–silver–gold layered design.

### 3.1 Bronze layer (raw)

Bronze assets load data with minimal transformation:

- `bronze_raw_customers`
- `bronze_raw_orders`
- `bronze_raw_items`
- `bronze_raw_products`
- `bronze_raw_stores`
- `bronze_raw_supplies`
- `bronze_raw_tickets`

Each asset returns a pandas `DataFrame` which is persisted to DuckDB by the `DuckDBIOManager`. Tables are named by their asset key:

- `bronze_bronze_raw_customers`
- `bronze_bronze_raw_orders`
- …
- `bronze_bronze_raw_tickets`

### 3.2 Silver layer (domain models)

Silver assets build clean domain tables suitable for BI and analytics.

- `dim_customers`  
  - Built from `bronze_raw_customers`.  
  - Renames `id` → `customer_id`, capitalizes first/last names, keeps customer attributes.

- `dim_orders`  
  - Built from `bronze_raw_orders`.  
  - Renames `id` → `order_id`, `user_id` → `customer_id`.  
  - Converts `order_date` to a timestamp.

- `fact_order_items`  
  - Built from `bronze_raw_items` and `bronze_raw_products`.  
  - Renames `id` → `order_item_id` (items) and `id` → `product_id` (products).  
  - Ensures `order_id`, `product_id`, `quantity`, `amount` exist.  
  - Joins items to products on `product_id`.  
  - Computes `line_total = amount * quantity`.

- `tickets`  
  - Built from `bronze_raw_tickets`.  
  - Normalizes keys and names:
    - ticket id: `id` → `ticket_id`
    - order: `order_id` or `orderID` → `order_id`
    - created time: `created_at` → `ticket_created_at`
    - status: `status` → `ticket_status`
    - channel: `channel` → `ticket_channel`
  - Validates presence of `ticket_id` and `order_id`.  
  - Parses `ticket_created_at` as a timestamp if present.

Silver assets are stored as DuckDB tables:

- `silver_dim_customers`
- `silver_dim_orders`
- `silver_fact_order_items`
- `silver_tickets`

### 3.3 Gold layer (analytics)

Gold assets expose business‑ready metrics.

- `gold_avg_order_value`  
  - Depends on `dim_orders` and `fact_order_items`.  
  - Aggregates `line_total` from `silver_fact_order_items` to compute `order_total` per `order_id`.  
  - Left‑joins to `silver_dim_orders` on `order_id`.  
  - Fills missing `order_total` with 0.  
  - Computes `overall_avg_order_value` as the mean of `order_total`.  
  - Outputs one row per order with:

    - `order_id`
    - `customer_id`
    - `order_date`
    - `order_total`
    - `overall_avg_order_value`

- `gold_tickets_per_order`  
  - Depends on `dim_orders` and `tickets`.  
  - Groups `silver_tickets` by `order_id` and counts `ticket_id` as `ticket_count`.  
  - Left‑joins onto `silver_dim_orders` on `order_id`.  
  - Fills missing `ticket_count` with 0.  
  - Produces one row per order with:

    - `order_id`
    - `customer_id`
    - `order_date`
    - `ticket_count`

Gold assets are stored as:

- `gold_gold_avg_order_value`
- `gold_gold_tickets_per_order`

---

## 4. Environment setup

From the project root (`dagster_jaffle_restaurant`):

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip
pip install dagster dagster-webserver duckdb pandas requests pylint python-dotenv

---

## 5. Running Dagster locally

1. Ensure the virtual environment is active:

source .venv/bin/activate


2. Start the Dagster development server:

dagster dev -m dagster_jaffle_restaurant.defs


3. Open the UI (usually at `http://127.0.0.1:3000`).

4. In the **Lineage** or **Assets** view:

- To run the entire pipeline, materialize `gold_avg_order_value` and `gold_tickets_per_order`.  
  Dagster will automatically materialize all upstream bronze and silver assets (CSV ingestion, tickets ingestion, domain models).

5. After runs complete, you can inspect:

- Run details and logs.  
- Asset materialization previews (sample rows) in the UI.

---

## 6. Inspecting data in DuckDB

All assets are stored in `data/warehouse.duckdb` via the custom DuckDB IO manager.

Start a Python REPL:

source .venv/bin/activate
python3


### 6.1 Show tables

import duckdb

con = duckdb.connect("data/warehouse.duckdb")
print(con.execute("SHOW TABLES").fetchdf())
con.close()


### 6.2 View gold_tickets_per_order

import duckdb

con = duckdb.connect("data/warehouse.duckdb")

df = con.execute(
"SELECT * FROM gold_gold_tickets_per_order ORDER BY order_id LIMIT 20"
).fetchdf()
print(df)

con.close()


### 6.3 Average tickets per order

import duckdb

con = duckdb.connect("data/warehouse.duckdb")

avg_tickets = con.execute(
"SELECT AVG(ticket_count) AS avg_tickets_per_order "
"FROM gold_gold_tickets_per_order"
).fetchdf()
print(avg_tickets)

con.close()


---

## 7. Code quality and linting

Pylint configuration is provided in `.pylintrc`.

To run linting:

source .venv/bin/activate
pylint dagster_jaffle_restaurant


This project uses small inline comments and simple docstrings to keep lint warnings manageable while keeping the code readable.

---

Thanks for reading!!
