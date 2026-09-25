"""Production Bronze loader.

Schema DDL lives in database/schemas/bronze.sql.
This script only loads data into the already-defined Bronze tables.
"""

from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import TIMESTAMP

load_dotenv(find_dotenv())

DB_URL = os.getenv("LOCAL_DATABASE_URL")
if not DB_URL:
    raise RuntimeError("LOCAL_DATABASE_URL is not set")
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

engine = create_engine(DB_URL)
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
SCHEMA_FILE = BASE_DIR / "database" / "schemas" / "bronze.sql"

JSON_FILES = {
    "customers": "operational/customers.json",
    "customer_profiles": "operational/customer_profiles.json",
    "customer_addresses": "operational/customer_addresses.json",
    "products": "operational/products.json",
    "product_categories": "operational/product_categories.json",
    "stores": "operational/stores.json",
    "sales_channels": "operational/sales_channels.json",
    "promotions": "operational/promotions.json",
    "orders": "operational/orders.json",
    "order_items": "operational/order_items.json",
    "order_promotions": "operational/order_promotions.json",
    "payment_events": "events/payment_events.json",
    "refund_events": "events/refund_events.json",
    "return_events": "events/return_events.json",
    "support_events": "events/support_events.json",
    "web_events": "events/web_events.json",
    "city_reference": "reference/city_reference.json",
}
CSV_FILES = {
    "inventory_snapshots": "inventory/inventory_snapshots.csv",
    "campaign_spend": "reference/campaign_spend.csv",
}


def initialize_schema(conn):
    conn.execute(text(SCHEMA_FILE.read_text(encoding="utf-8")))


def load_json(conn, run_id, file_path, table_name, loaded_at):
    path = RAW_DIR / file_path

    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_json(path)

    # Convert Pandas NaN/NaT to Python None so JSONB receives valid JSON null
    records = df.astype(object).where(pd.notna(df), None).to_dict(orient="records")

    out = pd.DataFrame({
        "raw": records,
        "timestamp": loaded_at
    })

    # Full rebuild makes repeated DAG runs deterministic for this milestone.
    conn.execute(text(f"TRUNCATE TABLE bronze.{table_name}"))

    out.to_sql(
        table_name,
        conn,
        schema="bronze",
        if_exists="append",
        index=False,
        dtype={
            "raw": JSONB,
            "timestamp": TIMESTAMP(timezone=True)
        }
    )

    return len(out)


def load_csv(conn, file_path, table_name, loaded_at):
    path = RAW_DIR / file_path
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    df["timestamp"] = loaded_at
    conn.execute(text(f'TRUNCATE TABLE bronze."{table_name}"'))
    df.to_sql(table_name, conn, schema="bronze", if_exists="append", index=False)
    return len(df)


def main():
    started = datetime.now(timezone.utc)
    run_id = os.getenv("PIPELINE_RUN_ID") or started.strftime("%Y%m%d%H%M%S")
    total = 0
    with engine.begin() as conn:
        initialize_schema(conn)
        try:
            for table, rel in JSON_FILES.items():
                n = load_json(conn, run_id, rel, table, started)
                total += n
                print(f"OK bronze.{table}: {n:,}")
            for table, rel in CSV_FILES.items():
                n = load_csv(conn, rel, table, started)
                total += n
                print(f"OK bronze.{table}: {n:,}")
        except Exception as exc:
            raise
    print(f"Bronze build complete. run_id={run_id}, rows={total:,}")


if __name__ == "__main__":
    main()
