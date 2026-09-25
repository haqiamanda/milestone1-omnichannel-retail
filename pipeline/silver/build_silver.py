"""Production Silver builder.

Schema DDL lives in database/schemas/silver.sql.
Transformation logic follows the validated Silver notebook.
"""

from datetime import datetime, timezone
from pathlib import Path
import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(find_dotenv())

DB_URL = os.getenv("LOCAL_DATABASE_URL")
if not DB_URL:
    raise RuntimeError("LOCAL_DATABASE_URL is not set")
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

engine = create_engine(DB_URL)
BASE_DIR = Path(__file__).resolve().parents[2]
SCHEMA_FILE = BASE_DIR / "database" / "schemas" / "silver.sql"


def initialize_schema(conn):
    conn.execute(text(SCHEMA_FILE.read_text(encoding="utf-8")))


def bronze_to_silver(conn, table_name):
    columns = pd.read_sql(
        text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='bronze' AND table_name=:table_name
        ORDER BY ordinal_position
        """), conn, params={"table_name": table_name}
    )["column_name"].tolist()

    if not columns:
        raise ValueError(f"bronze.{table_name} does not exist")

    if "raw" in columns:
        src = pd.read_sql(f'SELECT raw, timestamp FROM bronze."{table_name}"', conn)
        out = pd.json_normalize(src["raw"])
        out["ingested_at_utc"] = pd.to_datetime(src["timestamp"], utc=True)
    else:
        out = pd.read_sql(f'SELECT * FROM bronze."{table_name}"', conn)

    conn.execute(text(f'TRUNCATE TABLE silver."{table_name}"'))
    out.to_sql(table_name, conn, schema="silver", if_exists="append", index=False)
    print(f"OK silver.{table_name}: {len(out):,}")
    return len(out)


def main():
    started = datetime.now(timezone.utc)
    run_id = os.getenv("PIPELINE_RUN_ID") or started.strftime("%Y%m%d%H%M%S")
    total = 0
    with engine.begin() as conn:
        initialize_schema(conn)
        try:
            tables = pd.read_sql(
                text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='bronze' AND table_type='BASE TABLE'
                ORDER BY table_name
                """), conn
            )["table_name"].tolist()
            for table in tables:
                total += bronze_to_silver(conn, table)
        except Exception as exc:
            raise
    print(f"Silver build complete. run_id={run_id}, rows={total:,}")


if __name__ == "__main__":
    main()
