from datetime import datetime

import pendulum
import psycopg2

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


LOCAL_DATABASE_URL = "postgresql://postgres:postgres@host.docker.internal:5432/retail_analytics"


def validate_raw():
    from pathlib import Path

    raw_dir = Path("/opt/airflow/data/raw")

    required_files = [
        "events/payment_events.json",
        "events/refund_events.json",
        "events/return_events.json",
        "events/support_events.json",
        "events/web_events.json",
        "inventory/inventory_snapshots.csv",
        "operational/customers.json",
        "operational/customer_addresses.json",
        "operational/customer_profiles.json",
        "operational/orders.json",
        "operational/order_items.json",
        "operational/order_promotions.json",
        "operational/products.json",
        "operational/product_categories.json",
        "operational/promotions.json",
        "operational/sales_channels.json",
        "operational/stores.json",
        "reference/campaign_spend.csv",
        "reference/city_reference.json",
    ]

    missing_files = [
        str(raw_dir / file)
        for file in required_files
        if not (raw_dir / file).exists()
    ]

    if missing_files:
        raise FileNotFoundError(
            "Raw validation gagal. File berikut tidak ditemukan:\n"
            + "\n".join(missing_files)
        )

    print(f"Raw validation passed. {len(required_files)} required files found.")


def initialize_schemas():
    conn = psycopg2.connect(LOCAL_DATABASE_URL)
    conn.autocommit = True

    try:
        for schema_file in [
            "/opt/airflow/database/schemas/bronze.sql",
            "/opt/airflow/database/schemas/silver.sql",
        ]:
            with open(schema_file, "r", encoding="utf-8") as f:
                sql = f.read()

            with conn.cursor() as cur:
                cur.execute(sql)

            print(f"Schema initialized: {schema_file}")
    finally:
        conn.close()


def quality_gate():
    conn = psycopg2.connect(LOCAL_DATABASE_URL)

    checks = {
        "bronze": "bronze",
        "silver": "silver",
    }

    try:
        with conn.cursor() as cur:
            for layer, schema in checks.items():
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """,
                    (schema,),
                )

                tables = [row[0] for row in cur.fetchall()]

                if not tables:
                    raise ValueError(
                        f"Quality gate gagal: schema {schema} tidak memiliki tabel."
                    )

                for table in tables:
                    cur.execute(
                        f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                    )
                    count = cur.fetchone()[0]

                    print(f"{schema}.{table}: {count} rows")

                    if count == 0:
                        raise ValueError(
                            f"Quality gate gagal: {schema}.{table} kosong."
                        )

        print("Quality gate PASSED.")
    finally:
        conn.close()


def validate_gold():
    conn = psycopg2.connect(LOCAL_DATABASE_URL)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'gold'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )

            tables = [row[0] for row in cur.fetchall()]

            if not tables:
                raise ValueError("Gold validation gagal: schema gold tidak memiliki tabel.")

            for table in tables:
                cur.execute(
                    f'SELECT COUNT(*) FROM "gold"."{table}"'
                )
                count = cur.fetchone()[0]

                print(f"gold.{table}: {count} rows")

                if count == 0:
                    raise ValueError(
                        f"Gold validation gagal: gold.{table} kosong."
                    )

        print("Gold validation PASSED.")
    finally:
        conn.close()


with DAG(
    dag_id="omnichannel_retail_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["milestone1", "retail", "etl"],
) as dag:

    validate_raw_task = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw,
    )

    initialize_schemas_task = PythonOperator(
        task_id="initialize_schemas",
        python_callable=initialize_schemas,
    )

    build_bronze = BashOperator(
        task_id="build_bronze",
        bash_command="python /opt/airflow/pipeline/bronze/build_bronze.py",
        env={
            "LOCAL_DATABASE_URL": LOCAL_DATABASE_URL,
            "PIPELINE_RUN_ID": "{{ ts_nodash }}",
        },
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command="python /opt/airflow/pipeline/silver/build_silver.py",
        env={
            "LOCAL_DATABASE_URL": LOCAL_DATABASE_URL,
            "PIPELINE_RUN_ID": "{{ ts_nodash }}",
        },
    )

    quality_gate_task = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command="python /opt/airflow/pipeline/gold/build_gold.py",
        env={
            "LOCAL_DATABASE_URL": LOCAL_DATABASE_URL,
            "PIPELINE_RUN_ID": "{{ ts_nodash }}",
        },
    )

    validate_gold_task = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
    )

    (
        validate_raw_task
        >> initialize_schemas_task
        >> build_bronze
        >> build_silver
        >> quality_gate_task
        >> build_gold
        >> validate_gold_task
    )

