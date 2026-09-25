"""Build the five Gold contract tables.

The Gold contract is fixed. This script follows the existing notebook logic,
while preserving the validated gold.order_360 schema and de-duplicating
payment/refund events by event/business identifiers before aggregation.
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
pipeline_run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

GOLD_SCHEMA_FILE = BASE_DIR / "database" / "schemas" / "gold.sql"

def build_order_360(conn):
    return pd.read_sql(text("""
    WITH orders_dedup AS (
        SELECT * FROM (
            SELECT o.*, ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY updated_at_utc DESC, ingested_at_utc DESC, source_row_id
            ) rn
            FROM silver.orders o
        ) x WHERE rn = 1
    ),
    items AS (
        SELECT order_id,
               COUNT(DISTINCT order_item_id) item_count,
               SUM(quantity) unit_quantity,
               SUM(quantity * unit_price) gross_merchandise_value,
               SUM(item_discount_amount) discount_amount
        FROM silver.order_items GROUP BY order_id
    ),
    pay_events AS (
        SELECT DISTINCT event_id, "payload.order_id" AS order_id,
               "payload.amount"::numeric AS amount, event_type, occurred_at_utc
        FROM silver.payment_events
    ),
    payments AS (
        SELECT order_id,
               SUM(amount) FILTER (WHERE event_type='PAYMENT_CAPTURED') AS captured_payment_amount
        FROM pay_events GROUP BY order_id
    ),
    payment_status AS (
    SELECT order_id, event_type AS payment_status FROM (
        SELECT order_id, event_type, occurred_at_utc, event_id,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id
                   ORDER BY occurred_at_utc DESC, event_id DESC
               ) rn
        FROM pay_events
        ) x WHERE rn=1
    ),
    refund_events_dedup AS (
        SELECT DISTINCT event_id, "payload.order_id" order_id,
               "payload.refund_id" refund_id, "payload.amount"::numeric amount, event_type
        FROM silver.refund_events
    ),
    refunds AS (
        SELECT order_id, SUM(amount) refunded_amount FROM (
            SELECT order_id, refund_id, MAX(amount) amount
            FROM refund_events_dedup
            WHERE event_type='REFUND_COMPLETED'
            GROUP BY order_id, refund_id
        ) x GROUP BY order_id
    ),
    returns AS (
        SELECT order_id, event_type return_status FROM (
            SELECT "payload.order_id" order_id, event_type, occurred_at_utc, event_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY "payload.order_id"
                       ORDER BY occurred_at_utc DESC, event_id DESC
                   ) rn
            FROM silver.return_events
        ) x WHERE rn=1
    ),
    promos AS (
        SELECT order_id, COUNT(DISTINCT promotion_id) promotion_count
        FROM silver.order_promotions GROUP BY order_id
    ),
    ranked AS (
        SELECT order_id, customer_id,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id ORDER BY ordered_at_utc, order_id
               ) customer_order_rank
        FROM orders_dedup
    )
    SELECT o.order_id, o.customer_id, o.ordered_at_utc::date order_date,
           o.sales_channel, o.store_id,
           COALESCE(i.gross_merchandise_value,0) gross_merchandise_value,
           COALESCE(i.discount_amount,0) discount_amount,
           COALESCE(o.shipping_revenue,0) shipping_revenue,
           COALESCE(p.captured_payment_amount,0) captured_payment_amount,
           COALESCE(r.refunded_amount,0) refunded_amount,
           COALESCE(i.gross_merchandise_value,0)-COALESCE(i.discount_amount,0)
             +COALESCE(o.shipping_revenue,0)-COALESCE(r.refunded_amount,0) net_revenue,
           COALESCE(i.item_count,0) item_count,
           COALESCE(i.unit_quantity,0) unit_quantity,
           o.status order_status,
           COALESCE(ps.payment_status,'NO_PAYMENT') payment_status,
           COALESCE(rt.return_status,'NO_RETURN') return_status,
           COALESCE(pr.promotion_count,0) promotion_count,
           (rk.customer_order_rank=1) first_order_flag
    FROM orders_dedup o
    LEFT JOIN items i ON i.order_id=o.order_id
    LEFT JOIN payments p ON p.order_id=o.order_id
    LEFT JOIN payment_status ps ON ps.order_id=o.order_id
    LEFT JOIN refunds r ON r.order_id=o.order_id
    LEFT JOIN returns rt ON rt.order_id=o.order_id
    LEFT JOIN promos pr ON pr.order_id=o.order_id
    LEFT JOIN ranked rk ON rk.order_id=o.order_id
    ORDER BY o.order_id
    """), conn)


def build_other_gold(conn):
    customer_daily = pd.read_sql(text("""
    WITH base AS (
        SELECT customer_id, order_date metric_date, COUNT(*) order_count,
               SUM(unit_quantity) unit_quantity, SUM(gross_merchandise_value) gross_revenue,
               SUM(net_revenue) net_revenue, SUM(refunded_amount) refund_amount,
               COUNT(*) FILTER (WHERE return_status <> 'NO_RETURN') return_count,
               BOOL_OR(first_order_flag) new_customer_flag,
               BOOL_OR(NOT first_order_flag) repeat_customer_flag
        FROM _tmp_order_360 GROUP BY customer_id, order_date
    ),
    channels AS (
        SELECT customer_id, order_date metric_date, sales_channel,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id, order_date
                   ORDER BY COUNT(*) DESC, sales_channel
               ) rn
        FROM _tmp_order_360 GROUP BY customer_id, order_date, sales_channel
    ),
    support AS (
        SELECT "payload.customer_id" customer_id, occurred_at_utc::date metric_date,
               COUNT(DISTINCT COALESCE("payload.ticket_id", event_id)) support_contacts
        FROM silver.support_events
        WHERE event_type='SUPPORT_TICKET_CREATED'
        GROUP BY "payload.customer_id", occurred_at_utc::date
    )
    SELECT b.customer_id,b.metric_date,b.order_count,b.unit_quantity,b.gross_revenue,
           b.net_revenue,b.refund_amount,b.return_count,COALESCE(s.support_contacts,0) support_contacts,
           c.sales_channel active_channel,b.new_customer_flag,b.repeat_customer_flag
    FROM base b
    LEFT JOIN channels c ON c.customer_id=b.customer_id AND c.metric_date=b.metric_date AND c.rn=1
    LEFT JOIN support s ON s.customer_id=b.customer_id AND s.metric_date=b.metric_date
    ORDER BY b.customer_id,b.metric_date
    """), conn)

    product_daily = pd.read_sql(text("""
    WITH sales AS (
        SELECT oi.product_id,o.order_date metric_date,SUM(oi.quantity) units_sold,
               COUNT(DISTINCT oi.order_id) order_count,
               SUM(oi.quantity*oi.unit_price) gross_revenue
        FROM _tmp_order_360 o JOIN silver.order_items oi ON oi.order_id=o.order_id
        GROUP BY oi.product_id,o.order_date
    ),
    product_dates AS (
    SELECT product_id,
           snapshot_date::date AS metric_date,
           SUM(available_quantity) available_inventory,
           SUM(reserved_quantity) reserved_inventory
    FROM silver.inventory_snapshots
    GROUP BY product_id, snapshot_date::date
    ),
    cats AS (
        SELECT product_id,category_name,valid_from_utc,valid_to_utc,
               ROW_NUMBER() OVER (
                   PARTITION BY product_id,valid_from_utc
                   ORDER BY source_row_id DESC
               ) rn
        FROM silver.product_categories
    ),
    cat_by_day AS (
        SELECT d.product_id,d.metric_date,c.category_name
        FROM product_dates d LEFT JOIN cats c ON c.product_id=d.product_id
          AND d.metric_date >= c.valid_from_utc::date
          AND (c.valid_to_utc IS NULL OR d.metric_date < c.valid_to_utc::date)
          AND c.rn=1
    ),
    promo AS (
        SELECT oi.product_id,o.order_date metric_date,COUNT(DISTINCT op.promotion_id) promotion_count
        FROM silver.order_items oi JOIN _tmp_order_360 o ON o.order_id=oi.order_id
        LEFT JOIN silver.order_promotions op ON op.order_id=oi.order_id
        GROUP BY oi.product_id,o.order_date
    ),
    refund_units AS (
        SELECT oi.product_id,o.order_date metric_date,SUM(oi.quantity) refunded_units
        FROM silver.order_items oi JOIN _tmp_order_360 o ON o.order_id=oi.order_id
        WHERE o.refunded_amount > 0
        GROUP BY oi.product_id,o.order_date
    ),
    discounts AS (
        SELECT oi.product_id,o.order_date metric_date,SUM(oi.item_discount_amount) discount_amount
        FROM silver.order_items oi JOIN _tmp_order_360 o ON o.order_id=oi.order_id
        GROUP BY oi.product_id,o.order_date
    ),
    refund_amounts AS (
        SELECT oi.product_id,o.order_date metric_date,
               SUM(CASE WHEN o.refunded_amount>0 THEN oi.quantity*oi.unit_price ELSE 0 END) refund_amount
        FROM silver.order_items oi JOIN _tmp_order_360 o ON o.order_id=oi.order_id
        GROUP BY oi.product_id,o.order_date
    ),
    grid AS (
        SELECT product_id,metric_date FROM product_dates
        UNION
        SELECT product_id,metric_date FROM sales
    )
    SELECT g.product_id,g.metric_date,c.category_name active_category,
           COALESCE(s.units_sold,0) units_sold,COALESCE(s.order_count,0) order_count,
           COALESCE(s.gross_revenue,0) gross_revenue,
           COALESCE(s.gross_revenue,0)-COALESCE(d.discount_amount,0)-COALESCE(ra.refund_amount,0) net_revenue,
           COALESCE(ru.refunded_units,0) refunded_units,
           pd.available_inventory,pd.reserved_inventory,
           CASE WHEN pd.available_inventory IS NULL THEN NULL ELSE pd.available_inventory <= 0 END stockout_flag,
           COALESCE(p.promotion_count,0) promotion_count
    FROM grid g
    LEFT JOIN sales s ON s.product_id=g.product_id AND s.metric_date=g.metric_date
    LEFT JOIN cat_by_day c ON c.product_id=g.product_id AND c.metric_date=g.metric_date
    LEFT JOIN product_dates pd ON pd.product_id=g.product_id AND pd.metric_date=g.metric_date
    LEFT JOIN refund_units ru ON ru.product_id=g.product_id AND ru.metric_date=g.metric_date
    LEFT JOIN promo p ON p.product_id=g.product_id AND p.metric_date=g.metric_date
    LEFT JOIN discounts d ON d.product_id=g.product_id AND d.metric_date=g.metric_date
    LEFT JOIN refund_amounts ra ON ra.product_id=g.product_id AND ra.metric_date=g.metric_date
    ORDER BY g.product_id,g.metric_date
    """), conn)

    channel_campaign_daily = pd.read_sql(text("""
    WITH web AS (
        SELECT DISTINCT event_id,"payload.order_id" order_id,"payload.customer_id" customer_id,
               "payload.campaign_id" campaign_id,"payload.channel" sales_channel,occurred_at_utc
        FROM silver.web_events WHERE "payload.campaign_id" IS NOT NULL
    ),
    attr_ranked AS (
        SELECT *,ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY occurred_at_utc DESC,event_id DESC) rn
        FROM web WHERE order_id IS NOT NULL
    ),
    attr AS (
        SELECT o.order_date metric_date,a.sales_channel,a.campaign_id,a.order_id,o.customer_id,
               o.gross_merchandise_value gross_revenue,o.net_revenue,o.refunded_amount refunds
        FROM attr_ranked a JOIN _tmp_order_360 o ON o.order_id=a.order_id WHERE a.rn=1
    ),
    agg AS (
        SELECT metric_date,sales_channel,campaign_id,COUNT(DISTINCT order_id) attributed_orders,
               COUNT(DISTINCT customer_id) attributed_customers,SUM(gross_revenue) gross_revenue,
               SUM(net_revenue) net_revenue,SUM(refunds) refunds
        FROM attr GROUP BY metric_date,sales_channel,campaign_id
    ),
    spend AS (
    SELECT spend_date::date metric_date,
           channel sales_channel,
           campaign_id,
           SUM(spend_amount) campaign_spend
        FROM silver.campaign_spend GROUP BY spend_date,channel,campaign_id
    ),
    sessions AS (
        SELECT occurred_at_utc::date metric_date,"payload.channel" sales_channel,
               "payload.campaign_id" campaign_id,COUNT(DISTINCT "payload.session_id") sessions
        FROM silver.web_events WHERE "payload.campaign_id" IS NOT NULL
        GROUP BY occurred_at_utc::date,"payload.channel","payload.campaign_id"
    ),
    grid AS (
        SELECT metric_date,sales_channel,campaign_id FROM spend
        UNION
        SELECT metric_date,sales_channel,campaign_id FROM agg
    )
    SELECT g.metric_date,g.sales_channel,g.campaign_id,COALESCE(s.campaign_spend,0) campaign_spend,
           COALESCE(a.attributed_orders,0) attributed_orders,COALESCE(a.attributed_customers,0) attributed_customers,
           COALESCE(a.gross_revenue,0) gross_revenue,COALESCE(a.net_revenue,0) net_revenue,
           COALESCE(a.refunds,0) refunds,
           CASE WHEN COALESCE(s.campaign_spend,0)=0 THEN NULL ELSE COALESCE(a.net_revenue,0)/s.campaign_spend END roas,
           CASE WHEN COALESCE(se.sessions,0)=0 THEN NULL ELSE COALESCE(a.attributed_orders,0)::numeric/se.sessions END conversion_rate
    FROM grid g LEFT JOIN spend s USING(metric_date,sales_channel,campaign_id)
    LEFT JOIN agg a USING(metric_date,sales_channel,campaign_id)
    LEFT JOIN sessions se USING(metric_date,sales_channel,campaign_id)
    ORDER BY g.metric_date,g.sales_channel,g.campaign_id
    """), conn)

    customer_daily.to_sql("_tmp_customer_daily", conn, if_exists="replace", index=False)
    product_daily.to_sql("_tmp_product_daily", conn, if_exists="replace", index=False)

    executive_kpis_daily = pd.read_sql(text("""
    WITH orders AS (
        SELECT order_date metric_date,COUNT(*) total_orders,SUM(gross_merchandise_value) gross_revenue,
               SUM(net_revenue) net_revenue,AVG(gross_merchandise_value) average_order_value,
               COUNT(*) FILTER (WHERE refunded_amount>0) refunded_orders,
               COUNT(*) FILTER (WHERE payment_status='PAYMENT_CAPTURED') captured_orders,
               COUNT(*) FILTER (WHERE return_status<>'NO_RETURN') returned_orders,
               COUNT(*) FILTER (WHERE return_status<>'NO_RETURN' OR order_status IN ('COMPLETED','RETURNED','CANCELLED')) resolved_orders
        FROM _tmp_order_360 GROUP BY order_date
    ),
    cust AS (
        SELECT metric_date,COUNT(DISTINCT customer_id) active_customers,
               AVG(CASE WHEN repeat_customer_flag THEN 1.0 ELSE 0.0 END) repeat_customer_rate,
               SUM(support_contacts) support_contacts
        FROM _tmp_customer_daily GROUP BY metric_date
    ),
    stock AS (
        SELECT metric_date,AVG(CASE WHEN stockout_flag THEN 1.0 ELSE 0.0 END) stockout_rate
        FROM _tmp_product_daily WHERE stockout_flag IS NOT NULL GROUP BY metric_date
    )
    SELECT o.metric_date,o.total_orders,o.gross_revenue,o.net_revenue,o.average_order_value,
           CASE WHEN o.captured_orders=0 THEN 0 ELSE o.refunded_orders::numeric/o.captured_orders END refund_rate,
           CASE WHEN o.resolved_orders=0 THEN 0 ELSE o.returned_orders::numeric/o.resolved_orders END return_rate,
           COALESCE(c.repeat_customer_rate,0) repeat_customer_rate,COALESCE(s.stockout_rate,0) stockout_rate,
           COALESCE(c.active_customers,0) active_customers,
           CASE WHEN o.total_orders=0 THEN 0 ELSE COALESCE(c.support_contacts,0)::numeric/o.total_orders END support_contact_rate,
           NOW() AT TIME ZONE 'UTC' data_freshness_utc
    FROM orders o LEFT JOIN cust c USING(metric_date) LEFT JOIN stock s USING(metric_date)
    ORDER BY o.metric_date
    """), conn)

    return customer_daily, product_daily, channel_campaign_daily, executive_kpis_daily


def main() -> None:
    started = datetime.now(timezone.utc)
    run_id = os.getenv("PIPELINE_RUN_ID") or started.strftime("%Y%m%d%H%M%S")
    print(f"Pipeline run ID: {run_id}")
    with engine.begin() as conn:
        conn.execute(text(GOLD_SCHEMA_FILE.read_text(encoding="utf-8")))
        try:
            order_360 = build_order_360(conn)
            order_360["item_count"] = order_360["item_count"].fillna(0).astype(int)
            order_360["unit_quantity"] = order_360["unit_quantity"].fillna(0).astype(int)
            order_360["promotion_count"] = order_360["promotion_count"].fillna(0).astype(int)
            order_360["pipeline_run_id"] = run_id

            for table in ["order_360", "customer_daily", "product_daily", "channel_campaign_daily", "executive_kpis_daily"]:
                conn.execute(text(f"TRUNCATE TABLE gold.{table}"))

            order_360.to_sql("_tmp_order_360", conn, if_exists="replace", index=False)
            customer_daily, product_daily, channel_campaign_daily, executive_kpis_daily = build_other_gold(conn)

            for c in ["order_count", "unit_quantity", "return_count", "support_contacts"]:
                customer_daily[c] = customer_daily[c].fillna(0).astype(int)
            customer_daily["pipeline_run_id"] = run_id

            for c in ["units_sold", "order_count", "refunded_units", "promotion_count"]:
                product_daily[c] = product_daily[c].fillna(0).astype(int)
            product_daily["available_inventory"] = product_daily["available_inventory"].astype("Int64")
            product_daily["reserved_inventory"] = product_daily["reserved_inventory"].astype("Int64")
            product_daily["pipeline_run_id"] = run_id

            channel_campaign_daily["pipeline_run_id"] = run_id
            executive_kpis_daily["pipeline_run_id"] = run_id

            order_360.to_sql("order_360", conn, schema="gold", if_exists="append", index=False, method="multi")
            customer_daily.to_sql("customer_daily", conn, schema="gold", if_exists="append", index=False, method="multi")
            product_daily.to_sql("product_daily", conn, schema="gold", if_exists="append", index=False, method="multi")
            channel_campaign_daily.to_sql("channel_campaign_daily", conn, schema="gold", if_exists="append", index=False, method="multi")
            executive_kpis_daily.to_sql("executive_kpis_daily", conn, schema="gold", if_exists="append", index=False, method="multi")

            conn.execute(text("DROP TABLE IF EXISTS _tmp_order_360"))
            conn.execute(text("DROP TABLE IF EXISTS _tmp_customer_daily"))
            conn.execute(text("DROP TABLE IF EXISTS _tmp_product_daily"))
            total = sum(map(len, [order_360, customer_daily, product_daily, channel_campaign_daily, executive_kpis_daily]))
        except Exception as exc:
            raise

    print("Gold build complete: 5 tables")
    print(f"order_360: {len(order_360):,}")
    print(f"customer_daily: {len(customer_daily):,}")
    print(f"product_daily: {len(product_daily):,}")
    print(f"channel_campaign_daily: {len(channel_campaign_daily):,}")
    print(f"executive_kpis_daily: {len(executive_kpis_daily):,}")


if __name__ == "__main__":
    main()
