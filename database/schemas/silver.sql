
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver."campaign_spend" (
    "spend_date" TEXT,
    "campaign_id" TEXT,
    "channel" TEXT,
    "spend_amount" DOUBLE PRECISION,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."cities" (
    "city_id" TEXT NOT NULL,
    "city_name" TEXT,
    "country" TEXT,
    "loaded_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."city_reference" (
    "city_id" TEXT,
    "country" TEXT,
    "city_name" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."customer_addresses" (
    "city_id" TEXT,
    "address_id" TEXT,
    "customer_id" TEXT,
    "valid_to_utc" TEXT,
    "source_row_id" TEXT,
    "valid_from_utc" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."customer_profiles" (
    "city_id" TEXT,
    "customer_id" TEXT,
    "valid_to_utc" TEXT,
    "source_row_id" TEXT,
    "valid_from_utc" TEXT,
    "customer_segment" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."customers" (
    "city_id" TEXT,
    "customer_id" TEXT,
    "source_row_id" TEXT,
    "created_at_utc" TEXT,
    "customer_segment" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."inventory_snapshots" (
    "product_id" TEXT,
    "location_id" TEXT,
    "snapshot_date" TEXT,
    "available_quantity" BIGINT,
    "reserved_quantity" BIGINT,
    "unit_cost" DOUBLE PRECISION,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."order_items" (
    "order_id" TEXT,
    "quantity" BIGINT,
    "product_id" TEXT,
    "unit_price" DOUBLE PRECISION,
    "order_item_id" TEXT,
    "source_row_id" TEXT,
    "item_discount_amount" DOUBLE PRECISION,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."order_promotions" (
    "order_id" TEXT,
    "promotion_id" TEXT,
    "source_row_id" TEXT,
    "discount_amount" BIGINT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."orders" (
    "status" TEXT,
    "order_id" TEXT,
    "store_id" TEXT,
    "customer_id" TEXT,
    "sales_channel" TEXT,
    "source_row_id" TEXT,
    "ordered_at_utc" TEXT,
    "updated_at_utc" TEXT,
    "shipping_revenue" DOUBLE PRECISION,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."payment_events" (
    "event_id" TEXT,
    "event_type" TEXT,
    "ingested_at_utc" TIMESTAMPTZ,
    "occurred_at_utc" TEXT,
    "payload.amount" DOUBLE PRECISION,
    "payload.order_id" TEXT,
    "payload.payment_id" TEXT
);

CREATE TABLE IF NOT EXISTS silver."product_categories" (
    "product_id" TEXT,
    "category_id" TEXT,
    "valid_to_utc" TEXT,
    "category_name" TEXT,
    "source_row_id" TEXT,
    "valid_from_utc" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."products" (
    "sku" TEXT,
    "product_id" TEXT,
    "unit_price" DOUBLE PRECISION,
    "product_name" TEXT,
    "source_row_id" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."promotions" (
    "end_date" TEXT,
    "start_date" TEXT,
    "promotion_id" TEXT,
    "discount_rate" DOUBLE PRECISION,
    "source_row_id" TEXT,
    "promotion_code" TEXT,
    "promotion_type" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."refund_events" (
    "event_id" TEXT,
    "event_type" TEXT,
    "ingested_at_utc" TIMESTAMPTZ,
    "occurred_at_utc" TEXT,
    "payload.amount" DOUBLE PRECISION,
    "payload.order_id" TEXT,
    "payload.refund_id" TEXT
);

CREATE TABLE IF NOT EXISTS silver."return_events" (
    "event_id" TEXT,
    "event_type" TEXT,
    "ingested_at_utc" TIMESTAMPTZ,
    "occurred_at_utc" TEXT,
    "payload.order_id" TEXT,
    "payload.return_id" TEXT
);

CREATE TABLE IF NOT EXISTS silver."sales_channels" (
    "channel_id" TEXT,
    "channel_name" TEXT,
    "source_row_id" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."stores" (
    "city_id" TEXT,
    "store_id" TEXT,
    "store_name" TEXT,
    "location_type" TEXT,
    "source_row_id" TEXT,
    "ingested_at_utc" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver."support_events" (
    "event_id" TEXT,
    "event_type" TEXT,
    "ingested_at_utc" TIMESTAMPTZ,
    "occurred_at_utc" TEXT,
    "payload.reason" TEXT,
    "payload.order_id" TEXT,
    "payload.ticket_id" TEXT,
    "payload.customer_id" TEXT
);

CREATE TABLE IF NOT EXISTS silver."web_events" (
    "event_id" TEXT,
    "event_type" TEXT,
    "ingested_at_utc" TIMESTAMPTZ,
    "occurred_at_utc" TEXT,
    "payload.channel" TEXT,
    "payload.order_id" TEXT,
    "payload.session_id" TEXT,
    "payload.campaign_id" TEXT,
    "payload.customer_id" TEXT
);
