

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze."campaign_spend" (
    "spend_date" TEXT,
    "campaign_id" TEXT,
    "channel" TEXT,
    "spend_amount" DOUBLE PRECISION,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."city_reference" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."customer_addresses" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."customer_profiles" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."customers" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."inventory_snapshots" (
    "product_id" TEXT,
    "location_id" TEXT,
    "snapshot_date" TEXT,
    "available_quantity" BIGINT,
    "reserved_quantity" BIGINT,
    "unit_cost" DOUBLE PRECISION,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."order_items" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."order_promotions" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."orders" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."payment_events" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."product_categories" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."products" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."promotions" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."refund_events" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."return_events" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."sales_channels" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."stores" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."support_events" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bronze."web_events" (
    "raw" JSONB,
    "timestamp" TIMESTAMPTZ
);
