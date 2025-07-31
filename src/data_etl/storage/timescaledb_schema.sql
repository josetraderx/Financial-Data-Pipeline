-- TimescaleDB Schema for Multi-Timeframe Financial Data
-- Designed to meet the requirements of the NEXUS architecture challenge.

-- Enable the TimescaleDB extension (must be done by a superuser)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Main OHLCV Table for Raw Data (e.g., 1-minute bars)
-- This table will store the finest granularity data. Other timeframes will be aggregated from this.
CREATE TABLE "ohlcv_1m" (
    "time"          TIMESTAMPTZ       NOT NULL,
    "symbol"        TEXT              NOT NULL,
    "open"          DOUBLE PRECISION  NOT NULL,
    "high"          DOUBLE PRECISION  NOT NULL,
    "low"           DOUBLE PRECISION  NOT NULL,
    "close"         DOUBLE PRECISION  NOT NULL,
    "volume"        DOUBLE PRECISION  NOT NULL
);

-- 2. Create the Hypertable
-- We partition the data by the 'time' column, which is standard for time-series data.
-- chunk_time_interval is set to 7 days, a good balance for data management and query performance.
SELECT create_hypertable('ohlcv_1m', 'time', chunk_time_interval => INTERVAL '7 days');

-- 3. Indexing Strategy for Sub-Millisecond Retrieval
-- A composite index on (symbol, time) is crucial for fast lookups of a specific instrument over time.
-- TimescaleDB automatically creates an index on the time column for hypertables.
CREATE INDEX ON "ohlcv_1m" ("symbol", "time" DESC);

-- 4. Compression and Retention Policies
-- For a table with potentially billions of records, compression is key.
-- We compress data older than 14 days.
ALTER TABLE "ohlcv_1m" SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol'
);
SELECT add_compression_policy('ohlcv_1m', compress_after => INTERVAL '14 days');

-- Data retention policy: we drop raw 1-minute data older than 1 year to manage disk space.
-- Aggregated data in other tables can be kept for longer.
SELECT add_retention_policy('ohlcv_1m', drop_after => INTERVAL '1 year');


-- 5. Continuous Aggregates for Other Timeframes
-- Instead of creating separate tables, we use TimescaleDB's Continuous Aggregates
-- to automatically maintain aggregated views for other timeframes (1h, 1d, etc.).
-- This is highly efficient and ensures data consistency.

-- Example: 1-hour OHLCV aggregate
CREATE MATERIALIZED VIEW "ohlcv_1h"
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', "time") AS "bucket",
    "symbol",
    first("open", "time") AS "open",
    max("high") AS "high",
    min("low") AS "low",
    last("close", "time") AS "close",
    sum("volume") AS "volume"
FROM "ohlcv_1m"
GROUP BY "bucket", "symbol";

-- Add a policy to automatically refresh the aggregate view as new data arrives.
SELECT add_continuous_aggregate_policy('ohlcv_1h',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Similar views would be created for other required timeframes (5m, 15m, 4h, 1d, 1w, 1mo, 1y).
-- Each can have its own retention policy. For example, keep 1-hour data for 5 years.
SELECT add_retention_policy('ohlcv_1h', drop_after => INTERVAL '5 years');

/*
** Rationale for Handling 100+ Million Records:
**
** 1. Hypertables: Automatic partitioning by time (`chunk_time_interval`) keeps recent, frequently-accessed data in smaller, more manageable chunks, leading to faster queries.
** 2. Indexing: The composite index on `(symbol, time)` allows the database to quickly locate the relevant time chunks and then filter by symbol, avoiding full table scans.
** 3. Compression: TimescaleDB's native columnar compression significantly reduces storage footprint (up to 90%+) and can speed up queries that scan large amounts of historical data.
** 4. Continuous Aggregates: Queries for longer timeframes (e.g., daily or weekly charts) hit the pre-calculated, smaller aggregate views instead of processing millions of raw rows on the fly. This is the key to maintaining performance at scale.
*/
