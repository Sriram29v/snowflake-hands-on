-- ============================================================
-- EXERCISE 4: Kafka + Snowpipe + VARIANT (Semi-Structured Data)
-- Concepts: Event ingestion, JSON handling, schema-on-read
-- ============================================================

USE DATABASE interview_prep;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- Step 1: Create raw events table with VARIANT payload column
-- VARIANT stores JSON natively — no schema required upfront
CREATE TABLE IF NOT EXISTS events_raw (
    event_id     VARCHAR,
    user_id      INT,
    event_type   VARCHAR,  -- 'click', 'purchase', 'view'
    payload      VARIANT,  -- full JSON payload (like a Kafka message)
    ingested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Step 2: Create internal stage (Kafka connector writes files here)
CREATE STAGE IF NOT EXISTS kafka_stage
    FILE_FORMAT = (TYPE = 'JSON');

-- Step 3: Create Snowpipe to auto-ingest from stage into table
-- In production: AUTO_INGEST = TRUE + cloud event notification (SQS/SNS)
-- In trial: AUTO_INGEST = FALSE, trigger manually via REST API
CREATE PIPE IF NOT EXISTS kafka_pipe
    AUTO_INGEST = FALSE
AS
    COPY INTO events_raw (event_id, user_id, event_type, payload)
    FROM (
        SELECT
            $1:event_id::VARCHAR,
            $1:user_id::INT,
            $1:event_type::VARCHAR,
            $1
        FROM @kafka_stage
    );

-- Verify pipe created
SHOW PIPES;

-- Step 4: Simulate Kafka messages arriving (4 events, mixed schema)
-- Note: PARSE_JSON must be used with SELECT/UNION ALL, not VALUES clause
INSERT INTO events_raw (event_id, user_id, event_type, payload)
SELECT 'evt_001', 101, 'click',    PARSE_JSON('{"event_id":"evt_001","user_id":101,"event_type":"click","page":"/home","ts":"2026-04-27T10:00:00Z"}') UNION ALL
SELECT 'evt_002', 102, 'purchase', PARSE_JSON('{"event_id":"evt_002","user_id":102,"event_type":"purchase","amount":49.99,"item":"headphones","ts":"2026-04-27T10:01:00Z"}') UNION ALL
SELECT 'evt_003', 103, 'view',     PARSE_JSON('{"event_id":"evt_003","user_id":103,"event_type":"view","page":"/product/42","ts":"2026-04-27T10:02:00Z"}') UNION ALL
SELECT 'evt_004', 101, 'purchase', PARSE_JSON('{"event_id":"evt_004","user_id":101,"event_type":"purchase","amount":129.99,"item":"keyboard","ts":"2026-04-27T10:03:00Z"}');

-- Step 5: Query raw events
SELECT * FROM events_raw;

-- Step 6: Extract nested JSON fields using dot notation (schema-on-read)
-- Null values are correct -- click events have no amount, purchases have no page
SELECT
    event_id,
    event_type,
    payload:page::VARCHAR    AS page,
    payload:amount::FLOAT    AS amount,
    payload:item::VARCHAR    AS item,
    payload:ts::TIMESTAMP    AS event_time
FROM events_raw;

-- Step 7: Real analytics on top of Kafka events
SELECT
    event_type,
    COUNT(*)                        AS event_count,
    SUM(payload:amount::FLOAT)      AS total_revenue,
    COUNT(DISTINCT user_id)         AS unique_users
FROM events_raw
GROUP BY event_type
ORDER BY event_count DESC;

-- ============================================================
-- KEY INTERVIEW CONCEPTS
-- ============================================================
-- Q: How does Kafka integrate with Snowflake?
-- A: Kafka Connector (open source) reads from Kafka topics and
--    writes micro-batched files to a Snowflake internal/external
--    stage. Snowpipe then auto-ingests these files into tables
--    triggered by cloud event notifications (SQS/SNS/GCS).
--
-- Q: What is the VARIANT type?
-- A: Native semi-structured data type that stores JSON, Avro,
--    or Parquet without flattening. Queried with dot notation:
--    payload:amount::FLOAT. Snowflake does schema-on-read.
--
-- Q: Why is VARIANT powerful for Kafka pipelines?
-- A: Kafka message schemas evolve constantly. With VARIANT,
--    new fields just appear in the JSON without any ALTER TABLE.
--    You query what you need, ignore what you don't.
--
-- Q: What is the difference between Snowpipe and COPY INTO?
-- A: COPY INTO is batch/manual — you trigger it on a schedule.
--    Snowpipe is continuous — it fires automatically when new
--    files land in the stage via cloud event notifications.
--    Snowpipe Streaming (newer) skips staging entirely and
--    streams rows directly for even lower latency.
-- ============================================================
