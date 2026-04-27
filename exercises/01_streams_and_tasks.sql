-- ============================================================
-- EXERCISE 1: Streams & Tasks — CDC Pipeline
-- Concepts: Change Data Capture, idempotency, MERGE pattern
-- ============================================================

-- Step 1: Setup context
CREATE DATABASE IF NOT EXISTS interview_prep;
USE DATABASE interview_prep;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- Step 2: Create source (raw) table
CREATE TABLE IF NOT EXISTS sales_raw (
    id     INT,
    amount FLOAT,
    ts     TIMESTAMP
);

-- Step 3: Create a Stream on the source table
-- Stream tracks all INSERT/UPDATE/DELETE changes (CDC)
CREATE STREAM IF NOT EXISTS sales_stream ON TABLE sales_raw;

-- Step 4: Create target (clean) table
CREATE TABLE IF NOT EXISTS sales_clean (
    id     INT,
    amount FLOAT,
    ts     TIMESTAMP
);

-- Step 5: Create a Task to process stream changes every minute
-- NOTE: Tasks start SUSPENDED by default — must RESUME manually
CREATE OR REPLACE TASK process_sales
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = '1 minute'
AS
    -- Use MERGE (not INSERT) for idempotency — prevents duplicates
    MERGE INTO sales_clean t
    USING (
        SELECT id, amount, ts
        FROM   sales_stream
        WHERE  METADATA$ACTION = 'INSERT'
    ) s
    ON t.id = s.id
    WHEN NOT MATCHED THEN
        INSERT (id, amount, ts)
        VALUES (s.id, s.amount, s.ts);

-- Step 6: Resume the task (required!)
ALTER TASK process_sales RESUME;

-- Step 7: Insert test data into source table
INSERT INTO sales_raw VALUES (1, 100.0, CURRENT_TIMESTAMP);
INSERT INTO sales_raw VALUES (2, 250.0, CURRENT_TIMESTAMP);

-- Step 8: Check the stream — should show 2 rows with metadata columns
-- METADATA$ACTION     = INSERT | DELETE
-- METADATA$ISUPDATE   = TRUE if part of an UPDATE operation
-- METADATA$ROW_ID     = unique identifier for the changed row
SELECT * FROM sales_stream;

-- Step 9: Wait ~1 minute, then verify task consumed the stream
-- Stream will show 0 rows (consumed), sales_clean will have 2 rows
SELECT * FROM sales_clean;

-- ============================================================
-- KEY INTERVIEW CONCEPTS
-- ============================================================
-- Q: How do you know a stream has been consumed?
-- A: Querying the stream returns 0 rows — the task has processed
--    all pending changes. Stream resets until new DML occurs.
--
-- Q: Why MERGE instead of INSERT?
-- A: Idempotency — if the task fires multiple times, MERGE
--    prevents duplicate rows by upserting on a key column.
--
-- Q: What happens to UPDATEs in a stream?
-- A: Updates appear as a DELETE + INSERT pair, both rows present
--    with METADATA$ISUPDATE = TRUE
-- ============================================================
