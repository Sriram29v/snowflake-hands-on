-- ============================================================
-- EXERCISE 5: Time Travel — Disaster Recovery
-- Concepts: AT/BEFORE clause, data recovery, retention period
-- ============================================================

USE DATABASE interview_prep;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- Step 1: Create a table to demo Time Travel
CREATE TABLE IF NOT EXISTS customer_accounts (
    account_id  INT,
    name        VARCHAR,
    balance     FLOAT,
    status      VARCHAR
);

-- Step 2: Insert initial data
INSERT INTO customer_accounts VALUES
    (1, 'Alice',   5000.00, 'active'),
    (2, 'Bob',     3200.00, 'active'),
    (3, 'Charlie', 8100.00, 'active');

-- Step 3: Confirm initial state
SELECT * FROM customer_accounts;

-- Step 4: Capture timestamp BEFORE the accident
-- Copy this value -- you'll need it for recovery
SELECT CURRENT_TIMESTAMP;

-- Step 5: Simulate accidental DELETE (happens in production!)
DELETE FROM customer_accounts;

-- Step 6: Confirm the damage -- 0 rows
SELECT * FROM customer_accounts;

-- Step 7: Recover using Time Travel
-- Replace timestamp with the value from Step 4
SELECT * FROM customer_accounts
AT (TIMESTAMP => '2026-04-27 17:17:43.932 -0700'::TIMESTAMP_TZ);

-- Step 8: Fully restore the table
CREATE OR REPLACE TABLE customer_accounts AS
SELECT * FROM customer_accounts
AT (TIMESTAMP => '2026-04-27 17:17:43.932 -0700'::TIMESTAMP_TZ);

-- Step 9: Confirm full recovery
SELECT * FROM customer_accounts;

-- ============================================================
-- BONUS: Other Time Travel syntax options
-- ============================================================

-- Travel back by offset in seconds (e.g. 5 minutes ago)
SELECT * FROM customer_accounts
AT (OFFSET => -60 * 5);

-- Travel back to just before a specific query ran (using Query ID)
-- Get Query ID from query history
SELECT * FROM customer_accounts
BEFORE (STATEMENT => '01c3ffd0-0308-aeb5-002c-9b4b0002f042');

-- Check current data retention period (default = 1 day on Standard)
SHOW TABLES LIKE 'customer_accounts';

-- Extend retention to 7 days (Enterprise edition feature)
ALTER TABLE customer_accounts SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- ============================================================
-- KEY INTERVIEW CONCEPTS
-- ============================================================
-- Q: What is Time Travel?
-- A: Snowflake retains historical versions of data for up to
--    1 day (Standard) or 90 days (Enterprise). You can query
--    any past state using AT or BEFORE clause with a timestamp,
--    offset in seconds, or a Query ID.
--
-- Q: What is the difference between Time Travel and Fail-Safe?
-- A: Time Travel is user-accessible (you run the SQL).
--    Fail-Safe is an additional 7-day period AFTER Time Travel
--    expires, accessible only by Snowflake support for disaster
--    recovery. You cannot query Fail-Safe data yourself.
--
-- Q: How would you recover a dropped table?
-- A: UNDROP TABLE customer_accounts;
--    Works within the Time Travel retention window.
--
-- Q: What are the 3 ways to specify a Time Travel point?
-- A: 1. TIMESTAMP => '2026-04-27 17:17:43'::TIMESTAMP_TZ
--    2. OFFSET => -300  (seconds ago)
--    3. STATEMENT => 'query_id'  (before that query ran)
-- ============================================================
