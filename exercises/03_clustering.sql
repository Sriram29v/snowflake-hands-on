-- ============================================================
-- EXERCISE 3: Clustering Keys & Query Optimization
-- Concepts: Micro-partition pruning, cardinality, cost tradeoffs
-- ============================================================

USE DATABASE interview_prep;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;

-- Step 1: Create a realistic large table
CREATE TABLE IF NOT EXISTS sales_history (
    id          INT,
    customer_id INT,
    region      VARCHAR,
    amount      FLOAT,
    sale_date   DATE
);

-- Step 2: Generate 100K rows of synthetic data
INSERT INTO sales_history
SELECT
    seq4()                                          AS id,
    uniform(1, 1000, random())                      AS customer_id,
    CASE WHEN uniform(1,4,random()) = 1 THEN 'WEST'
         WHEN uniform(1,4,random()) = 2 THEN 'EAST'
         WHEN uniform(1,4,random()) = 3 THEN 'NORTH'
         ELSE 'SOUTH' END                           AS region,
    uniform(10, 5000, random())::FLOAT              AS amount,
    dateadd(day, -uniform(1, 365, random()), current_date()) AS sale_date
FROM TABLE(generator(rowcount => 100000));

-- Step 3: Add clustering key on most common filter columns
ALTER TABLE sales_history CLUSTER BY (sale_date, region);

-- Step 4: Check clustering health
-- Look for: average_overlaps (lower = better), average_depth
SELECT SYSTEM$CLUSTERING_INFORMATION('sales_history');

-- Step 5: Query WITHOUT clustering benefit (full scan)
SELECT region, SUM(amount)
FROM   sales_history
GROUP BY region;

-- Step 6: Query WITH clustering benefit (partition pruning)
-- Snowflake skips micro-partitions that don't match the filter
SELECT region, SUM(amount)
FROM   sales_history
WHERE  sale_date >= '2026-01-01'
AND    region = 'WEST'
GROUP BY region;

-- ============================================================
-- KEY INTERVIEW CONCEPTS
-- ============================================================
-- Q: When should you add a clustering key?
-- A: When queries consistently filter on specific columns AND
--    the table is large enough (multi-TB) that pruning helps.
--    Small tables don't benefit — Snowflake scans them fast anyway.
--
-- Q: What's the cardinality warning about?
-- A: High cardinality columns (daily dates = 365 values) cause
--    frequent re-clustering which is expensive. 
--    Fix: Use DATE_TRUNC('month', sale_date) instead — only 12 values.
--
-- Q: What's the difference between clustering and indexing?
-- A: Snowflake has NO indexes. Clustering organizes data into 
--    micro-partitions (50-500MB each) so the engine can skip
--    irrelevant partitions entirely — called partition pruning.
--
-- Q: How do you monitor clustering effectiveness?
-- A: SYSTEM$CLUSTERING_INFORMATION() returns:
--    - average_overlaps: how many partitions share the same key range
--    - average_depth: ideal = 1.0, higher = more overlap = worse
--    - total_partition_count: total micro-partitions in the table
-- ============================================================
