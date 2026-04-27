# ❄️ Snowflake Hands-On Learning

A personal learning journal documenting my hands-on exploration of Snowflake — coming from an Azure-first background (Azure Document Intelligence, Microsoft Fabric, Semantic Kernel, FastAPI). These are real exercises I ran in a Snowflake trial account, including the errors I hit and what I learned from them.

---

## 🧠 Background

My data engineering experience has been primarily on Azure — building multi-agent pipelines, Lakehouse architectures on Microsoft Fabric, and document intelligence systems. I started exploring Snowflake to understand how its patterns compare and contrast with what I already know.

---

## 📁 Exercises

| # | File | Concepts |
|---|---|---|
| 1 | `01_streams_and_tasks.sql` | CDC, Streams, Tasks, MERGE idempotency |
| 2 | `02_rbac.sql` | Role hierarchy, least privilege, governance |
| 3 | `03_clustering.sql` | Micro-partition pruning, clustering keys, cardinality |

---

## 📖 Learning Journal

### Exercise 1 — Streams & Tasks (CDC Pipeline)

**What I built:** A raw → clean data pipeline using Streams and Tasks to move data automatically via CDC.

**What I learned:**
- A Stream is essentially a changelog on a table — it tracks every INSERT, UPDATE, DELETE with metadata columns (`METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`)
- Tasks always start **suspended** — you must explicitly `ALTER TASK ... RESUME` or nothing runs
- After a task processes a stream, the stream returns 0 rows — this is not an error, it means the changes were consumed

**The bug I hit:**
I ended up with 4 rows in my clean table instead of 2. The task had fired twice and blindly inserted duplicates because I used `INSERT` instead of `MERGE`.

**My inference:**
Using `INSERT` in a task is dangerous in production — any retry, overlap, or scheduling quirk creates duplicates. The correct pattern is always `MERGE` with a key column so the operation is idempotent. This is the same principle as upserts in any distributed system.

**Architecture pattern this maps to:**
```
Kafka → Snowpipe → RAW table → Stream → Task (MERGE) → CLEAN table
```

---

### Exercise 2 — RBAC (Role-Based Access Control)

**What I built:** A two-role hierarchy with analyst (read-only) and engineer (read + write) roles, assigned to a user.

**What I learned:**
- Snowflake RBAC is hierarchical — `engineer_role` inherits all privileges of `analyst_role` via `GRANT ROLE analyst_role TO ROLE engineer_role`
- `grant_option = false` (the default) means a role cannot re-grant its privileges — prevents privilege escalation
- `SHOW GRANTS TO ROLE` returns a clean audit view of exactly what a role can do

**My inference:**
The rule of never granting directly to users (always through roles) makes offboarding clean — revoke one role instead of hunting down individual object-level grants. This is the same principle as IAM groups in Azure AD. The Snowflake approach just enforces it more explicitly at the SQL layer.

**Interesting comparison to Azure:**
Snowflake RBAC ≈ Azure RBAC + Entra ID groups, but lives entirely in SQL. No portal, no ARM templates. Everything is auditable and scriptable which is cleaner for CI/CD pipelines.

---

### Exercise 3 — Clustering Keys & Micro-Partitioning

**What I built:** A 100K row sales table with a clustering key on `(sale_date, region)`, then queried `SYSTEM$CLUSTERING_INFORMATION()` to evaluate it.

**What I learned:**
- Snowflake auto-manages micro-partitions (50–500MB each, columnar, compressed) — there are no manual indexes
- Clustering keys tell Snowflake how to co-locate data so the engine can skip irrelevant partitions (partition pruning)
- `SYSTEM$CLUSTERING_INFORMATION()` returns `average_overlaps` and `average_depth` — lower is better

**The warning I got:**
Snowflake flagged: *"Clustering key columns contain high cardinality key SALE_DATE which might result in expensive re-clustering"*

**My inference:**
High cardinality columns (365 distinct dates) force Snowflake to maintain more micro-partitions, making auto-clustering expensive. The fix is to reduce cardinality — `DATE_TRUNC('month', sale_date)` gives 12 values instead of 365. This is the same tradeoff as partition key design in Azure Fabric / Delta Lake — too granular and you pay in overhead, too coarse and pruning doesn't help.

**When NOT to use clustering keys:**
Small tables, columns with very high cardinality without truncation, or tables with random/unpredictable access patterns. Always validate with `SYSTEM$CLUSTERING_INFORMATION()` before and after.

---

## 🔑 Key Takeaways

1. **Snowflake vs Azure Fabric** — Both use columnar Lakehouse architectures, but Snowflake separates compute and storage more explicitly. Virtual Warehouses = Spark pools, but fully elastic per query.

2. **Streams are pointers, not copies** — They track offset-like positions on table changes. Once consumed, they reset. Very similar conceptually to Kafka consumer group offsets.

3. **Everything is SQL** — RBAC, clustering, task scheduling, CDC — all done in SQL. No separate orchestration UI needed for basic pipelines.

4. **Idempotency is non-negotiable** — Whether it's Fabric pipelines or Snowflake Tasks, always design for "what happens if this runs twice." MERGE > INSERT.

---

## 🚀 Setup

1. Create a free Snowflake trial at [snowflake.com](https://snowflake.com)
2. Always set context before running:
```sql
USE DATABASE your_db;
USE SCHEMA PUBLIC;
USE WAREHOUSE COMPUTE_WH;
```
3. Run exercises in order — each builds on the previous

---

*Learning in public. All errors included.*
