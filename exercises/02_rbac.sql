-- ============================================================
-- EXERCISE 2: Role-Based Access Control (RBAC)
-- Concepts: Role hierarchy, least privilege, access governance
-- ============================================================

USE DATABASE interview_prep;
USE SCHEMA PUBLIC;

-- Step 1: Create roles aligned to job functions (never grant to users directly)
CREATE ROLE IF NOT EXISTS analyst_role;
CREATE ROLE IF NOT EXISTS engineer_role;

-- Step 2: Build role hierarchy
-- engineer_role inherits ALL privileges of analyst_role
GRANT ROLE analyst_role TO ROLE engineer_role;

-- Step 3: Grant analyst privileges (read only — least privilege)
GRANT USAGE  ON DATABASE interview_prep              TO ROLE analyst_role;
GRANT USAGE  ON SCHEMA   interview_prep.public       TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA interview_prep.public TO ROLE analyst_role;

-- Step 4: Grant engineer privileges (write on top of read)
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA interview_prep.public TO ROLE engineer_role;

-- Step 5: Assign role to user (not privileges directly)
GRANT ROLE engineer_role TO USER SRIRAM29;

-- Step 6: Verify grants
SHOW GRANTS TO ROLE analyst_role;
SHOW GRANTS TO ROLE engineer_role;

-- ============================================================
-- KEY INTERVIEW CONCEPTS
-- ============================================================
-- Q: Why grant to roles instead of users directly?
-- A: When someone leaves, you revoke one role — not hunt down
--    individual privileges across dozens of objects.
--
-- Q: What is grant_option = false?
-- A: The role cannot re-grant its privileges to other roles.
--    Prevents privilege escalation / propagation.
--
-- Q: How would you implement column-level security?
-- A: Dynamic Data Masking policies — mask PII columns for
--    analyst_role, show plaintext to engineer_role:
--
--    CREATE MASKING POLICY mask_email AS (val STRING) 
--    RETURNS STRING ->
--      CASE WHEN CURRENT_ROLE() IN ('ENGINEER_ROLE') THEN val
--           ELSE '****@****.com'
--      END;
--
--    ALTER TABLE customers MODIFY COLUMN email 
--    SET MASKING POLICY mask_email;
-- ============================================================
