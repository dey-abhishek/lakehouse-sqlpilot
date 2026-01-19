-- ============================================================================
-- Lakehouse SQLPilot - Plan Registry Schema
-- Database: Lakebase PostgreSQL (databricks_postgres)
-- Purpose: Store and manage SQL execution plans with full audit trail
-- ============================================================================

-- ============================================================================
-- Main Plans Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS plans (
    -- Primary Key
    plan_id UUID PRIMARY KEY,
    
    -- Core Metadata
    plan_name VARCHAR(64) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    description TEXT,
    pattern_type VARCHAR(50) NOT NULL,
    
    -- Full Plan as JSONB (flexible schema)
    plan_json JSONB NOT NULL,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Version Control
    version VARCHAR(20) NOT NULL DEFAULT '1.0.0',
    
    -- Status Management
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    
    -- Tags for Organization
    tags JSONB DEFAULT '{}',
    
    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('draft', 'active', 'archived', 'deprecated')),
    CONSTRAINT unique_plan_name UNIQUE (plan_name, owner)
);

-- ============================================================================
-- Indexes for Performance
-- ============================================================================

-- Standard B-tree indexes for common queries
CREATE INDEX IF NOT EXISTS idx_plans_owner 
    ON plans(owner);

CREATE INDEX IF NOT EXISTS idx_plans_pattern_type 
    ON plans(pattern_type);

CREATE INDEX IF NOT EXISTS idx_plans_status 
    ON plans(status);

CREATE INDEX IF NOT EXISTS idx_plans_plan_name 
    ON plans(plan_name);

CREATE INDEX IF NOT EXISTS idx_plans_created_at 
    ON plans(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_plans_updated_at 
    ON plans(updated_at DESC);

-- Composite index for common filter combinations
CREATE INDEX IF NOT EXISTS idx_plans_owner_status 
    ON plans(owner, status);

CREATE INDEX IF NOT EXISTS idx_plans_pattern_status 
    ON plans(pattern_type, status);

-- GIN indexes for JSONB queries
CREATE INDEX IF NOT EXISTS idx_plans_plan_json 
    ON plans USING GIN (plan_json);

CREATE INDEX IF NOT EXISTS idx_plans_tags 
    ON plans USING GIN (tags);

-- ============================================================================
-- Trigger Function: Auto-update timestamp
-- ============================================================================

CREATE OR REPLACE FUNCTION update_plans_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Trigger: Update updated_at on every UPDATE
-- ============================================================================

DROP TRIGGER IF EXISTS trg_plans_updated_at ON plans;

CREATE TRIGGER trg_plans_updated_at
    BEFORE UPDATE ON plans
    FOR EACH ROW
    EXECUTE FUNCTION update_plans_updated_at();

-- ============================================================================
-- Helpful Views
-- ============================================================================

-- View: Active plans summary
CREATE OR REPLACE VIEW v_active_plans AS
SELECT 
    plan_id,
    plan_name,
    owner,
    pattern_type,
    description,
    version,
    created_at,
    updated_at,
    plan_json->'source'->>'catalog' AS source_catalog,
    plan_json->'source'->>'schema' AS source_schema,
    plan_json->'source'->>'table' AS source_table,
    plan_json->'target'->>'catalog' AS target_catalog,
    plan_json->'target'->>'schema' AS target_schema,
    plan_json->'target'->>'table' AS target_table
FROM plans
WHERE status = 'active'
ORDER BY created_at DESC;

-- View: Plan statistics by owner
CREATE OR REPLACE VIEW v_plan_stats_by_owner AS
SELECT 
    owner,
    COUNT(*) AS total_plans,
    COUNT(*) FILTER (WHERE status = 'active') AS active_plans,
    COUNT(*) FILTER (WHERE status = 'draft') AS draft_plans,
    COUNT(*) FILTER (WHERE status = 'archived') AS archived_plans,
    COUNT(DISTINCT pattern_type) AS unique_patterns,
    MAX(created_at) AS last_created,
    MAX(updated_at) AS last_updated
FROM plans
GROUP BY owner
ORDER BY total_plans DESC;

-- View: Plan statistics by pattern type
CREATE OR REPLACE VIEW v_plan_stats_by_pattern AS
SELECT 
    pattern_type,
    COUNT(*) AS total_plans,
    COUNT(*) FILTER (WHERE status = 'active') AS active_plans,
    COUNT(DISTINCT owner) AS unique_owners,
    MIN(created_at) AS first_created,
    MAX(created_at) AS last_created
FROM plans
GROUP BY pattern_type
ORDER BY total_plans DESC;

-- ============================================================================
-- Sample Queries (Commented Out)
-- ============================================================================

-- List all active plans
-- SELECT * FROM v_active_plans;

-- Find plans by owner
-- SELECT plan_name, pattern_type, created_at 
-- FROM plans 
-- WHERE owner = 'user@company.com' 
-- ORDER BY created_at DESC;

-- Find plans by pattern type
-- SELECT plan_name, owner, created_at 
-- FROM plans 
-- WHERE pattern_type = 'SCD2' AND status = 'active';

-- Search within plan JSON (e.g., find plans using specific catalog)
-- SELECT plan_name, owner 
-- FROM plans 
-- WHERE plan_json->'source'->>'catalog' = 'lakehouse-sqlpilot';

-- Get plan statistics
-- SELECT * FROM v_plan_stats_by_owner;
-- SELECT * FROM v_plan_stats_by_pattern;

-- Full-text search in plan JSON
-- SELECT plan_name, plan_json 
-- FROM plans 
-- WHERE plan_json::text ILIKE '%customer%';

-- ============================================================================
-- Grant Permissions (adjust as needed)
-- ============================================================================

-- Grant read access to all users (adjust user as needed)
-- GRANT SELECT ON plans TO PUBLIC;
-- GRANT SELECT ON v_active_plans TO PUBLIC;
-- GRANT SELECT ON v_plan_stats_by_owner TO PUBLIC;
-- GRANT SELECT ON v_plan_stats_by_pattern TO PUBLIC;

-- Grant full access to application user
-- GRANT ALL ON plans TO sqlpilot_app;

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Check table exists
SELECT 
    table_name,
    table_type
FROM information_schema.tables 
WHERE table_name = 'plans';

-- Check indexes
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'plans'
ORDER BY indexname;

-- Check triggers
SELECT 
    trigger_name,
    event_manipulation,
    action_statement
FROM information_schema.triggers 
WHERE event_object_table = 'plans';

-- Check constraints
SELECT 
    constraint_name,
    constraint_type
FROM information_schema.table_constraints 
WHERE table_name = 'plans';

-- ============================================================================
-- Schema Version Info
-- ============================================================================

COMMENT ON TABLE plans IS 'Lakehouse SQLPilot Plan Registry - stores governed SQL execution plans';
COMMENT ON COLUMN plans.plan_id IS 'Unique UUID identifier for the plan';
COMMENT ON COLUMN plans.plan_name IS 'Human-readable plan name (unique per owner)';
COMMENT ON COLUMN plans.owner IS 'Owner email address';
COMMENT ON COLUMN plans.pattern_type IS 'SQL pattern type (SCD2, INCREMENTAL_APPEND, etc.)';
COMMENT ON COLUMN plans.plan_json IS 'Full plan definition as JSONB';
COMMENT ON COLUMN plans.status IS 'Plan lifecycle status (draft, active, archived, deprecated)';
COMMENT ON COLUMN plans.tags IS 'Custom tags for organization and filtering';

-- ============================================================================
-- End of Schema
-- ============================================================================


