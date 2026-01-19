"""
Plan Registry - Plan persistence using Lakebase PostgreSQL
Stores and retrieves SQL plans in Lakebase for governed execution
"""

import json
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import structlog
from contextlib import contextmanager

logger = structlog.get_logger()


class PlanRegistry:
    """
    Plan Registry using Lakebase PostgreSQL backend
    
    Stores plans as JSONB for flexible querying while maintaining structure
    """
    
    def __init__(self, lakebase_backend):
        """
        Initialize plan registry with Lakebase backend
        
        Args:
            lakebase_backend: LakebaseBackend instance
        """
        self.backend = lakebase_backend
        self._initialize_schema()
    
    def _initialize_schema(self):
        """Create plans table if it doesn't exist"""
        # Check if table already exists
        check_table_sql = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'plans'
        );
        """
        
        schema_sql = """
        CREATE TABLE IF NOT EXISTS plans (
            plan_id UUID PRIMARY KEY,
            plan_name VARCHAR(64) NOT NULL,
            owner VARCHAR(255) NOT NULL,
            description TEXT,
            pattern_type VARCHAR(50) NOT NULL,
            plan_json JSONB NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            version VARCHAR(20) NOT NULL DEFAULT '1.0.0',
            status VARCHAR(20) NOT NULL DEFAULT 'draft',
            tags JSONB DEFAULT '{}',
            CONSTRAINT valid_status CHECK (status IN ('draft', 'active', 'archived', 'deprecated'))
        );
        """
        
        # Indexes - will be skipped if table already exists and we're not owner
        index_sql = """
        -- Indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_plans_owner ON plans(owner);
        CREATE INDEX IF NOT EXISTS idx_plans_pattern_type ON plans(pattern_type);
        CREATE INDEX IF NOT EXISTS idx_plans_status ON plans(status);
        CREATE INDEX IF NOT EXISTS idx_plans_plan_name ON plans(plan_name);
        CREATE INDEX IF NOT EXISTS idx_plans_created_at ON plans(created_at DESC);
        
        -- GIN index for JSONB queries
        CREATE INDEX IF NOT EXISTS idx_plans_plan_json ON plans USING GIN (plan_json);
        CREATE INDEX IF NOT EXISTS idx_plans_tags ON plans USING GIN (tags);
        """
        
        # Trigger SQL (requires ownership, only try if we own the table)
        trigger_sql = """
        -- Update timestamp trigger
        CREATE OR REPLACE FUNCTION update_plans_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS trg_plans_updated_at ON plans;
        CREATE TRIGGER trg_plans_updated_at
            BEFORE UPDATE ON plans
            FOR EACH ROW
            EXECUTE FUNCTION update_plans_updated_at();
        """
        
        try:
            with self.backend._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Check if table already exists
                    cursor.execute(check_table_sql)
                    table_exists = cursor.fetchone()[0]
                    
                    if table_exists:
                        # Table already exists, skip init (we're not the owner)
                        logger.info("plan_registry_table_exists", 
                                   message="Plans table already exists, skipping initialization")
                        return
                    
                    # Table doesn't exist, create it (we'll be the owner)
                    cursor.execute(schema_sql)
                    conn.commit()
                    
                    # Create indexes
                    try:
                        cursor.execute(index_sql)
                        conn.commit()
                        logger.info("plan_registry_indexes_created", 
                                   message="Indexes created successfully")
                    except Exception as index_error:
                        conn.rollback()
                        logger.warning("plan_registry_indexes_skipped", 
                                      error=str(index_error),
                                      message="Index creation skipped")
                    
                    # Try to create trigger
                    try:
                        cursor.execute(trigger_sql)
                        conn.commit()
                        logger.info("plan_registry_trigger_created", 
                                   message="Trigger created successfully")
                    except Exception as trigger_error:
                        conn.rollback()
                        logger.warning("plan_registry_trigger_skipped", 
                                      error=str(trigger_error),
                                      message="Trigger creation skipped (requires table ownership)")
                    
            logger.info("plan_registry_schema_initialized", 
                       message="Plans table created successfully")
        except Exception as e:
            logger.error("plan_registry_schema_init_failed", error=str(e))
            raise
    
    def save_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Save or update a plan
        
        Args:
            plan: Plan dictionary (validated JSON schema)
            
        Returns:
            Dict with plan_id, success status, message
        """
        try:
            # Extract metadata from plan
            plan_metadata = plan.get('plan_metadata', {})
            plan_id = plan_metadata.get('plan_id')
            
            # Generate UUID if not provided
            if not plan_id:
                plan_id = str(uuid.uuid4())
                plan['plan_metadata']['plan_id'] = plan_id
            
            plan_name = plan_metadata.get('plan_name')
            owner = plan_metadata.get('owner')
            description = plan_metadata.get('description', '')
            version = plan_metadata.get('version', '1.0.0')
            pattern_type = plan.get('pattern', {}).get('type')
            tags = plan_metadata.get('tags', {})
            
            # Check if plan exists (update vs insert)
            with self.backend._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT plan_id FROM plans WHERE plan_id = %s",
                        (plan_id,)
                    )
                    exists = cursor.fetchone() is not None
                    
                    if exists:
                        # Update existing plan
                        cursor.execute("""
                            UPDATE plans 
                            SET plan_name = %s,
                                owner = %s,
                                description = %s,
                                pattern_type = %s,
                                plan_json = %s::jsonb,
                                version = %s,
                                tags = %s::jsonb,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE plan_id = %s
                            RETURNING plan_id, created_at, updated_at
                        """, (
                            plan_name,
                            owner,
                            description,
                            pattern_type,
                            json.dumps(plan),
                            version,
                            json.dumps(tags),
                            plan_id
                        ))
                        result = cursor.fetchone()
                        conn.commit()
                        
                        logger.info("plan_updated", 
                                   plan_id=plan_id, 
                                   plan_name=plan_name,
                                   owner=owner)
                        
                        return {
                            "success": True,
                            "plan_id": plan_id,
                            "message": f"Plan '{plan_name}' updated successfully",
                            "created_at": result[1].isoformat() if result[1] else None,
                            "updated_at": result[2].isoformat() if result[2] else None
                        }
                    else:
                        # Insert new plan
                        cursor.execute("""
                            INSERT INTO plans 
                            (plan_id, plan_name, owner, description, pattern_type, 
                             plan_json, version, tags, status)
                            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, 'active')
                            RETURNING plan_id, created_at, updated_at
                        """, (
                            plan_id,
                            plan_name,
                            owner,
                            description,
                            pattern_type,
                            json.dumps(plan),
                            version,
                            json.dumps(tags)
                        ))
                        result = cursor.fetchone()
                        conn.commit()
                        
                        logger.info("plan_created", 
                                   plan_id=plan_id, 
                                   plan_name=plan_name,
                                   owner=owner)
                        
                        return {
                            "success": True,
                            "plan_id": plan_id,
                            "message": f"Plan '{plan_name}' created successfully",
                            "created_at": result[1].isoformat() if result[1] else None,
                            "updated_at": result[2].isoformat() if result[2] else None
                        }
                        
        except Exception as e:
            logger.error("plan_save_failed", error=str(e), plan_name=plan_name)
            raise
    
    def get_plan(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a plan by ID
        
        Args:
            plan_id: Plan UUID
            
        Returns:
            Plan dictionary or None if not found
        """
        try:
            with self.backend._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT plan_json, status, created_at, updated_at
                        FROM plans
                        WHERE plan_id = %s
                    """, (plan_id,))
                    
                    result = cursor.fetchone()
                    if result:
                        plan = result[0]  # JSONB is automatically parsed
                        plan['_metadata'] = {
                            'status': result[1],
                            'created_at': result[2].isoformat() if result[2] else None,
                            'updated_at': result[3].isoformat() if result[3] else None
                        }
                        return plan
                    else:
                        logger.warning("plan_not_found", plan_id=plan_id)
                        return None
                        
        except Exception as e:
            logger.error("plan_get_failed", error=str(e), plan_id=plan_id)
            raise
    
    def list_plans(self, 
                   owner: Optional[str] = None,
                   pattern_type: Optional[str] = None,
                   status: Optional[str] = None,
                   limit: int = 100,
                   offset: int = 0) -> Dict[str, Any]:
        """
        List plans with optional filters
        
        Args:
            owner: Filter by owner email
            pattern_type: Filter by pattern type
            status: Filter by status
            limit: Max results to return
            offset: Pagination offset
            
        Returns:
            Dict with plans array and total count
        """
        try:
            with self.backend._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Build WHERE clause
                    conditions = []
                    params = []
                    
                    if owner:
                        conditions.append("owner = %s")
                        params.append(owner)
                    
                    if pattern_type:
                        conditions.append("pattern_type = %s")
                        params.append(pattern_type)
                    
                    if status:
                        conditions.append("status = %s")
                        params.append(status)
                    
                    where_clause = " AND ".join(conditions) if conditions else "TRUE"
                    
                    # Get total count
                    count_sql = f"SELECT COUNT(*) FROM plans WHERE {where_clause}"
                    cursor.execute(count_sql, params)
                    total = cursor.fetchone()[0]
                    
                    # Get plans
                    list_sql = f"""
                        SELECT 
                            plan_id,
                            plan_name,
                            owner,
                            description,
                            pattern_type,
                            version,
                            status,
                            created_at,
                            updated_at
                        FROM plans
                        WHERE {where_clause}
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """
                    cursor.execute(list_sql, params + [limit, offset])
                    
                    plans = []
                    for row in cursor.fetchall():
                        plans.append({
                            'plan_id': str(row[0]),
                            'plan_name': row[1],
                            'owner': row[2],
                            'description': row[3],
                            'pattern_type': row[4],
                            'version': row[5],
                            'status': row[6],
                            'created_at': row[7].isoformat() if row[7] else None,
                            'updated_at': row[8].isoformat() if row[8] else None
                        })
                    
                    return {
                        'plans': plans,
                        'total': total,
                        'limit': limit,
                        'offset': offset
                    }
                    
        except Exception as e:
            logger.error("plan_list_failed", error=str(e))
            raise
    
    def delete_plan(self, plan_id: str) -> Dict[str, Any]:
        """
        Delete a plan (soft delete by setting status to 'archived')
        
        Args:
            plan_id: Plan UUID
            
        Returns:
            Success status
        """
        try:
            with self.backend._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE plans
                        SET status = 'archived',
                            updated_at = CURRENT_TIMESTAMP
                        WHERE plan_id = %s
                        RETURNING plan_name
                    """, (plan_id,))
                    
                    result = cursor.fetchone()
                    if result:
                        conn.commit()
                        logger.info("plan_deleted", plan_id=plan_id, plan_name=result[0])
                        return {
                            "success": True,
                            "message": f"Plan '{result[0]}' archived successfully"
                        }
                    else:
                        logger.warning("plan_delete_not_found", plan_id=plan_id)
                        return {
                            "success": False,
                            "message": "Plan not found"
                        }
                        
        except Exception as e:
            logger.error("plan_delete_failed", error=str(e), plan_id=plan_id)
            raise
    
    def update_plan_status(self, plan_id: str, status: str) -> Dict[str, Any]:
        """
        Update plan status
        
        Args:
            plan_id: Plan UUID
            status: New status (draft, active, archived, deprecated)
            
        Returns:
            Success status
        """
        valid_statuses = ['draft', 'active', 'archived', 'deprecated']
        if status not in valid_statuses:
            raise ValueError(f"Invalid status: {status}. Must be one of {valid_statuses}")
        
        try:
            with self.backend._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE plans
                        SET status = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE plan_id = %s
                        RETURNING plan_name
                    """, (status, plan_id))
                    
                    result = cursor.fetchone()
                    if result:
                        conn.commit()
                        logger.info("plan_status_updated", 
                                   plan_id=plan_id, 
                                   plan_name=result[0],
                                   new_status=status)
                        return {
                            "success": True,
                            "message": f"Plan '{result[0]}' status updated to '{status}'"
                        }
                    else:
                        return {
                            "success": False,
                            "message": "Plan not found"
                        }
                        
        except Exception as e:
            logger.error("plan_status_update_failed", error=str(e), plan_id=plan_id)
            raise


# Singleton instance (initialized on first use)
_plan_registry: Optional[PlanRegistry] = None


def get_plan_registry() -> PlanRegistry:
    """
    Get or create the global plan registry instance
    
    Returns:
        PlanRegistry instance
    """
    global _plan_registry
    
    if _plan_registry is None:
        from infrastructure.lakebase_backend import LakebaseBackend
        
        # Check if Lakebase is enabled
        import os
        lakebase_enabled = os.getenv("LAKEBASE_ENABLED", "false").lower() == "true"
        
        if not lakebase_enabled:
            raise RuntimeError(
                "Lakebase is not enabled. Set LAKEBASE_ENABLED=true in .env "
                "and configure LAKEBASE_HOST, LAKEBASE_USER, LAKEBASE_PASSWORD"
            )
        
        # Initialize Lakebase backend
        lakebase = LakebaseBackend(
            database=os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
        )
        
        # Initialize plan registry
        _plan_registry = PlanRegistry(lakebase)
        logger.info("plan_registry_initialized", message="Using Lakebase PostgreSQL")
    
    return _plan_registry

