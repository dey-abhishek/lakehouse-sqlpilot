"""
FastAPI Backend for Lakehouse SQLPilot
Provides REST API for the React frontend
"""

# Load environment variables first (before any other imports)
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env.dev for development
env_file = Path(__file__).parent.parent / '.env.dev'
if env_file.exists():
    load_dotenv(env_file)
    print(f"✅ Loaded environment from {env_file}")
else:
    print(f"⚠️  No .env.dev found at {env_file}")

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from compiler import SQLCompiler
from execution import SQLExecutor, ExecutionTracker
from unity_catalog import PermissionValidator
from preview import PreviewEngine
from plan_schema.v1.validator import PlanValidator
from databricks.sdk import WorkspaceClient

# Import plan registry (optional - only if Lakebase is enabled)
try:
    from plan_registry import get_plan_registry
    plan_registry_available = True
except Exception as e:
    plan_registry_available = False
    logger = None  # Will set up later
    if hasattr(e, '__name__') and 'RuntimeError' not in str(type(e)):
        import structlog
        logger = structlog.get_logger()
        logger.warning("plan_registry_not_available", error=str(e))

# Import security middleware
from security.middleware import (
    SecurityMiddleware,
    get_current_user,
    audit_log,
    SecurityConfig
)

# Import OAuth endpoints (try/except for optional OAuth)
try:
    from api.oauth_endpoints import router as oauth_router
    oauth_available = True
except ImportError:
    oauth_available = False
    oauth_router = None

# Initialize FastAPI app
app = FastAPI(
    title="Lakehouse SQLPilot API",
    description="Governed SQL execution control plane for Databricks",
    version="1.0.0"
)

# Add security middleware FIRST (before CORS)
app.add_middleware(SecurityMiddleware)

# CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=SecurityConfig.ALLOWED_ORIGINS if SecurityConfig.ALLOWED_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
# Use centralized OAuth token manager for all Databricks API calls
from infrastructure.oauth_token_manager import get_oauth_token_manager

_workspace_client = None
_oauth_manager = None

def get_workspace_client():
    """
    Get or create WorkspaceClient with OAuth token auto-refresh
    
    Uses centralized OAuth token manager for automatic token rotation.
    Tokens are refreshed 5 minutes before expiry.
    """
    global _workspace_client, _oauth_manager
    
    if _workspace_client is None:
        try:
            # Initialize OAuth token manager (singleton)
            _oauth_manager = get_oauth_token_manager(
                auto_refresh=True,
                refresh_buffer_minutes=5
            )
            
            # Get current token
            token = _oauth_manager.get_token()
            
            # Save env vars that might interfere
            saved_client_id = os.environ.pop('DATABRICKS_CLIENT_ID', None)
            saved_client_secret = os.environ.pop('DATABRICKS_CLIENT_SECRET', None)
            
            try:
                # Initialize WorkspaceClient with OAuth token ONLY
                # (avoid mixing auth methods)
                _workspace_client = WorkspaceClient(
                    host=f"https://{_oauth_manager.databricks_host}",
                    token=token
                )
            finally:
                # Restore env vars for other components
                if saved_client_id:
                    os.environ['DATABRICKS_CLIENT_ID'] = saved_client_id
                if saved_client_secret:
                    os.environ['DATABRICKS_CLIENT_SECRET'] = saved_client_secret
            
            import structlog
            logger = structlog.get_logger()
            logger.info(
                "workspace_client_initialized",
                host=_oauth_manager.databricks_host,
                auth="oauth_service_principal",
                auto_refresh=True
            )
            
        except Exception as e:
            import structlog
            logger = structlog.get_logger()
            logger.warning("workspace_client_init_failed", error=str(e))
            # Return None - features requiring workspace client will fail gracefully
            return None
    return _workspace_client

workspace_client = get_workspace_client()
compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
permission_validator = PermissionValidator(workspace_client) if workspace_client else None

# Models
class PlanValidationRequest(BaseModel):
    plan: Dict[str, Any]

class PlanCompileRequest(BaseModel):
    plan: Dict[str, Any]
    context: Optional[Dict[str, Any]] = None

class PreviewRequest(BaseModel):
    plan: Dict[str, Any]
    user: str
    warehouse_id: str
    include_sample_data: bool = True

class ExecutionRequest(BaseModel):
    plan_id: str
    plan_version: str
    sql: str
    warehouse_id: str
    executor_user: str
    timeout_seconds: int = 3600

class AgentSuggestionRequest(BaseModel):
    intent: str
    user: str
    context: Optional[Dict[str, Any]] = None

class PlanSaveRequest(BaseModel):
    plan: Dict[str, Any]
    user: str

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for Databricks Apps"""
    from datetime import datetime, timezone
    return {
        "status": "healthy",
        "service": "lakehouse-sqlpilot",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# API endpoints
@app.post("/api/v1/plans/validate")
async def validate_plan(
    request: PlanValidationRequest,
    user: dict = Depends(get_current_user)
):
    """Validate a plan against schema and semantic rules"""
    audit_log("plan_validation", user=user.get("email", "unknown"), plan_id=request.plan.get("plan_id"))
    try:
        is_valid, errors = compiler.validate_plan(request.plan)
        return {
            "valid": is_valid,
            "is_valid": is_valid,  # Keep both for backward compatibility
            "errors": errors
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/plans/compile")
async def compile_plan(request: PlanCompileRequest):
    """Compile a plan to SQL"""
    try:
        sql = compiler.compile(request.plan, request.context)
        return {
            "success": True,
            "sql": sql
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/plans/preview")
async def preview_plan(request: PreviewRequest):
    """Generate a preview of plan execution"""
    try:
        # Create preview engine with mock executor for now
        from unittest.mock import Mock
        mock_executor = Mock()
        mock_executor.preview_sql = Mock(return_value={
            'columns': [],
            'rows': [],
            'row_count': 0
        })
        
        preview_engine = PreviewEngine(compiler, permission_validator, mock_executor)
        preview = preview_engine.preview_plan(
            request.plan,
            request.user,
            request.warehouse_id,
            request.include_sample_data
        )
        return preview
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/v1/catalogs")
async def list_catalogs():
    """List available Unity Catalogs"""
    try:
        catalogs = list(workspace_client.catalogs.list())
        return {
            "catalogs": [
                {
                    "name": cat.name,
                    "comment": cat.comment,
                    "owner": cat.owner
                }
                for cat in catalogs
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/catalogs/{catalog}/schemas")
async def list_schemas(catalog: str):
    """List schemas in a catalog"""
    try:
        schemas = list(workspace_client.schemas.list(catalog_name=catalog))
        return {
            "schemas": [
                {
                    "name": schema.name,
                    "catalog": schema.catalog_name,
                    "owner": schema.owner
                }
                for schema in schemas
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/catalogs/{catalog}/schemas/{schema}/tables")
async def list_tables(catalog: str, schema: str):
    """List tables in a schema"""
    try:
        tables = list(workspace_client.tables.list(
            catalog_name=catalog,
            schema_name=schema
        ))
        return {
            "tables": [
                {
                    "name": table.name,
                    "catalog": table.catalog_name,
                    "schema": table.schema_name,
                    "table_type": table.table_type.value if table.table_type else None
                }
                for table in tables
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/columns")
async def get_table_columns(catalog: str, schema: str, table: str):
    """Get columns from a table"""
    try:
        table_info = workspace_client.tables.get(
            full_name=f"{catalog}.{schema}.{table}"
        )
        return {
            "columns": [
                {
                    "name": col.name,
                    "type": col.type_text or str(col.type_name) if col.type_name else "unknown"
                }
                for col in table_info.columns or []
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch columns: {str(e)}")

@app.get("/api/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/format")
async def get_table_format(catalog: str, schema: str, table: str):
    """Get table format (Delta, Iceberg, etc.) from Unity Catalog"""
    try:
        table_info = workspace_client.tables.get(
            full_name=f"{catalog}.{schema}.{table}"
        )
        
        # The data_source_format tells us if it's DELTA, ICEBERG, etc.
        data_source_format = table_info.data_source_format
        
        # Normalize to lowercase
        if data_source_format:
            format_lower = data_source_format.value.lower() if hasattr(data_source_format, 'value') else str(data_source_format).lower()
        else:
            format_lower = "unknown"
        
        return {
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "format": format_lower,  # "delta", "iceberg", etc.
            "table_type": table_info.table_type.value if table_info.table_type else None
        }
    except Exception as e:
        # Table doesn't exist or other error
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "does not exist" in str(e):
            raise HTTPException(status_code=404, detail=f"Table {catalog}.{schema}.{table} not found")
        raise HTTPException(status_code=500, detail=f"Failed to fetch table format: {str(e)}")

@app.get("/api/v1/warehouses")
async def list_warehouses():
    """List available SQL warehouses"""
    try:
        warehouses = list(workspace_client.warehouses.list())
        return {
            "warehouses": [
                {
                    "id": wh.id,
                    "name": wh.name,
                    "state": wh.state.value if wh.state else None,
                    "size": wh.cluster_size
                }
                for wh in warehouses
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/patterns")
async def list_patterns():
    """List supported SQL patterns"""
    return {
        "patterns": compiler.get_supported_patterns()
    }

@app.post("/api/v1/tables/check")
async def check_table_exists(request: dict):
    """Check if a table exists in Unity Catalog"""
    import structlog
    logger = structlog.get_logger()
    
    try:
        ws = get_workspace_client()
        
        catalog = request.get("catalog")
        schema = request.get("schema")
        table = request.get("table")
        warehouse_id = request.get("warehouse_id")
        
        full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
        logger.info("check_table_exists", table=full_table_name, warehouse_id=warehouse_id)
        
        if not all([catalog, schema, table, warehouse_id]):
            raise HTTPException(status_code=400, detail="Missing required fields: catalog, schema, table, warehouse_id")
        
        try:
            result = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"DESCRIBE TABLE `{catalog}`.`{schema}`.`{table}`",
                wait_timeout="10s"
            )
            
            # Check if the statement actually succeeded
            if result.status and result.status.state:
                state = result.status.state.value if hasattr(result.status.state, 'value') else str(result.status.state)
                logger.info("check_table_result", table=full_table_name, state=state)
                if state == "SUCCEEDED":
                    logger.info("table_exists", table=full_table_name, exists=True)
                    return {
                        "exists": True,
                        "table": f"`{catalog}`.`{schema}`.`{table}`"
                    }
                else:
                    # Statement didn't succeed - table likely doesn't exist
                    logger.info("table_exists", table=full_table_name, exists=False, reason="statement_failed")
                    return {
                        "exists": False,
                        "table": f"`{catalog}`.`{schema}`.`{table}`"
                    }
            
            # If we get here, assume table exists (statement completed without error)
            logger.info("table_exists", table=full_table_name, exists=True, reason="no_error")
            return {
                "exists": True,
                "table": f"`{catalog}`.`{schema}`.`{table}`"
            }
        except Exception as e:
            error_str = str(e)
            logger.info("check_table_exception", table=full_table_name, error=error_str)
            if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "cannot be found" in error_str or "does not exist" in error_str:
                logger.info("table_exists", table=full_table_name, exists=False, reason="not_found_exception")
                return {
                    "exists": False,
                    "table": f"`{catalog}`.`{schema}`.`{table}`"
                }
            # For other errors, re-raise
            raise
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/tables/create")
async def create_table(request: dict):
    """Create a target table based on source table and pattern type"""
    try:
        ws = get_workspace_client()
        
        plan_id = request.get("plan_id")
        warehouse_id = request.get("warehouse_id")
        
        if not all([plan_id, warehouse_id]):
            raise HTTPException(status_code=400, detail="Missing required fields: plan_id, warehouse_id")
        
        # Fetch the plan
        registry = get_plan_registry()
        plan = registry.get_plan(plan_id)
        if not plan:
            raise HTTPException(status_code=404, detail=f"Plan {plan_id} not found")
        
        # Extract table info
        source_catalog = plan.get("source", {}).get("catalog")
        source_schema = plan.get("source", {}).get("schema")
        source_table = plan.get("source", {}).get("table")
        target_catalog = plan.get("target", {}).get("catalog")
        target_schema = plan.get("target", {}).get("schema")
        target_table = plan.get("target", {}).get("table")
        pattern_type = plan.get("pattern", {}).get("type")
        source_columns = plan.get("source", {}).get("columns", [])
        
        if not all([source_catalog, source_schema, source_table, target_catalog, target_schema, target_table]):
            raise HTTPException(status_code=400, detail="Plan missing required source/target information")
        
        # First, verify the SOURCE table exists (we need it to create the target)
        try:
            ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"DESCRIBE TABLE `{source_catalog}`.`{source_schema}`.`{source_table}`",
                wait_timeout="10s"
            )
        except Exception as e:
            error_msg = str(e)
            if "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "cannot be found" in error_msg:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Source table `{source_catalog}`.`{source_schema}`.`{source_table}` does not exist. "
                           f"Please ensure the source table exists before creating the target table."
                )
            # Re-raise other errors
            raise HTTPException(status_code=500, detail=f"Error checking source table: {error_msg}")
        
        # Build CREATE TABLE statement based on pattern
        if pattern_type == "SCD2":
            pattern_config = plan.get("pattern_config", {})
            effective_date_col = pattern_config.get("effective_date_column", "effective_date")
            end_date_col = pattern_config.get("end_date_column", "end_date")
            current_flag_col = pattern_config.get("current_flag_column", "is_current")
            
            if source_columns:
                column_list = ", ".join([f"`{col}`" for col in source_columns])
            else:
                column_list = "*"
            
            create_table_sql = f"""
CREATE TABLE IF NOT EXISTS `{target_catalog}`.`{target_schema}`.`{target_table}` AS
SELECT 
    {column_list},
    CURRENT_TIMESTAMP() AS `{effective_date_col}`,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS `{end_date_col}`,
    TRUE AS `{current_flag_col}`
FROM `{source_catalog}`.`{source_schema}`.`{source_table}`
WHERE 1=0
"""
        elif pattern_type == "SNAPSHOT":
            # Snapshot pattern needs to include the snapshot_date_column
            pattern_config = plan.get("pattern_config", {})
            snapshot_col = pattern_config.get("snapshot_date_column", "snapshot_date")
            
            if source_columns:
                # Filter out snapshot column if it's already in source
                filtered_columns = [col for col in source_columns if col != snapshot_col]
                column_list = ", ".join([f"`{col}`" for col in filtered_columns])
            else:
                column_list = "*"
            
            create_table_sql = f"""
CREATE TABLE IF NOT EXISTS `{target_catalog}`.`{target_schema}`.`{target_table}` AS
SELECT 
    {column_list},
    CURRENT_DATE() AS `{snapshot_col}`
FROM `{source_catalog}`.`{source_schema}`.`{source_table}`
WHERE 1=0
"""
        else:
            create_table_sql = f"""
CREATE TABLE IF NOT EXISTS `{target_catalog}`.`{target_schema}`.`{target_table}`
LIKE `{source_catalog}`.`{source_schema}`.`{source_table}`
"""
        
        # Execute CREATE TABLE
        result = ws.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=create_table_sql,
            wait_timeout="30s"
        )
        
        return {
            "success": True,
            "table": f"`{target_catalog}`.`{target_schema}`.`{target_table}`",
            "statement_id": result.statement_id,
            "message": "Table created successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/tables/delete")
async def delete_table(request: dict):
    """
    Delete/drop a table from Unity Catalog
    
    Requires: DROP TABLE privilege on the table in Unity Catalog
    """
    import structlog
    logger = structlog.get_logger()
    
    try:
        ws = get_workspace_client()
        
        catalog = request.get("catalog")
        schema = request.get("schema")
        table = request.get("table")
        warehouse_id = request.get("warehouse_id")
        
        full_table_name = f"`{catalog}`.`{schema}`.`{table}`"
        logger.info("delete_table_request", table=full_table_name, warehouse_id=warehouse_id)
        
        if not all([catalog, schema, table, warehouse_id]):
            raise HTTPException(status_code=400, detail="Missing required fields: catalog, schema, table, warehouse_id")
        
        # Execute DROP TABLE
        # Note: Unity Catalog will enforce permissions
        # User must have DROP privilege on the table
        drop_sql = f"DROP TABLE IF EXISTS {full_table_name}"
        
        try:
            result = ws.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=drop_sql,
                wait_timeout="30s"
            )
            
            logger.info("table_deleted_success", table=full_table_name, statement_id=result.statement_id)
            
            return {
                "success": True,
                "table": full_table_name,
                "statement_id": result.statement_id,
                "message": f"Table {full_table_name} deleted successfully"
            }
        except Exception as e:
            error_msg = str(e)
            logger.error("table_deletion_failed", table=full_table_name, error=error_msg)
            
            # Check for permission-related errors
            if any(keyword in error_msg.upper() for keyword in [
                'PERMISSION', 'DENIED', 'INSUFFICIENT_PRIVILEGE', 
                'NOT_AUTHORIZED', 'ACCESS_DENIED', 'FORBIDDEN'
            ]):
                raise HTTPException(
                    status_code=403,
                    detail=f"Permission denied: You don't have DROP TABLE privilege on {full_table_name}. "
                           f"Please contact your Unity Catalog administrator to grant the necessary permissions."
                )
            
            # For other errors, provide the actual error message
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete table: {error_msg}"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error("delete_table_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.post("/api/v1/plans/execute")
async def execute_plan(request: ExecutionRequest):
    """Execute a compiled SQL plan with coordinated multi-statement support and tracking"""
    import structlog
    logger = structlog.get_logger()
    
    execution_record_id = None
    lakebase = None
    
    try:
        logger.info("execution_request_received", plan_id=request.plan_id, warehouse_id=request.warehouse_id)
        
        # Get workspace client (already initialized with OAuth)
        ws = get_workspace_client()
        
        logger.info("splitting_sql_statements", sql_length=len(request.sql))
        
        # Split SQL into individual statements
        # Remove comments and split on semicolons
        statements = []
        for line in request.sql.split('\n'):
            line = line.strip()
            # Skip comment lines
            if line.startswith('--') or not line:
                continue
            statements.append(line)
        
        # Join back and split on semicolons
        full_sql = ' '.join(statements)
        sql_statements = [s.strip() for s in full_sql.split(';') if s.strip()]
        
        logger.info("executing_statements", count=len(sql_statements))
        
        # Get plan details for tracking
        try:
            registry = get_plan_registry()
            plan = registry.get_plan(request.plan_id)
            plan_name = plan.get("plan_metadata", {}).get("plan_name", "unknown") if plan else "unknown"
        except:
            plan_name = "unknown"
        
        # Create execution tracking record in Lakebase
        try:
            from infrastructure.lakebase_backend import get_lakebase_backend
            lakebase = get_lakebase_backend()
            if lakebase:
                execution_record_id = lakebase.create_execution_record(
                    plan_id=request.plan_id,
                    plan_name=plan_name,
                    plan_version=request.plan_version,
                    executor_user=request.executor_user,
                    warehouse_id=request.warehouse_id,
                    total_statements=len(sql_statements)
                )
                logger.info("execution_tracking_started", execution_record_id=execution_record_id)
                
                # Update status to RUNNING
                lakebase.update_execution_status(execution_record_id, "RUNNING")
        except Exception as track_error:
            logger.warning("execution_tracking_failed", error=str(track_error))
            # Continue execution even if tracking fails
        
        # Execute each statement sequentially with coordination
        # Each statement must complete successfully before the next one starts
        execution_ids = []
        succeeded_count = 0
        failed_count = 0
        
        for idx, stmt in enumerate(sql_statements):
            try:
                logger.info("executing_statement", number=idx+1, length=len(stmt))
                
                # Execute and WAIT for completion (max allowed timeout is 50s)
                # For long-running queries, use 0s to submit and return immediately
                result = ws.statement_execution.execute_statement(
                    warehouse_id=request.warehouse_id,
                    statement=stmt,
                    wait_timeout="50s"  # Max allowed by Databricks (5-50s range)
                )
                
                # Check if statement completed successfully
                status = result.status.state.value if result.status else "UNKNOWN"
                logger.info("statement_executed", 
                           number=idx+1, 
                           statement_id=result.statement_id,
                           status=status)
                
                if status not in ["SUCCEEDED", "SUCCESS"]:
                    # Statement did not complete successfully - get error details
                    failed_count += 1
                    
                    # Try to get error message from result
                    error_details = "No error details available"
                    if result.status and hasattr(result.status, 'error'):
                        error_obj = result.status.error
                        if error_obj:
                            error_details = f"{error_obj.error_code}: {error_obj.message}" if hasattr(error_obj, 'message') else str(error_obj)
                    
                    error_msg = f"Statement {idx + 1} failed with status {status}. Error: {error_details}"
                    
                    logger.error("statement_failed_with_bad_status", 
                                number=idx+1,
                                status=status,
                                statement_id=result.statement_id,
                                error_details=error_details)
                    
                    # Update tracking with failure
                    if lakebase and execution_record_id:
                        lakebase.update_execution_status(
                            execution_record_id, 
                            "FAILED",
                            succeeded_statements=succeeded_count,
                            failed_statements=failed_count,
                            execution_details={"statements": execution_ids},
                            error_message=error_msg
                        )
                    
                    raise HTTPException(status_code=500, detail=error_msg)
                
                succeeded_count += 1
                execution_ids.append({
                    "statement_number": idx + 1,
                    "statement_id": result.statement_id,
                    "status": status
                })
                
                logger.info("statement_completed_successfully", number=idx+1)
                
            except HTTPException:
                raise
            except Exception as stmt_error:
                failed_count += 1
                logger.error("statement_execution_failed", 
                            number=idx+1, 
                            error=str(stmt_error),
                            statement_preview=stmt[:200])
                
                # Update tracking with failure
                if lakebase and execution_record_id:
                    lakebase.update_execution_status(
                        execution_record_id,
                        "FAILED",
                        succeeded_statements=succeeded_count,
                        failed_statements=failed_count,
                        execution_details={"statements": execution_ids},
                        error_message=f"Statement {idx + 1} failed: {str(stmt_error)}"
                    )
                
                # If any statement fails, stop execution and return detailed error
                raise HTTPException(
                    status_code=500, 
                    detail=f"Statement {idx + 1} failed: {str(stmt_error)}"
                )
        
        logger.info("execution_complete", total_statements=len(sql_statements))
        
        # Update tracking with success
        if lakebase and execution_record_id:
            lakebase.update_execution_status(
                execution_record_id,
                "SUCCEEDED",
                succeeded_statements=succeeded_count,
                failed_statements=failed_count,
                execution_details={"statements": execution_ids}
            )
        
        return {
            "success": True,
            "execution_ids": execution_ids,
            "total_statements": len(sql_statements),
            "message": f"Successfully executed {len(sql_statements)} statement(s) sequentially",
            "execution_record_id": execution_record_id
        }
    except HTTPException as http_exc:
        # HTTPException from statement failures - tracking already updated
        raise
    except Exception as e:
        logger.error("execution_failed", error=str(e))
        
        # Update tracking with failure if record was created
        if lakebase and execution_record_id:
            try:
                lakebase.update_execution_status(
                    execution_record_id,
                    "FAILED",
                    succeeded_statements=0,
                    failed_statements=0,
                    error_message=f"Execution failed: {str(e)}"
                )
            except Exception as track_err:
                logger.error("failed_to_update_tracking", error=str(track_err))
        
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/executions")
async def list_executions(
    limit: int = 50,
    offset: int = 0,
    status: str = None,
    executor_user: str = None
):
    """
    List plan executions with filtering
    
    Query Parameters:
        - limit: Maximum number of records (default: 50)
        - offset: Offset for pagination (default: 0)
        - status: Filter by status (SUBMITTED, RUNNING, SUCCEEDED, FAILED, PARTIAL)
        - executor_user: Filter by executor user email
    """
    try:
        from infrastructure.lakebase_backend import get_lakebase_backend
        lakebase = get_lakebase_backend()
        
        if not lakebase:
            # Lakebase not available
            return {
                "executions": [],
                "total": 0,
                "message": "Execution tracking not available (Lakebase not configured)"
            }
        
        executions = lakebase.list_executions(
            limit=limit,
            offset=offset,
            status=status,
            executor_user=executor_user
        )
        
        return {
            "executions": executions,
            "total": len(executions),
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/executions/{execution_id}")
async def get_execution_status(execution_id: str):
    """Get status of a plan execution"""
    try:
        # Get execution record from tracker
        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        
        if not all([host, token, warehouse_id]):
            raise HTTPException(
                status_code=500,
                detail="Databricks credentials not configured"
            )
        
        tracker = ExecutionTracker(host, token, warehouse_id)
        execution = tracker.get_execution(execution_id)
        
        if not execution:
            raise HTTPException(status_code=404, detail="Execution not found")
        
        return {
            "execution_id": execution_id,
            "state": execution.state,
            "query_id": execution.query_id,
            "error_message": execution.error_message,
            "rows_affected": execution.rows_affected,
            "started_at": execution.started_at.isoformat() if execution.started_at else None,
            "completed_at": execution.completed_at.isoformat() if execution.completed_at else None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/agent/suggest")
async def agent_suggest_plan(request: AgentSuggestionRequest):
    """Get AI agent suggestion for plan creation"""
    try:
        from agents.plan_suggestion_agent import PlanSuggestionAgent
        
        agent = PlanSuggestionAgent(workspace_client)
        suggestion = agent.suggest_plan(
            intent=request.intent,
            user=request.user,
            context=request.context or {}
        )
        
        return {
            "success": True,
            "suggested_plan": suggestion["plan"],
            "confidence": suggestion.get("confidence", 0.0),
            "explanation": suggestion.get("explanation", ""),
            "warnings": suggestion.get("warnings", [])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/plans")
async def save_plan(request: PlanSaveRequest):
    """Save a new plan or update existing plan"""
    try:
        # Validate plan first
        is_valid, errors = compiler.validate_plan(request.plan)
        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail={"message": "Plan validation failed", "errors": errors}
            )
        
        # Save to plan registry if available
        if plan_registry_available:
            try:
                registry = get_plan_registry()
                result = registry.save_plan(request.plan)
                return result
            except RuntimeError as e:
                # Lakebase not configured - fall back to in-memory
                import structlog
                logger = structlog.get_logger()
                logger.warning("plan_registry_unavailable_fallback", error=str(e))
        
        # Fallback: In-memory only (no persistence)
        plan_id = request.plan.get("plan_metadata", {}).get("plan_id", "generated-plan-id")
        
        return {
            "success": True,
            "plan_id": plan_id,
            "message": "Plan validated successfully (not persisted - enable Lakebase for persistence)"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/plans")
async def list_plans(owner: Optional[str] = None, pattern_type: Optional[str] = None):
    """List all plans with optional filters"""
    try:
        # Use plan registry if available
        if plan_registry_available:
            try:
                registry = get_plan_registry()
                return registry.list_plans(owner=owner, pattern_type=pattern_type)
            except RuntimeError:
                pass  # Fall through to mock data
        
        # Fallback: Return mock data
        plans = [
            {
                "plan_id": "1",
                "plan_name": "customer_daily_incremental",
                "version": "1.2.0",
                "pattern_type": "INCREMENTAL_APPEND",
                "owner": "data-team@company.com",
                "created_at": "2026-01-15T10:00:00Z",
                "status": "active"
            },
            {
                "plan_id": "2",
                "plan_name": "product_catalog_refresh",
                "version": "1.0.0",
                "pattern_type": "FULL_REPLACE",
                "owner": "data-team@company.com",
                "created_at": "2026-01-14T15:30:00Z",
                "status": "active"
            }
        ]
        
        # Apply filters
        if owner:
            plans = [p for p in plans if p["owner"] == owner]
        if pattern_type:
            plans = [p for p in plans if p["pattern_type"] == pattern_type]
        
        return {
            "plans": plans,
            "total": len(plans)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/plans/{plan_id}")
async def get_plan(plan_id: str):
    """Get a specific plan by ID"""
    try:
        # Use plan registry if available
        if plan_registry_available:
            try:
                registry = get_plan_registry()
                plan = registry.get_plan(plan_id)
                if plan:
                    return plan
                else:
                    raise HTTPException(status_code=404, detail="Plan not found")
            except RuntimeError:
                pass  # Fall through to mock data
        
        # Fallback: Return mock data
        if plan_id == "1":
            return {
                "plan_id": "1",
                "plan_name": "customer_daily_incremental",
                "version": "1.2.0",
                "pattern_type": "INCREMENTAL_APPEND",
                "owner": "data-team@company.com",
                "created_at": "2026-01-15T10:00:00Z",
                "status": "active",
                "config": {
                    "source_table": "prod_catalog.raw.customer_events",
                    "target_table": "prod_catalog.curated.customer_daily",
                    "watermark_column": "event_timestamp"
                }
            }
        else:
            raise HTTPException(status_code=404, detail="Plan not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Include OAuth router if available
if oauth_available and oauth_router:
    app.include_router(oauth_router)

# Serve React frontend static files
static_path = Path(__file__).parent.parent / "ui" / "plan-editor" / "dist"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=static_path), name="static")
    
    @app.get("/")
    async def serve_spa():
        """Serve React SPA"""
        return FileResponse(static_path / "index.html")

# For development
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)

