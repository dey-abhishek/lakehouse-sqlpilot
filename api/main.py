"""
FastAPI Backend for Lakehouse SQLPilot
Provides REST API for the React frontend
"""

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import os
from pathlib import Path

from compiler import SQLCompiler
from execution import SQLExecutor, ExecutionTracker
from unity_catalog import PermissionValidator
from preview import PreviewEngine
from plan_schema.v1.validator import PlanValidator
from databricks.sdk import WorkspaceClient

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
workspace_client = WorkspaceClient()
compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
permission_validator = PermissionValidator(workspace_client)

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

@app.post("/api/v1/plans/execute")
async def execute_plan(request: ExecutionRequest):
    """Execute a compiled SQL plan"""
    try:
        # Initialize executor with workspace credentials
        from databricks import sql
        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        
        if not host or not token:
            raise HTTPException(
                status_code=500,
                detail="Databricks credentials not configured"
            )
        
        # Create executor and tracker
        tracker = ExecutionTracker(host, token, request.warehouse_id)
        executor = SQLExecutor(host, token, request.warehouse_id, tracker)
        
        # Execute SQL
        execution_id = await executor.execute_async(
            plan_id=request.plan_id,
            plan_version=request.plan_version,
            sql_statement=request.sql,
            executor_user=request.executor_user,
            timeout_seconds=request.timeout_seconds
        )
        
        return {
            "success": True,
            "execution_id": execution_id,
            "message": "Execution started successfully"
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
        
        # In production, save to a plan registry (Unity Catalog or database)
        # For now, return success
        plan_id = request.plan.get("plan_id", "generated-plan-id")
        
        return {
            "success": True,
            "plan_id": plan_id,
            "message": "Plan saved successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/plans")
async def list_plans(owner: Optional[str] = None, pattern_type: Optional[str] = None):
    """List all plans with optional filters"""
    try:
        # In production, fetch from plan registry
        # For now, return mock data
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
        # In production, fetch from plan registry
        # For now, return mock data
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

