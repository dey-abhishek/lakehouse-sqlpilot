"""
Databricks REST API Client with OAuth Token Auto-Refresh

Provides convenient wrappers for Databricks REST API calls with automatic
OAuth token rotation via the centralized token manager.

Usage:
    from infrastructure.databricks_client import DatabricksClient
    
    client = DatabricksClient()
    
    # Unity Catalog
    catalogs = client.list_catalogs()
    schemas = client.list_schemas(catalog="main")
    tables = client.list_tables(catalog="main", schema="default")
    
    # SQL Warehouses
    warehouses = client.list_warehouses()
    warehouse_info = client.get_warehouse(warehouse_id="abc123")
    
    # Jobs
    jobs = client.list_jobs()
    job_run = client.run_job(job_id=123, params={})
"""

import os
from typing import Dict, Any, List, Optional
import requests

import structlog
from infrastructure.oauth_token_manager import get_oauth_token_manager

logger = structlog.get_logger(__name__)


class DatabricksClient:
    """
    Databricks REST API client with OAuth token auto-refresh
    
    All API calls automatically use fresh OAuth tokens via the
    centralized token manager.
    """
    
    def __init__(self,
                 databricks_host: Optional[str] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None):
        """
        Initialize Databricks client
        
        Args:
            databricks_host: Databricks workspace hostname
            client_id: Service principal client ID
            client_secret: Service principal client secret
        """
        self.databricks_host = databricks_host or os.getenv("DATABRICKS_SERVER_HOSTNAME")
        
        if not self.databricks_host:
            raise ValueError("DATABRICKS_SERVER_HOSTNAME required")
        
        # Initialize OAuth token manager (singleton)
        self.oauth_manager = get_oauth_token_manager(
            databricks_host=self.databricks_host,
            client_id=client_id,
            client_secret=client_secret,
            auto_refresh=True,
            refresh_buffer_minutes=5
        )
        
        self.base_url = f"https://{self.databricks_host}/api/2.0"
        
        logger.info(
            "databricks_client_initialized",
            host=self.databricks_host,
            base_url=self.base_url
        )
    
    def _request(self,
                 method: str,
                 endpoint: str,
                 params: Optional[Dict] = None,
                 json: Optional[Dict] = None,
                 timeout: int = 30) -> Dict[str, Any]:
        """
        Make authenticated API request with auto-refreshing OAuth token
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., "/unity-catalog/catalogs")
            params: Query parameters
            json: JSON body
            timeout: Request timeout in seconds
            
        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        
        # Get fresh OAuth token (auto-refreshes if needed)
        headers = self.oauth_manager.get_headers()
        
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json,
                timeout=timeout
            )
            
            response.raise_for_status()
            
            # Return JSON response if present
            if response.content:
                return response.json()
            return {}
            
        except requests.exceptions.HTTPError as e:
            logger.error(
                "databricks_api_error",
                method=method,
                endpoint=endpoint,
                status_code=e.response.status_code,
                error=str(e)
            )
            raise
        except Exception as e:
            logger.error(
                "databricks_api_request_failed",
                method=method,
                endpoint=endpoint,
                error=str(e)
            )
            raise
    
    # ========================================
    # Unity Catalog APIs
    # ========================================
    
    def list_catalogs(self) -> List[Dict[str, Any]]:
        """List all Unity Catalog catalogs"""
        response = self._request("GET", "/unity-catalog/catalogs")
        return response.get("catalogs", [])
    
    def get_catalog(self, catalog: str) -> Dict[str, Any]:
        """Get catalog details"""
        return self._request("GET", f"/unity-catalog/catalogs/{catalog}")
    
    def list_schemas(self, catalog: str) -> List[Dict[str, Any]]:
        """List all schemas in a catalog"""
        response = self._request(
            "GET",
            "/unity-catalog/schemas",
            params={"catalog_name": catalog}
        )
        return response.get("schemas", [])
    
    def get_schema(self, catalog: str, schema: str) -> Dict[str, Any]:
        """Get schema details"""
        return self._request("GET", f"/unity-catalog/schemas/{catalog}.{schema}")
    
    def list_tables(self, catalog: str, schema: str) -> List[Dict[str, Any]]:
        """List all tables in a schema"""
        response = self._request(
            "GET",
            "/unity-catalog/tables",
            params={
                "catalog_name": catalog,
                "schema_name": schema
            }
        )
        return response.get("tables", [])
    
    def get_table(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Get table details"""
        full_name = f"{catalog}.{schema}.{table}"
        return self._request("GET", f"/unity-catalog/tables/{full_name}")
    
    def list_table_columns(self, catalog: str, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get table column definitions"""
        table_info = self.get_table(catalog, schema, table)
        return table_info.get("columns", [])
    
    # ========================================
    # SQL Warehouse APIs
    # ========================================
    
    def list_warehouses(self) -> List[Dict[str, Any]]:
        """List all SQL warehouses"""
        response = self._request("GET", "/sql/warehouses")
        return response.get("warehouses", [])
    
    def get_warehouse(self, warehouse_id: str) -> Dict[str, Any]:
        """Get warehouse details"""
        return self._request("GET", f"/sql/warehouses/{warehouse_id}")
    
    def start_warehouse(self, warehouse_id: str) -> Dict[str, Any]:
        """Start a SQL warehouse"""
        return self._request("POST", f"/sql/warehouses/{warehouse_id}/start")
    
    def stop_warehouse(self, warehouse_id: str) -> Dict[str, Any]:
        """Stop a SQL warehouse"""
        return self._request("POST", f"/sql/warehouses/{warehouse_id}/stop")
    
    # ========================================
    # SQL Statement Execution API
    # ========================================
    
    def execute_statement(self,
                         warehouse_id: str,
                         statement: str,
                         catalog: Optional[str] = None,
                         schema: Optional[str] = None,
                         wait_timeout: str = "30s") -> Dict[str, Any]:
        """
        Execute SQL statement via Statement Execution API
        
        Args:
            warehouse_id: SQL warehouse ID
            statement: SQL statement to execute
            catalog: Optional catalog name
            schema: Optional schema name
            wait_timeout: Wait timeout (e.g., "30s", "5m")
            
        Returns:
            Statement execution result
        """
        payload = {
            "warehouse_id": warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout
        }
        
        if catalog:
            payload["catalog"] = catalog
        if schema:
            payload["schema"] = schema
        
        return self._request("POST", "/sql/statements", json=payload)
    
    def get_statement_status(self, statement_id: str) -> Dict[str, Any]:
        """Get SQL statement execution status"""
        return self._request("GET", f"/sql/statements/{statement_id}")
    
    def cancel_statement(self, statement_id: str) -> Dict[str, Any]:
        """Cancel SQL statement execution"""
        return self._request("POST", f"/sql/statements/{statement_id}/cancel")
    
    # ========================================
    # Jobs API
    # ========================================
    
    def list_jobs(self, limit: int = 25, offset: int = 0) -> List[Dict[str, Any]]:
        """List all jobs"""
        response = self._request(
            "GET",
            "/jobs/list",
            params={"limit": limit, "offset": offset}
        )
        return response.get("jobs", [])
    
    def get_job(self, job_id: int) -> Dict[str, Any]:
        """Get job details"""
        response = self._request("GET", "/jobs/get", params={"job_id": job_id})
        return response
    
    def run_job(self,
                job_id: int,
                notebook_params: Optional[Dict] = None,
                jar_params: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Run a job now
        
        Args:
            job_id: Job ID to run
            notebook_params: Notebook parameters
            jar_params: JAR parameters
            
        Returns:
            Run information including run_id
        """
        payload = {"job_id": job_id}
        
        if notebook_params:
            payload["notebook_params"] = notebook_params
        if jar_params:
            payload["jar_params"] = jar_params
        
        return self._request("POST", "/jobs/run-now", json=payload)
    
    def get_run(self, run_id: int) -> Dict[str, Any]:
        """Get job run details"""
        response = self._request("GET", "/jobs/runs/get", params={"run_id": run_id})
        return response
    
    def cancel_run(self, run_id: int) -> None:
        """Cancel a job run"""
        self._request("POST", "/jobs/runs/cancel", json={"run_id": run_id})
    
    # ========================================
    # Clusters API
    # ========================================
    
    def list_clusters(self) -> List[Dict[str, Any]]:
        """List all clusters"""
        response = self._request("GET", "/clusters/list")
        return response.get("clusters", [])
    
    def get_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """Get cluster details"""
        response = self._request("GET", "/clusters/get", params={"cluster_id": cluster_id})
        return response
    
    def start_cluster(self, cluster_id: str) -> None:
        """Start a cluster"""
        self._request("POST", "/clusters/start", json={"cluster_id": cluster_id})
    
    def restart_cluster(self, cluster_id: str) -> None:
        """Restart a cluster"""
        self._request("POST", "/clusters/restart", json={"cluster_id": cluster_id})
    
    def terminate_cluster(self, cluster_id: str) -> None:
        """Terminate a cluster"""
        self._request("POST", "/clusters/delete", json={"cluster_id": cluster_id})
    
    # ========================================
    # Token Info
    # ========================================
    
    def get_token_info(self) -> Dict[str, Any]:
        """Get current OAuth token information"""
        return self.oauth_manager.get_token_info()


# Singleton instance
_databricks_client: Optional[DatabricksClient] = None


def get_databricks_client() -> DatabricksClient:
    """Get global Databricks client instance (singleton)"""
    global _databricks_client
    
    if _databricks_client is None:
        _databricks_client = DatabricksClient()
    
    return _databricks_client


