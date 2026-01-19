"""
Tests for Databricks REST API Client

Tests the Databricks client with automatic OAuth token rotation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from infrastructure.databricks_client import (
    DatabricksClient,
    get_databricks_client
)


class TestDatabricksClient:
    """Test DatabricksClient"""
    
    @pytest.fixture
    def mock_oauth_manager(self):
        """Mock OAuth token manager"""
        with patch('infrastructure.databricks_client.get_oauth_token_manager') as mock:
            manager = Mock()
            manager.databricks_host = "test.databricks.com"
            manager.get_headers.return_value = {
                'Authorization': 'Bearer test_token',
                'Content-Type': 'application/json'
            }
            manager.get_token.return_value = 'test_token'
            mock.return_value = manager
            yield manager
    
    def test_initialization(self, mock_oauth_manager):
        """Test client initialization"""
        client = DatabricksClient(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret"
        )
        
        assert client.databricks_host == "test.databricks.com"
        assert client.base_url == "https://test.databricks.com/api/2.0"
        assert client.oauth_manager is not None
    
    def test_initialization_from_env(self, mock_oauth_manager):
        """Test client initialization from environment"""
        with patch.dict('os.environ', {
            'DATABRICKS_SERVER_HOSTNAME': 'env.databricks.com'
        }):
            client = DatabricksClient()
            assert client.databricks_host == 'env.databricks.com'
    
    def test_missing_host_raises_error(self):
        """Test initialization without host raises error"""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="DATABRICKS_SERVER_HOSTNAME required"):
                DatabricksClient()
    
    @patch('infrastructure.databricks_client.requests.request')
    def test_request_success(self, mock_request, mock_oauth_manager):
        """Test successful API request"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"result": "success"}'
        mock_response.json.return_value = {'result': 'success'}
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        result = client._request('GET', '/test-endpoint')
        
        assert result == {'result': 'success'}
        mock_request.assert_called_once()
    
    @patch('infrastructure.databricks_client.requests.request')
    def test_request_with_params(self, mock_request, mock_oauth_manager):
        """Test API request with query parameters"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"data": []}'
        mock_response.json.return_value = {'data': []}
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        result = client._request('GET', '/test', params={'limit': 10})
        
        call_args = mock_request.call_args
        assert call_args[1]['params'] == {'limit': 10}
    
    @patch('infrastructure.databricks_client.requests.request')
    def test_request_with_json_body(self, mock_request, mock_oauth_manager):
        """Test API request with JSON body"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"created": true}'
        mock_response.json.return_value = {'created': True}
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        result = client._request('POST', '/create', json={'name': 'test'})
        
        call_args = mock_request.call_args
        assert call_args[1]['json'] == {'name': 'test'}
    
    @patch('infrastructure.databricks_client.requests.request')
    def test_request_http_error(self, mock_request, mock_oauth_manager):
        """Test API request with HTTP error"""
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = requests.exceptions.HTTPError()
        http_error.response = mock_response  # Set the response attribute
        mock_response.raise_for_status.side_effect = http_error
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        
        with pytest.raises(requests.exceptions.HTTPError):
            client._request('GET', '/nonexistent')
    
    @patch('infrastructure.databricks_client.requests.request')
    def test_request_empty_response(self, mock_request, mock_oauth_manager):
        """Test API request with empty response"""
        mock_response = Mock()
        mock_response.status_code = 204
        mock_response.content = b''
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        result = client._request('DELETE', '/resource')
        
        assert result == {}


class TestUnityCatalogAPIs:
    """Test Unity Catalog API methods"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock Databricks client"""
        with patch('infrastructure.databricks_client.get_oauth_token_manager'):
            client = DatabricksClient(databricks_host="test.databricks.com")
            client._request = Mock()
            yield client
    
    def test_list_catalogs(self, mock_client):
        """Test listing catalogs"""
        mock_client._request.return_value = {
            'catalogs': [
                {'name': 'main', 'comment': 'Main catalog'},
                {'name': 'test', 'comment': 'Test catalog'}
            ]
        }
        
        catalogs = mock_client.list_catalogs()
        
        assert len(catalogs) == 2
        assert catalogs[0]['name'] == 'main'
        mock_client._request.assert_called_once_with('GET', '/unity-catalog/catalogs')
    
    def test_get_catalog(self, mock_client):
        """Test getting catalog details"""
        mock_client._request.return_value = {
            'name': 'main',
            'comment': 'Main catalog',
            'owner': 'admin'
        }
        
        catalog = mock_client.get_catalog('main')
        
        assert catalog['name'] == 'main'
        mock_client._request.assert_called_once_with(
            'GET',
            '/unity-catalog/catalogs/main'
        )
    
    def test_list_schemas(self, mock_client):
        """Test listing schemas"""
        mock_client._request.return_value = {
            'schemas': [
                {'name': 'default', 'catalog_name': 'main'},
                {'name': 'staging', 'catalog_name': 'main'}
            ]
        }
        
        schemas = mock_client.list_schemas('main')
        
        assert len(schemas) == 2
        mock_client._request.assert_called_once_with(
            'GET',
            '/unity-catalog/schemas',
            params={'catalog_name': 'main'}
        )
    
    def test_list_tables(self, mock_client):
        """Test listing tables"""
        mock_client._request.return_value = {
            'tables': [
                {'name': 'users', 'catalog_name': 'main', 'schema_name': 'default'},
                {'name': 'orders', 'catalog_name': 'main', 'schema_name': 'default'}
            ]
        }
        
        tables = mock_client.list_tables('main', 'default')
        
        assert len(tables) == 2
        mock_client._request.assert_called_once_with(
            'GET',
            '/unity-catalog/tables',
            params={'catalog_name': 'main', 'schema_name': 'default'}
        )
    
    def test_get_table(self, mock_client):
        """Test getting table details"""
        mock_client._request.return_value = {
            'name': 'users',
            'catalog_name': 'main',
            'schema_name': 'default',
            'columns': [
                {'name': 'id', 'type_name': 'INT'},
                {'name': 'name', 'type_name': 'STRING'}
            ]
        }
        
        table = mock_client.get_table('main', 'default', 'users')
        
        assert table['name'] == 'users'
        assert len(table['columns']) == 2
        mock_client._request.assert_called_once_with(
            'GET',
            '/unity-catalog/tables/main.default.users'
        )
    
    def test_list_table_columns(self, mock_client):
        """Test listing table columns"""
        mock_client._request.return_value = {
            'columns': [
                {'name': 'id', 'type_name': 'INT'},
                {'name': 'email', 'type_name': 'STRING'}
            ]
        }
        
        columns = mock_client.list_table_columns('main', 'default', 'users')
        
        assert len(columns) == 2
        assert columns[0]['name'] == 'id'


class TestSQLWarehouseAPIs:
    """Test SQL Warehouse API methods"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock Databricks client"""
        with patch('infrastructure.databricks_client.get_oauth_token_manager'):
            client = DatabricksClient(databricks_host="test.databricks.com")
            client._request = Mock()
            yield client
    
    def test_list_warehouses(self, mock_client):
        """Test listing warehouses"""
        mock_client._request.return_value = {
            'warehouses': [
                {'id': 'wh1', 'name': 'Main Warehouse'},
                {'id': 'wh2', 'name': 'Test Warehouse'}
            ]
        }
        
        warehouses = mock_client.list_warehouses()
        
        assert len(warehouses) == 2
        assert warehouses[0]['name'] == 'Main Warehouse'
        mock_client._request.assert_called_once_with('GET', '/sql/warehouses')
    
    def test_get_warehouse(self, mock_client):
        """Test getting warehouse details"""
        mock_client._request.return_value = {
            'id': 'wh1',
            'name': 'Main Warehouse',
            'state': 'RUNNING'
        }
        
        warehouse = mock_client.get_warehouse('wh1')
        
        assert warehouse['state'] == 'RUNNING'
        mock_client._request.assert_called_once_with('GET', '/sql/warehouses/wh1')
    
    def test_start_warehouse(self, mock_client):
        """Test starting warehouse"""
        mock_client._request.return_value = {'message': 'started'}
        
        result = mock_client.start_warehouse('wh1')
        
        assert result['message'] == 'started'
        mock_client._request.assert_called_once_with('POST', '/sql/warehouses/wh1/start')
    
    def test_stop_warehouse(self, mock_client):
        """Test stopping warehouse"""
        mock_client._request.return_value = {'message': 'stopped'}
        
        result = mock_client.stop_warehouse('wh1')
        
        assert result['message'] == 'stopped'
        mock_client._request.assert_called_once_with('POST', '/sql/warehouses/wh1/stop')


class TestSQLStatementAPIs:
    """Test SQL Statement Execution API methods"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock Databricks client"""
        with patch('infrastructure.databricks_client.get_oauth_token_manager'):
            client = DatabricksClient(databricks_host="test.databricks.com")
            client._request = Mock()
            yield client
    
    def test_execute_statement(self, mock_client):
        """Test executing SQL statement"""
        mock_client._request.return_value = {
            'statement_id': 'stmt123',
            'status': {'state': 'SUCCEEDED'}
        }
        
        result = mock_client.execute_statement(
            warehouse_id='wh1',
            statement='SELECT * FROM table',
            catalog='main',
            schema='default'
        )
        
        assert result['statement_id'] == 'stmt123'
        
        call_args = mock_client._request.call_args
        assert call_args[0] == ('POST', '/sql/statements')
        assert call_args[1]['json']['warehouse_id'] == 'wh1'
        assert call_args[1]['json']['statement'] == 'SELECT * FROM table'
        assert call_args[1]['json']['catalog'] == 'main'
        assert call_args[1]['json']['schema'] == 'default'
    
    def test_get_statement_status(self, mock_client):
        """Test getting statement status"""
        mock_client._request.return_value = {
            'statement_id': 'stmt123',
            'status': {'state': 'SUCCEEDED'}
        }
        
        result = mock_client.get_statement_status('stmt123')
        
        assert result['status']['state'] == 'SUCCEEDED'
        mock_client._request.assert_called_once_with('GET', '/sql/statements/stmt123')
    
    def test_cancel_statement(self, mock_client):
        """Test canceling statement"""
        mock_client._request.return_value = {'message': 'cancelled'}
        
        result = mock_client.cancel_statement('stmt123')
        
        assert result['message'] == 'cancelled'
        mock_client._request.assert_called_once_with('POST', '/sql/statements/stmt123/cancel')


class TestJobsAPIs:
    """Test Jobs API methods"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock Databricks client"""
        with patch('infrastructure.databricks_client.get_oauth_token_manager'):
            client = DatabricksClient(databricks_host="test.databricks.com")
            client._request = Mock()
            yield client
    
    def test_list_jobs(self, mock_client):
        """Test listing jobs"""
        mock_client._request.return_value = {
            'jobs': [
                {'job_id': 1, 'settings': {'name': 'Job 1'}},
                {'job_id': 2, 'settings': {'name': 'Job 2'}}
            ]
        }
        
        jobs = mock_client.list_jobs(limit=10)
        
        assert len(jobs) == 2
        mock_client._request.assert_called_once_with(
            'GET',
            '/jobs/list',
            params={'limit': 10, 'offset': 0}
        )
    
    def test_get_job(self, mock_client):
        """Test getting job details"""
        mock_client._request.return_value = {
            'job_id': 123,
            'settings': {'name': 'My Job'}
        }
        
        job = mock_client.get_job(123)
        
        assert job['job_id'] == 123
        mock_client._request.assert_called_once_with(
            'GET',
            '/jobs/get',
            params={'job_id': 123}
        )
    
    def test_run_job(self, mock_client):
        """Test running a job"""
        mock_client._request.return_value = {
            'run_id': 456,
            'number_in_job': 1
        }
        
        result = mock_client.run_job(
            job_id=123,
            notebook_params={'param1': 'value1'}
        )
        
        assert result['run_id'] == 456
        
        call_args = mock_client._request.call_args
        assert call_args[0] == ('POST', '/jobs/run-now')
        assert call_args[1]['json']['job_id'] == 123
        assert call_args[1]['json']['notebook_params'] == {'param1': 'value1'}
    
    def test_get_run(self, mock_client):
        """Test getting run details"""
        mock_client._request.return_value = {
            'run_id': 456,
            'state': {'life_cycle_state': 'RUNNING'}
        }
        
        run = mock_client.get_run(456)
        
        assert run['state']['life_cycle_state'] == 'RUNNING'
        mock_client._request.assert_called_once_with(
            'GET',
            '/jobs/runs/get',
            params={'run_id': 456}
        )
    
    def test_cancel_run(self, mock_client):
        """Test canceling a run"""
        mock_client._request.return_value = {}
        
        mock_client.cancel_run(456)
        
        mock_client._request.assert_called_once_with(
            'POST',
            '/jobs/runs/cancel',
            json={'run_id': 456}
        )


class TestClustersAPIs:
    """Test Clusters API methods"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock Databricks client"""
        with patch('infrastructure.databricks_client.get_oauth_token_manager'):
            client = DatabricksClient(databricks_host="test.databricks.com")
            client._request = Mock()
            yield client
    
    def test_list_clusters(self, mock_client):
        """Test listing clusters"""
        mock_client._request.return_value = {
            'clusters': [
                {'cluster_id': 'c1', 'cluster_name': 'Cluster 1'},
                {'cluster_id': 'c2', 'cluster_name': 'Cluster 2'}
            ]
        }
        
        clusters = mock_client.list_clusters()
        
        assert len(clusters) == 2
        mock_client._request.assert_called_once_with('GET', '/clusters/list')
    
    def test_get_cluster(self, mock_client):
        """Test getting cluster details"""
        mock_client._request.return_value = {
            'cluster_id': 'c1',
            'cluster_name': 'My Cluster',
            'state': 'RUNNING'
        }
        
        cluster = mock_client.get_cluster('c1')
        
        assert cluster['state'] == 'RUNNING'
        mock_client._request.assert_called_once_with(
            'GET',
            '/clusters/get',
            params={'cluster_id': 'c1'}
        )
    
    def test_start_cluster(self, mock_client):
        """Test starting cluster"""
        mock_client._request.return_value = {}
        
        mock_client.start_cluster('c1')
        
        mock_client._request.assert_called_once_with(
            'POST',
            '/clusters/start',
            json={'cluster_id': 'c1'}
        )
    
    def test_terminate_cluster(self, mock_client):
        """Test terminating cluster"""
        mock_client._request.return_value = {}
        
        mock_client.terminate_cluster('c1')
        
        mock_client._request.assert_called_once_with(
            'POST',
            '/clusters/delete',
            json={'cluster_id': 'c1'}
        )


class TestTokenInfo:
    """Test token info method"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock Databricks client"""
        with patch('infrastructure.databricks_client.get_oauth_token_manager') as mock:
            manager = Mock()
            manager.databricks_host = "test.databricks.com"
            manager.get_token_info.return_value = {
                'status': 'active',
                'expires_at': '2026-01-17T20:00:00Z',
                'minutes_until_expiry': 45
            }
            mock.return_value = manager
            
            client = DatabricksClient(databricks_host="test.databricks.com")
            yield client
    
    def test_get_token_info(self, mock_client):
        """Test getting token info"""
        info = mock_client.get_token_info()
        
        assert info['status'] == 'active'
        assert info['minutes_until_expiry'] == 45
        mock_client.oauth_manager.get_token_info.assert_called_once()


class TestSingletonClient:
    """Test singleton get_databricks_client function"""
    
    @patch('infrastructure.databricks_client.DatabricksClient')
    def test_get_databricks_client_singleton(self, mock_client_class):
        """Test get_databricks_client returns singleton"""
        # Reset singleton
        import infrastructure.databricks_client as db_module
        db_module._databricks_client = None
        
        mock_instance = Mock()
        mock_client_class.return_value = mock_instance
        
        client1 = get_databricks_client()
        client2 = get_databricks_client()
        
        # Should be same instance
        assert client1 is client2
        # DatabricksClient should only be called once
        mock_client_class.assert_called_once()

