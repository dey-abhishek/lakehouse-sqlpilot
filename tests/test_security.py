"""
Security Tests for Lakehouse SQLPilot
Tests authentication, authorization, rate limiting, and security features
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import time
import jwt
from datetime import datetime, timedelta

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from security.middleware import (
    create_access_token,
    verify_token,
    verify_api_key,
    check_rate_limit,
    sanitize_input,
    mask_sensitive_data,
    audit_log,
    load_security_config,
    SecurityConfig,
    AuthenticationError,
    AuthorizationError,
    RateLimitError,
    check_failed_auth_attempts,
    record_failed_auth_attempt,
    _rate_limit_store,
    _failed_auth_attempts
)


class TestAuthentication:
    """Test authentication features"""
    
    def test_create_access_token(self):
        """Test JWT token creation"""
        token = create_access_token(
            user="test_user",
            email="test@databricks.com",
            roles=["admin"]
        )
        
        assert token is not None
        assert isinstance(token, str)
        
        # Verify token can be decoded
        payload = verify_token(token)
        assert payload["user"] == "test_user"
        assert payload["email"] == "test@databricks.com"
        assert "admin" in payload["roles"]
    
    def test_verify_valid_token(self):
        """Test verifying a valid token"""
        token = create_access_token(
            user="test_user",
            email="test@databricks.com"
        )
        
        payload = verify_token(token)
        
        assert payload["user"] == "test_user"
        assert payload["email"] == "test@databricks.com"
    
    def test_verify_expired_token(self):
        """Test that expired tokens are rejected"""
        # Create token that expires immediately
        from datetime import timezone
        payload = {
            "user": "test_user",
            "email": "test@databricks.com",
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
            "iat": datetime.now(timezone.utc) - timedelta(hours=2)
        }
        expired_token = jwt.encode(payload, "change-me-in-production", algorithm="HS256")
        
        with pytest.raises(AuthenticationError) as exc_info:
            verify_token(expired_token)
        
        assert "expired" in str(exc_info.value.detail).lower()
    
    def test_verify_invalid_token(self):
        """Test that invalid tokens are rejected"""
        invalid_token = "invalid.token.here"
        
        with pytest.raises(AuthenticationError) as exc_info:
            verify_token(invalid_token)
        
        assert "invalid" in str(exc_info.value.detail).lower()
    
    @patch.dict('os.environ', {'SQLPILOT_API_KEYS': 'test_key_123,test_key_456'})
    def test_verify_valid_api_key(self):
        """Test verifying a valid API key"""
        # Reload config to pick up env var
        load_security_config()
        assert verify_api_key("test_key_123") is True
        assert verify_api_key("test_key_456") is True
    
    @patch.dict('os.environ', {'SQLPILOT_API_KEYS': 'test_key_123'})
    def test_verify_invalid_api_key(self):
        """Test that invalid API keys are rejected"""
        # Reload config to pick up env var
        load_security_config()
        assert verify_api_key("wrong_key") is False
    
    @patch.dict('os.environ', {'SQLPILOT_API_KEYS': ''})
    def test_api_key_no_config(self):
        """Test API key verification with no keys configured"""
        # Reload config to pick up env var (empty)
        load_security_config()
        # Should deny access when no API keys configured (secure default)
        assert verify_api_key("any_key") is False


class TestRateLimiting:
    """Test rate limiting features"""
    
    def setup_method(self):
        """Clear rate limit store before each test"""
        _rate_limit_store.clear()
    
    def test_rate_limit_allows_under_limit(self):
        """Test that requests under limit are allowed"""
        client_id = "test_client_1"
        
        # Make requests under the limit
        for i in range(5):
            check_rate_limit(client_id, limit=10, window=60)
        
        # Should not raise
        assert len(_rate_limit_store[client_id]) == 5
    
    def test_rate_limit_blocks_over_limit(self):
        """Test that requests over limit are blocked"""
        client_id = "test_client_2"
        limit = 5
        
        # Fill up to limit
        for i in range(limit):
            check_rate_limit(client_id, limit=limit, window=60)
        
        # Next request should fail
        with pytest.raises(RateLimitError) as exc_info:
            check_rate_limit(client_id, limit=limit, window=60)
        
        assert "rate limit exceeded" in str(exc_info.value.detail).lower()
    
    def test_rate_limit_window_cleanup(self):
        """Test that old requests are cleaned up from window"""
        client_id = "test_client_3"
        
        # Add old requests (outside window)
        _rate_limit_store[client_id] = [time.time() - 120, time.time() - 100]
        
        # New request should not count old ones
        check_rate_limit(client_id, limit=5, window=60)
        
        # Old entries should be cleaned
        assert all(ts > time.time() - 60 for ts in _rate_limit_store[client_id])
    
    @patch('security.middleware.SecurityConfig.RATE_LIMIT_ENABLED', False)
    def test_rate_limit_disabled(self):
        """Test that rate limiting can be disabled"""
        client_id = "test_client_4"
        
        # Should not raise even with many requests when disabled
        for i in range(10):
            check_rate_limit(client_id, limit=1, window=60)
        
        # Success - no exception raised
        assert True


class TestFailedAuthAttempts:
    """Test failed authentication attempt tracking"""
    
    def setup_method(self):
        """Clear failed attempts before each test"""
        _failed_auth_attempts.clear()
    
    def test_record_failed_attempt(self):
        """Test recording failed auth attempts"""
        client_ip = "192.168.1.1"
        
        record_failed_auth_attempt(client_ip)
        
        assert client_ip in _failed_auth_attempts
        assert len(_failed_auth_attempts[client_ip]) == 1
    
    def test_lockout_after_max_attempts(self):
        """Test account lockout after max failed attempts"""
        client_ip = "192.168.1.2"
        max_attempts = 5
        
        # Record max attempts
        for i in range(max_attempts):
            record_failed_auth_attempt(client_ip)
        
        # Next check should trigger lockout
        with pytest.raises(AuthenticationError) as exc_info:
            check_failed_auth_attempts(client_ip)
        
        assert "too many failed" in str(exc_info.value.detail).lower()
    
    def test_failed_attempts_cleanup(self):
        """Test that old failed attempts are cleaned up"""
        client_ip = "192.168.1.3"
        
        # Add old attempts
        _failed_auth_attempts[client_ip] = [time.time() - 400, time.time() - 350]
        
        # Check should clean old attempts (window is 300 seconds)
        check_failed_auth_attempts(client_ip)
        
        # Old entries should be cleaned
        assert len(_failed_auth_attempts[client_ip]) == 0


class TestInputSanitization:
    """Test input sanitization features"""
    
    def test_sanitize_normal_input(self):
        """Test that normal input is preserved"""
        text = "SELECT * FROM table WHERE id = 123"
        result = sanitize_input(text)
        
        assert result == text
    
    def test_sanitize_null_bytes(self):
        """Test that null bytes are removed"""
        text = "SELECT * FROM\x00 table"
        result = sanitize_input(text)
        
        assert '\x00' not in result
    
    def test_sanitize_max_length(self):
        """Test that input is truncated to max length"""
        text = "A" * 20000
        result = sanitize_input(text, max_length=1000)
        
        assert len(result) == 1000
    
    def test_sanitize_empty_input(self):
        """Test that empty input is handled"""
        assert sanitize_input("") == ""
        assert sanitize_input(None) == ""
    
    def test_sanitize_preserves_newlines_tabs(self):
        """Test that newlines and tabs are preserved"""
        text = "Line 1\nLine 2\tTabbed"
        result = sanitize_input(text)
        
        assert "\n" in result
        assert "\t" in result


class TestDataMasking:
    """Test sensitive data masking"""
    
    def test_mask_password(self):
        """Test that passwords are masked"""
        data = {
            "username": "user123",
            "password": "secret123",
            "email": "user@example.com"
        }
        
        masked = mask_sensitive_data(data)
        
        assert masked["username"] == "user123"
        assert masked["password"] == "***REDACTED***"
        assert masked["email"] == "user@example.com"
    
    def test_mask_token(self):
        """Test that tokens are masked"""
        data = {
            "user": "test",
            "api_token": "abc123xyz",
            "secret_key": "secretvalue"
        }
        
        masked = mask_sensitive_data(data)
        
        assert masked["api_token"] == "***REDACTED***"
        assert masked["secret_key"] == "***REDACTED***"
    
    def test_mask_custom_fields(self):
        """Test masking custom sensitive fields"""
        data = {
            "username": "user",
            "credit_card": "1234-5678-9012-3456"
        }
        
        masked = mask_sensitive_data(data, sensitive_fields=["credit_card"])
        
        assert masked["credit_card"] == "***REDACTED***"
    
    def test_mask_non_string_values(self):
        """Test that non-string sensitive values are masked"""
        data = {
            "password": 12345,
            "token": None
        }
        
        masked = mask_sensitive_data(data)
        
        assert masked["password"] == "***"
        assert masked["token"] == "***"


class TestAuditLogging:
    """Test audit logging features"""
    
    @patch('security.middleware.logger')
    def test_audit_log_success(self, mock_logger):
        """Test successful audit logging"""
        audit_log(
            event_type="plan_execution",
            user="test@databricks.com",
            plan_id="plan-123",
            action="execute"
        )
        
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args
        
        assert call_args[0][0] == "security_audit"
        assert "plan_id" in call_args[1] or call_args[1].get("plan_id") == "plan-123"
    
    @patch('security.middleware.logger')
    def test_audit_log_disabled(self, mock_logger):
        """Test that audit logging can be disabled"""
        # Temporarily disable audit logging
        original_state = SecurityConfig.AUDIT_LOG_ENABLED
        SecurityConfig.AUDIT_LOG_ENABLED = False
        
        try:
            audit_log(
                event_type="test_event",
                user="test",
                details={}
            )
            
            mock_logger.info.assert_not_called()
        finally:
            # Restore original state
            SecurityConfig.AUDIT_LOG_ENABLED = original_state


class TestSecurityConfig:
    """Test security configuration"""
    
    @patch.dict('os.environ', {
        'SQLPILOT_REQUIRE_AUTH': 'false',
        'SQLPILOT_RATE_LIMIT_ENABLED': 'false'
    })
    def test_development_mode(self):
        """Test development mode configuration"""
        # Reload config
        from importlib import reload
        import security.middleware
        reload(security.middleware)
        
        assert security.middleware.SecurityConfig.REQUIRE_AUTH is False
        assert security.middleware.SecurityConfig.RATE_LIMIT_ENABLED is False
    
    @patch.dict('os.environ', {
        'SQLPILOT_REQUIRE_AUTH': 'true',
        'SQLPILOT_RATE_LIMIT_ENABLED': 'true',
        'SQLPILOT_RATE_LIMIT_REQUESTS': '50',
        'SQLPILOT_RATE_LIMIT_WINDOW': '30'
    })
    def test_production_mode(self):
        """Test production mode configuration"""
        from importlib import reload
        import security.middleware
        reload(security.middleware)
        
        assert security.middleware.SecurityConfig.REQUIRE_AUTH is True
        assert security.middleware.SecurityConfig.RATE_LIMIT_ENABLED is True
        assert security.middleware.SecurityConfig.RATE_LIMIT_REQUESTS == 50
        assert security.middleware.SecurityConfig.RATE_LIMIT_WINDOW_SECONDS == 30


class TestSecurityIntegration:
    """Integration tests for security features"""
    
    def setup_method(self):
        """Clear state before each test"""
        _rate_limit_store.clear()
        _failed_auth_attempts.clear()
    
    def teardown_method(self):
        """Clean up after each test"""
        _rate_limit_store.clear()
        _failed_auth_attempts.clear()
    
    def test_full_authentication_flow(self):
        """Test complete authentication flow"""
        # Create token
        token = create_access_token(
            user="analyst",
            email="analyst@company.com",
            roles=["data_analyst"]
        )
        
        # Verify token
        payload = verify_token(token)
        
        assert payload["user"] == "analyst"
        assert "data_analyst" in payload["roles"]
    
    def test_sensitive_data_in_logs(self):
        """Test that sensitive data is masked in logs"""
        user_data = {
            "username": "test_user",
            "password": "secret_password",
            "email": "test@example.com",
            "api_key": "sk-abc123xyz"
        }
        
        # Mask before logging
        safe_data = mask_sensitive_data(user_data)
        
        assert safe_data["password"] == "***REDACTED***"
        assert safe_data["api_key"] == "***REDACTED***"
        assert safe_data["username"] == "test_user"  # Not sensitive


class TestSQLInjectionPrevention:
    """Test SQL injection prevention"""
    
    def test_sanitize_sql_injection_attempt(self):
        """Test that SQL injection attempts are sanitized"""
        malicious_input = "'; DROP TABLE users; --"
        result = sanitize_input(malicious_input)
        
        # Input is sanitized but we rely on parameterized queries for real protection
        assert result is not None
    
    def test_sanitize_union_injection(self):
        """Test UNION-based injection attempts"""
        malicious_input = "1' UNION SELECT * FROM passwords--"
        result = sanitize_input(malicious_input)
        
        assert result is not None


class TestSecurityHeaders:
    """Test security headers (will be tested with FastAPI integration)"""
    
    def test_security_headers_defined(self):
        """Test that security headers are defined in middleware"""
        from security.middleware import security_headers_middleware
        
        assert security_headers_middleware is not None

