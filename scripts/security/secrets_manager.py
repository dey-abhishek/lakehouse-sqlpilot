"""
Secrets Management for Lakehouse SQLPilot
Supports multiple secrets backends for secure credential storage
"""

import os
from typing import Optional, Dict, Any
from pathlib import Path
import json
import base64
from abc import ABC, abstractmethod
import structlog

logger = structlog.get_logger()


class SecretsBackend(ABC):
    """Abstract base class for secrets backends"""
    
    @abstractmethod
    def get_secret(self, key: str) -> Optional[str]:
        """Get a secret value by key"""
        pass
    
    @abstractmethod
    def set_secret(self, key: str, value: str) -> bool:
        """Set a secret value"""
        pass
    
    @abstractmethod
    def list_secrets(self) -> list:
        """List all available secret keys"""
        pass


class EnvironmentSecretsBackend(SecretsBackend):
    """Read secrets from environment variables (default, for development)"""
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from environment variable"""
        return os.getenv(key)
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set environment variable (runtime only)"""
        os.environ[key] = value
        return True
    
    def list_secrets(self) -> list:
        """List all environment variables"""
        return list(os.environ.keys())


class FileSecretsBackend(SecretsBackend):
    """Read secrets from encrypted file (development/testing)"""
    
    def __init__(self, secrets_file: str = ".secrets.json"):
        self.secrets_file = Path(secrets_file)
        self._secrets_cache: Optional[Dict[str, str]] = None
        self._encryption_key: Optional[bytes] = None
        
        # Try to load encryption key from environment
        key_b64 = os.getenv("SQLPILOT_SECRETS_KEY")
        if key_b64:
            try:
                self._encryption_key = base64.b64decode(key_b64)
            except Exception as e:
                logger.warning("invalid_encryption_key", error=str(e))
    
    def _load_secrets(self) -> Dict[str, str]:
        """Load secrets from file"""
        if self._secrets_cache is not None:
            return self._secrets_cache
        
        if not self.secrets_file.exists():
            logger.info("secrets_file_not_found", file=str(self.secrets_file))
            self._secrets_cache = {}
            return self._secrets_cache
        
        try:
            with open(self.secrets_file, 'r') as f:
                data = json.load(f)
            
            # If encrypted, decrypt
            if data.get('encrypted') and self._encryption_key:
                secrets = self._decrypt_secrets(data.get('secrets', {}))
            else:
                secrets = data.get('secrets', {})
            
            self._secrets_cache = secrets
            logger.info("secrets_loaded", count=len(secrets))
            return secrets
        
        except Exception as e:
            logger.error("failed_to_load_secrets", error=str(e))
            self._secrets_cache = {}
            return self._secrets_cache
    
    def _decrypt_secrets(self, encrypted_secrets: Dict[str, str]) -> Dict[str, str]:
        """Decrypt secrets (placeholder - implement with cryptography library)"""
        # In production, use proper encryption (Fernet, AES-GCM, etc.)
        # For now, just base64 decode as simple obfuscation
        decrypted = {}
        for key, value in encrypted_secrets.items():
            try:
                decrypted[key] = base64.b64decode(value).decode('utf-8')
            except:
                decrypted[key] = value
        return decrypted
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from file"""
        secrets = self._load_secrets()
        return secrets.get(key)
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in file"""
        secrets = self._load_secrets()
        secrets[key] = value
        
        try:
            data = {
                'encrypted': False,  # Set to True when implementing encryption
                'secrets': secrets
            }
            
            with open(self.secrets_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            self._secrets_cache = secrets
            logger.info("secret_saved", key=key)
            return True
        
        except Exception as e:
            logger.error("failed_to_save_secret", key=key, error=str(e))
            return False
    
    def list_secrets(self) -> list:
        """List all secret keys"""
        secrets = self._load_secrets()
        return list(secrets.keys())


class DatabricksSecretsBackend(SecretsBackend):
    """Read secrets from Databricks Secrets API (production)"""
    
    def __init__(self):
        try:
            from databricks.sdk import WorkspaceClient
            self.client = WorkspaceClient()
            self.scope = os.getenv("SQLPILOT_SECRETS_SCOPE", "sqlpilot")
            logger.info("databricks_secrets_initialized", scope=self.scope)
        except Exception as e:
            logger.warning("databricks_secrets_unavailable", error=str(e))
            self.client = None
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from Databricks Secrets"""
        if not self.client:
            return None
        
        try:
            # Databricks secret key format: scope/key
            secret = self.client.secrets.get_secret(self.scope, key)
            return secret.value
        except Exception as e:
            logger.debug("databricks_secret_not_found", key=key, error=str(e))
            return None
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in Databricks Secrets"""
        if not self.client:
            return False
        
        try:
            self.client.secrets.put_secret(
                scope=self.scope,
                key=key,
                string_value=value
            )
            logger.info("databricks_secret_saved", key=key, scope=self.scope)
            return True
        except Exception as e:
            logger.error("failed_to_save_databricks_secret", key=key, error=str(e))
            return False
    
    def list_secrets(self) -> list:
        """List all secrets in scope"""
        if not self.client:
            return []
        
        try:
            secrets = self.client.secrets.list_secrets(self.scope)
            return [s.key for s in secrets]
        except Exception as e:
            logger.error("failed_to_list_databricks_secrets", error=str(e))
            return []


class AWSSecretsBackend(SecretsBackend):
    """Read secrets from AWS Secrets Manager (production)"""
    
    def __init__(self):
        try:
            import boto3
            self.client = boto3.client('secretsmanager')
            self.prefix = os.getenv("SQLPILOT_AWS_SECRETS_PREFIX", "sqlpilot/")
            logger.info("aws_secrets_initialized", prefix=self.prefix)
        except Exception as e:
            logger.warning("aws_secrets_unavailable", error=str(e))
            self.client = None
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from AWS Secrets Manager"""
        if not self.client:
            return None
        
        try:
            secret_name = f"{self.prefix}{key}"
            response = self.client.get_secret_value(SecretId=secret_name)
            
            if 'SecretString' in response:
                return response['SecretString']
            else:
                # Binary secret
                return base64.b64decode(response['SecretBinary']).decode('utf-8')
        except Exception as e:
            logger.debug("aws_secret_not_found", key=key, error=str(e))
            return None
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in AWS Secrets Manager"""
        if not self.client:
            return False
        
        try:
            secret_name = f"{self.prefix}{key}"
            self.client.create_secret(Name=secret_name, SecretString=value)
            logger.info("aws_secret_saved", key=key)
            return True
        except self.client.exceptions.ResourceExistsException:
            # Update existing secret
            self.client.update_secret(SecretId=secret_name, SecretString=value)
            logger.info("aws_secret_updated", key=key)
            return True
        except Exception as e:
            logger.error("failed_to_save_aws_secret", key=key, error=str(e))
            return False
    
    def list_secrets(self) -> list:
        """List all secrets with prefix"""
        if not self.client:
            return []
        
        try:
            paginator = self.client.get_paginator('list_secrets')
            secrets = []
            for page in paginator.paginate():
                for secret in page['SecretList']:
                    if secret['Name'].startswith(self.prefix):
                        key = secret['Name'][len(self.prefix):]
                        secrets.append(key)
            return secrets
        except Exception as e:
            logger.error("failed_to_list_aws_secrets", error=str(e))
            return []


class AzureSecretsBackend(SecretsBackend):
    """Read secrets from Azure Key Vault (production)"""
    
    def __init__(self):
        try:
            from azure.keyvault.secrets import SecretClient
            from azure.identity import DefaultAzureCredential
            
            vault_url = os.getenv("AZURE_KEY_VAULT_URL")
            if not vault_url:
                raise ValueError("AZURE_KEY_VAULT_URL not set")
            
            credential = DefaultAzureCredential()
            self.client = SecretClient(vault_url=vault_url, credential=credential)
            self.prefix = os.getenv("SQLPILOT_AZURE_SECRETS_PREFIX", "sqlpilot-")
            logger.info("azure_secrets_initialized", vault_url=vault_url)
        except Exception as e:
            logger.warning("azure_secrets_unavailable", error=str(e))
            self.client = None
    
    def get_secret(self, key: str) -> Optional[str]:
        """Get secret from Azure Key Vault"""
        if not self.client:
            return None
        
        try:
            secret_name = f"{self.prefix}{key}".replace('_', '-')
            secret = self.client.get_secret(secret_name)
            return secret.value
        except Exception as e:
            logger.debug("azure_secret_not_found", key=key, error=str(e))
            return None
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in Azure Key Vault"""
        if not self.client:
            return False
        
        try:
            secret_name = f"{self.prefix}{key}".replace('_', '-')
            self.client.set_secret(secret_name, value)
            logger.info("azure_secret_saved", key=key)
            return True
        except Exception as e:
            logger.error("failed_to_save_azure_secret", key=key, error=str(e))
            return False
    
    def list_secrets(self) -> list:
        """List all secrets with prefix"""
        if not self.client:
            return []
        
        try:
            secrets = []
            for secret_properties in self.client.list_properties_of_secrets():
                if secret_properties.name.startswith(self.prefix):
                    key = secret_properties.name[len(self.prefix):].replace('-', '_')
                    secrets.append(key)
            return secrets
        except Exception as e:
            logger.error("failed_to_list_azure_secrets", error=str(e))
            return []


class SecretsManager:
    """
    Unified secrets manager with multiple backend support
    Falls back through backends in priority order
    """
    
    def __init__(self):
        self.backends: list[SecretsBackend] = []
        self._initialize_backends()
    
    def _initialize_backends(self):
        """Initialize secrets backends based on configuration"""
        backend_type = os.getenv("SQLPILOT_SECRETS_BACKEND", "env").lower()
        
        # Always add environment backend as ultimate fallback
        fallback = EnvironmentSecretsBackend()
        
        if backend_type == "file":
            self.backends.append(FileSecretsBackend())
            self.backends.append(fallback)
            logger.info("secrets_backend_initialized", type="file")
        
        elif backend_type == "databricks":
            self.backends.append(DatabricksSecretsBackend())
            self.backends.append(fallback)
            logger.info("secrets_backend_initialized", type="databricks")
        
        elif backend_type == "aws":
            self.backends.append(AWSSecretsBackend())
            self.backends.append(fallback)
            logger.info("secrets_backend_initialized", type="aws")
        
        elif backend_type == "azure":
            self.backends.append(AzureSecretsBackend())
            self.backends.append(fallback)
            logger.info("secrets_backend_initialized", type="azure")
        
        else:  # "env" or default
            self.backends.append(fallback)
            logger.info("secrets_backend_initialized", type="env")
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get secret from first available backend
        Falls back through backends in order
        """
        for backend in self.backends:
            try:
                value = backend.get_secret(key)
                if value is not None:
                    logger.debug("secret_retrieved", key=key, backend=backend.__class__.__name__)
                    return value
            except Exception as e:
                logger.debug("backend_failed", backend=backend.__class__.__name__, error=str(e))
                continue
        
        logger.debug("secret_not_found", key=key)
        return default
    
    def set_secret(self, key: str, value: str) -> bool:
        """Set secret in primary backend"""
        if not self.backends:
            return False
        
        return self.backends[0].set_secret(key, value)
    
    def list_secrets(self) -> list:
        """List all secrets from all backends"""
        all_secrets = set()
        for backend in self.backends:
            try:
                secrets = backend.list_secrets()
                all_secrets.update(secrets)
            except Exception as e:
                logger.debug("list_failed", backend=backend.__class__.__name__, error=str(e))
        
        return sorted(list(all_secrets))


# Global secrets manager instance
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager() -> SecretsManager:
    """Get or create global secrets manager instance"""
    global _secrets_manager
    if _secrets_manager is None:
        _secrets_manager = SecretsManager()
    return _secrets_manager


def get_secret(key: str, default: Optional[str] = None) -> Optional[str]:
    """Convenience function to get a secret"""
    return get_secrets_manager().get_secret(key, default)


def set_secret(key: str, value: str) -> bool:
    """Convenience function to set a secret"""
    return get_secrets_manager().set_secret(key, value)

