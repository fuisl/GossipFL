"""
Configuration module for the Gateway Server and Client.

This module provides configuration classes and default settings for the
gateway implementation.
"""

import os
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class GatewayServerConfig:
    """Configuration for the Gateway Server."""
    
    # Server settings
    host: str = "0.0.0.0"
    port: int = 8080
    
    # Node management settings
    node_timeout: float = 300.0  # 5 minutes
    heartbeat_interval: float = 30.0  # 30 seconds
    cleanup_interval: float = 60.0  # 1 minute
    
    # HTTP server settings
    request_timeout: float = 30.0
    max_connections: int = 100
    
    # Logging settings
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    @classmethod
    def from_env(cls) -> 'GatewayServerConfig':
        """Create configuration from environment variables."""
        return cls(
            host=os.getenv('GATEWAY_HOST', cls.host),
            port=int(os.getenv('GATEWAY_PORT', cls.port)),
            node_timeout=float(os.getenv('GATEWAY_NODE_TIMEOUT', cls.node_timeout)),
            heartbeat_interval=float(os.getenv('GATEWAY_HEARTBEAT_INTERVAL', cls.heartbeat_interval)),
            cleanup_interval=float(os.getenv('GATEWAY_CLEANUP_INTERVAL', cls.cleanup_interval)),
            request_timeout=float(os.getenv('GATEWAY_REQUEST_TIMEOUT', cls.request_timeout)),
            max_connections=int(os.getenv('GATEWAY_MAX_CONNECTIONS', cls.max_connections)),
            log_level=os.getenv('GATEWAY_LOG_LEVEL', cls.log_level),
            log_format=os.getenv('GATEWAY_LOG_FORMAT', cls.log_format)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'host': self.host,
            'port': self.port,
            'node_timeout': self.node_timeout,
            'heartbeat_interval': self.heartbeat_interval,
            'cleanup_interval': self.cleanup_interval,
            'request_timeout': self.request_timeout,
            'max_connections': self.max_connections,
            'log_level': self.log_level,
            'log_format': self.log_format
        }


@dataclass
class GatewayClientConfig:
    """Configuration for the Gateway Client."""
    
    # Gateway server settings
    gateway_host: str = "localhost"
    gateway_port: int = 8080
    
    # Client settings
    timeout: float = 10.0
    heartbeat_interval: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0
    
    # Node settings
    node_id: int = 0
    ip_address: str = "127.0.0.1"
    port: int = 5000
    capabilities: list = None
    metadata: dict = None
    
    def __post_init__(self):
        """Initialize default values."""
        if self.capabilities is None:
            self.capabilities = ['raft', 'gossip']
        if self.metadata is None:
            self.metadata = {}
    
    @classmethod
    def from_env(cls) -> 'GatewayClientConfig':
        """Create configuration from environment variables."""
        capabilities = os.getenv('GATEWAY_CLIENT_CAPABILITIES', 'raft,gossip')
        capabilities_list = [cap.strip() for cap in capabilities.split(',')]
        
        return cls(
            gateway_host=os.getenv('GATEWAY_HOST', cls.gateway_host),
            gateway_port=int(os.getenv('GATEWAY_PORT', cls.gateway_port)),
            timeout=float(os.getenv('GATEWAY_CLIENT_TIMEOUT', cls.timeout)),
            heartbeat_interval=float(os.getenv('GATEWAY_CLIENT_HEARTBEAT_INTERVAL', cls.heartbeat_interval)),
            retry_attempts=int(os.getenv('GATEWAY_CLIENT_RETRY_ATTEMPTS', cls.retry_attempts)),
            retry_delay=float(os.getenv('GATEWAY_CLIENT_RETRY_DELAY', cls.retry_delay)),
            node_id=int(os.getenv('NODE_ID', cls.node_id)),
            ip_address=os.getenv('NODE_IP', cls.ip_address),
            port=int(os.getenv('NODE_PORT', cls.port)),
            capabilities=capabilities_list,
            metadata={}
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'gateway_host': self.gateway_host,
            'gateway_port': self.gateway_port,
            'timeout': self.timeout,
            'heartbeat_interval': self.heartbeat_interval,
            'retry_attempts': self.retry_attempts,
            'retry_delay': self.retry_delay,
            'node_id': self.node_id,
            'ip_address': self.ip_address,
            'port': self.port,
            'capabilities': self.capabilities,
            'metadata': self.metadata
        }


# Default configurations
DEFAULT_SERVER_CONFIG = GatewayServerConfig()
DEFAULT_CLIENT_CONFIG = GatewayClientConfig()


def load_config_from_file(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a file.
    
    Supports JSON and YAML formats.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
    """
    import json
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        if config_path.endswith('.json'):
            return json.load(f)
        elif config_path.endswith('.yaml') or config_path.endswith('.yml'):
            try:
                import yaml
                return yaml.safe_load(f)
            except ImportError:
                raise ImportError("PyYAML is required for YAML configuration files")
        else:
            raise ValueError(f"Unsupported configuration file format: {config_path}")


def create_server_config(config_dict: Dict[str, Any]) -> GatewayServerConfig:
    """
    Create server configuration from dictionary.
    
    Args:
        config_dict: Configuration dictionary
        
    Returns:
        GatewayServerConfig instance
    """
    return GatewayServerConfig(**config_dict)


def create_client_config(config_dict: Dict[str, Any]) -> GatewayClientConfig:
    """
    Create client configuration from dictionary.
    
    Args:
        config_dict: Configuration dictionary
        
    Returns:
        GatewayClientConfig instance
    """
    return GatewayClientConfig(**config_dict)


# Example configuration files content
EXAMPLE_SERVER_CONFIG = """
{
    "host": "0.0.0.0",
    "port": 8080,
    "node_timeout": 300.0,
    "heartbeat_interval": 30.0,
    "cleanup_interval": 60.0,
    "request_timeout": 30.0,
    "max_connections": 100,
    "log_level": "INFO",
    "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}
"""

EXAMPLE_CLIENT_CONFIG = """
{
    "gateway_host": "localhost",
    "gateway_port": 8080,
    "timeout": 10.0,
    "heartbeat_interval": 30.0,
    "retry_attempts": 3,
    "retry_delay": 1.0,
    "node_id": 1,
    "ip_address": "127.0.0.1",
    "port": 5001,
    "capabilities": ["raft", "gossip"],
    "metadata": {"role": "worker"}
}
"""
