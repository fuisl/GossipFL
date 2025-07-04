"""
Communication Manager Factory

This module provides a factory for creating communication managers with
different configurations, supporting both static and dynamic membership.
"""

import logging
from typing import Optional, Dict, List, Any
from .grpc_comm_manager import GRPCCommManager
from .dynamic_grpc_comm_manager import DynamicGRPCCommManager


class CommManagerFactory:
    """Factory for creating communication managers."""
    
    @staticmethod
    def create_comm_manager(
        communication_type: str = "dynamic_grpc",
        host: str = "localhost",
        port: int = 8890,
        node_id: int = 0,
        client_num: int = 1,
        topic: str = "fedml",
        # Dynamic/Gateway parameters
        gateway_host: str = "localhost",
        gateway_port: int = 8090,
        capabilities: List[str] = None,
        metadata: Dict[str, Any] = None,
        use_gateway: bool = True,
        # Static configuration parameters
        ip_config_path: Optional[str] = None,
        **kwargs
    ):
        """
        Create a communication manager based on the specified type.
        
        Args:
            communication_type: Type of communication manager ("grpc", "dynamic_grpc", "mqtt", "mpi")
            host: Local host IP address
            port: Local port number
            node_id: Unique node identifier
            client_num: Total number of clients
            topic: Communication topic
            gateway_host: Gateway server hostname (for dynamic_grpc)
            gateway_port: Gateway server port (for dynamic_grpc)
            capabilities: Node capabilities (for dynamic_grpc)
            metadata: Additional metadata (for dynamic_grpc)
            use_gateway: Whether to use gateway service discovery (for dynamic_grpc)
            ip_config_path: Path to static IP configuration file
            **kwargs: Additional parameters
            
        Returns:
            Communication manager instance
        """
        
        if communication_type == "dynamic_grpc":
            return DynamicGRPCCommManager(
                host=host,
                port=port,
                node_id=node_id,
                client_num=client_num,
                topic=topic,
                gateway_host=gateway_host,
                gateway_port=gateway_port,
                capabilities=capabilities,
                metadata=metadata,
                ip_config_path=ip_config_path,
                use_gateway=use_gateway
            )
        
        elif communication_type == "grpc":
            if not ip_config_path:
                raise ValueError("Static gRPC communication requires ip_config_path")
            
            return GRPCCommManager(
                host=host,
                port=port,
                ip_config_path=ip_config_path,
                topic=topic,
                client_id=node_id,
                client_num=client_num
            )
        
        elif communication_type == "mqtt":
            from ..mqtt.mqtt_comm_manager import MqttCommManager
            # Add MQTT specific parameters
            return MqttCommManager(**kwargs)
        
        elif communication_type == "mpi":
            from ..mpi.com_manager import MpiCommunicationManager
            # Add MPI specific parameters
            return MpiCommunicationManager(**kwargs)
        
        else:
            raise ValueError(f"Unsupported communication type: {communication_type}")
    
    @staticmethod
    def create_dynamic_grpc_manager(
        host: str,
        port: int,
        node_id: int,
        client_num: int = 1,
        gateway_host: str = "localhost",
        gateway_port: int = 8090,
        capabilities: List[str] = None,
        metadata: Dict[str, Any] = None,
        fallback_ip_config: Optional[str] = None
    ) -> DynamicGRPCCommManager:
        """
        Create a dynamic gRPC communication manager with service discovery.
        
        This is a convenience method for creating dynamic managers with
        sensible defaults.
        """
        return DynamicGRPCCommManager(
            host=host,
            port=port,
            node_id=node_id,
            client_num=client_num,
            gateway_host=gateway_host,
            gateway_port=gateway_port,
            capabilities=capabilities or ['grpc', 'fedml'],
            metadata=metadata or {},
            ip_config_path=fallback_ip_config,
            use_gateway=True
        )
    
    @staticmethod
    def create_static_grpc_manager(
        host: str,
        port: int,
        node_id: int,
        ip_config_path: str,
        client_num: int = 1,
        topic: str = "fedml"
    ) -> GRPCCommManager:
        """
        Create a static gRPC communication manager.
        
        This uses the traditional static IP configuration.
        """
        return GRPCCommManager(
            host=host,
            port=port,
            ip_config_path=ip_config_path,
            topic=topic,
            client_id=node_id,
            client_num=client_num
        )


class AutoCommManager:
    """
    Automatic communication manager that tries dynamic discovery first,
    then falls back to static configuration if available.
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        node_id: int,
        client_num: int = 1,
        gateway_host: str = "localhost",
        gateway_port: int = 8090,
        capabilities: List[str] = None,
        metadata: Dict[str, Any] = None,
        static_fallback_path: Optional[str] = None
    ):
        """
        Initialize auto communication manager.
        
        Args:
            host: Local host IP
            port: Local port
            node_id: Node identifier
            client_num: Number of clients
            gateway_host: Gateway hostname
            gateway_port: Gateway port
            capabilities: Node capabilities
            metadata: Node metadata
            static_fallback_path: Path to static IP config (fallback)
        """
        self.host = host
        self.port = port
        self.node_id = node_id
        self.client_num = client_num
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.capabilities = capabilities or ['grpc', 'fedml']
        self.metadata = metadata or {}
        self.static_fallback_path = static_fallback_path
        self.comm_manager = None
        
    def create_manager(self):
        """Create communication manager with automatic fallback."""
        try:
            # Try dynamic gRPC with gateway first
            logging.info("Attempting to create dynamic gRPC communication manager...")
            
            self.comm_manager = CommManagerFactory.create_dynamic_grpc_manager(
                host=self.host,
                port=self.port,
                node_id=self.node_id,
                client_num=self.client_num,
                gateway_host=self.gateway_host,
                gateway_port=self.gateway_port,
                capabilities=self.capabilities,
                metadata=self.metadata,
                fallback_ip_config=self.static_fallback_path
            )
            
            logging.info("Dynamic gRPC communication manager created successfully")
            return self.comm_manager
            
        except Exception as e:
            logging.warning(f"Failed to create dynamic gRPC manager: {e}")
            
            # Try static fallback if available
            if self.static_fallback_path:
                try:
                    logging.info("Falling back to static gRPC communication manager...")
                    
                    self.comm_manager = CommManagerFactory.create_static_grpc_manager(
                        host=self.host,
                        port=self.port,
                        node_id=self.node_id,
                        ip_config_path=self.static_fallback_path,
                        client_num=self.client_num
                    )
                    
                    logging.info("Static gRPC communication manager created successfully")
                    return self.comm_manager
                    
                except Exception as fallback_error:
                    logging.error(f"Static fallback also failed: {fallback_error}")
                    raise fallback_error
            else:
                logging.error("No static fallback configuration available")
                raise e
    
    def get_manager(self):
        """Get the created communication manager."""
        if not self.comm_manager:
            return self.create_manager()
        return self.comm_manager


def create_communication_manager(
    comm_type: str = "auto",
    host: str = "localhost",
    port: int = 8890,
    node_id: int = 0,
    client_num: int = 1,
    **kwargs
):
    """
    Convenience function to create communication managers.
    
    Args:
        comm_type: Type of communication manager
                  - "auto": Try dynamic, fallback to static
                  - "dynamic": Dynamic gRPC with service discovery
                  - "static": Static gRPC with IP config file
                  - "grpc": Alias for static
        host: Host IP address
        port: Port number
        node_id: Node identifier
        client_num: Number of clients
        **kwargs: Additional parameters
        
    Returns:
        Communication manager instance
    """
    
    if comm_type == "auto":
        auto_manager = AutoCommManager(
            host=host,
            port=port,
            node_id=node_id,
            client_num=client_num,
            **kwargs
        )
        return auto_manager.create_manager()
    
    elif comm_type == "dynamic":
        return CommManagerFactory.create_comm_manager(
            communication_type="dynamic_grpc",
            host=host,
            port=port,
            node_id=node_id,
            client_num=client_num,
            **kwargs
        )
    
    elif comm_type in ["static", "grpc"]:
        return CommManagerFactory.create_comm_manager(
            communication_type="grpc",
            host=host,
            port=port,
            node_id=node_id,
            client_num=client_num,
            **kwargs
        )
    
    else:
        return CommManagerFactory.create_comm_manager(
            communication_type=comm_type,
            host=host,
            port=port,
            node_id=node_id,
            client_num=client_num,
            **kwargs
        )
