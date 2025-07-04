"""
gRPC Gateway Client for RAFT+GossipFL Node Discovery

This module provides a decoupled gRPC-based client interface for nodes to
communicate with the gateway server for discovery and registration purposes.
The client is designed to integrate seamlessly with existing gRPC infrastructure.
"""

import logging
import time
import threading
from typing import Dict, List, Optional, Tuple, Callable
import grpc

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2
from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc


class GRPCGatewayClient:
    """gRPC client for communicating with the gateway server."""
    
    def __init__(self, gateway_host: str, gateway_port: int, 
                 timeout: float = 10.0, max_retry_attempts: int = 3):
        """
        Initialize the gRPC gateway client.
        
        Args:
            gateway_host: Gateway server hostname or IP
            gateway_port: Gateway server port
            timeout: Request timeout in seconds
            max_retry_attempts: Maximum retry attempts for failed requests
        """
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.timeout = timeout
        self.max_retry_attempts = max_retry_attempts
        self.gateway_address = f"{gateway_host}:{gateway_port}"
        
        # Client state
        self.node_id = None
        self.is_registered = False
        self.last_heartbeat = 0
        self.heartbeat_interval = 30.0  # seconds
        self.heartbeat_thread = None
        self.heartbeat_running = False
        
        # gRPC connection
        self._channel = None
        self._stub = None
        self._connection_lock = threading.RLock()
        
        # Callbacks for events
        self.on_registration_success: Optional[Callable] = None
        self.on_registration_failure: Optional[Callable] = None
        self.on_heartbeat_failure: Optional[Callable] = None
        self.on_connection_lost: Optional[Callable] = None
        
        logging.info(f"gRPC Gateway client initialized for {self.gateway_address}")
    
    def _get_stub(self):
        """Get or create gRPC stub with connection management."""
        with self._connection_lock:
            if self._channel is None or self._stub is None:
                self._channel = grpc.insecure_channel(
                    self.gateway_address,
                    options=[
                        ('grpc.max_send_message_length', 1000 * 1024 * 1024),
                        ('grpc.max_receive_message_length', 1000 * 1024 * 1024),
                    ]
                )
                self._stub = grpc_comm_manager_pb2_grpc.GatewayServiceStub(self._channel)
            return self._stub
    
    def _close_connection(self):
        """Close gRPC connection."""
        with self._connection_lock:
            if self._channel:
                self._channel.close()
                self._channel = None
                self._stub = None
    
    def _make_request(self, request_func, request_obj, operation_name: str):
        """
        Make a gRPC request with retry logic and error handling.
        
        Args:
            request_func: gRPC method to call
            request_obj: Request object
            operation_name: Name of the operation for logging
            
        Returns:
            Response object or None if failed
        """
        last_exception = None
        
        for attempt in range(self.max_retry_attempts):
            try:
                stub = self._get_stub()
                response = request_func(request_obj, timeout=self.timeout)
                return response
                
            except grpc.RpcError as e:
                last_exception = e
                logging.warning(f"{operation_name} attempt {attempt + 1} failed: {e.code()} - {e.details()}")
                
                # Close connection on certain errors to force reconnection
                if e.code() in [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED]:
                    self._close_connection()
                
                if attempt < self.max_retry_attempts - 1:
                    time.sleep(min(2 ** attempt, 10))  # Exponential backoff with cap
                    
            except Exception as e:
                last_exception = e
                logging.error(f"{operation_name} unexpected error: {e}")
                break
        
        # All attempts failed
        logging.error(f"{operation_name} failed after {self.max_retry_attempts} attempts: {last_exception}")
        if self.on_connection_lost:
            self.on_connection_lost(last_exception)
        
        return None
    
    def health_check(self) -> Optional[Dict]:
        """
        Check gateway server health.
        
        Returns:
            Health status information or None if failed
        """
        request = grpc_comm_manager_pb2.HealthCheckRequest()
        response = self._make_request(
            self._get_stub().HealthCheck,
            request,
            "health_check"
        )
        
        if response:
            return {
                "status": response.status,
                "timestamp": response.timestamp,
                "version": response.version
            }
        return None
    
    def register_node(self, node_id: int, ip_address: str, port: int, 
                     capabilities: List[str], metadata: Dict[str, str] = None) -> Optional[Dict]:
        """
        Register this node with the gateway.
        
        Args:
            node_id: Unique node identifier
            ip_address: Node's IP address
            port: Node's port
            capabilities: List of node capabilities (e.g., ['raft', 'gossip'])
            metadata: Additional node metadata
            
        Returns:
            Registration response from gateway or None if failed
        """
        if metadata:
            metadata = {str(k): str(v) for k, v in metadata.items()}
        
        request = grpc_comm_manager_pb2.RegisterNodeRequest(
            node_id=node_id,
            ip_address=ip_address,
            port=port,
            capabilities=capabilities,
            metadata=metadata or {}
        )
        
        response = self._make_request(
            self._get_stub().RegisterNode,
            request,
            "register_node"
        )
        
        if response:
            # Update client state
            self.node_id = node_id
            self.is_registered = True
            self.last_heartbeat = time.time()
            
            # Start heartbeat if registration successful
            if response.status in ['registered', 'updated']:
                self._start_heartbeat()
                if self.on_registration_success:
                    self.on_registration_success(response)
            
            # Convert response to dictionary
            result = {
                "status": response.status,
                "node_id": response.node_id,
                "is_bootstrap": response.is_bootstrap,
                "registry_version": response.registry_version,
                "message": response.message,
                "nodes": [self._protobuf_to_node_dict(node) for node in response.nodes],
                "leader": self._protobuf_to_node_dict(response.leader) if response.leader.node_id != 0 else None
            }
            
            logging.info(f"Node {node_id} registered with gateway: {response.status}")
            return result
        else:
            if self.on_registration_failure:
                self.on_registration_failure("Failed to register with gateway")
            return None
    
    def get_nodes(self, exclude_self: bool = True) -> Optional[List[Dict]]:
        """
        Get list of active nodes from gateway.
        
        Args:
            exclude_self: Whether to exclude this node from the list
            
        Returns:
            List of active node information or None if failed
        """
        request = grpc_comm_manager_pb2.GetNodesRequest(
            requesting_node_id=self.node_id if exclude_self and self.node_id else 0
        )
        
        response = self._make_request(
            self._get_stub().GetNodes,
            request,
            "get_nodes"
        )
        
        if response:
            return [self._protobuf_to_node_dict(node) for node in response.nodes]
        return None
    
    def get_leader(self) -> Optional[Dict]:
        """
        Get current leader information from gateway.
        
        Returns:
            Leader node information or None if no leader or failed
        """
        request = grpc_comm_manager_pb2.GetLeaderRequest()
        response = self._make_request(
            self._get_stub().GetLeader,
            request,
            "get_leader"
        )
        
        if response and response.leader.node_id != 0:
            return self._protobuf_to_node_dict(response.leader)
        return None
    
    def update_leader(self, leader_id: int) -> bool:
        """
        Update leader information in gateway.
        
        Args:
            leader_id: ID of the new leader
            
        Returns:
            True if update successful
        """
        request = grpc_comm_manager_pb2.UpdateLeaderRequest(leader_id=leader_id)
        response = self._make_request(
            self._get_stub().UpdateLeader,
            request,
            "update_leader"
        )
        
        return response and response.status == "updated"
    
    def heartbeat(self) -> Optional[Dict]:
        """
        Send heartbeat to gateway.
        
        Returns:
            Heartbeat response or None if failed
        """
        if not self.is_registered or not self.node_id:
            logging.warning("Cannot send heartbeat: node not registered")
            return None
        
        request = grpc_comm_manager_pb2.HeartbeatRequest(node_id=self.node_id)
        response = self._make_request(
            self._get_stub().Heartbeat,
            request,
            "heartbeat"
        )
        
        if response:
            self.last_heartbeat = time.time()
            return {
                "status": response.status,
                "registry_version": response.registry_version
            }
        else:
            if self.on_heartbeat_failure:
                self.on_heartbeat_failure("Heartbeat failed")
        return None
    
    def remove_node(self, node_id: int) -> bool:
        """
        Remove a node from the gateway registry.
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            True if removal successful
        """
        request = grpc_comm_manager_pb2.RemoveNodeRequest(node_id=node_id)
        response = self._make_request(
            self._get_stub().RemoveNode,
            request,
            "remove_node"
        )
        
        return response and response.status == "removed"
    
    def get_stats(self) -> Optional[Dict]:
        """
        Get gateway statistics.
        
        Returns:
            Gateway statistics or None if failed
        """
        request = grpc_comm_manager_pb2.GetStatsRequest()
        response = self._make_request(
            self._get_stub().GetStats,
            request,
            "get_stats"
        )
        
        if response:
            return {
                "total_nodes": response.total_nodes,
                "active_nodes": response.active_nodes,
                "leader_id": response.leader_id if response.leader_id != 0 else None,
                "registry_version": response.registry_version,
                "bootstrap_initiated": response.bootstrap_initiated,
                "uptime": response.uptime
            }
        return None
    
    def _protobuf_to_node_dict(self, pb_node) -> Dict:
        """Convert protobuf NodeInfo to dictionary."""
        return {
            "node_id": pb_node.node_id,
            "ip_address": pb_node.ip_address,
            "port": pb_node.port,
            "capabilities": list(pb_node.capabilities),
            "status": pb_node.status,
            "last_seen": pb_node.last_seen,
            "join_timestamp": pb_node.join_timestamp,
            "metadata": dict(pb_node.metadata)
        }
    
    def _start_heartbeat(self):
        """Start heartbeat thread."""
        if self.heartbeat_running:
            return
        
        self.heartbeat_running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        logging.info("Heartbeat thread started")
    
    def _heartbeat_loop(self):
        """Heartbeat loop that runs in a separate thread."""
        while self.heartbeat_running:
            try:
                if time.time() - self.last_heartbeat >= self.heartbeat_interval:
                    heartbeat_result = self.heartbeat()
                    if heartbeat_result:
                        logging.debug(f"Heartbeat sent for node {self.node_id}")
                    else:
                        logging.warning(f"Heartbeat failed for node {self.node_id}")
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logging.error(f"Heartbeat loop error: {e}")
                time.sleep(30)  # Wait longer on error
    
    def stop_heartbeat(self):
        """Stop the heartbeat thread."""
        if not self.heartbeat_running:
            return
        
        self.heartbeat_running = False
        
        if self.heartbeat_thread:
            self.heartbeat_thread.join(timeout=5)
        
        logging.info("Heartbeat thread stopped")
    
    def discover_cluster(self, node_id: int, ip_address: str, port: int, 
                        capabilities: List[str], metadata: Dict[str, str] = None) -> Tuple[bool, Dict]:
        """
        Discover cluster and register with gateway.
        
        This is a convenience method that combines registration and discovery.
        
        Args:
            node_id: Unique node identifier
            ip_address: Node's IP address
            port: Node's port
            capabilities: List of node capabilities
            metadata: Additional node metadata
            
        Returns:
            Tuple of (is_bootstrap, cluster_info)
            - is_bootstrap: True if this is the first node (bootstrap)
            - cluster_info: Dictionary containing nodes and leader information
            
        Raises:
            Exception: If discovery fails
        """
        # Register with gateway
        registration_response = self.register_node(
            node_id, ip_address, port, capabilities, metadata
        )
        
        if not registration_response:
            raise Exception("Failed to register with gateway")
        
        is_bootstrap = registration_response.get('is_bootstrap', False)
        
        # Get cluster information
        cluster_info = {
            'nodes': registration_response.get('nodes', []),
            'leader': registration_response.get('leader'),
            'registry_version': registration_response.get('registry_version', 0)
        }
        
        logging.info(f"Cluster discovery complete. Bootstrap: {is_bootstrap}, "
                    f"Nodes: {len(cluster_info['nodes'])}")
        
        return is_bootstrap, cluster_info
    
    def is_gateway_available(self) -> bool:
        """
        Check if gateway is available.
        
        Returns:
            True if gateway is reachable
        """
        try:
            health_status = self.health_check()
            return health_status is not None
        except Exception:
            return False
    
    def shutdown(self):
        """Shutdown the gateway client."""
        self.stop_heartbeat()
        self._close_connection()
        self.is_registered = False
        logging.info("gRPC Gateway client shutdown")


class GatewayDiscoveryInterface:
    """
    Interface for gateway discovery functionality.
    
    This interface can be implemented by classes that need gateway discovery
    capabilities, providing a clean separation of concerns.
    """
    
    def register_with_gateway(self, node_id: int, ip_address: str, port: int, 
                            capabilities: List[str], metadata: Dict[str, str] = None) -> bool:
        """Register this node with the gateway."""
        raise NotImplementedError
    
    def get_cluster_nodes(self) -> List[Dict]:
        """Get list of cluster nodes from gateway."""
        raise NotImplementedError
    
    def get_cluster_leader(self) -> Optional[Dict]:
        """Get cluster leader information from gateway."""
        raise NotImplementedError
    
    def is_bootstrap_node(self) -> bool:
        """Check if this is a bootstrap node."""
        raise NotImplementedError
    
    def update_gateway_leader(self, leader_id: int) -> bool:
        """Update leader information in gateway."""
        raise NotImplementedError


class GRPCGatewayDiscoveryMixin(GatewayDiscoveryInterface):
    """
    Mixin class to add gRPC gateway discovery capabilities to existing classes.
    
    This can be used to extend existing node classes with gateway functionality
    while maintaining loose coupling.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gateway_client: Optional[GRPCGatewayClient] = None
        self.discovery_info: Optional[Dict] = None
    
    def initialize_gateway_discovery(self, gateway_host: str, gateway_port: int, 
                                   node_id: int, ip_address: str, port: int, 
                                   capabilities: List[str], metadata: Dict[str, str] = None,
                                   on_registration_success: Callable = None,
                                   on_registration_failure: Callable = None) -> bool:
        """
        Initialize gRPC gateway discovery.
        
        Args:
            gateway_host: Gateway server hostname or IP
            gateway_port: Gateway server port
            node_id: Unique node identifier
            ip_address: Node's IP address
            port: Node's port
            capabilities: List of node capabilities
            metadata: Additional node metadata
            on_registration_success: Callback for successful registration
            on_registration_failure: Callback for registration failure
            
        Returns:
            True if initialization successful
        """
        try:
            self.gateway_client = GRPCGatewayClient(gateway_host, gateway_port)
            
            # Set callbacks
            if on_registration_success:
                self.gateway_client.on_registration_success = on_registration_success
            if on_registration_failure:
                self.gateway_client.on_registration_failure = on_registration_failure
            
            # Discover cluster
            is_bootstrap, cluster_info = self.gateway_client.discover_cluster(
                node_id, ip_address, port, capabilities, metadata
            )
            
            self.discovery_info = {
                'is_bootstrap': is_bootstrap,
                'cluster_info': cluster_info,
                'gateway_available': True
            }
            
            logging.info(f"Gateway discovery initialized. Bootstrap: {is_bootstrap}")
            return True
            
        except Exception as e:
            logging.error(f"Gateway discovery failed: {e}")
            self.discovery_info = {
                'is_bootstrap': False,
                'cluster_info': {'nodes': [], 'leader': None},
                'gateway_available': False,
                'error': str(e)
            }
            return False
    
    def register_with_gateway(self, node_id: int, ip_address: str, port: int, 
                            capabilities: List[str], metadata: Dict[str, str] = None) -> bool:
        """Register this node with the gateway."""
        if not self.gateway_client:
            return False
        
        try:
            result = self.gateway_client.register_node(node_id, ip_address, port, capabilities, metadata)
            return result is not None
        except Exception as e:
            logging.error(f"Gateway registration failed: {e}")
            return False
    
    def get_cluster_nodes(self) -> List[Dict]:
        """Get list of cluster nodes from gateway."""
        if not self.gateway_client:
            return []
        
        try:
            nodes = self.gateway_client.get_nodes()
            return nodes or []
        except Exception as e:
            logging.error(f"Failed to get cluster nodes: {e}")
            return []
    
    def get_cluster_leader(self) -> Optional[Dict]:
        """Get cluster leader information from gateway."""
        if not self.gateway_client:
            return None
        
        try:
            return self.gateway_client.get_leader()
        except Exception as e:
            logging.error(f"Failed to get cluster leader: {e}")
            return None
    
    def is_bootstrap_node(self) -> bool:
        """Check if this is a bootstrap node."""
        return self.discovery_info and self.discovery_info.get('is_bootstrap', False)
    
    def update_gateway_leader(self, leader_id: int) -> bool:
        """Update leader information in gateway."""
        if not self.gateway_client:
            return False
        
        try:
            return self.gateway_client.update_leader(leader_id)
        except Exception as e:
            logging.error(f"Failed to update leader in gateway: {e}")
            return False
    
    def get_discovery_info(self) -> Dict:
        """Get discovery information."""
        return self.discovery_info or {}
    
    def shutdown_gateway_discovery(self):
        """Shutdown gateway discovery."""
        if self.gateway_client:
            self.gateway_client.shutdown()
            self.gateway_client = None
        self.discovery_info = None


def main():
    """Main function for testing the gRPC gateway client."""
    import argparse
    
    parser = argparse.ArgumentParser(description='gRPC Gateway Client Test')
    parser.add_argument('--gateway-host', default='localhost', help='Gateway host')
    parser.add_argument('--gateway-port', type=int, default=8090, help='Gateway port')
    parser.add_argument('--node-id', type=int, required=True, help='Node ID')
    parser.add_argument('--ip', default='127.0.0.1', help='Node IP')
    parser.add_argument('--port', type=int, default=5000, help='Node port')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create gateway client
    client = GRPCGatewayClient(args.gateway_host, args.gateway_port)
    
    try:
        # Test discovery
        is_bootstrap, cluster_info = client.discover_cluster(
            node_id=args.node_id,
            ip_address=args.ip,
            port=args.port,
            capabilities=['raft', 'gossip', 'test'],
            metadata={'test': 'true'}
        )
        
        print(f"Bootstrap: {is_bootstrap}")
        print(f"Cluster nodes: {len(cluster_info['nodes'])}")
        print(f"Leader: {cluster_info['leader']}")
        
        # Test other operations
        stats = client.get_stats()
        if stats:
            print(f"Gateway stats: {stats}")
        
        # Keep client alive for testing
        print("Client running. Press Ctrl+C to stop.")
        while True:
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("Stopping client...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.shutdown()


if __name__ == '__main__':
    main()
