"""
Refactored Dynamic gRPC Communication Manager

This refactored version integrates with the new pure service discovery architecture:
1. Removes polling mechanisms and replaces with direct gRPC calls
2. Eliminates old gateway client dependencies
3. Uses the new pure service discovery protocol
4. Provides event-driven node registration and discovery
5. Maintains local caching with robust fallback mechanisms

Key improvements:
- No more polling threads or redundant state management
- Direct integration with pure service discovery service
- Event-driven architecture aligned with the new RAFT hint system
- Robust fault tolerance and graceful degradation
"""

import os
import sys
import pickle
import threading
import time
import logging
from concurrent import futures
from typing import List, Dict, Optional, Set, Callable, Tuple

import grpc

from . import grpc_comm_manager_pb2_grpc, grpc_comm_manager_pb2
from ..base_com_manager import BaseCommunicationManager
from ..message import Message
from ..observer import Observer
from ..constants import CommunicationConstants
from .grpc_server import GRPCCOMMServicer
from fedml_core.mlops.mlops_profiler_event import MLOpsProfilerEvent

# Import RAFT message constants
try:
    from algorithms.RAFT_GossipFL.raft_messages import RaftMessage
except ImportError:
    # Create a dummy RaftMessage class if import fails
    class RaftMessage:
        MSG_TYPE_REQUEST_VOTE = 101
        MSG_TYPE_VOTE_RESPONSE = 102
        MSG_TYPE_APPEND_ENTRIES = 103
        MSG_TYPE_APPEND_RESPONSE = 104
        MSG_TYPE_PREVOTE_REQUEST = 110
        MSG_TYPE_PREVOTE_RESPONSE = 111
        MSG_TYPE_INSTALL_SNAPSHOT = 112
        MSG_TYPE_STATE_SNAPSHOT = 105
        MSG_TYPE_LEADER_REDIRECT = 109
        MSG_TYPE_STATE_REQUEST = 106
        MSG_TYPE_JOIN_REQUEST = 120
        MSG_TYPE_JOIN_RESPONSE = 121

# Thread-safe locks
config_lock = threading.RLock()
message_lock = threading.Lock()


class ConnectionFailureHandler:
    """Handles connection failures and automatic node removal."""
    
    def __init__(self, comm_manager):
        self.comm_manager = comm_manager
        self.failure_counts = {}  # Track failure counts per node
        self.failure_lock = threading.RLock()
        self.max_failures_before_removal = 3  # Remove after 3 consecutive failures
        
    def is_connection_failure(self, error: Exception) -> bool:
        """Check if the error indicates a connection failure that should trigger node removal."""
        if isinstance(error, grpc.RpcError):
            if error.code() == grpc.StatusCode.UNAVAILABLE:
                details = error.details() or ""
                # Check for specific connection failure patterns
                connection_failure_patterns = [
                    "connection refused",
                    "failed to connect",
                    "unavailable",
                    "no route to host",
                    "network is unreachable",
                    "connection timed out"
                ]
                return any(pattern in details.lower() for pattern in connection_failure_patterns)
        elif isinstance(error, ConnectionError):
            return True
        return False
    
    def handle_connection_failure(self, node_id: int, error: Exception) -> bool:
        """
        Handle connection failure and potentially remove node.
        
        Args:
            node_id: ID of the failed node
            error: The connection error
            
        Returns:
            bool: True if node was removed, False otherwise
        """
        if not self.is_connection_failure(error):
            return False
            
        with self.failure_lock:
            # Increment failure count
            self.failure_counts[node_id] = self.failure_counts.get(node_id, 0) + 1
            failure_count = self.failure_counts[node_id]
            
            logging.warning(f"Connection failure to node {node_id} (attempt {failure_count}): {error}")
            
            # Check if we should remove the node
            if failure_count >= self.max_failures_before_removal:
                logging.error(f"Node {node_id} failed {failure_count} times, removing from cluster")
                return self._remove_failed_node(node_id, str(error))
                
        return False
    
    def reset_failure_count(self, node_id: int):
        """Reset failure count for a node (called on successful connection)."""
        with self.failure_lock:
            if node_id in self.failure_counts:
                del self.failure_counts[node_id]
    
    def _remove_failed_node(self, node_id: int, reason: str) -> bool:
        """Remove a failed node from the cluster."""
        try:
            # Remove from local registry immediately
            self.comm_manager._remove_node_from_registry(node_id)
            
            # If we have a service discovery bridge, propose membership change through RAFT
            if (self.comm_manager.bridge_registered and 
                self.comm_manager.service_discovery_bridge):
                
                # Propose membership change through RAFT consensus
                success = self.comm_manager.service_discovery_bridge.propose_membership_change(
                    action="remove",
                    node_id=node_id,
                    reason=f"Connection failure: {reason}"
                )
                
                if success:
                    logging.info(f"Proposed RAFT membership change to remove node {node_id}")
                else:
                    logging.warning(f"Failed to propose RAFT membership change for node {node_id}")
                    # If we're not the leader, try to notify the leader about the failure
                    self._notify_leader_of_failure(node_id, reason)
                
                return success
            else:
                # In non-bridge mode, try to notify service discovery directly
                if (self.comm_manager.use_service_discovery and 
                    self.comm_manager.service_discovery_client):
                    
                    success = self.comm_manager.service_discovery_client.unregister_node(node_id)
                    if success:
                        logging.info(f"Unregistered failed node {node_id} from service discovery")
                    else:
                        logging.warning(f"Failed to unregister node {node_id} from service discovery")
                    
                    return success
                
            return True  # Local removal was successful
            
        except Exception as e:
            logging.error(f"Error removing failed node {node_id}: {e}")
            return False
    
    def _notify_leader_of_failure(self, node_id: int, reason: str):
        """Notify the current leader about a node failure if we're not the leader."""
        try:
            if (self.comm_manager.service_discovery_bridge and 
                hasattr(self.comm_manager.service_discovery_bridge, 'get_leader_id')):
                
                leader_id = self.comm_manager.service_discovery_bridge.get_leader_id()
                if leader_id and leader_id != self.comm_manager.node_id:
                    # Create a failure notification message
                    # This would typically be sent through the normal message system
                    logging.info(f"Notifying leader {leader_id} about failed node {node_id}: {reason}")
                    # Note: Implementation would depend on your specific message protocol
                    # for communicating failures to the leader
                    
        except Exception as e:
            logging.error(f"Error notifying leader of node failure: {e}")


class NodeInfo:
    """Information about a node in the cluster."""
    
    def __init__(self, node_id: int, ip_address: str, port: int, 
                 capabilities: List[str] = None, metadata: Dict = None):
        self.node_id = node_id
        self.ip_address = ip_address
        self.port = port
        self.capabilities = capabilities or []
        self.metadata = metadata or {}
        self.last_seen = time.time()
        self.is_reachable = True
        
    def update_reachability(self, is_reachable: bool):
        """Update node reachability status."""
        self.is_reachable = is_reachable
        self.last_seen = time.time()
    
    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return {
            'node_id': self.node_id,
            'ip_address': self.ip_address,
            'port': self.port,
            'capabilities': self.capabilities,
            'metadata': self.metadata,
            'last_seen': self.last_seen,
            'is_reachable': self.is_reachable
        }


class ServiceDiscoveryClient:
    """
    Direct gRPC client for the pure service discovery service.
    
    This replaces the old GRPCGatewayClient and provides a clean interface
    to the new pure service discovery system.
    """
    
    def __init__(self, service_host: str, service_port: int, 
                 timeout: float = 5.0, max_retry_attempts: int = 2):
        """
        Initialize the service discovery client.
        
        Args:
            service_host: Service discovery server hostname or IP
            service_port: Service discovery server port
            timeout: Request timeout in seconds
            max_retry_attempts: Maximum retry attempts for failed requests
        """
        self.service_host = service_host
        self.service_port = service_port
        self.timeout = timeout
        self.max_retry_attempts = max_retry_attempts
        self.service_address = f"{service_host}:{service_port}"
        
        # gRPC connection management
        self._channel = None
        self._stub = None
        self._connection_lock = threading.RLock()
        
        logging.info(f"Service discovery client initialized for {self.service_address}")
    
    def _get_stub(self):
        """Get or create gRPC stub with connection management."""
        with self._connection_lock:
            if self._channel is None or self._stub is None:
                self._channel = grpc.insecure_channel(
                    self.service_address,
                    options=[
                        ('grpc.max_send_message_length', 1000 * 1024 * 1024),
                        ('grpc.max_receive_message_length', 1000 * 1024 * 1024),
                        ('grpc.keepalive_time_ms', 30000),
                        ('grpc.keepalive_timeout_ms', 5000),
                        ('grpc.keepalive_permit_without_calls', True),
                    ]
                )
                self._stub = grpc_comm_manager_pb2_grpc.GatewayServiceStub(self._channel)
            return self._stub
    
    def _close_connection(self):
        """Close gRPC connection."""
        with self._connection_lock:
            if self._channel:
                try:
                    self._channel.close()
                except:
                    pass
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
                logging.debug(f"Service discovery {operation_name} successful")
                return response
                
            except grpc.RpcError as e:
                last_exception = e
                error_code = e.code()
                
                if error_code == grpc.StatusCode.UNAVAILABLE:
                    logging.warning(f"Service discovery unavailable (attempt {attempt + 1}/{self.max_retry_attempts})")
                    self._close_connection()
                    if attempt < self.max_retry_attempts - 1:
                        time.sleep(min(2 ** attempt, 10))  # Exponential backoff
                elif error_code == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logging.warning(f"Service discovery timeout (attempt {attempt + 1}/{self.max_retry_attempts})")
                    if attempt < self.max_retry_attempts - 1:
                        time.sleep(1)
                else:
                    logging.error(f"Service discovery {operation_name} failed: {e}")
                    break
                    
            except Exception as e:
                last_exception = e
                logging.error(f"Service discovery {operation_name} error: {e}")
                break
        
        logging.error(f"Service discovery {operation_name} failed after {self.max_retry_attempts} attempts")
        return None
    
    def register_node(self, node_id: int, ip_address: str, port: int, 
                     capabilities: List[str], metadata: Dict[str, str] = None) -> bool:
        """
        Register a node with the service discovery service.
        
        Args:
            node_id: Unique node identifier
            ip_address: Node's IP address
            port: Node's port
            capabilities: List of node capabilities
            metadata: Additional node metadata
            
        Returns:
            True if registration successful, False otherwise
        """
        try:
            request = grpc_comm_manager_pb2.RegisterNodeRequest(
                node_id=node_id,
                ip_address=ip_address,
                port=port,
                capabilities=capabilities or [],
                metadata=metadata or {}
            )
            
            response = self._make_request(
                lambda req, timeout: self._get_stub().RegisterNode(req, timeout=timeout),
                request,
                "register_node"
            )
            
            if response:
                logging.info(f"Node {node_id} registered successfully")
                return True
            else:
                logging.error(f"Failed to register node {node_id}")
                return False
                
        except Exception as e:
            logging.error(f"Node registration error: {e}")
            return False
    
    def discover_cluster(self, node_id: int, ip_address: str, port: int, 
                        capabilities: List[str], metadata: Dict[str, str] = None) -> Tuple[bool, Dict]:
        """
        Discover cluster and determine if this node is the bootstrap node.
        
        This method uses the existing RegisterNode call which provides the same functionality.
        
        Args:
            node_id: Unique node identifier
            ip_address: Node's IP address
            port: Node's port
            capabilities: List of node capabilities
            metadata: Additional node metadata
            
        Returns:
            Tuple of (is_bootstrap, cluster_info)
        """
        try:
            # Call register_node directly and get the response object
            request = grpc_comm_manager_pb2.RegisterNodeRequest(
                node_id=node_id,
                ip_address=ip_address,
                port=port,
                capabilities=capabilities or [],
                metadata=metadata or {}
            )
            
            response = self._make_request(
                lambda req, timeout: self._get_stub().RegisterNode(req, timeout=timeout),
                request,
                "register_node"
            )
            
            if response:
                # Parse response into cluster info format
                nodes = []
                for node_info in response.nodes:
                    nodes.append({
                        'node_id': node_info.node_id,
                        'ip_address': node_info.ip_address,
                        'port': node_info.port,
                        'capabilities': list(node_info.capabilities),
                        'metadata': dict(node_info.metadata),
                        'last_seen': node_info.last_seen
                    })
                
                cluster_info = {
                    'nodes': nodes,
                    'total_nodes': len(nodes),
                    'bootstrap_node': response.node_id if response.is_bootstrap else None,
                    'leader_node': response.leader.node_id if response.leader else None
                }
                
                # Store leader hint for future use
                self._set_leader_hint(response.leader.node_id if response.leader else None)
                
                logging.info(f"Cluster discovery successful: {len(nodes)} nodes, bootstrap: {response.is_bootstrap}")
                return response.is_bootstrap, cluster_info
            else:
                logging.error("Cluster discovery failed")
                return True, {'nodes': [], 'total_nodes': 0, 'bootstrap_node': None}  # Assume bootstrap if discovery fails
                
        except Exception as e:
            logging.error(f"Cluster discovery error: {e}")
            return True, {'nodes': [], 'total_nodes': 0, 'bootstrap_node': None}  # Assume bootstrap if discovery fails
    
    def get_nodes(self) -> List[Dict]:
        """
        Get list of all registered nodes.
        
        Returns:
            List of node information dictionaries
        """
        try:
            request = grpc_comm_manager_pb2.GetNodesRequest()
            
            response = self._make_request(
                lambda req, timeout: self._get_stub().GetNodes(req, timeout=timeout),
                request,
                "get_nodes"
            )
            
            if response:
                nodes = []
                for node_info in response.nodes:
                    nodes.append({
                        'node_id': node_info.node_id,
                        'ip_address': node_info.ip_address,
                        'port': node_info.port,
                        'capabilities': list(node_info.capabilities),
                        'metadata': dict(node_info.metadata),
                        'last_seen': node_info.last_seen
                    })
                
                logging.debug(f"Retrieved {len(nodes)} nodes from service discovery")
                return nodes
            else:
                logging.warning("Failed to retrieve nodes from service discovery")
                return []
                
        except Exception as e:
            logging.error(f"Get nodes error: {e}")
            return []
    
    def unregister_node(self, node_id: int) -> bool:
        """
        Unregister a node from the service discovery service.
        
        Args:
            node_id: Node identifier to unregister
            
        Returns:
            True if unregistration successful, False otherwise
        """
        try:
            # Use RemoveNode instead of UnregisterNode
            request = grpc_comm_manager_pb2.RemoveNodeRequest(node_id=node_id)
            
            response = self._make_request(
                lambda req, timeout: self._get_stub().RemoveNode(req, timeout=timeout),
                request,
                "unregister_node"
            )
            
            if response and response.status == "removed":
                logging.info(f"Node {node_id} unregistered successfully")
                return True
            else:
                logging.error(f"Failed to unregister node {node_id}: {response.message if response else 'No response'}")
                return False
                
        except Exception as e:
            logging.error(f"Node unregistration error: {e}")
            return False
    
    def close(self):
        """Close the client connection."""
        # Don't close connection here - let cleanup handle it
        logging.debug("Service discovery client close requested")
    
    def _set_leader_hint(self, leader_node):
        """
        Store the leader hint received from service discovery.
        
        Args:
            leader_node: Leader node information from discovery service
        """
        try:
            if leader_node and hasattr(leader_node, 'node_id'):
                self.leader_hint = leader_node.node_id
                logging.debug(f"Service discovery client: Set leader hint to {self.leader_hint}")
            else:
                self.leader_hint = None
                logging.debug("Service discovery client: Cleared leader hint")
        except Exception as e:
            logging.error(f"Service discovery client: Error setting leader hint: {e}")
            self.leader_hint = None
    
    def get_leader_hint(self):
        """
        Get the stored leader hint.
        
        Returns:
            int or None: Leader node ID hint, or None if not available
        """
        return getattr(self, 'leader_hint', None)
    
    def force_close(self):
        """Force close the client connection."""
        self._close_connection()
        logging.info("Service discovery client closed")


class DynamicGRPCCommManager(BaseCommunicationManager):
    def add_raft_handler(self, msg_type, handler):
        """
        Register a handler for a specific RAFT message type.
        Args:
            msg_type: The RAFT message type (str or int or tuple/list of types)
            handler: Callable with signature (msg_type, msg_params)
        """
        if not hasattr(self, '_raft_handlers'):
            self._raft_handlers = {}
        # Allow registering for multiple types at once
        if isinstance(msg_type, (list, tuple, set)):
            for t in msg_type:
                self._raft_handlers[t] = handler
        else:
            self._raft_handlers[msg_type] = handler

    """
    Refactored Dynamic gRPC Communication Manager.
    
    This version eliminates polling, uses direct service discovery calls,
    and integrates with the new pure service discovery architecture.
    
    Key features:
    - No polling threads or redundant state management
    - Direct gRPC calls to service discovery
    - Event-driven registration and discovery
    - Robust local caching with fallback mechanisms
    - Clean separation of concerns
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        node_id: int,
        client_num: int = 0,
        topic: str = "fedml",
        service_discovery_host: str = "localhost",
        service_discovery_port: int = 8090,
        capabilities: List[str] = None,
        metadata: Dict = None,
        ip_config_path: str = None,  # Optional fallback for static config
        use_service_discovery: bool = True,
        node_cache_timeout: float = 300.0,  # 5 minutes
        on_node_discovered: Optional[Callable] = None,
        on_node_lost: Optional[Callable] = None
    ):
        """
        Initialize the Refactored Dynamic gRPC Communication Manager.
        
        Args:
            host: Local host IP address
            port: Local port number
            node_id: Unique node identifier
            client_num: Total number of clients (for thread pool sizing)
            topic: Communication topic
            service_discovery_host: Service discovery server hostname
            service_discovery_port: Service discovery server port
            capabilities: List of node capabilities
            metadata: Additional node metadata
            ip_config_path: Optional path to static IP config (fallback)
            use_service_discovery: Whether to use service discovery
            node_cache_timeout: How long to cache node information (seconds)
            on_node_discovered: Callback for when a new node is discovered
            on_node_lost: Callback for when a node is lost
        """
        # Initialize base class
        BaseCommunicationManager.__init__(self)
        
        # Basic configuration
        self.host = host
        self.port = str(port)
        self.node_id = node_id
        self.client_num = client_num
        self._topic = topic
        self.use_service_discovery = use_service_discovery
        self.node_cache_timeout = node_cache_timeout
        
        # Callbacks
        self.on_node_discovered = on_node_discovered
        self.on_node_lost = on_node_lost
        
        # Bridge integration support
        self.service_discovery_bridge = None
        self.bridge_registered = False
        
        # Node classification
        self.rank = node_id
        if node_id == 0:
            self.node_type = "server"
            logging.info("############# THIS IS FL SERVER ################")
        else:
            self.node_type = "client"
            logging.info("------------- THIS IS FL CLIENT ----------------")
        
        # Observer pattern for message handling
        self._observers: List[Observer] = []
        
        # Node registry and local cache
        self.node_registry: Dict[int, NodeInfo] = {}
        self.ip_config: Dict[str, str] = {}
        self.is_bootstrap_node_flag = False
        self.cluster_info: Dict = {}
        
        # Service discovery client
        self.service_discovery_client: Optional[ServiceDiscoveryClient] = None
        
        # gRPC server configuration
        self.opts = [
            ("grpc.max_send_message_length", 1000 * 1024 * 1024),
            ("grpc.max_receive_message_length", 1000 * 1024 * 1024),
            ("grpc.enable_http_proxy", 0),
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 5000),
            ("grpc.keepalive_permit_without_calls", True),
        ]
        
        # Initialize gRPC server
        self.grpc_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=max(client_num, 10)),
            options=self.opts,
        )
        
        self.grpc_servicer = GRPCCOMMServicer(host, int(port), client_num, node_id)
        grpc_comm_manager_pb2_grpc.add_gRPCCommManagerServicer_to_server(
            self.grpc_servicer, self.grpc_server
        )
        
        # Start local gRPC server
        self.grpc_server.add_insecure_port("{}:{}".format("0.0.0.0", port))
        self.grpc_server.start()
        self.is_running = True
        
        logging.info(f"gRPC server started. Listening on port {port}")
        
        # Initialize connection failure handler  
        self.connection_failure_handler = ConnectionFailureHandler(self)
        # Allow configuration of max failures (can be set later by calling code)
        self.connection_failure_handler.max_failures_before_removal = 3  # default value
        
        # Initialize discovery and registration
        if use_service_discovery:
            self._initialize_service_discovery(
                service_discovery_host, 
                service_discovery_port, 
                capabilities, 
                metadata
            )
        elif ip_config_path:
            # Fallback to static configuration
            self._load_static_config(ip_config_path)
            self.is_bootstrap_node_flag = True  # Assume bootstrap in static mode
            self._add_self_to_registry(capabilities, metadata)
            logging.info("Using static IP configuration as fallback")
        else:
            # No service discovery or static config - assume bootstrap
            self.is_bootstrap_node_flag = True
            self._add_self_to_registry(capabilities, metadata)
            logging.warning("No service discovery or static configuration provided - assuming bootstrap mode")
    
    def configure_failure_detection(self, max_failures_before_removal: int = 3):
        """
        Configure connection failure detection parameters.
        
        Args:
            max_failures_before_removal: Number of consecutive failures before removing a node
        """
        if hasattr(self, 'connection_failure_handler'):
            self.connection_failure_handler.max_failures_before_removal = max_failures_before_removal
            logging.info(f"Updated failure detection: max_failures={max_failures_before_removal}")
    
    def _initialize_service_discovery(self, service_host: str, service_port: int, 
                                    capabilities: List[str], metadata: Dict):
        """Initialize service discovery and register this node."""
        try:
            # Create service discovery client
            self.service_discovery_client = ServiceDiscoveryClient(
                service_host, 
                service_port,
                timeout=5.0,  # 5 second timeout
                max_retry_attempts=2
            )
            
            # Discover cluster and determine bootstrap status
            logging.info(f"Discovering cluster through service discovery at {service_host}:{service_port}")
            
            is_bootstrap, cluster_info = self.service_discovery_client.discover_cluster(
                node_id=self.node_id,
                ip_address=self.host,
                port=int(self.port),
                capabilities=capabilities or ['grpc', 'fedml'],
                metadata=metadata or {'type': self.node_type}
            )
            
            # Update local state
            self.is_bootstrap_node_flag = is_bootstrap
            self.cluster_info = cluster_info
            
            # Note: discover_cluster() already registers the node, so no separate registration needed
            
            # Update local node registry with discovered nodes
            self._update_node_registry_from_discovery(cluster_info)
            
            # Add self to registry
            self._add_self_to_registry(capabilities, metadata)
            
            logging.info(f"Node {self.node_id} service discovery initialized:")
            logging.info(f"  Bootstrap: {self.is_bootstrap_node_flag}")
            logging.info(f"  Cluster nodes: {len(self.node_registry)}")
            
            # Notify about successful initialization
            self._handle_discovery_success()
                
        except Exception as e:
            logging.error(f"Service discovery initialization error: {e}")
            self._handle_discovery_failure(str(e))
    
    def _update_node_registry_from_discovery(self, cluster_info: Dict):
        """Update local node registry from discovery information."""
        try:
            with config_lock:
                # Track changes for notifications
                old_nodes = set(self.node_registry.keys())
                
                # Clear existing registry (except self)
                self.node_registry = {k: v for k, v in self.node_registry.items() if k == self.node_id}
                
                # Add discovered nodes
                for node_info in cluster_info.get('nodes', []):
                    node_id = node_info['node_id']
                    if node_id != self.node_id:  # Don't add self from discovery
                        self.node_registry[node_id] = NodeInfo(
                            node_id=node_id,
                            ip_address=node_info['ip_address'],
                            port=node_info['port'],
                            capabilities=node_info.get('capabilities', []),
                            metadata=node_info.get('metadata', {})
                        )
                
                # Update IP configuration
                self._rebuild_ip_config()
                
                # Notify about changes
                new_nodes = set(self.node_registry.keys())
                added_nodes = new_nodes - old_nodes
                removed_nodes = old_nodes - new_nodes
                
                if added_nodes and self.on_node_discovered:
                    for node_id in added_nodes:
                        if node_id != self.node_id:
                            self.on_node_discovered(node_id, self.node_registry[node_id])
                
                if removed_nodes and self.on_node_lost:
                    for node_id in removed_nodes:
                        if node_id != self.node_id:
                            self.on_node_lost(node_id)
                
                if added_nodes or removed_nodes:
                    logging.info(f"Node registry updated: +{len(added_nodes)} -{len(removed_nodes)}")
                    
        except Exception as e:
            logging.error(f"Failed to update node registry: {e}")

    def add_node_to_registry(self, node_info: NodeInfo):
        """
        Add a node to the local registry.
        
        Args:
            node_info: NodeInfo object containing node details
        """
        with config_lock:
            logging.debug(f"Node info: {node_info}")
            if isinstance(node_info, dict):
                # Convert dict to NodeInfo object
                node_info_obj = NodeInfo(
                    node_id=node_info['node_id'],
                    ip_address=node_info['ip_address'],
                    port=int(node_info.get('port', node_info.get('port', 9000))),
                    capabilities=node_info.get('capabilities', ['grpc', 'fedml']),
                    metadata=node_info.get('metadata', {})
                )
            elif isinstance(node_info, NodeInfo):
                node_info_obj = node_info
            else:
                logging.error(f"Invalid node_info type: {type(node_info)}")
                return

            # Add to node registry
            self.node_registry[node_info_obj.node_id] = node_info_obj
            self._rebuild_ip_config()
            logging.info(f"Node {node_info_obj.node_id} added to registry")
    
    def _add_self_to_registry(self, capabilities: List[str], metadata: Dict):
        """Add this node to the local registry."""
        with config_lock:
            self_node = NodeInfo(
                node_id=self.node_id,
                ip_address=self.host,
                port=int(self.port),
                capabilities=capabilities or ['grpc', 'fedml'],
                metadata=metadata or {'type': self.node_type}
            )
            self.node_registry[self.node_id] = self_node
            self._rebuild_ip_config()
    
    def _rebuild_ip_config(self):
        """Rebuild IP configuration from node registry."""
        with config_lock:
            self.ip_config = {}
            for node_id, node_info in self.node_registry.items():
                self.ip_config[str(node_id)] = node_info.ip_address
    
    def _handle_discovery_success(self):
        """Handle successful service discovery initialization."""
        logging.info(f"Service discovery initialized successfully for node {self.node_id}")
        self._notify_connection_ready()
    
    def _handle_discovery_failure(self, error: str):
        """Handle service discovery failure."""
        logging.warning(f"Service discovery failed: {error}")
        logging.info("Continuing in standalone mode - dynamic membership disabled")
        
        # Set up minimal local configuration
        self.is_bootstrap_node_flag = True  # Assume bootstrap if discovery fails
        self.cluster_info = {'nodes': [], 'total_nodes': 0, 'bootstrap_node': None}
        
        # Add self to node registry
        self._add_self_to_registry(['grpc', 'fedml'], {'type': self.node_type, 'mode': 'standalone'})
        
        logging.info(f"Node {self.node_id} initialized in standalone mode")
        
        # Still notify connection ready for local operation
        self._notify_connection_ready()
    
    def _load_static_config(self, config_path: str):
        """Load static IP configuration as fallback."""
        try:
            with open(config_path, 'r') as f:
                config_data = f.read()
            
            # Parse static configuration
            for line in config_data.strip().split('\n'):
                if line.strip() and not line.strip().startswith('#'):
                    parts = line.strip().split(',')
                    if len(parts) >= 2:
                        receiver_id = parts[0].strip()
                        ip_address = parts[1].strip()
                        
                        # Add to IP config
                        self.ip_config[receiver_id] = ip_address
                        
                        # Add to node registry
                        port = CommunicationConstants.GRPC_BASE_PORT + int(receiver_id)
                        node_info = NodeInfo(
                            node_id=int(receiver_id),
                            ip_address=ip_address,
                            port=port,
                            capabilities=['grpc', 'fedml'],
                            metadata={'source': 'static_config'}
                        )
                        self.node_registry[int(receiver_id)] = node_info
                        
            logging.info(f"Static configuration loaded: {len(self.ip_config)} nodes")
            
        except Exception as e:
            logging.error(f"Failed to load static configuration: {e}")
    
    def refresh_node_registry(self):
        """
        Manual registry refresh from service discovery.
        
        This is only used for initial bootstrap or manual refresh requests.
        During normal operation, the leader will keep the registry updated
        through RAFT consensus via bridge notifications.
        """
        if not self.use_service_discovery or not self.service_discovery_client:
            logging.debug("Service discovery not available for registry refresh")
            return
        
        try:
            # # Check if we're in bridge mode
            # if self.bridge_registered and self.service_discovery_bridge:
            #     # In bridge mode, registry updates come from RAFT leader
            #     logging.debug("Bridge mode: registry updates come from RAFT leader, skipping manual refresh")
            #     return
            
            # Non-bridge mode: update directly from service discovery
            logging.info("Manual registry refresh from service discovery")
            nodes = self.service_discovery_client.get_nodes()
            
            if nodes:
                cluster_info = {'nodes': nodes, 'total_nodes': len(nodes)}
                self._update_node_registry_from_discovery(cluster_info)
                logging.debug(f"Registry refreshed: {len(nodes)} nodes")
            else:
                logging.warning("No nodes returned from service discovery refresh")
                
        except Exception as e:
            logging.error(f"Registry refresh failed: {e}")
    
    def discover_new_nodes(self) -> List[NodeInfo]:
        """
        Discover new nodes that have joined the cluster.
        
        In bridge mode, this is not needed as the leader will notify us
        of new nodes through RAFT consensus.
        
        Returns:
            List of newly discovered nodes
        """
        if not self.use_service_discovery:
            return []
        
        # In bridge mode, node discovery is handled by RAFT leader
        if self.bridge_registered and self.service_discovery_bridge:
            logging.debug("Bridge mode: node discovery handled by RAFT leader")
            return []
        
        try:
            # Non-bridge mode: manual discovery
            current_nodes = self.service_discovery_client.get_nodes()
            
            # Find new nodes
            new_nodes = []
            with config_lock:
                for node_info in current_nodes:
                    node_id = node_info['node_id']
                    if node_id not in self.node_registry and node_id != self.node_id:
                        new_node = NodeInfo(
                            node_id=node_id,
                            ip_address=node_info['ip_address'],
                            port=node_info['port'],
                            capabilities=node_info.get('capabilities', []),
                            metadata=node_info.get('metadata', {})
                        )
                        new_nodes.append(new_node)
                        self.node_registry[node_id] = new_node
            
            if new_nodes:
                logging.info(f"Discovered {len(new_nodes)} new nodes")
                self._rebuild_ip_config()
                
                # Notify about new nodes
                for node in new_nodes:
                    if self.on_node_discovered:
                        self.on_node_discovered(node.node_id, node)
            
            return new_nodes
            
        except Exception as e:
            logging.error(f"Node discovery failed: {e}")
            return []
    
    def send_message(self, msg: Message):
        """Send message using node registry information."""
        logging.debug(f"Sending message: {msg.to_string()}")

        # Serialize message
        pickle_dump_start_time = time.time()
        msg_pkl = pickle.dumps(msg)
        
        # Try to import MLOpsProfilerEvent, but don't fail if it's not available
        try:
            from fedml_core.mlops import MLOpsProfilerEvent
            MLOpsProfilerEvent.log_to_wandb({"PickleDumpsTime": time.time() - pickle_dump_start_time})
        except:
            pass  # MLOps profiling is optional
        
        receiver_id = msg.get_receiver_id()
        
        # Get receiver info from node registry
        if receiver_id not in self.node_registry:
            # In bridge mode, we don't manually refresh - the leader will notify us
            if self.bridge_registered and self.service_discovery_bridge:
                logging.info(f"Receiver node {receiver_id} not in registry. Attempting discovery...")
                # Try a single refresh, but don't trigger duplicate join requests
                self.refresh_node_registry()
                
                if receiver_id not in self.node_registry:
                    logging.error(f"Receiver node {receiver_id} not found in registry and discovery failed")
                    raise ValueError(f"Receiver node {receiver_id} not found in registry and discovery failed")
            else:
                # Non-bridge mode: try manual refresh
                logging.info(f"Receiver node {receiver_id} not in registry, attempting discovery")
                self.refresh_node_registry()
                
                if receiver_id not in self.node_registry:
                    raise ValueError(f"Receiver node {receiver_id} not found in registry and discovery failed")
        
        receiver_info = self.node_registry[receiver_id]
        channel_url = f"{receiver_info.ip_address}:{receiver_info.port}"
        
        logging.debug(f"Sending message to {channel_url}")
        
        try:
            channel = grpc.insecure_channel(channel_url, options=self.opts)
            stub = grpc_comm_manager_pb2_grpc.gRPCCommManagerStub(channel)
            
            request = grpc_comm_manager_pb2.CommRequest()
            request.client_id = self.rank  # Use rank as client_id
            request.message = msg_pkl
            
            tick = time.time()
            stub.sendMessage(request)
            
            # Try to log profiling info, but don't fail if not available
            try:
                from fedml_core.mlops import MLOpsProfilerEvent
                MLOpsProfilerEvent.log_to_wandb({"Comm/send_delay": time.time() - tick})
            except:
                pass
                
            logging.debug("Message sent successfully")
            
            # Reset failure count on successful send
            self.connection_failure_handler.reset_failure_count(receiver_id)
            
            # Update node reachability
            self._update_node_reachability(receiver_id, True)
            
        except Exception as e:
            logging.error(f"Failed to send message to {channel_url}: {e}")
            
            # Handle connection failure - this may remove the node if it's persistently unreachable
            node_removed = self.connection_failure_handler.handle_connection_failure(receiver_id, e)
            
            # Update node reachability
            self._update_node_reachability(receiver_id, False)
            
            if node_removed:
                logging.warning(f"Node {receiver_id} was removed due to persistent connection failures")
                # Re-raise the exception since the node was removed
                raise
            
            # If node wasn't removed, try recovery strategies
            # In bridge mode, don't retry with manual refresh - let RAFT handle it
            if not (self.bridge_registered and self.service_discovery_bridge):
                # Non-bridge mode: try manual refresh and retry once
                if self.use_service_discovery:
                    logging.info("Attempting registry refresh before retry")
                    self.refresh_node_registry()
                    
                    if receiver_id in self.node_registry:
                        # Retry with updated information
                        try:
                            receiver_info = self.node_registry[receiver_id]
                            channel_url = f"{receiver_info.ip_address}:{receiver_info.port}"
                            
                            channel = grpc.insecure_channel(channel_url, options=self.opts)
                            stub = grpc_comm_manager_pb2_grpc.gRPCCommManagerStub(channel)
                            
                            request = grpc_comm_manager_pb2.CommRequest()
                            request.client_id = self.rank
                            request.message = msg_pkl
                            
                            stub.sendMessage(request)
                            logging.info("Message sent successfully on retry")
                            
                            # Reset failure count on successful retry
                            self.connection_failure_handler.reset_failure_count(receiver_id)
                            
                            # Update node reachability
                            self._update_node_reachability(receiver_id, True)
                            
                        except Exception as retry_e:
                            logging.error(f"Retry also failed: {retry_e}")
                            # Handle retry failure as well
                            self.connection_failure_handler.handle_connection_failure(receiver_id, retry_e)
                            raise
                    else:
                        raise
                else:
                    raise
            else:
                raise
        finally:
            try:
                channel.close()
            except:
                pass
    
    def _get_node_ip(self, node_id: int) -> Optional[str]:
        """Get IP address for a node."""
        with config_lock:
            # Try node registry first
            if node_id in self.node_registry:
                return self.node_registry[node_id].ip_address
            
            # Try IP config
            if str(node_id) in self.ip_config:
                return self.ip_config[str(node_id)]
            
            return None
    
    def _update_node_reachability(self, node_id: int, is_reachable: bool):
        """Update node reachability status."""
        with config_lock:
            if node_id in self.node_registry:
                self.node_registry[node_id].update_reachability(is_reachable)
                
                # Notify about node loss if it becomes unreachable
                if not is_reachable and self.on_node_lost:
                    self.on_node_lost(node_id)
    
    def _remove_node_from_registry(self, node_id: int):
        """Remove a node from the local registry and clean up connections."""
        with config_lock:
            if node_id in self.node_registry:
                try:
                    # Get node info before removal for logging
                    node_info = self.node_registry[node_id]
                    address = f"{node_info.ip_address}:{node_info.port}"
                    
                    # Remove from registry
                    del self.node_registry[node_id]
                    
                    # Rebuild IP config
                    self._rebuild_ip_config()
                    
                    # Reset failure count
                    self.connection_failure_handler.reset_failure_count(node_id)
                    
                    # Notify RAFT system about node removal
                    self._notify_raft_about_node_removal(node_id)
                    
                    logging.info(f"Removed node {node_id} ({address}) from local registry")
                    
                    # Notify callbacks
                    if self.on_node_lost:
                        self.on_node_lost(node_id)
                        
                except Exception as e:
                    logging.error(f"Error removing node {node_id} from registry: {e}")
            else:
                logging.debug(f"Node {node_id} not found in registry for removal")
    
    def add_observer(self, observer: Observer):
        """Add message observer."""
        self._observers.append(observer)
    
    def remove_observer(self, observer: Observer):
        """Remove message observer."""
        if observer in self._observers:
            self._observers.remove(observer)

    def start_message_handling(self):
        """Start message handling in a dedicated thread."""
        # Note: _notify_connection_ready() is already called by _handle_discovery_success()
        
        # Start message handling in a separate thread to avoid blocking
        self.message_handling_thread = threading.Thread(
            target=self.message_handling_subroutine,
            name=f"MessageHandler-{self.node_id}",
            daemon=True  # Daemon thread so it exits when main thread exits
        )
        self.message_handling_thread.start()
        
        logging.info(f"Message handling thread started for node {self.node_id}")
    
    def _notify_connection_ready(self):
        """Notify observers that connection is ready."""
        logging.info(f"Communication manager ready for node {self.node_id}")
        
        # Notify observers
        for observer in self._observers:
            try:
                observer.connection_ready()
            except AttributeError:
                # Observer doesn't have connection_ready method
                pass
            except Exception as e:
                logging.error(f"Observer notification failed: {e}")
    
    def message_handling_subroutine(self):
        """Handle incoming messages."""
        while self.is_running:
            try:
                # Check for messages in the gRPC servicer queue with timeout
                try:
                    msg_bytes = self.grpc_servicer.message_q.get(timeout=1.0)  # 1 second timeout
                except:
                    # Timeout or queue empty - continue to check is_running
                    continue

                # Deserialize the message
                try:
                    msg_params = pickle.loads(msg_bytes)
                    msg_type = msg_params.get_type()
                except (pickle.UnpicklingError, AttributeError) as e:
                    logging.error(f"Failed to deserialize message: {e}")
                    continue

                logging.debug(f"Received message type: {msg_type}")

                # Check if this is a RAFT message that should be handled by bridge/handler
                raft_message_types = {
                    RaftMessage.MSG_TYPE_REQUEST_VOTE,
                    RaftMessage.MSG_TYPE_VOTE_RESPONSE,
                    RaftMessage.MSG_TYPE_APPEND_ENTRIES,
                    RaftMessage.MSG_TYPE_APPEND_RESPONSE,
                    RaftMessage.MSG_TYPE_PREVOTE_REQUEST,
                    RaftMessage.MSG_TYPE_PREVOTE_RESPONSE,
                    RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT,
                    RaftMessage.MSG_TYPE_STATE_SNAPSHOT,
                    RaftMessage.MSG_TYPE_LEADER_REDIRECT,
                    RaftMessage.MSG_TYPE_STATE_REQUEST,
                    RaftMessage.MSG_TYPE_JOIN_REQUEST,
                    RaftMessage.MSG_TYPE_JOIN_RESPONSE
                }
                is_raft_message = (msg_params.get("message_source") == "raft" or 
                                 msg_type in raft_message_types)

                handled_by_raft = False
                if is_raft_message:
                    # Prefer explicit RAFT handler for this message type if registered
                    if hasattr(self, '_raft_handlers') and self._raft_handlers:
                        handler = self._raft_handlers.get(msg_type)
                        if handler:
                            logging.debug(f"Dispatching RAFT message to handler for type {msg_type}")
                            try:
                                # Extract parameters from Message object for RAFT handlers
                                if hasattr(msg_params, 'get_params'):
                                    params_dict = msg_params.get_params()
                                else:
                                    # Fallback: assume msg_params is already a dict
                                    params_dict = msg_params
                                handler(params_dict)  # Call handler with just the parameters dict
                                handled_by_raft = True
                            except Exception as e:
                                logging.error(f"RAFT handler error: {e}")
                        else:
                            logging.debug(f"No RAFT handler registered for type {msg_type}")
                    elif self.is_bridge_active():
                        # Fallback: if bridge is active, try to notify as observer
                        logging.debug(f"Routing RAFT message to bridge as observer: {msg_type}")
                        # (Bridge should be registered as observer if needed)

                # Notify observers (always, or skip for RAFT if handled? Here: always notify)
                for observer in self._observers:
                    try:
                        observer.receive_message(msg_type, msg_params)
                    except Exception as e:
                        logging.error(f"Observer receive_message error: {e}")

            except Exception as e:
                logging.error(f"Message handling error: {e}")
                if not self.is_running:
                    break
                time.sleep(0.1)  # Brief pause before retry
    
    # Node management methods
    
    def get_cluster_nodes_info(self) -> Dict[int, NodeInfo]:
        """Get information about all cluster nodes."""
        with config_lock:
            return self.node_registry.copy()
    
    def get_reachable_nodes(self) -> List[int]:
        """Get list of reachable node IDs."""
        with config_lock:
            return [node_id for node_id, node_info in self.node_registry.items() 
                   if node_info.is_reachable]
    
    def get_node_info(self, node_id: int) -> Optional[NodeInfo]:
        """Get information about a specific node."""
        with config_lock:
            return self.node_registry.get(node_id)
    
    def is_node_reachable(self, node_id: int) -> bool:
        """Check if a node is reachable."""
        with config_lock:
            node_info = self.node_registry.get(node_id)
            return node_info.is_reachable if node_info else False
    
    def get_cluster_size(self) -> int:
        """Get the current size of the cluster."""
        with config_lock:
            return len(self.node_registry)
    
    def get_leader_hint(self) -> Optional[int]:
        """Get the leader node ID hint from service discovery."""
        return getattr(self, '_leader_hint', None)
    
    def _set_leader_hint(self, leader_id: Optional[int]):
        """Set the leader hint from service discovery."""
        self._leader_hint = leader_id
    
    def is_bootstrap_node(self) -> bool:
        """Check if this node is the bootstrap node."""
        return self.is_bootstrap_node_flag
    
    def cleanup(self):
        """Clean up resources (idempotent)."""
        # Use a class-level flag to prevent multiple cleanup attempts
        if not hasattr(self, '_cleanup_called'):
            self._cleanup_called = threading.Event()
        
        if self._cleanup_called.is_set():
            logging.debug(f"Cleanup already called for node {self.node_id}, skipping...")
            return
        
        # Set the flag to prevent re-entry
        self._cleanup_called.set()
        
        try:
            # Stop message handling
            self.is_running = False
            
            # Unregister service discovery bridge
            self.unregister_service_discovery_bridge()
            
            # Unregister from service discovery
            if self.use_service_discovery and self.service_discovery_client:
                try:
                    logging.info(f"Unregistering node {self.node_id} from service discovery")
                    success = self.service_discovery_client.unregister_node(self.node_id)
                    if success:
                        logging.info(f"Node {self.node_id} unregistered successfully")
                    else:
                        logging.warning(f"Failed to unregister node {self.node_id}")
                except Exception as e:
                    logging.warning(f"Service discovery unregister error: {e}")
                finally:
                    # Close the client
                    try:
                        if hasattr(self.service_discovery_client, 'force_close'):
                            self.service_discovery_client.force_close()
                        self.service_discovery_client = None
                    except:
                        pass
            
            # Stop gRPC server
            if hasattr(self, 'grpc_server') and self.grpc_server:
                try:
                    self.grpc_server.stop(grace=2)  # Give a brief grace period
                    logging.debug(f"gRPC server stopped for node {self.node_id}")
                except Exception as e:
                    logging.debug(f"gRPC server stop error: {e}")
                finally:
                    self.grpc_server = None
            
            logging.info(f"Communication manager cleanup completed for node {self.node_id}")
            
        except Exception as e:
            logging.error(f"Cleanup error: {e}")
            # Don't force exit - let the process handle it naturally
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.cleanup()
        except:
            pass
    
    # Bridge Integration Methods
    
    def register_service_discovery_bridge(self, bridge):
        """
        Register a service discovery bridge to process membership events.
        
        The bridge will receive service discovery events and process them
        through RAFT consensus for dynamic membership management.
        
        Args:
            bridge: RaftServiceDiscoveryBridge instance
        """
        try:
            if self.bridge_registered and self.service_discovery_bridge == bridge:
                logging.debug(f"Bridge already registered, skipping duplicate registration")
                return False
            
            # Set bridge reference
            self.service_discovery_bridge = bridge
            
            # Store original callbacks before bridge replaces them
            self._original_on_node_discovered = self.on_node_discovered
            self._original_on_node_lost = self.on_node_lost
            
            # Set up bridge callbacks to notify comm manager
            bridge.set_comm_manager_callbacks(
                on_membership_change=self._on_bridge_membership_change,
                on_node_registry_update=self._on_bridge_node_registry_update
            )
            
            # Register bridge with itself (bridge will replace discovery callbacks)
            bridge.register_with_comm_manager(self)
            
            self.bridge_registered = True
            
            logging.info(f"Service discovery bridge registered for node {self.node_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to register service discovery bridge: {e}")
            return False
        
    def _on_bridge_membership_change(self, action: str, node_id: int, node_info: Dict = None):
        """
        Handle membership changes from the bridge.
        
        This is called when the bridge has processed a membership change through RAFT
        and needs to notify the communication manager to update its registry.
        
        Args:
            action: 'add' or 'remove'
            node_id: ID of the node that changed
            node_info: Connection info for added nodes
        """
        try:
            logging.info(f"Bridge notified membership change: {action} node {node_id}")
            
            if action == 'add' and node_info:
                # Add to local registry
                with config_lock:
                    if node_id not in self.node_registry:
                        self.node_registry[node_id] = NodeInfo(
                            node_id=node_id,
                            ip_address=node_info.get('ip_address', 'localhost'),
                            port=node_info.get('port', 9000 + node_id),
                            capabilities=node_info.get('capabilities', ['grpc', 'fedml']),
                            metadata=node_info.get('metadata', {})
                        )
                        self._rebuild_ip_config()
                        
                        # Notify RAFT system about the updated node list
                        self._notify_raft_about_membership_change()
                        
                        # Notify original callback if it exists
                        if self._original_on_node_discovered:
                            self._original_on_node_discovered(node_id, self.node_registry[node_id])
                        
                        logging.info(f"Added node {node_id} to registry via bridge notification")
            
            elif action == 'remove':
                # Remove from local registry
                with config_lock:
                    if node_id in self.node_registry:
                        del self.node_registry[node_id]
                        self._rebuild_ip_config()
                        
                        # Notify RAFT system about the updated node list
                        self._notify_raft_about_membership_change()
                        
                        # Notify original callback if it exists
                        if self._original_on_node_lost:
                            self._original_on_node_lost(node_id)
                        
                        logging.info(f"Removed node {node_id} from registry via bridge notification")
            
        except Exception as e:
            logging.error(f"Error handling bridge membership change: {e}")

    def _on_bridge_node_registry_update(self, full_node_list: List[Dict]):
        """
        Handle complete node registry update from bridge.
        
        This is called when the bridge has received a complete cluster state
        and needs to synchronize the communication manager's registry.
        
        Args:
            full_node_list: Complete list of nodes with connection info
        """
        try:
            logging.info(f"Bridge requested full registry update: {len(full_node_list)} nodes")
            
            with config_lock:
                # Save current state for comparison
                old_nodes = set(self.node_registry.keys())
                
                # Clear registry (except self)
                self.node_registry = {k: v for k, v in self.node_registry.items() if k == self.node_id}
                
                # Add all nodes from the update
                for node_info in full_node_list:
                    node_id = node_info['node_id']
                    if node_id != self.node_id:  # Don't overwrite self
                        self.node_registry[node_id] = NodeInfo(
                            node_id=node_id,
                            ip_address=node_info['ip_address'],
                            port=node_info['port'],
                            capabilities=node_info.get('capabilities', ['grpc', 'fedml']),
                            metadata=node_info.get('metadata', {})
                        )
                
                # Rebuild IP configuration
                self._rebuild_ip_config()
                
                # Notify about changes
                new_nodes = set(self.node_registry.keys())
                added_nodes = new_nodes - old_nodes
                removed_nodes = old_nodes - new_nodes
                
                # Notify original callbacks
                if self._original_on_node_discovered:
                    for node_id in added_nodes:
                        if node_id != self.node_id:
                            self._original_on_node_discovered(node_id, self.node_registry[node_id])
                
                if self._original_on_node_lost:
                    for node_id in removed_nodes:
                        if node_id != self.node_id:
                            self._original_on_node_lost(node_id)
                
                logging.info(f"Registry updated via bridge: +{len(added_nodes)} -{len(removed_nodes)} nodes")
            
        except Exception as e:
            logging.error(f"Error handling bridge registry update: {e}")
    
    def unregister_service_discovery_bridge(self):
        """Unregister the service discovery bridge."""
        try:
            if not self.bridge_registered or not self.service_discovery_bridge:
                return
            
            # Cleanup bridge
            self.service_discovery_bridge.cleanup()
            self.service_discovery_bridge = None
            self.bridge_registered = False
            
            logging.info(f"Service discovery bridge unregistered for node {self.node_id}")
            
        except Exception as e:
            logging.error(f"Error unregistering service discovery bridge: {e}")
    
    def get_bridge_statistics(self) -> Optional[Dict]:
        """
        Get statistics from the registered bridge.
        
        Returns:
            dict: Bridge statistics or None if no bridge registered
        """
        if self.bridge_registered and self.service_discovery_bridge:
            return self.service_discovery_bridge.get_statistics()
        return None
    
    def send_raft_message(self, msg: Message):
        """Send RAFT message through the communication manager."""
        # Handle RAFT message types that may need special handling
        if hasattr(self, '_raft_handlers') and msg.get_type() in self._raft_handlers:
            try:
                self._raft_handlers[msg.get_type()](msg.get_type(), msg.get_params())
            except Exception as e:
                logging.error(f"Error in RAFT handler for message type {msg.get_type()}: {e}")
        
        # Send the message normally
        self.send_message(msg)
    
    def handle_node_failure_notification(self, sender_id: int, failed_node_id: int, reason: str):
        """Handle notification from another node about a connection failure."""
        try:
            if (self.bridge_registered and self.service_discovery_bridge and
                hasattr(self.service_discovery_bridge, 'is_leader') and
                self.service_discovery_bridge.is_leader()):
                
                logging.info(f"Received failure notification from node {sender_id} about node {failed_node_id}: {reason}")
                
                # As leader, we can propose the membership change
                success = self.comm_manager.service_discovery_bridge.propose_membership_change(
                    action="remove",
                    node_id=failed_node_id,
                    reason=f"Reported by node {sender_id}: {reason}"
                )
                
                if success:
                    logging.info(f"Leader proposed removal of failed node {failed_node_id} based on report from {sender_id}")
                else:
                    logging.warning(f"Leader failed to propose removal of node {failed_node_id}")
                    
        except Exception as e:
            logging.error(f"Error handling node failure notification: {e}")
    
    def is_bridge_active(self) -> bool:
        """
        Check if service discovery bridge is active.
        
        Returns:
            bool: True if bridge is registered and active
        """
        return (self.bridge_registered and 
                self.service_discovery_bridge and 
                self.service_discovery_bridge.state.value == "active")
    
    def get_comm_manager(self):
        """
        Get the communication manager instance.
        
        This is useful for components that need to interact with the comm manager
        directly, such as RAFT consensus or other distributed algorithms.
        
        Returns:
            gRPCCommManager: The communication manager instance
        """
        return self
    
    def _notify_raft_about_node_removal(self, removed_node_id: int):
        """
        Notify RAFT system about node removal to update known_nodes.
        
        This ensures the RAFT consensus system stops trying to communicate
        with nodes that have been removed from the registry due to connection failures.
        
        Args:
            removed_node_id: ID of the node that was removed
        """
        try:
            # Get current list of nodes (excluding the removed one)
            current_node_ids = list(self.node_registry.keys())
            
            # Notify through the bridge if available
            if self.bridge_registered and self.service_discovery_bridge:
                # Check if the bridge has access to RAFT node
                if (hasattr(self.service_discovery_bridge, 'raft_consensus') and 
                    self.service_discovery_bridge.raft_consensus and
                    hasattr(self.service_discovery_bridge.raft_consensus, 'raft_node')):
                    
                    raft_node = self.service_discovery_bridge.raft_consensus.raft_node
                    if hasattr(raft_node, 'update_known_nodes'):
                        logging.info(f"Updating RAFT known_nodes after removing {removed_node_id}: {current_node_ids}")
                        raft_node.update_known_nodes(current_node_ids)
                        return
                        
            # Fallback: try to notify through observers that might be RAFT-related
            for observer in self._observers:
                # Check if observer has RAFT-related methods
                if hasattr(observer, 'handle_node_removal'):
                    try:
                        observer.handle_node_removal(removed_node_id, current_node_ids)
                        logging.debug(f"Notified observer about node {removed_node_id} removal")
                    except Exception as e:
                        logging.warning(f"Observer failed to handle node removal: {e}")
                        
                # Check if observer has access to RAFT node (e.g., worker manager)
                elif hasattr(observer, 'raft_consensus'):
                    try:
                        raft_consensus = observer.raft_consensus
                        if (hasattr(raft_consensus, 'raft_node') and 
                            hasattr(raft_consensus.raft_node, 'update_known_nodes')):
                            logging.info(f"Updating RAFT known_nodes via observer after removing {removed_node_id}: {current_node_ids}")
                            raft_consensus.raft_node.update_known_nodes(current_node_ids)
                    except Exception as e:
                        logging.warning(f"Failed to update RAFT known_nodes via observer: {e}")
                        
        except Exception as e:
            logging.error(f"Error notifying RAFT about node {removed_node_id} removal: {e}")
    
    def _notify_raft_about_membership_change(self):
        """
        Notify RAFT system about membership changes to update known_nodes.
        
        This is a general method that updates the RAFT system with the current
        list of nodes in the registry.
        """
        try:
            # Get current list of nodes
            current_node_ids = list(self.node_registry.keys())
            
            # Notify through the bridge if available
            if self.bridge_registered and self.service_discovery_bridge:
                # Check if the bridge has access to RAFT node
                if (hasattr(self.service_discovery_bridge, 'raft_consensus') and 