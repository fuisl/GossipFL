"""
Dynamic gRPC Communication Manager

This module provides a refactored version of GRPCCommManager that supports:
1. Dynamic node membership through service discovery
2. Integration with the gateway service
3. Runtime IP configuration updates
4. Separation of discovery and operational messages
5. Bootstrap coordination
"""

import os
import sys
import pickle
import threading
import time
import logging
from concurrent import futures
from typing import List, Dict, Optional, Set

import grpc

from ..grpc import grpc_comm_manager_pb2_grpc, grpc_comm_manager_pb2
from ...communication.base_com_manager import BaseCommunicationManager
from ...communication.message import Message
from ...communication.observer import Observer
from ..constants import CommunicationConstants
from ...communication.grpc.grpc_server import GRPCCOMMServicer
from fedml_core.mlops.mlops_profiler_event import MLOpsProfilerEvent

# Add path to access gateway client - avoid __init__.py issues
sys.path.append(os.path.join(os.path.dirname(__file__), "../../../.."))
sys.path.append(os.path.join(os.path.dirname(__file__), "../../../../algorithms/RAFT_GossipFL"))

# Direct imports to avoid __init__.py issues
from algorithms.RAFT_GossipFL.grpc_gateway_client import GRPCGatewayClient, GRPCGatewayDiscoveryMixin

# Thread-safe lock for IP configuration updates
config_lock = threading.RLock()
message_lock = threading.Lock()


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


class DynamicGRPCCommManager(BaseCommunicationManager, GRPCGatewayDiscoveryMixin):
    """
    Dynamic gRPC Communication Manager with service discovery integration.
    
    This manager supports:
    - Dynamic node membership through gateway service
    - Runtime IP configuration updates
    - Bootstrap coordination
    - Separation of discovery and operational messages
    - Automatic failover and recovery
    """
    
    def __init__(
        self,
        host: str,
        port: int,
        node_id: int,
        client_num: int = 0,
        topic: str = "fedml",
        gateway_host: str = "localhost",
        gateway_port: int = 8090,
        capabilities: List[str] = None,
        metadata: Dict = None,
        ip_config_path: str = None,  # Optional fallback for static config
        use_gateway: bool = True
    ):
        """
        Initialize the Dynamic gRPC Communication Manager.
        
        Args:
            host: Local host IP address
            port: Local port number
            node_id: Unique node identifier
            client_num: Total number of clients (for thread pool sizing)
            topic: Communication topic
            gateway_host: Gateway server hostname
            gateway_port: Gateway server port
            capabilities: List of node capabilities
            metadata: Additional node metadata
            ip_config_path: Optional path to static IP config (fallback)
            use_gateway: Whether to use gateway service discovery
        """
        # Initialize base classes
        BaseCommunicationManager.__init__(self)
        GRPCGatewayDiscoveryMixin.__init__(self)
        
        # Basic configuration
        self.host = host
        self.port = str(port)
        self.node_id = node_id
        self.client_num = client_num
        self._topic = topic
        self.use_gateway = use_gateway
        
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
        
        # Node registry and IP configuration
        self.node_registry: Dict[int, NodeInfo] = {}
        self.ip_config: Dict[str, str] = {}
        self.is_bootstrap_node_flag = False
        self.cluster_leader_id: Optional[int] = None
        
        # Registry refresh mechanism
        self.registry_refresh_interval = 2.0  # seconds (reduced for testing)
        self.registry_refresh_thread = None
        self.registry_refresh_running = False
        
        # gRPC server configuration
        self.opts = [
            ("grpc.max_send_message_length", 1000 * 1024 * 1024),
            ("grpc.max_receive_message_length", 1000 * 1024 * 1024),
            ("grpc.enable_http_proxy", 0),
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
        
        # Initialize discovery service
        if use_gateway:
            self._initialize_service_discovery(gateway_host, gateway_port, capabilities, metadata)
        elif ip_config_path:
            # Fallback to static configuration
            self._load_static_config(ip_config_path)
            logging.info("Using static IP configuration as fallback")
        else:
            logging.warning("No service discovery or static configuration provided")
    
    def _initialize_service_discovery(self, gateway_host: str, gateway_port: int, 
                                    capabilities: List[str], metadata: Dict):
        """Initialize service discovery with gateway."""
        try:
            # Set up discovery callbacks
            def on_registration_success(response):
                logging.info(f"Node {self.node_id} registered with gateway successfully")
                self._handle_discovery_success(response)
            
            def on_registration_failure(error):
                logging.error(f"Node {self.node_id} failed to register with gateway: {error}")
                self._handle_discovery_failure(error)
            
            # Test gateway connectivity first
            logging.info(f"Testing gateway connectivity to {gateway_host}:{gateway_port}...")
            
            # Initialize gateway discovery with timeout
            success = self.initialize_gateway_discovery(
                gateway_host=gateway_host,
                gateway_port=gateway_port,
                node_id=self.node_id,
                ip_address=self.host,
                port=int(self.port),
                capabilities=capabilities or ['grpc', 'fedml'],
                metadata=metadata or {'type': self.node_type},
                on_registration_success=on_registration_success,
                on_registration_failure=on_registration_failure
            )
            
            if success:
                # Update local state based on discovery
                self.is_bootstrap_node_flag = self.is_bootstrap_node()
                self._update_node_registry_from_discovery()
                
                logging.info(f"Node {self.node_id} service discovery initialized:")
                logging.info(f"  Bootstrap: {self.is_bootstrap_node_flag}")
                logging.info(f"  Cluster nodes: {len(self.node_registry)}")
                
                if self.is_bootstrap_node_flag:
                    self._handle_bootstrap_mode()
                else:
                    self._handle_join_mode()
            else:
                logging.error("Failed to initialize service discovery")
                self._handle_discovery_failure("Discovery initialization failed")
                
        except Exception as e:
            logging.error(f"Service discovery initialization error: {e}")
            self._handle_discovery_failure(str(e))
    
    def _handle_discovery_success(self, response):
        """Handle successful discovery registration."""
        # Update node registry from discovery response
        self._update_node_registry_from_discovery()
        
        # Update IP configuration
        self._rebuild_ip_config()
        
        # Notify observers about successful connection
        self._notify_connection_ready()
    
    def _handle_discovery_failure(self, error):
        """Handle discovery failure."""
        logging.warning(f"Discovery failed: {error}")
        logging.info("Continuing in standalone mode - dynamic membership disabled")
        
        # Set up minimal local configuration
        self.is_bootstrap_node_flag = True  # Assume bootstrap if discovery fails
        self.cluster_leader_id = self.node_id
        
        # Add self to node registry
        self_node = NodeInfo(
            node_id=self.node_id,
            ip_address=self.host,
            port=int(self.port),
            capabilities=['grpc', 'fedml'],
            metadata={'type': self.node_type, 'mode': 'standalone'}
        )
        
        with config_lock:
            self.node_registry[self.node_id] = self_node
            self._rebuild_ip_config()
        
        logging.info(f"Node {self.node_id} initialized in standalone mode")
        
        # Still notify connection ready for local operation
        self._notify_connection_ready()
    
    def _handle_bootstrap_mode(self):
        """Handle bootstrap mode initialization."""
        logging.info(f"Node {self.node_id} initializing as BOOTSTRAP node")
        
        # Set self as leader
        self.cluster_leader_id = self.node_id
        
        # Add self to node registry
        self_node = NodeInfo(
            node_id=self.node_id,
            ip_address=self.host,
            port=int(self.port),
            capabilities=['grpc', 'fedml'],
            metadata={'type': self.node_type, 'role': 'bootstrap'}
        )
        
        with config_lock:
            self.node_registry[self.node_id] = self_node
            self._rebuild_ip_config()
        
        # Start registry refresh thread
        self._start_registry_refresh()
        
        logging.info(f"Bootstrap node {self.node_id} initialized as leader")
    
    def _handle_join_mode(self):
        """Handle joining existing cluster."""
        logging.info(f"Node {self.node_id} joining existing cluster")
        
        # Get cluster information
        cluster_nodes = self.get_cluster_nodes()
        leader_info = self.get_cluster_leader()
        
        if leader_info:
            self.cluster_leader_id = leader_info['node_id']
            logging.info(f"Cluster leader is node {self.cluster_leader_id}")
        
        logging.info(f"Joined cluster with {len(cluster_nodes)} existing nodes")
        
        # Update node registry
        self._update_node_registry_from_discovery()
        self._rebuild_ip_config()
        
        # Start registry refresh thread
        self._start_registry_refresh()
    
    def _update_node_registry_from_discovery(self):
        """Update node registry from gateway discovery."""
        if not self.gateway_client:
            return
            
        try:
            # Get current cluster nodes
            cluster_nodes = self.get_cluster_nodes()
            leader_info = self.get_cluster_leader()
            
            logging.debug(f"Cluster nodes from gateway: {cluster_nodes}")
            logging.debug(f"Leader info from gateway: {leader_info}")
            
            with config_lock:
                # Clear existing registry
                self.node_registry.clear()
                
                # Add cluster nodes
                for node_data in cluster_nodes:
                    # Check if node_data is a dict (expected) or just an ID (error case)
                    if isinstance(node_data, dict):
                        node_info = NodeInfo(
                            node_id=node_data['node_id'],
                            ip_address=node_data['ip_address'],
                            port=node_data['port'],
                            capabilities=node_data.get('capabilities', []),
                            metadata=node_data.get('metadata', {})
                        )
                        self.node_registry[node_data['node_id']] = node_info
                    else:
                        logging.warning(f"Unexpected node data format: {node_data} (type: {type(node_data)})")
                
                # Add leader if not already in registry
                if leader_info and isinstance(leader_info, dict) and leader_info['node_id'] not in self.node_registry:
                    leader_node = NodeInfo(
                        node_id=leader_info['node_id'],
                        ip_address=leader_info['ip_address'],
                        port=leader_info['port'],
                        capabilities=leader_info.get('capabilities', []),
                        metadata=leader_info.get('metadata', {})
                    )
                    self.node_registry[leader_info['node_id']] = leader_node
                
                # Add self to registry
                self_node = NodeInfo(
                    node_id=self.node_id,
                    ip_address=self.host,
                    port=int(self.port),
                    capabilities=['grpc', 'fedml'],
                    metadata={'type': self.node_type}
                )
                self.node_registry[self.node_id] = self_node
                
                # Update leader ID
                if leader_info and isinstance(leader_info, dict):
                    self.cluster_leader_id = leader_info['node_id']
                
                logging.info(f"Node registry updated: {len(self.node_registry)} nodes")
                
        except Exception as e:
            logging.error(f"Failed to update node registry: {e}")
            import traceback
            traceback.print_exc()
    
    def _rebuild_ip_config(self):
        """Rebuild IP configuration from node registry."""
        with config_lock:
            self.ip_config.clear()
            
            for node_id, node_info in self.node_registry.items():
                # Store IP address for backward compatibility
                self.ip_config[str(node_id)] = node_info.ip_address
                
            logging.debug(f"IP configuration rebuilt: {len(self.ip_config)} entries")
            logging.debug(f"Node registry has {len(self.node_registry)} nodes: {list(self.node_registry.keys())}")
    
    def _load_static_config(self, path: str):
        """Load static IP configuration from CSV file."""
        try:
            import csv
            with open(path, newline="") as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader)  # Skip header
                
                for row in csv_reader:
                    receiver_id, receiver_ip = row
                    self.ip_config[receiver_id] = receiver_ip
                    
                    # Create node info from static config
                    node_info = NodeInfo(
                        node_id=int(receiver_id),
                        ip_address=receiver_ip,
                        port=CommunicationConstants.GRPC_BASE_PORT + int(receiver_id),
                        capabilities=['grpc', 'fedml'],
                        metadata={'source': 'static_config'}
                    )
                    self.node_registry[int(receiver_id)] = node_info
                    
            logging.info(f"Static configuration loaded: {len(self.ip_config)} nodes")
            
        except Exception as e:
            logging.error(f"Failed to load static configuration: {e}")
    
    def send_message(self, msg: Message):
        """Override send_message to use actual ports from node registry."""
        import pickle
        import time
        
        logging.info(f"Sending message: {msg}")
        
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
        
        # Get receiver info from node registry (not hardcoded calculation)
        if receiver_id not in self.node_registry:
            raise ValueError(f"Receiver node {receiver_id} not found in registry")
        
        receiver_info = self.node_registry[receiver_id]
        channel_url = f"{receiver_info.ip_address}:{receiver_info.port}"
        
        logging.info(f"Sending message to {channel_url}")
        
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
            
        except Exception as e:
            logging.error(f"Failed to send message to {channel_url}: {e}")
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
    
    def add_observer(self, observer: Observer):
        """Add message observer."""
        self._observers.append(observer)
    
    def remove_observer(self, observer: Observer):
        """Remove message observer."""
        if observer in self._observers:
            self._observers.remove(observer)
    
    def handle_receive_message(self):
        """Start message handling."""
        self._notify_connection_ready()
        self.message_handling_subroutine()
    
    def message_handling_subroutine(self):
        """Handle incoming messages."""
        start_listening_time = time.time()
        MLOpsProfilerEvent.log_to_wandb({"ListenStart": start_listening_time})
        
        while self.is_running:
            if self.grpc_servicer.message_q.qsize() > 0:
                message_lock.acquire()
                try:
                    busy_time_start = time.time()
                    
                    # Get message from queue
                    msg_pkl = self.grpc_servicer.message_q.get()
                    
                    # Deserialize message
                    unpickle_start_time = time.time()
                    msg = pickle.loads(msg_pkl)
                    MLOpsProfilerEvent.log_to_wandb({"UnpickleTime": time.time() - unpickle_start_time})
                    
                    # Process message
                    msg_type = msg.get_type()
                    
                    # Check if this is a discovery message
                    if self._is_discovery_message(msg_type):
                        self._handle_discovery_message(msg_type, msg)
                    else:
                        # Regular operational message
                        for observer in self._observers:
                            handler_start_time = time.time()
                            observer.receive_message(msg_type, msg)
                            MLOpsProfilerEvent.log_to_wandb({"MessageHandlerTime": time.time() - handler_start_time})
                    
                    MLOpsProfilerEvent.log_to_wandb({"BusyTime": time.time() - busy_time_start})
                    
                finally:
                    message_lock.release()
            
            time.sleep(0.0001)
        
        MLOpsProfilerEvent.log_to_wandb({"TotalTime": time.time() - start_listening_time})
    
    def _is_discovery_message(self, msg_type: str) -> bool:
        """Check if message is a discovery message."""
        discovery_types = [
            CommunicationConstants.MSG_TYPE_GATEWAY_REGISTER,
            CommunicationConstants.MSG_TYPE_GATEWAY_GET_NODES,
            CommunicationConstants.MSG_TYPE_GATEWAY_GET_LEADER,
            CommunicationConstants.MSG_TYPE_GATEWAY_UPDATE_LEADER,
            CommunicationConstants.MSG_TYPE_GATEWAY_HEARTBEAT,
            CommunicationConstants.MSG_TYPE_GATEWAY_HEALTH_CHECK,
            CommunicationConstants.MSG_TYPE_GATEWAY_REMOVE_NODE,
            CommunicationConstants.MSG_TYPE_GATEWAY_GET_STATS,
        ]
        return msg_type in discovery_types
    
    def _handle_discovery_message(self, msg_type: str, msg: Message):
        """Handle discovery-related messages."""
        logging.info(f"Handling discovery message: {msg_type}")
        
        # Process discovery messages
        if msg_type == CommunicationConstants.MSG_TYPE_GATEWAY_UPDATE_LEADER:
            # Update leader information
            leader_id = msg.get_content().get('leader_id')
            if leader_id:
                self.cluster_leader_id = leader_id
                logging.info(f"Leader updated to node {leader_id}")
        
        # Add more discovery message handling as needed
    
    def stop_receive_message(self):
        """Stop message handling."""
        self.is_running = False
        self.grpc_server.stop(None)
        
        # Shutdown gateway discovery
        if self.use_gateway:
            self.shutdown_gateway_discovery()
    
    def notify(self, message: Message):
        """Notify observers of a message."""
        msg_type = message.get_type()
        for observer in self._observers:
            observer.receive_message(msg_type, message)
    
    def _notify_connection_ready(self):
        """Notify observers that connection is ready."""
        msg_type = CommunicationConstants.MSG_TYPE_CONNECTION_IS_READY
        msg_params = Message(type=msg_type, sender_id=self.rank, receiver_id=self.rank)
        
        for observer in self._observers:
            observer.receive_message(msg_type, msg_params)
    
    def _start_registry_refresh(self):
        """Start the registry refresh thread."""
        if self.use_gateway and not self.registry_refresh_running:
            self.registry_refresh_running = True
            self.registry_refresh_thread = threading.Thread(
                target=self._registry_refresh_worker,
                daemon=True
            )
            self.registry_refresh_thread.start()
            logging.info(f"Registry refresh thread started (interval: {self.registry_refresh_interval}s)")
    
    def _stop_registry_refresh(self):
        """Stop the registry refresh thread."""
        if self.registry_refresh_running:
            self.registry_refresh_running = False
            if self.registry_refresh_thread:
                self.registry_refresh_thread.join(timeout=5.0)
            logging.info("Registry refresh thread stopped")
    
    def _registry_refresh_worker(self):
        """Worker thread for periodic registry refresh."""
        while self.registry_refresh_running:
            try:
                time.sleep(self.registry_refresh_interval)
                if self.registry_refresh_running and self.gateway_client:
                    old_nodes = set(self.node_registry.keys())
                    self._update_node_registry_from_discovery()
                    self._rebuild_ip_config()
                    new_nodes = set(self.node_registry.keys())
                    
                    # Log changes
                    if old_nodes != new_nodes:
                        added = new_nodes - old_nodes
                        removed = old_nodes - new_nodes
                        if added:
                            logging.info(f"Node {self.node_id} detected new nodes: {added}")
                        if removed:
                            logging.info(f"Node {self.node_id} detected removed nodes: {removed}")
                        
            except Exception as e:
                logging.error(f"Registry refresh error: {e}")
                time.sleep(1.0)  # Brief pause before retry
    
    # Dynamic membership management methods
    
    def get_cluster_nodes_info(self) -> Dict[int, NodeInfo]:
        """Get information about all cluster nodes."""
        with config_lock:
            return self.node_registry.copy()
    
    def get_reachable_nodes(self) -> List[int]:
        """Get list of reachable node IDs."""
        with config_lock:
            return [node_id for node_id, node_info in self.node_registry.items() 
                   if node_info.is_reachable]
    
    def refresh_node_registry(self):
        """Refresh node registry from gateway."""
        if self.use_gateway and self.gateway_client:
            self._update_node_registry_from_discovery()
            self._rebuild_ip_config()
    
    def is_bootstrap_node_local(self) -> bool:
        """Check if this node is the bootstrap node."""
        return self.is_bootstrap_node_flag
    
    def get_cluster_leader(self) -> Optional[int]:
        """Get current cluster leader ID."""
        return self.cluster_leader_id
    
    def update_cluster_leader(self, leader_id: int):
        """Update cluster leader information."""
        if self.use_gateway and self.gateway_client:
            success = self.update_gateway_leader(leader_id)
            if success:
                self.cluster_leader_id = leader_id
                logging.info(f"Cluster leader updated to node {leader_id}")
                return True
        return False

    def cleanup(self):
        """Cleanup resources when shutting down."""
        # Stop registry refresh thread
        self._stop_registry_refresh()
        
        # Call parent cleanup if available
        if hasattr(super(), 'cleanup'):
            super().cleanup()

    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.cleanup()
        except:
            pass  # Ignore errors during cleanup
