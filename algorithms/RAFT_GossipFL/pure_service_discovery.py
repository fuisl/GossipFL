#!/usr/bin/env python3

"""
Pure Service Discovery Implementation for RAFT+GossipFL

This implementation follows the gateway.md architecture strictly:
- Pure discovery service (no RAFT participation)
- Bootstrap coordination for first node
- Eventually consistent registry
- Operational independence from RAFT
- Minimal state management focused on discovery

Based on gateway.md principles:
- Gateway Server Role: Pure Discovery Service
- Non-Critical Operation: System continues without gateway
- Bootstrap Registry: Maintains list of active clients for newcomers
- Fixed Address: Well-known endpoint for initial contact
"""

import logging
import threading
import time
import grpc
from concurrent import futures
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
import argparse
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
import socket

# Import existing protobuf definitions
try:
    from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2
    from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc
except ImportError:
    # Fallback for testing
    import sys
    sys.path.append('/home/fuisloy/data1tb/GossipFL')
    from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2
    from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc


class NodeStatus(Enum):
    """Node status for discovery purposes only."""
    JOINING = "joining"
    ACTIVE = "active"
    INACTIVE = "inactive"
    LEAVING = "leaving"


@dataclass
class DiscoveryNodeInfo:
    """
    Minimal node information for service discovery.
    
    This contains only what's needed for discovery - no RAFT state,
    no cluster coordination information.
    """
    node_id: int
    ip_address: str
    port: int
    capabilities: List[str] = field(default_factory=list)
    status: NodeStatus = NodeStatus.JOINING
    last_seen: float = field(default_factory=time.time)
    join_timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, str] = field(default_factory=dict)
    
    def to_protobuf(self):
        """Convert to protobuf representation."""
        return grpc_comm_manager_pb2.NodeInfo(
            node_id=self.node_id,
            ip_address=self.ip_address,
            port=self.port,
            capabilities=self.capabilities,
            status=self.status.value,
            last_seen=self.last_seen,
            join_timestamp=self.join_timestamp,
            metadata=self.metadata
        )
    
    def is_active(self) -> bool:
        """Check if node is considered active."""
        return self.status == NodeStatus.ACTIVE
    
    def is_stale(self, timeout: float) -> bool:
        """Check if node hasn't been seen recently."""
        return time.time() - self.last_seen > timeout


class PureServiceDiscovery:
    """
    Pure service discovery implementation.
    
    This class handles ONLY:
    1. Node registration and discovery
    2. Bootstrap coordination (first node detection)
    3. Basic node liveness tracking
    4. Registry maintenance
    
    It does NOT:
    - Participate in RAFT consensus
    - Maintain detailed cluster state
    - Handle operational communication
    - Broadcast cluster events
    """
    
    def __init__(self, node_timeout: float = 300.0, cleanup_interval: float = 60.0):
        self.nodes: Dict[int, DiscoveryNodeInfo] = {}
        self.bootstrap_initiated = False
        self.registry_version = 0
        self.last_known_leader: Optional[int] = None  # Cache for discovery only
        self.node_timeout = node_timeout
        self.cleanup_interval = cleanup_interval
        self.start_time = time.time()
        
        # Thread safety
        self.lock = threading.RLock()
        
        # State consistency for discovery
        self.last_update_time = time.time()
        
        logging.info("Pure service discovery initialized")
    
    def register_node(self, node_id: int, ip_address: str, port: int, 
                     capabilities: List[str], metadata: Dict[str, str] = None) -> Dict:
        """
        Register a node for service discovery.
        
        Returns bootstrap information if this is the first node.
        """
        with self.lock:
            current_time = time.time()
            
            # Check if this is the first node (bootstrap case)
            is_bootstrap = not self.bootstrap_initiated and len(self.nodes) == 0
            
            # Create or update node info
            if node_id in self.nodes:
                node = self.nodes[node_id]
                node.ip_address = ip_address
                node.port = port
                node.capabilities = capabilities or []
                node.metadata = metadata or {}
                node.last_seen = current_time
                node.status = NodeStatus.ACTIVE
                logging.info(f"Discovery: Updated existing node {node_id}")
            else:
                node = DiscoveryNodeInfo(
                    node_id=node_id,
                    ip_address=ip_address,
                    port=port,
                    capabilities=capabilities or [],
                    metadata=metadata or {},
                    status=NodeStatus.JOINING,
                    last_seen=current_time,
                    join_timestamp=current_time
                )
                self.nodes[node_id] = node
                logging.info(f"Discovery: Registered new node {node_id}")
            
            # Handle bootstrap
            if is_bootstrap:
                self.bootstrap_initiated = True
                self.last_known_leader = node_id
                node.status = NodeStatus.ACTIVE
                logging.info(f"Discovery: Bootstrap initiated with node {node_id}")
            else:
                # Mark as active after successful registration
                node.status = NodeStatus.ACTIVE
            
            # Update registry version
            self.registry_version += 1
            self.last_update_time = current_time
            
            return {
                "status": "registered",
                "node_id": node_id,
                "is_bootstrap": is_bootstrap,
                "registry_version": self.registry_version
            }
    
    def get_node_list(self, requesting_node_id: Optional[int] = None) -> List[DiscoveryNodeInfo]:
        """
        Get list of nodes for discovery.
        
        Returns only active nodes suitable for joining.
        """
        with self.lock:
            # Return only active nodes
            active_nodes = [
                node for node in self.nodes.values() 
                if node.is_active() and (requesting_node_id is None or node.node_id != requesting_node_id)
            ]
            
            logging.debug(f"Discovery: Returning {len(active_nodes)} active nodes")
            return active_nodes
    
    def get_bootstrap_info(self) -> Dict:
        """
        Get bootstrap information for new nodes.
        
        Returns information needed for a new node to join the cluster.
        """
        with self.lock:
            active_nodes = self.get_node_list()
            
            return {
                "bootstrap_initiated": self.bootstrap_initiated,
                "active_nodes": len(active_nodes),
                "registry_version": self.registry_version,
                "last_known_leader": self.last_known_leader,
                "needs_bootstrap": len(active_nodes) == 0
            }
    
    def update_node_status(self, node_id: int, status: NodeStatus) -> bool:
        """Update node status for discovery purposes."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].status = status
                self.nodes[node_id].last_seen = time.time()
                self.registry_version += 1
                logging.info(f"Discovery: Updated node {node_id} status to {status}")
                return True
            return False
    
    def heartbeat(self, node_id: int) -> Dict:
        """
        Process heartbeat from a node.
        
        This is for discovery liveness only, not operational health.
        """
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].last_seen = time.time()
                if self.nodes[node_id].status == NodeStatus.JOINING:
                    self.nodes[node_id].status = NodeStatus.ACTIVE
                    self.registry_version += 1
                
                return {
                    "status": "received",
                    "registry_version": self.registry_version
                }
            else:
                return {
                    "status": "unknown_node",
                    "registry_version": self.registry_version
                }
    
    def remove_node(self, node_id: int) -> bool:
        """Remove a node from discovery registry."""
        with self.lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                self.registry_version += 1
                logging.info(f"Discovery: Removed node {node_id}")
                return True
            return False
    
    def cleanup_stale_nodes(self) -> List[int]:
        """
        Remove nodes that haven't been seen recently.
        
        This is for registry hygiene only.
        """
        with self.lock:
            current_time = time.time()
            stale_nodes = []
            
            for node_id, node in list(self.nodes.items()):
                if node.is_stale(self.node_timeout):
                    stale_nodes.append(node_id)
                    del self.nodes[node_id]
                    logging.info(f"Discovery: Cleaned up stale node {node_id}")
            
            if stale_nodes:
                self.registry_version += 1
                self.last_update_time = current_time
            
            return stale_nodes
    
    def get_stats(self) -> Dict:
        """Get discovery service statistics."""
        with self.lock:
            active_count = sum(1 for node in self.nodes.values() if node.is_active())
            
            return {
                "total_nodes": len(self.nodes),
                "active_nodes": active_count,
                "registry_version": self.registry_version,
                "bootstrap_initiated": self.bootstrap_initiated,
                "last_known_leader": self.last_known_leader,
                "uptime": time.time() - self.start_time,
                "last_update": self.last_update_time
            }
    
    def leader_hint(self, leader_id: Optional[int]) -> bool:
        """
        Accept a hint about the current leader.
        
        This is for discovery purposes only - to help new nodes
        find the leader faster. It's not authoritative.
        """
        with self.lock:
            if leader_id is None:
                self.last_known_leader = None
                logging.info("Discovery: Leader hint cleared")
                return True
            
            if leader_id in self.nodes and self.nodes[leader_id].is_active():
                self.last_known_leader = leader_id
                logging.info(f"Discovery: Leader hint updated to {leader_id}")
                return True
            
            logging.warning(f"Discovery: Ignoring leader hint for unknown/inactive node {leader_id}")
            return False


class PureServiceDiscoveryServicer(grpc_comm_manager_pb2_grpc.GatewayServiceServicer):
    """
    gRPC servicer for pure service discovery.
    
    This implements only the discovery services:
    - Node registration
    - Node discovery
    - Bootstrap coordination
    - Basic liveness tracking
    """
    
    def __init__(self, discovery: PureServiceDiscovery):
        self.discovery = discovery
    
    def RegisterNode(self, request, context):
        """Handle node registration for discovery."""
        try:
            result = self.discovery.register_node(
                node_id=request.node_id,
                ip_address=request.ip_address,
                port=request.port,
                capabilities=list(request.capabilities),
                metadata=dict(request.metadata)
            )
            
            # Get discovery information
            nodes = self.discovery.get_node_list(request.node_id)
            bootstrap_info = self.discovery.get_bootstrap_info()
            
            response = grpc_comm_manager_pb2.RegisterNodeResponse(
                status=result["status"],
                node_id=result["node_id"],
                is_bootstrap=result["is_bootstrap"],
                registry_version=result["registry_version"],
                nodes=[node.to_protobuf() for node in nodes],
                leader=None,  # Discovery doesn't maintain leader state
                message=f"Node registered for discovery. Bootstrap: {result['is_bootstrap']}"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery registration error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.RegisterNodeResponse()
    
    def GetNodes(self, request, context):
        """Handle node discovery requests."""
        try:
            nodes = self.discovery.get_node_list(
                request.requesting_node_id if request.requesting_node_id != 0 else None
            )
            
            response = grpc_comm_manager_pb2.GetNodesResponse(
                nodes=[node.to_protobuf() for node in nodes],
                registry_version=self.discovery.registry_version
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery get nodes error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetNodesResponse()
    
    def GetLeader(self, request, context):
        """Handle leader discovery requests."""
        try:
            # Discovery service only provides hints, not authoritative leader info
            leader_id = self.discovery.last_known_leader
            leader_node = None
            
            if leader_id and leader_id in self.discovery.nodes:
                leader_node = self.discovery.nodes[leader_id]
            
            response = grpc_comm_manager_pb2.GetLeaderResponse(
                leader=leader_node.to_protobuf() if leader_node else None,
                registry_version=self.discovery.registry_version,
                message="Leader hint provided (not authoritative)"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery get leader error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetLeaderResponse()
    
    def UpdateLeader(self, request, context):
        """Handle leader hint updates."""
        try:
            success = self.discovery.leader_hint(request.leader_id)
            
            response = grpc_comm_manager_pb2.UpdateLeaderResponse(
                status="updated" if success else "ignored",
                leader_id=request.leader_id,
                message="Leader hint updated" if success else "Leader hint ignored"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery update leader error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.UpdateLeaderResponse()
    
    def Heartbeat(self, request, context):
        """Handle heartbeat for discovery liveness."""
        try:
            result = self.discovery.heartbeat(request.node_id)
            
            response = grpc_comm_manager_pb2.HeartbeatResponse(
                status=result["status"],
                registry_version=result["registry_version"]
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery heartbeat error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.HeartbeatResponse()
    
    def RemoveNode(self, request, context):
        """Handle node removal from discovery."""
        try:
            success = self.discovery.remove_node(request.node_id)
            
            response = grpc_comm_manager_pb2.RemoveNodeResponse(
                status="removed" if success else "not_found",
                node_id=request.node_id,
                message="Node removed from discovery" if success else "Node not found"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery remove node error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.RemoveNodeResponse()
    
    def GetStats(self, request, context):
        """Handle discovery statistics requests."""
        try:
            stats = self.discovery.get_stats()
            
            response = grpc_comm_manager_pb2.GetStatsResponse(
                total_nodes=stats["total_nodes"],
                active_nodes=stats["active_nodes"],
                leader_id=stats["last_known_leader"] or 0,
                registry_version=stats["registry_version"],
                bootstrap_initiated=stats["bootstrap_initiated"],
                uptime=stats["uptime"]
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery get stats error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetStatsResponse()
    
    def HealthCheck(self, request, context):
        """Handle health check requests."""
        try:
            response = grpc_comm_manager_pb2.HealthCheckResponse(
                status="healthy",
                timestamp=time.time(),
                version="1.0.0"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Discovery health check error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.HealthCheckResponse()


class PureServiceDiscoveryServer:
    """
    Pure service discovery server implementation.
    
    This follows the gateway.md architecture:
    - Standalone service (not part of RAFT cluster)
    - Fixed address for discovery
    - Pure discovery functionality
    - Minimal state management
    - Operational independence
    """
    
    def __init__(self, host: str = '0.0.0.0', port: int = 8090, 
                 max_workers: int = 10, node_timeout: float = 300.0):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        
        # Pure service discovery
        self.discovery = PureServiceDiscovery(node_timeout=node_timeout)
        
        # gRPC server
        self.server = None
        self.cleanup_thread = None
        self.is_running = False
        
        # HTTP server for leader hints
        self.http_server = None
        self.http_thread = None
        
        logging.info(f"Pure service discovery server initialized on {host}:{port}")
    
    def start(self):
        """Start the pure service discovery server."""
        if self.is_running:
            logging.warning("Discovery server is already running")
            return
        
        try:
            # Create gRPC server
            self.server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=self.max_workers),
                options=[
                    ('grpc.max_send_message_length', 1000 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 1000 * 1024 * 1024),
                ]
            )
            
            # Add discovery servicer
            discovery_servicer = PureServiceDiscoveryServicer(self.discovery)
            grpc_comm_manager_pb2_grpc.add_GatewayServiceServicer_to_server(
                discovery_servicer, self.server
            )
            
            # Add insecure port
            listen_addr = f'{self.host}:{self.port}'
            self.server.add_insecure_port(listen_addr)
            
            # Start gRPC server
            self.server.start()
            
            # Start HTTP server for leader hints
            self._start_http_server()
            
            # Start cleanup thread
            self.cleanup_thread = threading.Thread(target=self._cleanup_loop)
            self.cleanup_thread.daemon = True
            self.cleanup_thread.start()
            
            self.is_running = True
            logging.info(f"Pure service discovery server started on {listen_addr}")
            
        except Exception as e:
            logging.error(f"Failed to start discovery server: {e}")
            raise
    
    def stop(self):
        """Stop the discovery server (idempotent)."""
        # Use a class-level flag to prevent multiple cleanup attempts
        if not hasattr(self, '_stop_called'):
            self._stop_called = threading.Event()
        
        if self._stop_called.is_set():
            logging.debug("Stop already called for pure service discovery server, skipping...")
            return
        
        # Set the flag to prevent re-entry
        self._stop_called.set()
        
        try:
            if not self.is_running:
                logging.debug("Pure service discovery server was already stopped")
                return
            
            self.is_running = False
            
            # Stop gRPC server
            if self.server:
                try:
                    self.server.stop(grace=2)  # Brief grace period
                    logging.debug("gRPC server stopped")
                except Exception as e:
                    logging.warning(f"Error stopping gRPC server: {e}")
                finally:
                    self.server = None
            
            # Stop HTTP server
            if self.http_server:
                try:
                    self.http_server.shutdown()
                    logging.debug("HTTP server stopped")
                except Exception as e:
                    logging.warning(f"Error stopping HTTP server: {e}")
                finally:
                    self.http_server = None
            
            # Stop cleanup thread
            if self.cleanup_thread:
                try:
                    self.cleanup_thread.join(timeout=2)  # Brief timeout
                except Exception as e:
                    logging.warning(f"Error joining cleanup thread: {e}")
                finally:
                    self.cleanup_thread = None
            
            # Stop HTTP thread
            if self.http_thread:
                try:
                    self.http_thread.join(timeout=2)  # Brief timeout
                except Exception as e:
                    logging.warning(f"Error joining HTTP thread: {e}")
                finally:
                    self.http_thread = None
            
            logging.info("Pure service discovery server stopped")
            
        except Exception as e:
            logging.error(f"Error during service discovery stop: {e}")
            # Don't force exit - let the process handle it naturally
    
    def wait_for_termination(self):
        """Wait for the server to terminate."""
        if self.server:
            self.server.wait_for_termination()
    
    def _start_http_server(self):
        """Start HTTP server for leader hints."""
        try:
            # Create HTTP request handler
            discovery_service = self.discovery
            
            class DiscoveryHTTPHandler(BaseHTTPRequestHandler):
                def do_POST(self):
                    if self.path == '/leader_hint':
                        try:
                            content_length = int(self.headers.get('Content-Length', 0))
                            post_data = self.rfile.read(content_length)
                            hint_data = json.loads(post_data.decode('utf-8'))
                            
                            # Process leader hint
                            leader_id = hint_data.get('leader_id')
                            if leader_id:
                                success = discovery_service.leader_hint(leader_id)
                                
                                if success:
                                    self.send_response(200)
                                    self.send_header('Content-Type', 'application/json')
                                    self.end_headers()
                                    response = {
                                        "status": "accepted",
                                        "leader_id": leader_id,
                                        "registry_version": discovery_service.registry_version
                                    }
                                    self.wfile.write(json.dumps(response).encode('utf-8'))
                                else:
                                    self.send_response(400)
                                    self.send_header('Content-Type', 'application/json')
                                    self.end_headers()
                                    response = {"status": "rejected", "message": "Leader not in registry"}
                                    self.wfile.write(json.dumps(response).encode('utf-8'))
                            else:
                                self.send_response(400)
                                self.send_header('Content-Type', 'application/json')
                                self.end_headers()
                                response = {"status": "error", "message": "Missing leader_id"}
                                self.wfile.write(json.dumps(response).encode('utf-8'))
                                
                        except Exception as e:
                            logging.error(f"Error processing leader hint: {e}")
                            self.send_response(500)
                            self.send_header('Content-Type', 'application/json')
                            self.end_headers()
                            response = {"status": "error", "message": str(e)}
                            self.wfile.write(json.dumps(response).encode('utf-8'))
                    else:
                        self.send_response(404)
                        self.end_headers()
                
                def do_GET(self):
                    if self.path == '/stats':
                        try:
                            stats = discovery_service.get_stats()
                            self.send_response(200)
                            self.send_header('Content-Type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps(stats).encode('utf-8'))
                        except Exception as e:
                            logging.error(f"Error getting stats: {e}")
                            self.send_response(500)
                            self.end_headers()
                    else:
                        self.send_response(404)
                        self.end_headers()
                
                def log_message(self, format, *args):
                    # Suppress default HTTP logging
                    pass
            
            # Find an available port for HTTP server
            http_port = self.port + 1000  # Offset from gRPC port
            while http_port < self.port + 1100:  # Try up to 100 ports
                try:
                    self.http_server = HTTPServer(('', http_port), DiscoveryHTTPHandler)
                    break
                except OSError:
                    http_port += 1
            
            if not self.http_server:
                raise Exception("Could not find available port for HTTP server")
            
            # Start HTTP server in separate thread
            self.http_thread = threading.Thread(target=self.http_server.serve_forever)
            self.http_thread.daemon = True
            self.http_thread.start()
            
            logging.info(f"HTTP server for leader hints started on port {http_port}")
            
        except Exception as e:
            logging.error(f"Failed to start HTTP server: {e}")
            # Continue without HTTP server - it's optional
    
    def _cleanup_loop(self):
        """Periodic cleanup of stale nodes."""
        while self.is_running:
            try:
                stale_nodes = self.discovery.cleanup_stale_nodes()
                if stale_nodes:
                    logging.info(f"Discovery: Cleaned up stale nodes: {stale_nodes}")
                
                time.sleep(self.discovery.cleanup_interval)
                
            except Exception as e:
                logging.error(f"Discovery cleanup error: {e}")
                time.sleep(10)  # Sleep longer on error
    
    def get_discovery_service(self) -> PureServiceDiscovery:
        """Get the discovery service instance."""
        return self.discovery


def main():
    """Main function for running the pure service discovery server."""
    
    parser = argparse.ArgumentParser(description='Pure Service Discovery Server for RAFT+GossipFL')
    parser.add_argument('--host', default='0.0.0.0', help='Discovery server host address')
    parser.add_argument('--port', type=int, default=8090, help='Discovery server port')
    parser.add_argument('--max-workers', type=int, default=10, help='Max worker threads')
    parser.add_argument('--node-timeout', type=float, default=300.0, help='Node timeout in seconds')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start pure service discovery server
    discovery_server = PureServiceDiscoveryServer(
        host=args.host, 
        port=args.port, 
        max_workers=args.max_workers,
        node_timeout=args.node_timeout
    )
    
    try:
        discovery_server.start()
        logging.info("Pure service discovery server is running. Press Ctrl+C to stop.")
        discovery_server.wait_for_termination()
        
    except KeyboardInterrupt:
        logging.info("Shutting down service discovery server...")
        discovery_server.stop()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        discovery_server.stop()


if __name__ == "__main__":
    main()
