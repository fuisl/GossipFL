"""
gRPC-based Gateway Server for RAFT+GossipFL Dynamic Node Discovery

This module implements a gRPC-based gateway server that integrates with the
existing gRPC communication infrastructure while providing service discovery
capabilities for dynamic node joining.
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
from enum import Enum
import grpc
from concurrent import futures

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2
from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc


class NodeStatus(Enum):
    """Status of a node in the gateway registry."""
    JOINING = "joining"
    ACTIVE = "active"
    SUSPECTED = "suspected"
    FAILED = "failed"
    LEAVING = "leaving"


@dataclass
class GatewayNodeInfo:
    """Information about a registered node."""
    node_id: int
    ip_address: str
    port: int
    capabilities: List[str]
    status: NodeStatus
    last_seen: float
    join_timestamp: float
    metadata: Dict[str, str]
    
    def to_protobuf(self) -> grpc_comm_manager_pb2.NodeInfo:
        """Convert to protobuf NodeInfo message."""
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
    
    @classmethod
    def from_protobuf(cls, pb_node: grpc_comm_manager_pb2.NodeInfo) -> 'GatewayNodeInfo':
        """Create from protobuf NodeInfo message."""
        return cls(
            node_id=pb_node.node_id,
            ip_address=pb_node.ip_address,
            port=pb_node.port,
            capabilities=list(pb_node.capabilities),
            status=NodeStatus(pb_node.status),
            last_seen=pb_node.last_seen,
            join_timestamp=pb_node.join_timestamp,
            metadata=dict(pb_node.metadata)
        )


class GRPCGatewayState:
    """Manages the gateway's state and registry using thread-safe operations."""
    
    def __init__(self):
        self.nodes: Dict[int, GatewayNodeInfo] = {}
        self.bootstrap_initiated = False
        self.leader_id: Optional[int] = None
        self.registry_version = 0
        self.lock = threading.RLock()
        self.start_time = time.time()
        
        # Configuration
        self.node_timeout = 300.0  # 5 minutes timeout
        self.heartbeat_interval = 30.0  # 30 seconds heartbeat
        
    def register_node(self, node_id: int, ip_address: str, port: int, 
                     capabilities: List[str], metadata: Dict[str, str] = None) -> Dict:
        """Register a new node or update existing node."""
        with self.lock:
            current_time = time.time()
            
            if node_id in self.nodes:
                # Update existing node
                node = self.nodes[node_id]
                node.ip_address = ip_address
                node.port = port
                node.capabilities = capabilities
                node.last_seen = current_time
                node.status = NodeStatus.ACTIVE
                if metadata:
                    node.metadata.update(metadata)
                
                logging.info(f"Updated existing node {node_id}")
                return {
                    "status": "updated",
                    "node_id": node_id,
                    "is_bootstrap": False,
                    "registry_version": self.registry_version
                }
            else:
                # Register new node
                node = GatewayNodeInfo(
                    node_id=node_id,
                    ip_address=ip_address,
                    port=port,
                    capabilities=capabilities,
                    status=NodeStatus.JOINING,
                    last_seen=current_time,
                    join_timestamp=current_time,
                    metadata=metadata or {}
                )
                
                self.nodes[node_id] = node
                self.registry_version += 1
                
                # Check if this is bootstrap scenario
                is_bootstrap = not self.bootstrap_initiated and len(self.nodes) == 1
                if is_bootstrap:
                    self.bootstrap_initiated = True
                    self.leader_id = node_id
                    logging.info(f"Bootstrap initiated with node {node_id} as leader")
                
                # Update node status to active
                node.status = NodeStatus.ACTIVE
                
                logging.info(f"Registered new node {node_id}, bootstrap: {is_bootstrap}")
                
                return {
                    "status": "registered",
                    "node_id": node_id,
                    "is_bootstrap": is_bootstrap,
                    "registry_version": self.registry_version
                }
    
    def get_node_list(self, requesting_node_id: Optional[int] = None) -> List[GatewayNodeInfo]:
        """Get list of active nodes."""
        with self.lock:
            active_nodes = []
            for node_id, node in self.nodes.items():
                if node.status == NodeStatus.ACTIVE and node_id != requesting_node_id:
                    active_nodes.append(node)
            return active_nodes
    
    def get_leader_info(self) -> Optional[GatewayNodeInfo]:
        """Get current leader information."""
        with self.lock:
            if self.leader_id and self.leader_id in self.nodes:
                leader = self.nodes[self.leader_id]
                if leader.status == NodeStatus.ACTIVE:
                    return leader
            return None
    
    def update_leader(self, new_leader_id: int) -> bool:
        """Update leader information."""
        with self.lock:
            if new_leader_id in self.nodes:
                self.leader_id = new_leader_id
                logging.info(f"Leader updated to node {new_leader_id}")
                return True
            return False
    
    def heartbeat(self, node_id: int) -> Dict:
        """Process heartbeat from a node."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].last_seen = time.time()
                return {
                    "status": "acknowledged",
                    "registry_version": self.registry_version
                }
            return {"status": "not_found"}
    
    def remove_node(self, node_id: int) -> bool:
        """Remove a node from the registry."""
        with self.lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                self.registry_version += 1
                
                # Handle leader failure
                if self.leader_id == node_id:
                    self.leader_id = None
                    logging.warning(f"Leader node {node_id} removed")
                
                logging.info(f"Removed node {node_id}")
                return True
            return False
    
    def cleanup_inactive_nodes(self) -> List[int]:
        """Remove nodes that haven't sent heartbeats recently."""
        with self.lock:
            current_time = time.time()
            inactive_nodes = []
            
            for node_id, node in list(self.nodes.items()):
                if current_time - node.last_seen > self.node_timeout:
                    inactive_nodes.append(node_id)
                    self.remove_node(node_id)
            
            return inactive_nodes
    
    def get_stats(self) -> Dict:
        """Get gateway statistics."""
        with self.lock:
            stats = {
                "total_nodes": len(self.nodes),
                "active_nodes": len([n for n in self.nodes.values() if n.status == NodeStatus.ACTIVE]),
                "leader_id": self.leader_id or 0,
                "registry_version": self.registry_version,
                "bootstrap_initiated": self.bootstrap_initiated,
                "uptime": time.time() - self.start_time
            }
            return stats


class GRPCGatewayServicer(grpc_comm_manager_pb2_grpc.GatewayServiceServicer):
    """gRPC servicer implementation for the gateway service."""
    
    def __init__(self, gateway_state: GRPCGatewayState):
        self.gateway_state = gateway_state
        
    def RegisterNode(self, request, context):
        """Handle node registration requests."""
        try:
            result = self.gateway_state.register_node(
                node_id=request.node_id,
                ip_address=request.ip_address,
                port=request.port,
                capabilities=list(request.capabilities),
                metadata=dict(request.metadata)
            )
            
            # Get additional information for response
            nodes = self.gateway_state.get_node_list(request.node_id)
            leader = self.gateway_state.get_leader_info()
            
            response = grpc_comm_manager_pb2.RegisterNodeResponse(
                status=result["status"],
                node_id=result["node_id"],
                is_bootstrap=result["is_bootstrap"],
                registry_version=result["registry_version"],
                nodes=[node.to_protobuf() for node in nodes],
                leader=leader.to_protobuf() if leader else None,
                message="Registration successful"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Registration error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.RegisterNodeResponse()
    
    def GetNodes(self, request, context):
        """Handle get nodes requests."""
        try:
            nodes = self.gateway_state.get_node_list(
                request.requesting_node_id if request.requesting_node_id != 0 else None
            )
            
            response = grpc_comm_manager_pb2.GetNodesResponse(
                nodes=[node.to_protobuf() for node in nodes],
                registry_version=self.gateway_state.registry_version
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Get nodes error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetNodesResponse()
    
    def GetLeader(self, request, context):
        """Handle get leader requests."""
        try:
            leader = self.gateway_state.get_leader_info()
            
            response = grpc_comm_manager_pb2.GetLeaderResponse(
                leader=leader.to_protobuf() if leader else None,
                registry_version=self.gateway_state.registry_version,
                message="Leader found" if leader else "No leader registered"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Get leader error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetLeaderResponse()
    
    def UpdateLeader(self, request, context):
        """Handle leader update requests."""
        try:
            success = self.gateway_state.update_leader(request.leader_id)
            
            response = grpc_comm_manager_pb2.UpdateLeaderResponse(
                status="updated" if success else "failed",
                leader_id=request.leader_id,
                message="Leader updated successfully" if success else "Leader not found in registry"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Update leader error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.UpdateLeaderResponse()
    
    def Heartbeat(self, request, context):
        """Handle heartbeat requests."""
        try:
            result = self.gateway_state.heartbeat(request.node_id)
            
            response = grpc_comm_manager_pb2.HeartbeatResponse(
                status=result["status"],
                registry_version=result.get("registry_version", 0)
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Heartbeat error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.HeartbeatResponse()
    
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
            logging.error(f"Health check error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.HealthCheckResponse()
    
    def RemoveNode(self, request, context):
        """Handle node removal requests."""
        try:
            success = self.gateway_state.remove_node(request.node_id)
            
            response = grpc_comm_manager_pb2.RemoveNodeResponse(
                status="removed" if success else "not_found",
                node_id=request.node_id,
                message="Node removed successfully" if success else "Node not found"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Remove node error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.RemoveNodeResponse()
    
    def GetStats(self, request, context):
        """Handle stats requests."""
        try:
            stats = self.gateway_state.get_stats()
            
            response = grpc_comm_manager_pb2.GetStatsResponse(
                total_nodes=stats["total_nodes"],
                active_nodes=stats["active_nodes"],
                leader_id=stats["leader_id"],
                registry_version=stats["registry_version"],
                bootstrap_initiated=stats["bootstrap_initiated"],
                uptime=stats["uptime"]
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Get stats error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetStatsResponse()


class GRPCGatewayServer:
    """Main gRPC gateway server class."""
    
    def __init__(self, host='0.0.0.0', port=8090, max_workers=10):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.gateway_state = GRPCGatewayState()
        self.server = None
        self.cleanup_thread = None
        self.is_running = False
        
    def start(self):
        """Start the gRPC gateway server."""
        if self.is_running:
            logging.warning("Gateway server is already running")
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
            
            # Add gateway servicer
            gateway_servicer = GRPCGatewayServicer(self.gateway_state)
            grpc_comm_manager_pb2_grpc.add_GatewayServiceServicer_to_server(
                gateway_servicer, self.server
            )
            
            # Add insecure port
            listen_addr = f'{self.host}:{self.port}'
            self.server.add_insecure_port(listen_addr)
            
            # Start server
            self.server.start()
            
            # Start cleanup thread
            self.cleanup_thread = threading.Thread(target=self._cleanup_loop)
            self.cleanup_thread.daemon = True
            self.cleanup_thread.start()
            
            self.is_running = True
            logging.info(f"gRPC Gateway server started on {listen_addr}")
            
        except Exception as e:
            logging.error(f"Failed to start gRPC gateway server: {e}")
            raise
    
    def stop(self):
        """Stop the gRPC gateway server."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.server:
            self.server.stop(grace=5)
        
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        
        logging.info("gRPC Gateway server stopped")
    
    def wait_for_termination(self):
        """Wait for the server to terminate."""
        if self.server:
            self.server.wait_for_termination()
    
    def _cleanup_loop(self):
        """Periodic cleanup of inactive nodes."""
        while self.is_running:
            try:
                inactive_nodes = self.gateway_state.cleanup_inactive_nodes()
                if inactive_nodes:
                    logging.info(f"Cleaned up inactive nodes: {inactive_nodes}")
                
                time.sleep(self.gateway_state.heartbeat_interval)
                
            except Exception as e:
                logging.error(f"Error in cleanup loop: {e}")
                time.sleep(10)  # Sleep longer on error
    
    def get_state(self) -> GRPCGatewayState:
        """Get the current gateway state."""
        return self.gateway_state


def main():
    """Main function for running the gRPC gateway server standalone."""
    import argparse
    
    parser = argparse.ArgumentParser(description='gRPC Gateway Server for RAFT+GossipFL')
    parser.add_argument('--host', default='0.0.0.0', help='Gateway host address')
    parser.add_argument('--port', type=int, default=8090, help='Gateway port')
    parser.add_argument('--max-workers', type=int, default=10, help='Max worker threads')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start gateway server
    gateway = GRPCGatewayServer(host=args.host, port=args.port, max_workers=args.max_workers)
    
    try:
        gateway.start()
        logging.info("gRPC Gateway server is running. Press Ctrl+C to stop.")
        gateway.wait_for_termination()
        
    except KeyboardInterrupt:
        logging.info("Shutting down gRPC gateway server...")
        gateway.stop()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        gateway.stop()


if __name__ == '__main__':
    main()
