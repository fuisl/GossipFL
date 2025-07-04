"""
Gateway Server for RAFT+GossipFL Dynamic Node Discovery

This module implements a lightweight service discovery gateway that:
1. Maintains a registry of active nodes
2. Provides node discovery for new joiners
3. Handles bootstrap coordination
4. Operates independently of the main cluster operations

The gateway follows the principle of being a non-critical discovery service
that doesn't interfere with cluster operations once nodes are connected.
"""

import json
import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import socketserver


class NodeStatus(Enum):
    """Status of a node in the gateway registry."""
    JOINING = "joining"
    ACTIVE = "active"
    SUSPECTED = "suspected"
    FAILED = "failed"
    LEAVING = "leaving"


@dataclass
class NodeInfo:
    """Information about a registered node."""
    node_id: int
    ip_address: str
    port: int
    capabilities: List[str]
    status: NodeStatus
    last_seen: float
    join_timestamp: float
    metadata: Dict = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data['status'] = self.status.value
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'NodeInfo':
        """Create NodeInfo from dictionary."""
        data['status'] = NodeStatus(data['status'])
        return cls(**data)


class GatewayState:
    """Manages the gateway's state and registry."""
    
    def __init__(self):
        self.nodes: Dict[int, NodeInfo] = {}
        self.bootstrap_initiated = False
        self.leader_id: Optional[int] = None
        self.registry_version = 0
        self.lock = threading.RLock()
        
        # Configuration
        self.node_timeout = 300.0  # 5 minutes timeout
        self.heartbeat_interval = 30.0  # 30 seconds heartbeat
        
    def register_node(self, node_id: int, ip_address: str, port: int, 
                     capabilities: List[str], metadata: Dict = None) -> Dict:
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
                node = NodeInfo(
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
    
    def get_node_list(self, requesting_node_id: Optional[int] = None) -> List[Dict]:
        """Get list of active nodes."""
        with self.lock:
            active_nodes = []
            for node_id, node in self.nodes.items():
                if node.status == NodeStatus.ACTIVE and node_id != requesting_node_id:
                    active_nodes.append(node.to_dict())
            return active_nodes
    
    def get_leader_info(self) -> Optional[Dict]:
        """Get current leader information."""
        with self.lock:
            if self.leader_id and self.leader_id in self.nodes:
                leader = self.nodes[self.leader_id]
                if leader.status == NodeStatus.ACTIVE:
                    return leader.to_dict()
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
                "leader_id": self.leader_id,
                "registry_version": self.registry_version,
                "bootstrap_initiated": self.bootstrap_initiated,
                "uptime": time.time() - getattr(self, 'start_time', time.time())
            }
            return stats


class GatewayHTTPHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the gateway server."""
    
    def __init__(self, *args, gateway_state: GatewayState, **kwargs):
        self.gateway_state = gateway_state
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        try:
            parsed_url = urlparse(self.path)
            path = parsed_url.path
            query_params = parse_qs(parsed_url.query)
            
            if path == '/health':
                self._handle_health_check()
            elif path == '/nodes':
                self._handle_get_nodes(query_params)
            elif path == '/leader':
                self._handle_get_leader()
            elif path == '/stats':
                self._handle_get_stats()
            else:
                self._send_error(404, "Not Found")
                
        except Exception as e:
            logging.error(f"Error handling GET request: {e}")
            self._send_error(500, str(e))
    
    def do_POST(self):
        """Handle POST requests."""
        try:
            parsed_url = urlparse(self.path)
            path = parsed_url.path
            
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            
            if content_length > 0:
                try:
                    data = json.loads(body.decode('utf-8'))
                except json.JSONDecodeError:
                    self._send_error(400, "Invalid JSON")
                    return
            else:
                data = {}
            
            if path == '/register':
                self._handle_register(data)
            elif path == '/heartbeat':
                self._handle_heartbeat(data)
            elif path == '/leader':
                self._handle_update_leader(data)
            elif path == '/remove':
                self._handle_remove_node(data)
            else:
                self._send_error(404, "Not Found")
                
        except Exception as e:
            logging.error(f"Error handling POST request: {e}")
            self._send_error(500, str(e))
    
    def _handle_health_check(self):
        """Handle health check request."""
        response = {
            "status": "healthy",
            "timestamp": time.time(),
            "version": "1.0.0"
        }
        self._send_json_response(response)
    
    def _handle_get_nodes(self, query_params):
        """Handle get nodes request."""
        requesting_node_id = None
        if 'node_id' in query_params:
            try:
                requesting_node_id = int(query_params['node_id'][0])
            except (ValueError, IndexError):
                pass
        
        nodes = self.gateway_state.get_node_list(requesting_node_id)
        response = {
            "nodes": nodes,
            "registry_version": self.gateway_state.registry_version
        }
        self._send_json_response(response)
    
    def _handle_get_leader(self):
        """Handle get leader request."""
        leader = self.gateway_state.get_leader_info()
        if leader:
            response = {
                "leader": leader,
                "registry_version": self.gateway_state.registry_version
            }
        else:
            response = {
                "leader": None,
                "message": "No leader currently registered"
            }
        self._send_json_response(response)
    
    def _handle_get_stats(self):
        """Handle get stats request."""
        stats = self.gateway_state.get_stats()
        self._send_json_response(stats)
    
    def _handle_register(self, data):
        """Handle node registration."""
        required_fields = ['node_id', 'ip_address', 'port', 'capabilities']
        
        for field in required_fields:
            if field not in data:
                self._send_error(400, f"Missing required field: {field}")
                return
        
        try:
            result = self.gateway_state.register_node(
                node_id=int(data['node_id']),
                ip_address=data['ip_address'],
                port=int(data['port']),
                capabilities=data['capabilities'],
                metadata=data.get('metadata', {})
            )
            
            # Include additional information for the registering node
            response = {
                **result,
                "nodes": self.gateway_state.get_node_list(int(data['node_id'])),
                "leader": self.gateway_state.get_leader_info()
            }
            
            self._send_json_response(response)
            
        except ValueError as e:
            self._send_error(400, f"Invalid data: {e}")
    
    def _handle_heartbeat(self, data):
        """Handle node heartbeat."""
        if 'node_id' not in data:
            self._send_error(400, "Missing node_id")
            return
        
        try:
            result = self.gateway_state.heartbeat(int(data['node_id']))
            self._send_json_response(result)
        except ValueError:
            self._send_error(400, "Invalid node_id")
    
    def _handle_update_leader(self, data):
        """Handle leader update."""
        if 'leader_id' not in data:
            self._send_error(400, "Missing leader_id")
            return
        
        try:
            success = self.gateway_state.update_leader(int(data['leader_id']))
            if success:
                response = {"status": "updated", "leader_id": int(data['leader_id'])}
            else:
                response = {"status": "failed", "message": "Leader not found in registry"}
            self._send_json_response(response)
        except ValueError:
            self._send_error(400, "Invalid leader_id")
    
    def _handle_remove_node(self, data):
        """Handle node removal."""
        if 'node_id' not in data:
            self._send_error(400, "Missing node_id")
            return
        
        try:
            success = self.gateway_state.remove_node(int(data['node_id']))
            if success:
                response = {"status": "removed", "node_id": int(data['node_id'])}
            else:
                response = {"status": "not_found", "node_id": int(data['node_id'])}
            self._send_json_response(response)
        except ValueError:
            self._send_error(400, "Invalid node_id")
    
    def _send_json_response(self, data, status_code=200):
        """Send JSON response."""
        response_body = json.dumps(data, indent=2)
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body.encode('utf-8'))
    
    def _send_error(self, status_code, message):
        """Send error response."""
        error_response = {
            "error": message,
            "status_code": status_code,
            "timestamp": time.time()
        }
        self._send_json_response(error_response, status_code)
    
    def log_message(self, format, *args):
        """Override to use Python logging."""
        logging.info(f"Gateway HTTP: {format % args}")


class ThreadedHTTPServer(socketserver.ThreadingMixIn, HTTPServer):
    """Threaded HTTP server for handling concurrent requests."""
    daemon_threads = True


class GatewayServer:
    """Main gateway server class."""
    
    def __init__(self, host='0.0.0.0', port=8080):
        self.host = host
        self.port = port
        self.gateway_state = GatewayState()
        self.gateway_state.start_time = time.time()
        self.server = None
        self.server_thread = None
        self.cleanup_thread = None
        self.is_running = False
        
    def start(self):
        """Start the gateway server."""
        if self.is_running:
            logging.warning("Gateway server is already running")
            return
        
        try:
            # Create HTTP server with custom handler
            def handler(*args, **kwargs):
                GatewayHTTPHandler(*args, gateway_state=self.gateway_state, **kwargs)
            
            self.server = ThreadedHTTPServer((self.host, self.port), handler)
            
            # Start server in a separate thread
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.daemon = True
            self.server_thread.start()
            
            # Start cleanup thread
            self.cleanup_thread = threading.Thread(target=self._cleanup_loop)
            self.cleanup_thread.daemon = True
            self.cleanup_thread.start()
            
            self.is_running = True
            logging.info(f"Gateway server started on {self.host}:{self.port}")
            
        except Exception as e:
            logging.error(f"Failed to start gateway server: {e}")
            raise
    
    def stop(self):
        """Stop the gateway server."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        
        if self.server_thread:
            self.server_thread.join(timeout=5)
        
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        
        logging.info("Gateway server stopped")
    
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
    
    def get_state(self) -> GatewayState:
        """Get the current gateway state."""
        return self.gateway_state


def main():
    """Main function for running the gateway server standalone."""
    import argparse
    
    parser = argparse.ArgumentParser(description='RAFT+GossipFL Gateway Server')
    parser.add_argument('--host', default='0.0.0.0', help='Gateway host address')
    parser.add_argument('--port', type=int, default=8080, help='Gateway port')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start gateway server
    gateway = GatewayServer(host=args.host, port=args.port)
    
    try:
        gateway.start()
        logging.info("Gateway server is running. Press Ctrl+C to stop.")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Shutting down gateway server...")
        gateway.stop()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        gateway.stop()


if __name__ == '__main__':
    main()
