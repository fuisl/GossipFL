"""
Gateway Client for RAFT+GossipFL Node Discovery

This module provides a client interface for nodes to communicate with the
gateway server for discovery and registration purposes.
"""

import json
import logging
import time
import threading
from typing import Dict, List, Optional, Tuple
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError


class GatewayClient:
    """Client for communicating with the gateway server."""
    
    def __init__(self, gateway_host: str, gateway_port: int, timeout: int = 10):
        """
        Initialize the gateway client.
        
        Args:
            gateway_host: Gateway server hostname or IP
            gateway_port: Gateway server port
            timeout: Request timeout in seconds
        """
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.timeout = timeout
        self.base_url = f"http://{gateway_host}:{gateway_port}"
        
        # Client state
        self.node_id = None
        self.is_registered = False
        self.last_heartbeat = 0
        self.heartbeat_interval = 30.0  # seconds
        self.heartbeat_thread = None
        self.heartbeat_running = False
        
        logging.info(f"Gateway client initialized for {self.base_url}")
    
    def _make_request(self, method: str, endpoint: str, data: Dict = None) -> Dict:
        """
        Make HTTP request to the gateway server.
        
        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint
            data: Request data (for POST requests)
            
        Returns:
            Response data as dictionary
            
        Raises:
            Exception: If request fails
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            if method == 'GET':
                response = urlopen(url, timeout=self.timeout)
            elif method == 'POST':
                request_data = json.dumps(data or {}).encode('utf-8')
                headers = {'Content-Type': 'application/json'}
                request = Request(url, data=request_data, headers=headers)
                response = urlopen(request, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response_data = response.read().decode('utf-8')
            return json.loads(response_data)
            
        except HTTPError as e:
            error_msg = f"HTTP error {e.code}: {e.reason}"
            try:
                error_data = json.loads(e.read().decode('utf-8'))
                error_msg = error_data.get('error', error_msg)
            except:
                pass
            raise Exception(f"Gateway request failed: {error_msg}")
            
        except URLError as e:
            raise Exception(f"Gateway connection failed: {e.reason}")
            
        except Exception as e:
            raise Exception(f"Gateway request error: {e}")
    
    def health_check(self) -> Dict:
        """
        Check gateway server health.
        
        Returns:
            Health status information
        """
        return self._make_request('GET', '/health')
    
    def register_node(self, node_id: int, ip_address: str, port: int, 
                     capabilities: List[str], metadata: Dict = None) -> Dict:
        """
        Register this node with the gateway.
        
        Args:
            node_id: Unique node identifier
            ip_address: Node's IP address
            port: Node's port
            capabilities: List of node capabilities (e.g., ['raft', 'gossip'])
            metadata: Additional node metadata
            
        Returns:
            Registration response from gateway
        """
        data = {
            'node_id': node_id,
            'ip_address': ip_address,
            'port': port,
            'capabilities': capabilities,
            'metadata': metadata or {}
        }
        
        response = self._make_request('POST', '/register', data)
        
        # Update client state
        self.node_id = node_id
        self.is_registered = True
        self.last_heartbeat = time.time()
        
        # Start heartbeat if registration successful
        if response.get('status') in ['registered', 'updated']:
            self._start_heartbeat()
        
        logging.info(f"Node {node_id} registered with gateway: {response.get('status')}")
        return response
    
    def get_nodes(self, exclude_self: bool = True) -> List[Dict]:
        """
        Get list of active nodes from gateway.
        
        Args:
            exclude_self: Whether to exclude this node from the list
            
        Returns:
            List of active node information
        """
        endpoint = '/nodes'
        if exclude_self and self.node_id:
            endpoint += f'?node_id={self.node_id}'
        
        response = self._make_request('GET', endpoint)
        return response.get('nodes', [])
    
    def get_leader(self) -> Optional[Dict]:
        """
        Get current leader information from gateway.
        
        Returns:
            Leader node information or None if no leader
        """
        response = self._make_request('GET', '/leader')
        return response.get('leader')
    
    def update_leader(self, leader_id: int) -> bool:
        """
        Update leader information in gateway.
        
        Args:
            leader_id: ID of the new leader
            
        Returns:
            True if update successful
        """
        data = {'leader_id': leader_id}
        response = self._make_request('POST', '/leader', data)
        return response.get('status') == 'updated'
    
    def heartbeat(self) -> Dict:
        """
        Send heartbeat to gateway.
        
        Returns:
            Heartbeat response
        """
        if not self.is_registered or not self.node_id:
            raise Exception("Node not registered with gateway")
        
        data = {'node_id': self.node_id}
        response = self._make_request('POST', '/heartbeat', data)
        self.last_heartbeat = time.time()
        return response
    
    def remove_node(self, node_id: int) -> bool:
        """
        Remove a node from the gateway registry.
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            True if removal successful
        """
        data = {'node_id': node_id}
        response = self._make_request('POST', '/remove', data)
        return response.get('status') == 'removed'
    
    def get_stats(self) -> Dict:
        """
        Get gateway statistics.
        
        Returns:
            Gateway statistics
        """
        return self._make_request('GET', '/stats')
    
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
                    self.heartbeat()
                    logging.debug(f"Heartbeat sent for node {self.node_id}")
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logging.warning(f"Heartbeat failed: {e}")
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
                        capabilities: List[str], metadata: Dict = None) -> Tuple[bool, Dict]:
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
        """
        try:
            # Register with gateway
            registration_response = self.register_node(
                node_id, ip_address, port, capabilities, metadata
            )
            
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
            
        except Exception as e:
            logging.error(f"Cluster discovery failed: {e}")
            raise
    
    def is_gateway_available(self) -> bool:
        """
        Check if gateway is available.
        
        Returns:
            True if gateway is reachable
        """
        try:
            self.health_check()
            return True
        except Exception:
            return False
    
    def shutdown(self):
        """Shutdown the gateway client."""
        self.stop_heartbeat()
        self.is_registered = False
        logging.info("Gateway client shutdown")


class GatewayDiscoveryMixin:
    """
    Mixin class to add gateway discovery capabilities to existing classes.
    
    This can be used to extend existing node classes with gateway functionality.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gateway_client = None
        self.discovery_info = None
    
    def initialize_gateway_discovery(self, gateway_host: str, gateway_port: int, 
                                   node_id: int, ip_address: str, port: int, 
                                   capabilities: List[str], metadata: Dict = None):
        """
        Initialize gateway discovery.
        
        Args:
            gateway_host: Gateway server hostname or IP
            gateway_port: Gateway server port
            node_id: Unique node identifier
            ip_address: Node's IP address
            port: Node's port
            capabilities: List of node capabilities
            metadata: Additional node metadata
        """
        self.gateway_client = GatewayClient(gateway_host, gateway_port)
        
        try:
            is_bootstrap, cluster_info = self.gateway_client.discover_cluster(
                node_id, ip_address, port, capabilities, metadata
            )
            
            self.discovery_info = {
                'is_bootstrap': is_bootstrap,
                'cluster_info': cluster_info,
                'gateway_available': True
            }
            
            logging.info(f"Gateway discovery initialized. Bootstrap: {is_bootstrap}")
            
        except Exception as e:
            logging.error(f"Gateway discovery failed: {e}")
            self.discovery_info = {
                'is_bootstrap': False,
                'cluster_info': {'nodes': [], 'leader': None},
                'gateway_available': False,
                'error': str(e)
            }
    
    def get_discovery_info(self) -> Dict:
        """Get discovery information."""
        return self.discovery_info or {}
    
    def is_bootstrap_node(self) -> bool:
        """Check if this is a bootstrap node."""
        return self.discovery_info and self.discovery_info.get('is_bootstrap', False)
    
    def get_cluster_nodes(self) -> List[Dict]:
        """Get list of cluster nodes."""
        if not self.discovery_info:
            return []
        return self.discovery_info.get('cluster_info', {}).get('nodes', [])
    
    def get_cluster_leader(self) -> Optional[Dict]:
        """Get cluster leader information."""
        if not self.discovery_info:
            return None
        return self.discovery_info.get('cluster_info', {}).get('leader')
    
    def update_gateway_leader(self, leader_id: int) -> bool:
        """Update leader information in gateway."""
        if not self.gateway_client:
            return False
        
        try:
            return self.gateway_client.update_leader(leader_id)
        except Exception as e:
            logging.warning(f"Failed to update leader in gateway: {e}")
            return False
    
    def shutdown_gateway_discovery(self):
        """Shutdown gateway discovery."""
        if self.gateway_client:
            self.gateway_client.shutdown()
            self.gateway_client = None
        self.discovery_info = None


def main():
    """Main function for testing the gateway client."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Gateway Client Test')
    parser.add_argument('--gateway-host', default='localhost', help='Gateway host')
    parser.add_argument('--gateway-port', type=int, default=8080, help='Gateway port')
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
    client = GatewayClient(args.gateway_host, args.gateway_port)
    
    try:
        # Test discovery
        is_bootstrap, cluster_info = client.discover_cluster(
            node_id=args.node_id,
            ip_address=args.ip,
            port=args.port,
            capabilities=['raft', 'gossip', 'test'],
            metadata={'test': True}
        )
        
        print(f"Bootstrap: {is_bootstrap}")
        print(f"Cluster nodes: {len(cluster_info['nodes'])}")
        print(f"Leader: {cluster_info['leader']}")
        
        # Test other operations
        print(f"Gateway stats: {client.get_stats()}")
        
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
