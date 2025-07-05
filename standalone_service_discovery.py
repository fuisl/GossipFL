#!/usr/bin/env python3
"""
Standalone Service Discovery Server for RAFT+GossipFL Manual Testing

This script bootstraps a pure service discovery server that can be used
for manual testing of the RAFT consensus implementation. It provides:

1. Bootstrap coordination for the first node
2. Node registration and discovery
3. Basic liveness tracking
4. Real-time monitoring dashboard

Usage:
    python standalone_service_discovery.py --port 8080 --monitor-port 8081

Test Scenarios:
    - Single node bootstrap detection
    - Multi-node cluster formation
    - Dynamic node joining/leaving
    - Service discovery failure/recovery
"""

import sys
import os
import argparse
import logging
import time
import threading
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Add the project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from algorithms.RAFT_GossipFL.pure_service_discovery import PureServiceDiscovery, PureServiceDiscoveryServicer
import grpc
from concurrent import futures
from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc


class ServiceDiscoveryMonitor(BaseHTTPRequestHandler):
    """HTTP server for monitoring service discovery state."""
    
    def __init__(self, discovery_service, *args, **kwargs):
        self.discovery_service = discovery_service
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests for monitoring."""
        try:
            parsed_path = urlparse(self.path)
            
            if parsed_path.path == '/':
                self.send_dashboard()
            elif parsed_path.path == '/api/stats':
                self.send_stats()
            elif parsed_path.path == '/api/nodes':
                self.send_nodes()
            elif parsed_path.path == '/api/bootstrap':
                self.send_bootstrap_info()
            else:
                self.send_error(404, "Not Found")
                
        except Exception as e:
            logging.error(f"Monitor error: {e}")
            self.send_error(500, str(e))
    
    def send_dashboard(self):
        """Send the monitoring dashboard."""
        stats = self.discovery_service.get_stats()
        nodes = self.discovery_service.get_node_list()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>RAFT Service Discovery Monitor</title>
            <meta http-equiv="refresh" content="5">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .status {{ padding: 10px; margin: 10px 0; border-radius: 5px; }}
                .healthy {{ background-color: #d4edda; color: #155724; }}
                .warning {{ background-color: #fff3cd; color: #856404; }}
                .error {{ background-color: #f8d7da; color: #721c24; }}
                .nodes {{ margin: 20px 0; }}
                .node {{ padding: 10px; margin: 5px; border: 1px solid #ddd; border-radius: 5px; }}
                .metrics {{ display: flex; gap: 20px; margin: 20px 0; }}
                .metric {{ padding: 15px; border: 1px solid #ddd; border-radius: 5px; text-align: center; }}
            </style>
        </head>
        <body>
            <h1>RAFT Service Discovery Monitor</h1>
            
            <div class="status {'healthy' if stats['active_nodes'] > 0 else 'warning'}">
                <strong>Status:</strong> {'Active' if stats['active_nodes'] > 0 else 'Waiting for nodes'}
            </div>
            
            <div class="metrics">
                <div class="metric">
                    <h3>{stats['total_nodes']}</h3>
                    <p>Total Nodes</p>
                </div>
                <div class="metric">
                    <h3>{stats['active_nodes']}</h3>
                    <p>Active Nodes</p>
                </div>
                <div class="metric">
                    <h3>{stats['registry_version']}</h3>
                    <p>Registry Version</p>
                </div>
                <div class="metric">
                    <h3>{stats.get('last_known_leader', 'None')}</h3>
                    <p>Last Known Leader</p>
                </div>
            </div>
            
            <div class="nodes">
                <h2>Registered Nodes</h2>
                {''.join([f'''
                <div class="node">
                    <strong>Node {node.node_id}</strong> - {node.ip_address}:{node.port}<br>
                    Status: {node.status.value} | Last Seen: {time.strftime('%H:%M:%S', time.localtime(node.last_seen))}<br>
                    Capabilities: {', '.join(node.capabilities)}<br>
                    Join Time: {time.strftime('%H:%M:%S', time.localtime(node.join_timestamp))}
                </div>
                ''' for node in nodes])}
            </div>
            
            <div class="status">
                <strong>Bootstrap Status:</strong> {'Initiated' if stats['bootstrap_initiated'] else 'Not started'}<br>
                <strong>Uptime:</strong> {int(stats['uptime'])} seconds<br>
                <strong>Last Update:</strong> {time.strftime('%H:%M:%S', time.localtime(stats['last_update']))}
            </div>
            
            <p><em>Auto-refresh every 5 seconds</em></p>
        </body>
        </html>
        """
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())
    
    def send_stats(self):
        """Send JSON stats."""
        stats = self.discovery_service.get_stats()
        self.send_json(stats)
    
    def send_nodes(self):
        """Send JSON node list."""
        nodes = self.discovery_service.get_node_list()
        node_data = []
        for node in nodes:
            node_data.append({
                'node_id': node.node_id,
                'ip_address': node.ip_address,
                'port': node.port,
                'status': node.status.value,
                'capabilities': node.capabilities,
                'last_seen': node.last_seen,
                'join_timestamp': node.join_timestamp
            })
        self.send_json(node_data)
    
    def send_bootstrap_info(self):
        """Send bootstrap information."""
        bootstrap_info = self.discovery_service.get_bootstrap_info()
        self.send_json(bootstrap_info)
    
    def send_json(self, data):
        """Send JSON response."""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode())
    
    def send_error(self, code, message):
        """Send error response."""
        self.send_response(code)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(message.encode())
    
    def log_message(self, format, *args):
        """Suppress HTTP server logs."""
        pass


def create_monitor_server(discovery_service, port):
    """Create the monitoring HTTP server."""
    def handler(*args, **kwargs):
        return ServiceDiscoveryMonitor(discovery_service, *args, **kwargs)
    
    httpd = HTTPServer(('', port), handler)
    return httpd


def cleanup_thread(discovery_service, interval=60):
    """Background thread for cleanup operations."""
    while True:
        try:
            time.sleep(interval)
            stale_nodes = discovery_service.cleanup_stale_nodes()
            if stale_nodes:
                logging.info(f"Cleaned up stale nodes: {stale_nodes}")
        except Exception as e:
            logging.error(f"Cleanup error: {e}")


def main():
    """Main function to start the service discovery server."""
    parser = argparse.ArgumentParser(description="Standalone Service Discovery Server")
    parser.add_argument('--port', type=int, default=8080, help='gRPC service port')
    parser.add_argument('--monitor-port', type=int, default=8081, help='HTTP monitoring port')
    parser.add_argument('--node-timeout', type=float, default=300.0, help='Node timeout in seconds')
    parser.add_argument('--cleanup-interval', type=float, default=60.0, help='Cleanup interval in seconds')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    parser.add_argument('--bind-address', default='0.0.0.0', help='Bind address')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Create the discovery service
        discovery_service = PureServiceDiscovery(
            node_timeout=args.node_timeout,
            cleanup_interval=args.cleanup_interval
        )
        
        # Create gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        servicer = PureServiceDiscoveryServicer(discovery_service)
        grpc_comm_manager_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        
        server.add_insecure_port(f'{args.bind_address}:{args.port}')
        
        # Create monitoring server
        monitor_server = create_monitor_server(discovery_service, args.monitor_port)
        
        # Start cleanup thread
        cleanup_thread_handle = threading.Thread(
            target=cleanup_thread,
            args=(discovery_service, args.cleanup_interval),
            daemon=True
        )
        cleanup_thread_handle.start()
        
        # Start servers
        server.start()
        logger.info(f"Service Discovery gRPC server started on {args.bind_address}:{args.port}")
        
        monitor_thread = threading.Thread(target=monitor_server.serve_forever, daemon=True)
        monitor_thread.start()
        logger.info(f"Monitoring HTTP server started on http://localhost:{args.monitor_port}")
        
        # Print usage information
        print("=" * 80)
        print("🚀 RAFT Service Discovery Server Started")
        print("=" * 80)
        print(f"gRPC Service:     {args.bind_address}:{args.port}")
        print(f"Monitoring:       http://localhost:{args.monitor_port}")
        print(f"Node Timeout:     {args.node_timeout}s")
        print(f"Cleanup Interval: {args.cleanup_interval}s")
        print()
        print("Test Commands:")
        print("  curl http://localhost:{}/api/stats           # Get stats".format(args.monitor_port))
        print("  curl http://localhost:{}/api/nodes           # Get nodes".format(args.monitor_port))
        print("  curl http://localhost:{}/api/bootstrap       # Get bootstrap info".format(args.monitor_port))
        print()
        print("Manual Testing Scenarios:")
        print("  1. Start this server first")
        print("  2. Run standalone_raft_node.py with different node IDs")
        print("  3. Watch the monitoring dashboard for cluster formation")
        print("  4. Test leader election by killing the leader node")
        print("  5. Test dynamic joining by adding nodes after cluster formation")
        print()
        print("Press Ctrl+C to stop...")
        print("=" * 80)
        
        # Keep the server running
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            
    except Exception as e:
        logger.error(f"Server error: {e}")
        return 1
    
    finally:
        # Cleanup
        try:
            server.stop(0)
            monitor_server.shutdown()
            logger.info("Service Discovery server stopped")
        except:
            pass
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
