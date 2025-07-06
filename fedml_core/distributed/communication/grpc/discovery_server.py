#!/usr/bin/env python3

"""
Pure Service Discovery Server for Manual Testing

This script starts a standalone pure service discovery server that can be used
for manual testing of the refactored communication manager.

Usage:
    python discovery_server.py [--host HOST] [--port PORT] [--log-level LEVEL]

Example:
    python discovery_server.py --host 0.0.0.0 --port 8090 --log-level INFO
"""

import sys
import os
import argparse
import logging
import signal
import time
import threading
from typing import Optional

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

from algorithms.RAFT_GossipFL.pure_service_discovery import PureServiceDiscoveryServer


class PureDiscoveryServer:
    """
    Standalone pure service discovery server for testing.
    
    This wraps the pure service discovery implementation and provides
    a simple interface for manual testing.
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8090, log_level: str = "INFO"):
        """
        Initialize the pure discovery server.
        
        Args:
            host: Server host address
            port: Server port
            log_level: Logging level
        """
        self.host = host
        self.port = port
        self.log_level = log_level
        self.server: Optional[PureServiceDiscoveryServer] = None
        
        # Configure logging - Fix duplicate logging by configuring only once
        self.logger = logging.getLogger(__name__)
        
        # Only configure if root logger has no handlers (prevents duplicate logging)
        if not logging.root.handlers:
            logging.basicConfig(
                level=getattr(logging, log_level.upper()),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.StreamHandler(sys.stdout),
                    logging.FileHandler('pure_discovery_server.log')
                ]
            )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Pure discovery server initialized - {host}:{port}")
    
    def start(self):
        """Start the pure discovery server."""
        try:
            self.logger.info(f"Starting pure discovery server on {self.host}:{self.port}")
            
            # Create and start the pure service discovery server
            self.server = PureServiceDiscoveryServer(
                host=self.host,
                port=self.port,
                node_timeout=300.0,  # 5 minutes
                max_workers=10
            )
            
            self.server.start()
            
            self.logger.info(f"Pure discovery server started successfully on {self.host}:{self.port}")
            self.logger.info("Server is ready to accept connections")
            
            # Print some helpful information
            self._print_server_info()
            
        except Exception as e:
            self.logger.error(f"Failed to start pure discovery server: {e}")
            raise
    
    def stop(self):
        """Stop the pure discovery server."""
        if self.server:
            self.logger.info("Stopping pure discovery server...")
            self.server.stop()
            self.server = None
            self.logger.info("Pure discovery server stopped")
    
    def wait_for_termination(self):
        """Wait for the server to terminate."""
        if self.server:
            try:
                self.server.wait_for_termination()
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal")
                self.stop()
    
    def _print_server_info(self):
        """Print server information."""
        print("\n" + "="*70)
        print("PURE SERVICE DISCOVERY SERVER STARTED")
        print("="*70)
        print(f"Host: {self.host}")
        print(f"Port: {self.port}")
        print(f"Log Level: {self.log_level}")
        print(f"Server Address: {self.host}:{self.port}")
        print("\nThis is a PURE service discovery server that:")
        print("- Handles node registration and discovery")
        print("- Coordinates bootstrap for the first node")
        print("- Maintains an eventually consistent registry")
        print("- Operates independently of RAFT consensus")
        print("\nThe server is now ready to accept node registrations.")
        print("You can now start dynamic nodes to test the service discovery.")
        print("\nTo stop the server, press Ctrl+C")
        print("="*70)
        print()
    
    def get_stats(self):
        """Get server statistics."""
        if self.server:
            return self.server.get_discovery_service().get_stats()
        return {}
    
    def get_nodes(self):
        """Get registered nodes."""
        if self.server:
            return self.server.get_discovery_service().get_node_list()
        return []


def setup_signal_handlers(server: PureDiscoveryServer):
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}")
        print("Shutting down gracefully...")
        server.stop()
        # Give a moment for cleanup
        time.sleep(0.5)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def print_stats_periodically(server: PureDiscoveryServer, interval: int):
    """Print server stats periodically."""
    while True:
        try:
            time.sleep(interval)
            stats = server.get_stats()
            nodes = server.get_nodes()
            
            if stats:
                print(f"\n--- Pure Discovery Server Stats ---")
                print(f"Total Nodes: {stats.get('total_nodes', 0)}")
                print(f"Active Nodes: {stats.get('active_nodes', 0)}")
                print(f"Bootstrap Node: {stats.get('bootstrap_node_id', 'None')}")
                print(f"Registry Version: {stats.get('registry_version', 0)}")
                print(f"Bootstrap Completed: {stats.get('bootstrap_completed', False)}")
                print(f"Uptime: {stats.get('uptime', 0):.1f} seconds")
                
                if nodes:
                    print(f"\nRegistered Nodes:")
                    for node in nodes:
                        # Handle DiscoveryNodeInfo dataclass objects
                        status = node.status.value if hasattr(node.status, 'value') else str(node.status)
                        last_seen = node.last_seen
                        time_since_seen = time.time() - last_seen
                        print(f"  Node {node.node_id}: {node.ip_address}:{node.port} "
                              f"[{status}] (last seen {time_since_seen:.1f}s ago)")
                
                print("-----------------------------------\n")
                
        except Exception as e:
            print(f"Error getting stats: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Pure Service Discovery Server for Manual Testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python discovery_server.py
  python discovery_server.py --host 0.0.0.0 --port 8090
  python discovery_server.py --host localhost --port 8091 --log-level DEBUG
        """
    )
    
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Server host address (default: 0.0.0.0)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=8090,
        help="Server port (default: 8090)"
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--stats-interval",
        type=int,
        default=30,
        help="Interval in seconds to print server stats (default: 30, 0 to disable)"
    )
    
    args = parser.parse_args()
    
    # Create and start the server
    server = PureDiscoveryServer(
        host=args.host,
        port=args.port,
        log_level=args.log_level
    )
    
    try:
        # Set up signal handlers for graceful shutdown
        setup_signal_handlers(server)
        
        # Start the server
        server.start()
        
        # Optional: Print stats periodically
        if args.stats_interval > 0:
            stats_thread = threading.Thread(
                target=print_stats_periodically,
                args=(server, args.stats_interval),
                daemon=True
            )
            stats_thread.start()
        
        # Wait for termination
        server.wait_for_termination()
        
    except KeyboardInterrupt:
        # This should not be reached due to signal handler, but kept as backup
        print("\nShutdown signal received...")
        server.stop()
    except Exception as e:
        print(f"Error: {e}")
        server.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()