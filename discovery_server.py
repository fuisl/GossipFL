#!/usr/bin/env python3

"""
Discovery Server for Manual Testing

This script starts a standalone service discovery server that can be used
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
from typing import Optional

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from algorithms.RAFT_GossipFL.grpc_gateway_server import GRPCGatewayServer


class DiscoveryServer:
    """
    Standalone discovery server for testing.
    
    This wraps the pure service discovery implementation and provides
    a simple interface for manual testing.
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8090, log_level: str = "INFO"):
        """
        Initialize the discovery server.
        
        Args:
            host: Server host address
            port: Server port
            log_level: Logging level
        """
        self.host = host
        self.port = port
        self.log_level = log_level
        self.server: Optional[GRPCGatewayServer] = None
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('discovery_server.log')
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Discovery server initialized - {host}:{port}")
    
    def start(self):
        """Start the discovery server."""
        try:
            self.logger.info(f"Starting discovery server on {self.host}:{self.port}")
            
            # Create and start the gateway server
            self.server = GRPCGatewayServer(
                host=self.host,
                port=self.port,
                max_workers=10
            )
            
            self.server.start()
            
            self.logger.info(f"Discovery server started successfully on {self.host}:{self.port}")
            self.logger.info("Server is ready to accept connections")
            
            # Print some helpful information
            self._print_server_info()
            
        except Exception as e:
            self.logger.error(f"Failed to start discovery server: {e}")
            raise
    
    def stop(self):
        """Stop the discovery server."""
        if self.server:
            self.logger.info("Stopping discovery server...")
            self.server.stop()
            self.server = None
            self.logger.info("Discovery server stopped")
    
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
        print("\n" + "="*60)
        print("DISCOVERY SERVER STARTED")
        print("="*60)
        print(f"Host: {self.host}")
        print(f"Port: {self.port}")
        print(f"Log Level: {self.log_level}")
        print(f"Server Address: {self.host}:{self.port}")
        print("\nThe server is now ready to accept node registrations.")
        print("You can now start dynamic nodes to test the service discovery.")
        print("\nTo stop the server, press Ctrl+C")
        print("="*60)
        print()
    
    def get_stats(self):
        """Get server statistics."""
        if self.server:
            return self.server.get_state().get_stats()
        return {}


def setup_signal_handlers(server: DiscoveryServer):
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}")
        server.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Discovery Server for Manual Testing",
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
    server = DiscoveryServer(
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
            import threading
            
            def print_stats():
                while True:
                    try:
                        time.sleep(args.stats_interval)
                        stats = server.get_stats()
                        if stats:
                            print(f"\n--- Server Stats ---")
                            print(f"Total Nodes: {stats.get('total_nodes', 0)}")
                            print(f"Active Nodes: {stats.get('active_nodes', 0)}")
                            print(f"Leader ID: {stats.get('leader_id', 'None')}")
                            print(f"Registry Version: {stats.get('registry_version', 0)}")
                            print(f"Bootstrap Initiated: {stats.get('bootstrap_initiated', False)}")
                            print(f"Uptime: {stats.get('uptime', 0):.1f} seconds")
                            print("-------------------\n")
                    except Exception as e:
                        print(f"Error getting stats: {e}")
            
            stats_thread = threading.Thread(target=print_stats, daemon=True)
            stats_thread.start()
        
        # Wait for termination
        server.wait_for_termination()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.stop()
    except Exception as e:
        print(f"Error: {e}")
        server.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
