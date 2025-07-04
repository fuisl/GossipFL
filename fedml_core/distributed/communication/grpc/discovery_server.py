#!/usr/bin/env python3
"""
Discovery Server for Dynamic Communication Testing

This script starts a gateway server that manages node discovery and membership.
Run this first before starting any nodes.

Usage:
    python discovery_server.py [--port PORT] [--host HOST]
"""

import argparse
import logging
import signal
import sys
import time
import os

# Add paths
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL')
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL/algorithms/RAFT_GossipFL')

# Direct import to avoid __init__.py issues
from algorithms.RAFT_GossipFL.grpc_gateway_server import GRPCGatewayServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DiscoveryServer:
    """Discovery server that manages the gateway."""
    
    def __init__(self, host='localhost', port=8090):
        self.host = host
        self.port = port
        self.gateway = None
        self.running = False
        
    def start(self):
        """Start the discovery server."""
        logger.info("="*60)
        logger.info("🚀 Starting Discovery Server")
        logger.info("="*60)
        
        try:
            # Create and start gateway
            self.gateway = GRPCGatewayServer(host=self.host, port=self.port)
            self.gateway.start()
            
            self.running = True
            logger.info(f"✅ Discovery server started on {self.host}:{self.port}")
            logger.info("📡 Waiting for nodes to connect...")
            logger.info("💡 Press Ctrl+C to stop the server")
            
            # Keep server running
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 Shutdown requested by user")
            self.stop()
        except Exception as e:
            logger.error(f"❌ Server error: {e}")
            self.stop()
    
    def stop(self):
        """Stop the discovery server."""
        logger.info("🔄 Stopping discovery server...")
        self.running = False
        
        if self.gateway:
            self.gateway.stop()
            logger.info("✅ Discovery server stopped")
        
        logger.info("👋 Goodbye!")
    
    def print_status(self):
        """Print current server status."""
        if self.gateway:
            # You could add methods to get current node count, etc.
            logger.info(f"📊 Server Status: Running on {self.host}:{self.port}")
        else:
            logger.info("📊 Server Status: Not running")

def setup_signal_handlers(server):
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info(f"\n🔔 Received signal {signum}")
        server.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Discovery Server for Dynamic Communication')
    parser.add_argument('--host', default='localhost', help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=8090, help='Port to bind to (default: 8090)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create and start server
    server = DiscoveryServer(host=args.host, port=args.port)
    setup_signal_handlers(server)
    
    try:
        server.start()
    except Exception as e:
        logger.error(f"❌ Failed to start server: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
