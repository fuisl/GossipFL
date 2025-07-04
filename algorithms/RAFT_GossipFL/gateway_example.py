#!/usr/bin/env python3
"""
Example usage of the Gateway Server and Client for RAFT+GossipFL.

This script demonstrates how to use the gateway implementation for
dynamic node discovery in a distributed system.
"""

import sys
import time
import threading
import argparse
import logging
from typing import List

# Add the current directory to the path to import modules
sys.path.append('.')

from gateway_server import GatewayServer
from gateway_client import GatewayClient
from gateway_config import GatewayServerConfig, GatewayClientConfig


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def run_gateway_server(config: GatewayServerConfig):
    """Run the gateway server."""
    print(f"Starting Gateway Server on {config.host}:{config.port}")
    
    gateway = GatewayServer(host=config.host, port=config.port)
    
    try:
        gateway.start()
        print("Gateway server started successfully!")
        print("Press Ctrl+C to stop the server")
        
        # Keep the server running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping gateway server...")
        gateway.stop()
        print("Gateway server stopped")


def run_gateway_client(config: GatewayClientConfig):
    """Run a gateway client."""
    print(f"Starting Gateway Client (Node {config.node_id})")
    
    client = GatewayClient(config.gateway_host, config.gateway_port)
    
    try:
        # Discover cluster
        print("Discovering cluster...")
        is_bootstrap, cluster_info = client.discover_cluster(
            node_id=config.node_id,
            ip_address=config.ip_address,
            port=config.port,
            capabilities=config.capabilities,
            metadata=config.metadata
        )
        
        if is_bootstrap:
            print("🎉 This is the bootstrap node! (First node in cluster)")
        else:
            print(f"✅ Joined cluster with {len(cluster_info['nodes'])} existing nodes")
        
        # Display cluster information
        print("\nCluster Information:")
        print(f"  Leader: {cluster_info.get('leader', 'None')}")
        print(f"  Nodes: {len(cluster_info['nodes'])}")
        
        for node in cluster_info['nodes']:
            print(f"    - Node {node['node_id']}: {node['ip_address']}:{node['port']}")
        
        # Get additional information
        print("\nAdditional Operations:")
        
        # Get current stats
        stats = client.get_stats()
        print(f"  Gateway Stats: {stats}")
        
        # Keep client running
        print("\nClient is running. Press Ctrl+C to stop.")
        while True:
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\nStopping gateway client...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.shutdown()
        print("Gateway client stopped")


def run_multi_node_example():
    """Run a multi-node example."""
    print("Running Multi-Node Example")
    print("=" * 50)
    
    # Start gateway server
    gateway = GatewayServer(host='localhost', port=8080)
    gateway.start()
    
    # Wait for server to start
    time.sleep(1)
    
    clients = []
    
    try:
        # Create multiple clients
        for i in range(3):
            config = GatewayClientConfig(
                gateway_host='localhost',
                gateway_port=8080,
                node_id=i + 1,
                ip_address='127.0.0.1',
                port=5000 + i + 1,
                capabilities=['raft', 'gossip', 'test'],
                metadata={'role': f'node_{i+1}'}
            )
            
            client = GatewayClient(config.gateway_host, config.gateway_port)
            
            print(f"\nStarting Node {config.node_id}...")
            is_bootstrap, cluster_info = client.discover_cluster(
                node_id=config.node_id,
                ip_address=config.ip_address,
                port=config.port,
                capabilities=config.capabilities,
                metadata=config.metadata
            )
            
            if is_bootstrap:
                print(f"  Node {config.node_id}: Bootstrap node")
            else:
                print(f"  Node {config.node_id}: Joined cluster with {len(cluster_info['nodes'])} nodes")
            
            clients.append(client)
            time.sleep(1)  # Small delay between nodes
        
        # Display final cluster state
        print("\nFinal Cluster State:")
        final_stats = clients[0].get_stats()
        print(f"  Total nodes: {final_stats['total_nodes']}")
        print(f"  Active nodes: {final_stats['active_nodes']}")
        print(f"  Leader: {final_stats['leader_id']}")
        
        # Let the cluster run for a bit
        print("\nCluster running for 10 seconds...")
        time.sleep(10)
        
        print("✅ Multi-node example completed successfully!")
        
    except Exception as e:
        print(f"❌ Multi-node example failed: {e}")
    finally:
        # Cleanup
        for client in clients:
            client.shutdown()
        gateway.stop()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Gateway Example')
    parser.add_argument('mode', choices=['server', 'client', 'multi-node'], 
                       help='Mode to run')
    parser.add_argument('--host', default='localhost', help='Gateway host')
    parser.add_argument('--port', type=int, default=8080, help='Gateway port')
    parser.add_argument('--node-id', type=int, default=1, help='Node ID (for client mode)')
    parser.add_argument('--node-ip', default='127.0.0.1', help='Node IP (for client mode)')
    parser.add_argument('--node-port', type=int, default=5001, help='Node port (for client mode)')
    parser.add_argument('--log-level', default='INFO', help='Log level')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    if args.mode == 'server':
        config = GatewayServerConfig(host=args.host, port=args.port)
        run_gateway_server(config)
        
    elif args.mode == 'client':
        config = GatewayClientConfig(
            gateway_host=args.host,
            gateway_port=args.port,
            node_id=args.node_id,
            ip_address=args.node_ip,
            port=args.node_port
        )
        run_gateway_client(config)
        
    elif args.mode == 'multi-node':
        run_multi_node_example()


if __name__ == '__main__':
    main()
