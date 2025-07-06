#!/usr/bin/env python3
"""
RAFT Cluster Test Suite

This script helps you test the complete RAFT consensus system with service discovery.
It can start a service discovery server and multiple RAFT nodes for testing.

Usage:
    # Start service discovery server
    python test_raft_cluster.py --mode server

    # Start a RAFT node
    python test_raft_cluster.py --mode node --node-id 1

    # Start multiple nodes at once
    python test_raft_cluster.py --mode cluster --num-nodes 3

    # Run complete end-to-end test
    python test_raft_cluster.py --mode test --num-nodes 3
"""

import sys
import os
import argparse
import logging
import signal
import time
import threading
import subprocess
import json
from typing import List, Dict, Optional

# Add the project root to the path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from algorithms.RAFT_GossipFL.pure_service_discovery import PureServiceDiscoveryServer
from algorithms.RAFT_GossipFL.raft_node_with_bridge import RaftNodeWithBridge


class PureDiscoveryServer:
    """Wrapper for the pure service discovery server for easy testing."""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8090, log_level: str = "INFO"):
        self.host = host
        self.port = port
        self.server = None
        
    def start(self):
        """Start the pure service discovery server."""
        self.server = PureServiceDiscoveryServer(
            host=self.host,
            port=self.port,
            node_timeout=300.0,  # 5 minutes
            max_workers=10
        )
        self.server.start()
        return True
        
    def stop(self):
        """Stop the pure service discovery server."""
        if self.server:
            self.server.stop()
            self.server = None


class RaftClusterTester:
    """Test suite for RAFT cluster with service discovery."""
    
    def __init__(self, discovery_host: str = "localhost", discovery_port: int = 8090):
        self.discovery_host = discovery_host
        self.discovery_port = discovery_port
        self.discovery_server: Optional[PureDiscoveryServer] = None
        self.raft_nodes: List[RaftNodeWithBridge] = []
        self.node_processes: List[subprocess.Popen] = []
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('RaftClusterTester')
    
    def start_discovery_server(self):
        """Start the service discovery server."""
        self.logger.info(f"Starting service discovery server on {self.discovery_host}:{self.discovery_port}")
        
        try:
            self.discovery_server = PureDiscoveryServer(
                host=self.discovery_host,
                port=self.discovery_port,
                log_level="INFO"
            )
            self.discovery_server.start()
            
            # Wait for server to be ready
            time.sleep(3)
            
            self.logger.info("Service discovery server started successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start service discovery server: {e}")
            return False
    
    def stop_discovery_server(self):
        """Stop the service discovery server."""
        if self.discovery_server:
            self.logger.info("Stopping service discovery server...")
            self.discovery_server.stop()
            self.discovery_server = None
    
    def start_raft_node(self, node_id: int, host: str = "localhost", port: int = None) -> RaftNodeWithBridge:
        """Start a single RAFT node."""
        self.logger.info(f"Starting RAFT node {node_id}...")
        
        try:
            node = RaftNodeWithBridge(
                node_id=node_id,
                host=host,
                port=port or (8890 + node_id),
                discovery_host=self.discovery_host,
                discovery_port=self.discovery_port,
                log_level="INFO"
            )
            
            node.start()
            self.raft_nodes.append(node)
            
            self.logger.info(f"RAFT node {node_id} started successfully")
            return node
            
        except Exception as e:
            self.logger.error(f"Failed to start RAFT node {node_id}: {e}")
            raise
    
    def start_raft_node_process(self, node_id: int, host: str = "localhost", port: int = None):
        """Start a RAFT node in a separate process."""
        self.logger.info(f"Starting RAFT node {node_id} in separate process...")
        
        script_path = os.path.join(os.path.dirname(__file__), 'raft_node_with_bridge.py')
        port = port or (8890 + node_id)
        
        cmd = [
            sys.executable, script_path,
            '--node-id', str(node_id),
            '--host', host,
            '--port', str(port),
            '--discovery-host', self.discovery_host,
            '--discovery-port', str(self.discovery_port),
            '--log-level', 'INFO'
        ]
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.node_processes.append(process)
            
            self.logger.info(f"RAFT node {node_id} process started (PID: {process.pid})")
            return process
            
        except Exception as e:
            self.logger.error(f"Failed to start RAFT node {node_id} process: {e}")
            raise
    
    def start_cluster(self, num_nodes: int, use_processes: bool = True):
        """Start a cluster of RAFT nodes."""
        self.logger.info(f"Starting RAFT cluster with {num_nodes} nodes...")
        
        for i in range(num_nodes):
            node_id = i + 1
            
            if use_processes:
                self.start_raft_node_process(node_id)
            else:
                self.start_raft_node(node_id)
            
            # Small delay between node starts
            time.sleep(2)
        
        self.logger.info(f"Started {num_nodes} RAFT nodes")
    
    def stop_all_nodes(self):
        """Stop all RAFT nodes."""
        self.logger.info("Stopping all RAFT nodes...")
        
        # Stop in-process nodes
        for node in self.raft_nodes:
            try:
                node.stop()
            except Exception as e:
                self.logger.error(f"Error stopping node {node.node_id}: {e}")
        
        self.raft_nodes.clear()
        
        # Stop process nodes
        for process in self.node_processes:
            try:
                process.terminate()
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            except Exception as e:
                self.logger.error(f"Error stopping node process {process.pid}: {e}")
        
        self.node_processes.clear()
    
    def get_cluster_status(self) -> Dict:
        """Get status of all nodes in the cluster."""
        status = {
            'discovery_server': {
                'running': self.discovery_server is not None,
                'host': self.discovery_host,
                'port': self.discovery_port
            },
            'nodes': []
        }
        
        for node in self.raft_nodes:
            try:
                node_status = node.get_status()
                status['nodes'].append(node_status)
            except Exception as e:
                status['nodes'].append({
                    'node_id': node.node_id,
                    'error': str(e)
                })
        
        return status
    
    def print_cluster_status(self):
        """Print cluster status in a readable format."""
        status = self.get_cluster_status()
        
        print("\n" + "="*60)
        print("RAFT CLUSTER STATUS")
        print("="*60)
        
        # Discovery server status
        ds = status['discovery_server']
        print(f"Discovery Server: {'Running' if ds['running'] else 'Stopped'} ({ds['host']}:{ds['port']})")
        
        # Node status
        print(f"\nNodes ({len(status['nodes'])}):")
        for node_status in status['nodes']:
            if 'error' in node_status:
                print(f"  Node {node_status['node_id']}: ERROR - {node_status['error']}")
            else:
                print(f"  Node {node_status['node_id']}: {node_status.get('raft_state', 'UNKNOWN')} "
                     f"(Term: {node_status.get('raft_term', '?')}, "
                     f"Leader: {node_status.get('leader_id', 'None')})")
        
        print("="*60 + "\n")
    
    def run_election_test(self):
        """Run an election test on the cluster."""
        self.logger.info("Running election test...")
        
        if not self.raft_nodes:
            self.logger.warning("No nodes available for election test")
            return
        
        # Trigger election on first node
        first_node = self.raft_nodes[0]
        self.logger.info(f"Triggering election on node {first_node.node_id}")
        first_node.trigger_election()
        
        # Wait for election to complete
        time.sleep(5)
        
        # Check results
        self.print_cluster_status()
    
    def run_log_replication_test(self):
        """Run a log replication test."""
        self.logger.info("Running log replication test...")
        
        # Find the leader
        leader = None
        for node in self.raft_nodes:
            if node.raft_node.state.name == 'LEADER':
                leader = node
                break
        
        if not leader:
            self.logger.warning("No leader found for log replication test")
            return
        
        # Add some log entries
        for i in range(3):
            entry = {
                'type': 'test_entry',
                'data': f"Test data {i}",
                'timestamp': time.time()
            }
            
            log_index = leader.add_log_entry(entry)
            self.logger.info(f"Added log entry {i} at index {log_index}")
            time.sleep(1)
        
        # Wait for replication
        time.sleep(3)
        
        # Check log consistency
        self.print_cluster_status()
    
    def run_comprehensive_test(self, num_nodes: int = 3):
        """Run a comprehensive test of the RAFT cluster."""
        self.logger.info(f"Starting comprehensive RAFT test with {num_nodes} nodes...")
        
        try:
            # Step 1: Start discovery server
            if not self.start_discovery_server():
                return False
            
            # Step 2: Start cluster
            self.start_cluster(num_nodes, use_processes=False)
            
            # Step 3: Wait for cluster to stabilize
            self.logger.info("Waiting for cluster to stabilize...")
            time.sleep(10)
            
            # Step 4: Print initial status
            self.logger.info("Initial cluster status:")
            self.print_cluster_status()
            
            # Step 5: Run election test
            self.run_election_test()
            
            # Step 6: Run log replication test
            self.run_log_replication_test()
            
            # Step 7: Final status
            self.logger.info("Final cluster status:")
            self.print_cluster_status()
            
            self.logger.info("Comprehensive test completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"Comprehensive test failed: {e}")
            return False
        
        finally:
            # Cleanup
            self.stop_all_nodes()
            self.stop_discovery_server()
    
    def cleanup(self):
        """Cleanup all resources."""
        self.stop_all_nodes()
        self.stop_discovery_server()


def main():
    """Main function for the test suite."""
    parser = argparse.ArgumentParser(description='RAFT Cluster Test Suite')
    parser.add_argument('--mode', choices=['server', 'node', 'cluster', 'test'], required=True,
                       help='Operation mode')
    parser.add_argument('--node-id', type=int, help='Node ID (for node mode)')
    parser.add_argument('--num-nodes', type=int, default=3, help='Number of nodes (for cluster/test mode)')
    parser.add_argument('--discovery-host', default='localhost', help='Discovery server host')
    parser.add_argument('--discovery-port', type=int, default=8090, help='Discovery server port')
    parser.add_argument('--host', default='localhost', help='Node host (for node mode)')
    parser.add_argument('--port', type=int, help='Node port (for node mode)')
    
    args = parser.parse_args()
    
    tester = RaftClusterTester(args.discovery_host, args.discovery_port)
    
    # Signal handler for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        tester.cleanup()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        if args.mode == 'server':
            # Start only the discovery server
            print("Starting service discovery server...")
            if tester.start_discovery_server():
                print(f"Service discovery server running on {args.discovery_host}:{args.discovery_port}")
                print("Press Ctrl+C to stop")
                
                # Keep server running
                while True:
                    time.sleep(1)
        
        elif args.mode == 'node':
            # Start a single node
            if not args.node_id:
                print("Error: --node-id required for node mode")
                sys.exit(1)
            
            print(f"Starting RAFT node {args.node_id}...")
            node = tester.start_raft_node(args.node_id, args.host, args.port)
            
            print(f"RAFT node {args.node_id} is running!")
            print("Press Ctrl+C to stop")
            
            # Keep node running
            while True:
                time.sleep(1)
        
        elif args.mode == 'cluster':
            # Start discovery server and multiple nodes
            print(f"Starting discovery server and {args.num_nodes} RAFT nodes...")
            
            if not tester.start_discovery_server():
                sys.exit(1)
            
            tester.start_cluster(args.num_nodes, use_processes=True)
            
            print(f"RAFT cluster with {args.num_nodes} nodes is running!")
            print("Commands:")
            print("  status - Show cluster status")
            print("  quit - Stop the cluster")
            
            # Interactive mode
            while True:
                try:
                    cmd = input("Cluster> ").strip().lower()
                    
                    if cmd == 'status':
                        tester.print_cluster_status()
                    
                    elif cmd in ['quit', 'exit']:
                        break
                    
                    elif cmd == 'help':
                        print("Commands: status, quit")
                    
                    elif cmd:
                        print(f"Unknown command: {cmd}")
                        
                except EOFError:
                    break
        
        elif args.mode == 'test':
            # Run comprehensive test
            success = tester.run_comprehensive_test(args.num_nodes)
            sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        pass
    
    finally:
        tester.cleanup()


if __name__ == '__main__':
    main()
