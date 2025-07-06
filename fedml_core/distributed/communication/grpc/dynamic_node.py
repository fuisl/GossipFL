#!/usr/bin/env python3

"""
Dynamic Node for Manual Testing

This script creates a dynamic node using the refactored communication manager
that integrates with the pure service discovery system.

Usage:
    python refactored_dynamic_node.py [--node-id ID] [--host HOST] [--port PORT] [--discovery-host HOST] [--discovery-port PORT]

Example:
    python refactored_dynamic_node.py --node-id 1 --host localhost --port 8891 --discovery-host localhost --discovery-port 8090
"""

import sys
import os
import argparse
import logging
import signal
import time
import threading
from typing import Optional, List
import random

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))

from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager
from fedml_core.distributed.communication.message import Message
from fedml_core.distributed.communication.observer import Observer


class TestObserver(Observer):
    """Test observer for demonstration purposes."""
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.logger = logging.getLogger(f"TestObserver-{node_id}")
    
    def receive_message(self, msg_type, msg_params):
        """Handle received messages."""
        self.logger.info(f"Node {self.node_id} received message: type={msg_type}")
        
        # Echo back the message type for testing
        if hasattr(msg_params, 'get_type'):
            self.logger.info(f"Message details: {msg_params.get_type()}")
    
    def connection_ready(self):
        """Handle connection ready event."""
        self.logger.info(f"Node {self.node_id} connection is ready")


class RefactoredDynamicNode:
    """
    Dynamic node for testing the refactored communication manager.
    
    This demonstrates how to use the new pure service discovery architecture.
    """
    
    def __init__(
        self,
        node_id: int,
        host: str = "localhost",
        port: int = 8891,
        discovery_host: str = "localhost",
        discovery_port: int = 8090,
        log_level: str = "INFO"
    ):
        """
        Initialize the dynamic node.
        
        Args:
            node_id: Unique node identifier
            host: Local host address
            port: Local port
            discovery_host: Service discovery server host
            discovery_port: Service discovery server port
            log_level: Logging level
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.discovery_host = discovery_host
        self.discovery_port = discovery_port
        self.log_level = log_level
        
        # Communication manager
        self.comm_manager: Optional[DynamicGRPCCommManager] = None
        self.observer: Optional[TestObserver] = None
        
        # Node state
        self.running = False
        self.message_sender_thread = None
        
        # Configure logging - Fix duplicate logging by configuring only once
        self.logger = logging.getLogger(f"RefactoredDynamicNode-{node_id}")
        
        # Only configure if root logger has no handlers (prevents duplicate logging)
        if not logging.root.handlers:
            logging.basicConfig(
                level=getattr(logging, log_level.upper()),
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.StreamHandler(sys.stdout),
                    logging.FileHandler(f'refactored_dynamic_node_{node_id}.log')
                ]
            )
        
        self.logger = logging.getLogger(f"RefactoredDynamicNode-{node_id}")
        self.logger.info(f"Refactored dynamic node {node_id} initialized")
    
    def start(self):
        """Start the dynamic node."""
        try:
            self.logger.info(f"Starting refactored dynamic node {self.node_id}")
            
            # Define callbacks for node discovery events
            def on_node_discovered(discovered_node_id, node_info):
                self.logger.info(f"Node {self.node_id} discovered new node {discovered_node_id}: "
                               f"{node_info.ip_address}:{node_info.port}")
            
            def on_node_lost(lost_node_id):
                self.logger.info(f"Node {self.node_id} lost connection to node {lost_node_id}")
            
            # Create the refactored communication manager
            self.comm_manager = DynamicGRPCCommManager(
                host=self.host,
                port=self.port,
                node_id=self.node_id,
                client_num=10,  # Support up to 10 clients
                service_discovery_host=self.discovery_host,
                service_discovery_port=self.discovery_port,
                capabilities=['grpc', 'fedml', 'gossip'],
                metadata={
                    'type': 'test_node',
                    'version': '1.0.0',
                    'started_at': str(time.time())
                },
                use_service_discovery=True,
                on_node_discovered=on_node_discovered,
                on_node_lost=on_node_lost
            )
            
            # Create and add observer
            self.observer = TestObserver(self.node_id)
            self.comm_manager.add_observer(self.observer)
            
            # Start message handling in a separate thread
            import threading
            self.message_handler_thread = threading.Thread(
                target=self.comm_manager.handle_receive_message,
                daemon=True
            )
            self.message_handler_thread.start()
            
            self.running = True
            
            self.logger.info(f"Refactored dynamic node {self.node_id} started successfully")
            self.logger.info(f"Node type: {'Bootstrap' if self.comm_manager.is_bootstrap_node() else 'Regular'}")
            self.logger.info(f"Cluster size: {self.comm_manager.get_cluster_size()}")
            
            # Print node information
            self._print_node_info()
            
            # Start periodic message sending for testing
            self._start_message_sender()
            
        except Exception as e:
            self.logger.error(f"Failed to start refactored dynamic node {self.node_id}: {e}")
            raise
    
    def stop(self):
        """Stop the dynamic node (idempotent)."""
        # Use a class-level flag to prevent multiple cleanup attempts
        if not hasattr(self, '_stop_called'):
            self._stop_called = threading.Event()
        
        if self._stop_called.is_set():
            self.logger.debug(f"Stop already called for node {self.node_id}, skipping...")
            return
        
        # Set the flag to prevent re-entry
        self._stop_called.set()
        
        try:
            if self.running:
                self.logger.info(f"Stopping refactored dynamic node {self.node_id}")
                self.running = False
                
                # Clean up communication manager
                if self.comm_manager:
                    try:
                        self.comm_manager.cleanup()
                    except Exception as e:
                        self.logger.error(f"Error during cleanup: {e}")
                        import traceback
                        traceback.print_exc()
                    finally:
                        self.comm_manager = None
                
                self.logger.info(f"Refactored dynamic node {self.node_id} stopped")
            else:
                self.logger.debug(f"Node {self.node_id} was already stopped")
        except Exception as e:
            self.logger.error(f"Error during stop: {e}")
            import traceback
            traceback.print_exc()
    
    def _print_node_info(self):
        """Print node information."""
        print("\n" + "="*60)
        print(f"REFACTORED DYNAMIC NODE {self.node_id} STARTED")
        print("="*60)
        print(f"Node ID: {self.node_id}")
        print(f"Host: {self.host}")
        print(f"Port: {self.port}")
        print(f"Discovery Server: {self.discovery_host}:{self.discovery_port}")
        print(f"Log Level: {self.log_level}")
        
        if self.comm_manager:
            print(f"Bootstrap Node: {self.comm_manager.is_bootstrap_node()}")
            print(f"Cluster Size: {self.comm_manager.get_cluster_size()}")
            
            # Print peer nodes
            cluster_info = self.comm_manager.get_cluster_nodes_info()
            peer_nodes = [nid for nid in cluster_info.keys() if nid != self.node_id]
            if peer_nodes:
                print(f"Peer Nodes: {peer_nodes}")
            else:
                print("Peer Nodes: None (first node or isolated)")
        
        print("\nThis node uses the REFACTORED architecture:")
        print("- No polling mechanisms")
        print("- Direct gRPC calls to pure service discovery")
        print("- Event-driven registration and discovery")
        print("- Robust fault tolerance and graceful degradation")
        print("\nNode will:")
        print("- Send test messages to other nodes periodically")
        print("- Respond to messages from other nodes")
        print("- Maintain connection with service discovery")
        print("\nTo stop the node, press Ctrl+C")
        print("="*60)
        print()
    
    def _start_message_sender(self):
        """Start periodic message sending for testing."""
        def message_sender():
            message_counter = 0
            
            while self.running:
                try:
                    time.sleep(10 + random.uniform(0, 5))  # Send every 10-15 seconds
                    
                    if not self.running:
                        break
                    
                    # Get available peer nodes
                    cluster_info = self.comm_manager.get_cluster_nodes_info()
                    peer_nodes = [nid for nid in cluster_info.keys() if nid != self.node_id]
                    
                    if peer_nodes:
                        # Choose a random peer to send message to
                        target_node = random.choice(peer_nodes)
                        
                        # Create test message
                        message_counter += 1
                        test_message = Message(
                            type="test_message",
                            sender_id=self.node_id,
                            receiver_id=target_node
                        )
                        test_message.add_params("counter", message_counter)
                        test_message.add_params("timestamp", time.time())
                        test_message.add_params("data", f"Hello from refactored node {self.node_id}!")
                        
                        # Send the message
                        self.logger.info(f"Sending test message #{message_counter} to node {target_node}")
                        self.comm_manager.send_message(test_message)
                        
                    else:
                        self.logger.debug(f"No peer nodes available for messaging")
                        
                except Exception as e:
                    if self.running:
                        self.logger.error(f"Error in message sender: {e}")
                    time.sleep(5)  # Wait before retry
        
        self.message_sender_thread = threading.Thread(target=message_sender, daemon=True)
        self.message_sender_thread.start()
        self.logger.info("Periodic message sender started")
    
    def send_manual_message(self, target_node_id: int, message_content: str):
        """Send a manual message to another node."""
        if not self.comm_manager:
            self.logger.error("Communication manager not initialized")
            return
        
        try:
            # Create manual message using correct Message API
            manual_message = Message(
                type="manual_message",
                sender_id=self.node_id,
                receiver_id=target_node_id
            )
            manual_message.add_params("content", message_content)
            manual_message.add_params("timestamp", time.time())
            
            # Send the message
            self.comm_manager.send_message(manual_message)
            self.logger.info(f"Sent manual message to node {target_node_id}: {message_content}")
            
        except Exception as e:
            self.logger.error(f"Failed to send manual message: {e}")
    
    def get_status(self):
        """Get node status information."""
        if not self.comm_manager:
            return {"status": "not_started"}
        
        return {
            "node_id": self.node_id,
            "running": self.running,
            "is_bootstrap": self.comm_manager.is_bootstrap_node(),
            "cluster_size": self.comm_manager.get_cluster_size(),
            "reachable_nodes": self.comm_manager.get_reachable_nodes(),
            "peer_nodes": [nid for nid in self.comm_manager.get_cluster_nodes_info().keys() if nid != self.node_id]
        }
    
    def discover_new_nodes(self):
        """Manually trigger node discovery."""
        if self.comm_manager:
            new_nodes = self.comm_manager.discover_new_nodes()
            self.logger.info(f"Manual discovery found {len(new_nodes)} new nodes")
            return new_nodes
        return []


def setup_signal_handlers(node: RefactoredDynamicNode):
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        
        # Simple, idempotent cleanup
        try:
            node.stop()
        except Exception as e:
            print(f"Error during cleanup: {e}")
        
        # Exit cleanly
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def print_status_periodically(node: RefactoredDynamicNode, interval: int):
    """Print node status periodically."""
    while node.running:
        try:
            time.sleep(interval)
            if not node.running:
                break
                
            status = node.get_status()
            
            print(f"\n--- Refactored Node {status['node_id']} Status ---")
            print(f"Running: {status['running']}")
            print(f"Bootstrap: {status['is_bootstrap']}")
            print(f"Cluster Size: {status['cluster_size']}")
            print(f"Peer Nodes: {status['peer_nodes']}")
            print(f"Reachable Nodes: {status['reachable_nodes']}")
            print("--------------------------------------\n")
            
        except Exception as e:
            if node.running:
                print(f"Error getting status: {e}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Refactored Dynamic Node for Manual Testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python refactored_dynamic_node.py --node-id 1
  python refactored_dynamic_node.py --node-id 2 --port 8892
  python refactored_dynamic_node.py --node-id 3 --host 192.168.1.100 --port 8893 --discovery-host 192.168.1.50
        """
    )
    
    parser.add_argument(
        "--node-id",
        type=int,
        required=True,
        help="Unique node identifier"
    )
    
    parser.add_argument(
        "--host",
        default="localhost",
        help="Node host address (default: localhost)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Node port (default: 8890 + node_id)"
    )
    
    parser.add_argument(
        "--discovery-host",
        default="localhost",
        help="Service discovery server host (default: localhost)"
    )
    
    parser.add_argument(
        "--discovery-port",
        type=int,
        default=8090,
        help="Service discovery server port (default: 8090)"
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    
    parser.add_argument(
        "--status-interval",
        type=int,
        default=60,
        help="Interval in seconds to print node status (default: 60, 0 to disable)"
    )
    
    args = parser.parse_args()
    
    # Default port calculation
    if args.port is None:
        args.port = 8890 + args.node_id
    
    # Create and start the node
    node = RefactoredDynamicNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        discovery_host=args.discovery_host,
        discovery_port=args.discovery_port,
        log_level=args.log_level
    )
    
    try:
        # Set up signal handlers for graceful shutdown
        setup_signal_handlers(node)
        
        # Start the node
        node.start()
        
        # Optional: Print status periodically
        if args.status_interval > 0:
            status_thread = threading.Thread(
                target=print_status_periodically,
                args=(node, args.status_interval),
                daemon=True
            )
            status_thread.start()
        
        # Keep the main thread alive
        while node.running:
            time.sleep(1)
        
    except KeyboardInterrupt:
        print("\nShutdown signal received...")
        node.stop()
    except Exception as e:
        print(f"Unexpected error: {e}")
        node.stop()
    finally:
        # Ensure cleanup happens
        node.stop()
        print("Node shutdown complete.")


if __name__ == "__main__":
    main()
