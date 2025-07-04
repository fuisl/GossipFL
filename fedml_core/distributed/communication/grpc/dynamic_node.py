#!/usr/bin/env python3
"""
Dynamic Communication Node

This script creates a node that can join/leave the network dynamically.
Each node can send messages to other nodes and receive messages.

Usage:
    python dynamic_node.py [--node-id ID] [--port PORT] [--gateway-host HOST] [--gateway-port PORT]
    
Commands (once running):
    - 'list' or 'l': List all nodes in the network
    - 'send <node_id> <message>': Send a message to a specific node
    - 'broadcast <message>': Send a message to all nodes
    - 'leader': Show current leader
    - 'status': Show node status
    - 'quit' or 'q': Exit the node
    - 'help' or 'h': Show help
"""

import argparse
import logging
import signal
import sys
import time
import threading
import random
import os
from typing import Dict, List

# Add paths
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL')
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL/algorithms/RAFT_GossipFL')

# Direct imports to avoid __init__.py issues
from fedml_core.distributed.communication.grpc.dynamic_grpc_comm_manager import DynamicGRPCCommManager
from fedml_core.distributed.communication.message import Message
from fedml_core.distributed.communication.observer import Observer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NodeObserver(Observer):
    """Observer for handling received messages."""
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.received_messages = []
        self.connection_ready = False
    
    def receive_message(self, msg_type: str, msg: Message):
        """Handle received messages."""
        if msg_type == "CONNECTION_IS_READY":
            self.connection_ready = True
            print(f"🔗 Node {self.node_id} connection is ready")
        else:
            self.received_messages.append((msg_type, msg))
            sender = msg.sender_id
            content = msg.get_content()
            print(f"📨 [Node {self.node_id}] Received from Node {sender}: {content}")

class DynamicNode:
    """A dynamic node that can join/leave the network."""
    
    def __init__(self, node_id: int, port: int, gateway_host: str, gateway_port: int):
        self.node_id = node_id
        self.port = port
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        
        # Communication manager
        self.comm_manager = None
        self.observer = None
        self.message_handler_thread = None
        self.running = False
        self.interactive_mode = True
        
        # Node info
        self.capabilities = ['grpc', 'fedml', 'dynamic_test']
        self.metadata = {
            'role': 'test_node',
            'started_at': str(time.time()),
            'version': '1.0.0'
        }
    
    def start(self):
        """Start the node and join the network."""
        logger.info("="*60)
        logger.info(f"🚀 Starting Node {self.node_id}")
        logger.info("="*60)
        
        try:
            # Create communication manager
            self.comm_manager = DynamicGRPCCommManager(
                host='127.0.0.1',
                port=self.port,
                node_id=self.node_id,
                client_num=10,  # Support up to 10 nodes
                gateway_host=self.gateway_host,
                gateway_port=self.gateway_port,
                capabilities=self.capabilities,
                metadata=self.metadata
            )
            
            # Create observer
            self.observer = NodeObserver(self.node_id)
            self.comm_manager.add_observer(self.observer)
            
            # Start message handling
            self.message_handler_thread = threading.Thread(
                target=self.comm_manager.handle_receive_message,
                daemon=True
            )
            self.message_handler_thread.start()
            
            # Wait for initialization
            time.sleep(2)
            
            self.running = True
            
            # Show initial status
            self.show_status()
            
            logger.info("✅ Node started successfully")
            logger.info("💡 Type 'help' for available commands")
            
            if self.interactive_mode:
                self.run_interactive()
            else:
                self.run_background()
                
        except Exception as e:
            logger.error(f"❌ Failed to start node: {e}")
            self.stop()
    
    def stop(self):
        """Stop the node and leave the network."""
        logger.info(f"🔄 Stopping Node {self.node_id}...")
        self.running = False
        
        if self.comm_manager:
            self.comm_manager.cleanup()
            self.comm_manager.stop_receive_message()
        
        logger.info("✅ Node stopped")
        logger.info("👋 Goodbye!")
    
    def show_status(self):
        """Show current node status."""
        if not self.comm_manager:
            print("❌ Node not initialized")
            return
            
        is_bootstrap = self.comm_manager.is_bootstrap_node_local()
        leader_id = self.comm_manager.get_cluster_leader()
        nodes = self.comm_manager.get_cluster_nodes_info()
        
        print("\n" + "="*40)
        print(f"📊 Node {self.node_id} Status")
        print("="*40)
        print(f"🏠 Address: 127.0.0.1:{self.port}")
        print(f"🎯 Gateway: {self.gateway_host}:{self.gateway_port}")
        print(f"👑 Bootstrap: {'Yes' if is_bootstrap else 'No'}")
        print(f"🎪 Leader: Node {leader_id}" if leader_id else "🎪 Leader: None")
        print(f"🌐 Connected nodes: {len(nodes)}")
        print(f"📋 Node IDs: {sorted(nodes.keys())}")
        print("="*40)
    
    def list_nodes(self):
        """List all nodes in the network."""
        if not self.comm_manager:
            print("❌ Node not initialized")
            return
            
        nodes = self.comm_manager.get_cluster_nodes_info()
        
        print("\n" + "="*50)
        print("🌐 Network Nodes")
        print("="*50)
        
        if not nodes:
            print("No nodes found")
            return
        
        for node_id, node_info in sorted(nodes.items()):
            status = "🟢" if node_info.is_reachable else "🔴"
            bootstrap = "👑" if node_id == self.node_id and self.comm_manager.is_bootstrap_node_local() else "  "
            print(f"{status} {bootstrap} Node {node_id:2d} - {node_info.ip_address}:{node_info.port}")
        
        print("="*50)
    
    def send_message(self, target_node_id: int, message_content: str):
        """Send a message to a specific node."""
        if not self.comm_manager:
            print("❌ Node not initialized")
            return
            
        try:
            # Create message
            msg = Message()
            msg.sender_id = self.node_id
            msg.receiver_id = target_node_id
            msg.set_type("USER_MESSAGE")
            msg.set_content({
                "text": message_content,
                "timestamp": time.time(),
                "sender_name": f"Node-{self.node_id}"
            })
            
            # Send message
            self.comm_manager.send_message(msg)
            print(f"📤 Sent to Node {target_node_id}: {message_content}")
            
        except Exception as e:
            print(f"❌ Failed to send message: {e}")
    
    def broadcast_message(self, message_content: str):
        """Broadcast a message to all nodes."""
        if not self.comm_manager:
            print("❌ Node not initialized")
            return
            
        nodes = self.comm_manager.get_cluster_nodes_info()
        sent_count = 0
        
        for node_id in nodes.keys():
            if node_id != self.node_id:  # Don't send to self
                try:
                    self.send_message(node_id, message_content)
                    sent_count += 1
                except Exception as e:
                    print(f"❌ Failed to send to Node {node_id}: {e}")
        
        print(f"📡 Broadcast sent to {sent_count} nodes")
    
    def run_interactive(self):
        """Run in interactive mode."""
        print("\n💬 Interactive mode - Type commands:")
        
        while self.running:
            try:
                command = input(f"Node-{self.node_id}> ").strip()
                
                if not command:
                    continue
                
                parts = command.split()
                cmd = parts[0].lower()
                
                if cmd in ['quit', 'q', 'exit']:
                    break
                elif cmd in ['help', 'h']:
                    self.show_help()
                elif cmd in ['list', 'l']:
                    self.list_nodes()
                elif cmd in ['status', 's']:
                    self.show_status()
                elif cmd == 'leader':
                    leader_id = self.comm_manager.get_cluster_leader()
                    print(f"👑 Current leader: Node {leader_id}" if leader_id else "👑 No leader")
                elif cmd == 'send' and len(parts) >= 3:
                    try:
                        target_id = int(parts[1])
                        message = ' '.join(parts[2:])
                        self.send_message(target_id, message)
                    except ValueError:
                        print("❌ Invalid node ID")
                elif cmd == 'broadcast' and len(parts) >= 2:
                    message = ' '.join(parts[1:])
                    self.broadcast_message(message)
                elif cmd == 'refresh':
                    if self.comm_manager:
                        self.comm_manager.refresh_node_registry()
                        print("🔄 Node registry refreshed")
                else:
                    print(f"❌ Unknown command: {command}")
                    print("💡 Type 'help' for available commands")
                    
            except KeyboardInterrupt:
                print("\n🛑 Shutdown requested")
                break
            except EOFError:
                print("\n🛑 EOF received")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def run_background(self):
        """Run in background mode."""
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("\n🛑 Shutdown requested")
    
    def show_help(self):
        """Show help message."""
        print("\n" + "="*50)
        print("💡 Available Commands")
        print("="*50)
        print("  help, h           - Show this help")
        print("  list, l           - List all nodes")
        print("  status, s         - Show node status")
        print("  leader            - Show current leader")
        print("  send <id> <msg>   - Send message to node")
        print("  broadcast <msg>   - Broadcast to all nodes")
        print("  refresh           - Refresh node registry")
        print("  quit, q           - Exit node")
        print("="*50)
        print("Examples:")
        print("  send 2 Hello Node 2!")
        print("  broadcast Hello everyone!")
        print("="*50)

def setup_signal_handlers(node):
    """Setup signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info(f"\n🔔 Received signal {signum}")
        node.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Dynamic Communication Node')
    parser.add_argument('--node-id', type=int, help='Node ID (auto-generated if not provided)')
    parser.add_argument('--port', type=int, help='Port to bind to (auto-generated if not provided)')
    parser.add_argument('--gateway-host', default='localhost', help='Gateway host (default: localhost)')
    parser.add_argument('--gateway-port', type=int, default=8090, help='Gateway port (default: 8090)')
    parser.add_argument('--background', action='store_true', help='Run in background mode (no interactive)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Generate node ID if not provided
    if args.node_id is None:
        args.node_id = random.randint(1, 9999)
        print(f"🎲 Generated Node ID: {args.node_id}")
    
    # Generate port if not provided
    if args.port is None:
        args.port = 8900 + (args.node_id % 100)
        print(f"🎲 Generated Port: {args.port}")
    
    # Create and start node
    node = DynamicNode(
        node_id=args.node_id,
        port=args.port,
        gateway_host=args.gateway_host,
        gateway_port=args.gateway_port
    )
    
    if args.background:
        node.interactive_mode = False
    
    setup_signal_handlers(node)
    
    try:
        node.start()
    except Exception as e:
        logger.error(f"❌ Failed to start node: {e}")
        sys.exit(1)
    finally:
        node.stop()

if __name__ == '__main__':
    main()
