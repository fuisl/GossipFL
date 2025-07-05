#!/usr/bin/env python3
"""
RAFT Node with Service Discovery Integration

This creates a complete RAFT node that integrates with the existing 
FedML service discovery infrastructure through the bridge pattern.

Usage:
    python raft_node_with_bridge.py --node-id 1 --discovery-host localhost --discovery-port 8090
"""

import sys
import os
import argparse
import logging
import signal
import time
import threading
from typing import Optional, Dict, List, Set
from unittest.mock import Mock

# Add the project root to the path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)

from algorithms.RAFT_GossipFL.raft_node import RaftNode, RaftState
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
from algorithms.RAFT_GossipFL.service_discovery_bridge import RaftServiceDiscoveryBridge
from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager
from fedml_core.distributed.communication.observer import Observer
from fedml_core.distributed.communication.message import Message


class RaftMessageObserver(Observer):
    """Observer for handling RAFT-specific messages."""
    
    def __init__(self, raft_node: 'RaftNodeWithBridge'):
        self.raft_node = raft_node
        self.logger = logging.getLogger(f"RaftMessageObserver-{raft_node.node_id}")
    
    def receive_message(self, msg_type, msg_params):
        """Handle received messages."""
        try:
            # Route RAFT messages to the consensus manager
            if msg_type.startswith('raft_'):
                self.raft_node.handle_raft_message(msg_type, msg_params)
            else:
                self.logger.debug(f"Received non-RAFT message: {msg_type}")
                
        except Exception as e:
            self.logger.error(f"Error handling message {msg_type}: {e}")
    
    def connection_ready(self):
        """Handle connection ready event."""
        self.logger.info(f"Node {self.raft_node.node_id} gRPC connection is ready")


class RaftWorkerManager:
    """Mock worker manager that provides RAFT message sending capabilities."""
    
    def __init__(self, comm_manager: DynamicGRPCCommManager, node_id: int):
        self.comm_manager = comm_manager
        self.node_id = node_id
        self.round_idx = 0
        self.logger = logging.getLogger(f"RaftWorkerManager-{node_id}")
    
    def send_message_to_node(self, target_node_id: int, message_data: Dict):
        """Send a message to a specific node."""
        try:
            # Create FedML message
            message = Message(
                msg_type=message_data.get('type', 'raft_message'),
                sender=self.node_id,
                receiver=target_node_id
            )
            message.add_params("data", message_data)
            
            # Send via communication manager
            self.comm_manager.send_message(message)
            self.logger.debug(f"Sent message to node {target_node_id}: {message_data.get('type')}")
            
        except Exception as e:
            self.logger.error(f"Failed to send message to node {target_node_id}: {e}")
    
    def broadcast_message(self, message_data: Dict):
        """Broadcast a message to all known nodes."""
        try:
            # Get all nodes from communication manager
            cluster_nodes = self.comm_manager.get_cluster_nodes_info()
            
            for target_node_id in cluster_nodes.keys():
                if target_node_id != self.node_id:
                    self.send_message_to_node(target_node_id, message_data)
                    
            self.logger.debug(f"Broadcasted message to {len(cluster_nodes)-1} nodes: {message_data.get('type')}")
            
        except Exception as e:
            self.logger.error(f"Failed to broadcast message: {e}")
    
    # RAFT-specific message sending methods
    def send_vote_request(self, target_node_id: int, term: int, candidate_id: int, 
                         last_log_index: int, last_log_term: int):
        """Send a vote request."""
        message_data = {
            'type': 'raft_vote_request',
            'term': term,
            'candidate_id': candidate_id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        }
        self.send_message_to_node(target_node_id, message_data)
    
    def send_vote_response(self, target_node_id: int, term: int, vote_granted: bool):
        """Send a vote response."""
        message_data = {
            'type': 'raft_vote_response',
            'term': term,
            'vote_granted': vote_granted,
            'voter_id': self.node_id
        }
        self.send_message_to_node(target_node_id, message_data)
    
    def send_append_entries(self, target_node_id: int, term: int, prev_log_index: int,
                           prev_log_term: int, entries: List, leader_commit: int):
        """Send append entries (heartbeat or log replication)."""
        message_data = {
            'type': 'raft_append_entries',
            'term': term,
            'leader_id': self.node_id,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries,
            'leader_commit': leader_commit
        }
        self.send_message_to_node(target_node_id, message_data)
    
    def send_append_response(self, target_node_id: int, term: int, success: bool, match_index: int):
        """Send append entries response."""
        message_data = {
            'type': 'raft_append_response',
            'term': term,
            'success': success,
            'match_index': match_index,
            'follower_id': self.node_id
        }
        self.send_message_to_node(target_node_id, message_data)
    
    def on_membership_change(self, new_nodes: Set[int], round_num: int):
        # Tell the communication manager “here’s your new cluster membership”
        # so it can update its node_registry and IP map.
        cluster_info = {'nodes': [
            {'node_id': nid,
             'ip_address': self.comm_manager._get_node_ip(nid),
             'port': self.comm_manager.get_node_info(nid).port}
            for nid in new_nodes
        ]}
        self.comm_manager._update_node_registry_from_discovery(cluster_info)


class RaftNodeWithBridge:
    """
    Complete RAFT node with service discovery bridge integration.
    
    This node integrates:
    - RAFT consensus algorithm
    - Service discovery through FedML's infrastructure
    - Bridge pattern for dynamic membership management
    - gRPC communication
    """
    
    def __init__(
        self,
        node_id: int,
        host: str = "localhost",
        port: int = None,
        discovery_host: str = "localhost",
        discovery_port: int = 8090,
        capabilities: List[str] = None,
        metadata: Dict = None,
        log_level: str = "INFO"
    ):
        """
        Initialize the RAFT node with bridge integration.
        
        Args:
            node_id: Unique node identifier
            host: Local host address
            port: Local gRPC port (default: 8890 + node_id)
            discovery_host: Service discovery server host
            discovery_port: Service discovery server port
            capabilities: Node capabilities
            metadata: Additional node metadata
            log_level: Logging level
        """
        self.node_id = node_id
        self.host = host
        self.port = port or (8890 + node_id)
        self.discovery_host = discovery_host
        self.discovery_port = discovery_port
        self.capabilities = capabilities or ['grpc', 'fedml', 'raft']
        self.metadata = metadata or {'type': 'raft_node', 'version': '1.0'}
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f"RaftNodeWithBridge-{node_id}")
        
        # Core components
        self.comm_manager: Optional[DynamicGRPCCommManager] = None
        self.worker_manager: Optional[RaftWorkerManager] = None
        self.raft_node: Optional[RaftNode] = None
        self.raft_consensus: Optional[RaftConsensus] = None
        self.service_discovery_bridge: Optional[RaftServiceDiscoveryBridge] = None
        self.message_observer: Optional[RaftMessageObserver] = None
        
        # State management
        self.running = False
        self.startup_complete = False
        
        self.logger.info(f"RAFT node initialized: ID={node_id}, Host={host}:{self.port}")
    
    def start(self):
        """Start the RAFT node with full integration."""
        try:
            self.logger.info(f"Starting RAFT node {self.node_id}...")
            
            # Step 1: Initialize communication manager
            self._initialize_communication()
            
            # Step 2: Initialize RAFT components
            self._initialize_raft_components()
            
            # Step 3: Set up service discovery bridge
            self._initialize_service_discovery_bridge()
            
            # Step 4: Start RAFT consensus
            self._start_raft_consensus()
            
            # Step 5: Final setup
            self._complete_startup()
            
            self.running = True
            self.startup_complete = True
            
            self.logger.info(f"RAFT node {self.node_id} started successfully!")
            self._print_status()
            
        except Exception as e:
            self.logger.error(f"Failed to start RAFT node {self.node_id}: {e}")
            self.cleanup()
            raise
    
    def stop(self):
        """Stop the RAFT node and cleanup resources."""
        self.logger.info(f"Stopping RAFT node {self.node_id}...")
        
        self.running = False
        self.cleanup()
        
        self.logger.info(f"RAFT node {self.node_id} stopped")
    
    def cleanup(self):
        """Cleanup all resources."""
        try:
            # Stop RAFT consensus
            if self.raft_consensus:
                self.raft_consensus.stop()
            
            # Cleanup communication manager
            if self.comm_manager:
                self.comm_manager.cleanup()
            
            self.logger.info(f"Cleanup completed for node {self.node_id}")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
    
    def _initialize_communication(self):
        """Initialize the gRPC communication manager with service discovery."""
        self.logger.info("Initializing communication manager...")
        
        # Create communication manager with service discovery
        self.comm_manager = DynamicGRPCCommManager(
            host=self.host,
            port=self.port,
            node_id=self.node_id,
            client_num=0,  # Dynamic mode
            service_discovery_host=self.discovery_host,
            service_discovery_port=self.discovery_port,
            capabilities=self.capabilities,
            metadata=self.metadata,
            use_service_discovery=True,
            node_cache_timeout=300.0,  # 5 minutes
        )
        
        # Create message observer
        self.message_observer = RaftMessageObserver(self)
        self.comm_manager.add_observer(self.message_observer)
        
        # Communication manager is already started during initialization
        self.logger.info("Communication manager initialized and ready")
    
    def _initialize_raft_components(self):
        """Initialize RAFT node and related components."""
        self.logger.info("Initializing RAFT components...")
        
        # Create mock args for RAFT
        args = Mock()
        args.client_id = self.node_id
        args.client_num_in_total = None  # Dynamic
        args.comm_round = 0
        args.min_election_timeout = 150
        args.max_election_timeout = 300
        args.heartbeat_interval = 50
        args.raft_log_compaction_threshold = 100
        
        # Initialize RAFT node
        self.raft_node = RaftNode(node_id=self.node_id, args=args)
        
        # Create worker manager wrapper
        self.worker_manager = RaftWorkerManager(self.comm_manager, self.node_id)
        
        # Initialize RAFT consensus
        self.raft_consensus = RaftConsensus(
            args=args,
            raft_node=self.raft_node,
            worker_manager=self.worker_manager
        )
        
        self.logger.info(f"RAFT components initialized - Initial state: {self.raft_node.state}")
    
    def _initialize_service_discovery_bridge(self):
        """Initialize and register the service discovery bridge."""
        self.logger.info("Initializing service discovery bridge...")
        
        # Create service discovery bridge
        self.service_discovery_bridge = RaftServiceDiscoveryBridge(
            node_id=self.node_id,
            raft_consensus=self.raft_consensus,
            validation_timeout=5.0,
            sync_timeout=10.0
        )
        
        # Register bridge with communication manager
        success = self.comm_manager.register_service_discovery_bridge(self.service_discovery_bridge)
        if not success:
            raise RuntimeError("Failed to register service discovery bridge with communication manager")
        
        # Register bridge with consensus manager
        success = self.raft_consensus.register_service_discovery_bridge(self.service_discovery_bridge)
        if not success:
            raise RuntimeError("Failed to register service discovery bridge with consensus manager")
        
        self.logger.info("Service discovery bridge initialized and registered")
    
    def _start_raft_consensus(self):
        """Start the RAFT consensus algorithm."""
        self.logger.info("Starting RAFT consensus...")
        
        # Initialize RAFT state based on discovered nodes
        cluster_nodes = self.comm_manager.get_cluster_nodes_info()
        discovered_nodes = list(cluster_nodes.keys())
        
        if discovered_nodes:
            # Update known nodes in RAFT
            self.raft_node.update_known_nodes(node_ids=discovered_nodes)
            
            # Determine if this is the bootstrap node (first or only node)
            is_bootstrap = len(discovered_nodes) == 1 and self.node_id in discovered_nodes
            
            if is_bootstrap:
                self.logger.info("This is the bootstrap node - will become leader through normal election")
                # For bootstrap single-node cluster, transition to FOLLOWER state
                # The election timeout will trigger and it will become CANDIDATE then LEADER
                self.raft_node.state = RaftState.FOLLOWER
                self.raft_node.current_term = 0  # Start from term 0
                self.raft_node.voted_for = None
                # Reset election timeout so it can trigger quickly for bootstrap
                self.raft_node.reset_election_timeout()
                self.logger.info("Bootstrap node initialized as FOLLOWER - election will trigger soon")
            
            self.logger.info(f"Known nodes from discovery: {discovered_nodes}")
        
        # Start RAFT consensus
        self.raft_consensus.start()
        
        self.logger.info(f"RAFT consensus started - State: {self.raft_node.state}")
    
    def _complete_startup(self):
        """Complete the startup process."""
        self.logger.info("Completing startup process...")
        
        # Allow some time for initial RAFT operations
        time.sleep(2)
        
        self.logger.info("Startup process completed")
    
    def handle_raft_message(self, msg_type: str, msg_params):
        """Handle incoming RAFT messages."""
        try:
            data = msg_params.get('data', {})
            
            if msg_type == 'raft_vote_request':
                self.raft_consensus.handle_vote_request(
                    candidate_id=data.get('candidate_id'),
                    term=data.get('term'),
                    last_log_index=data.get('last_log_index'),
                    last_log_term=data.get('last_log_term')
                )
            
            elif msg_type == 'raft_vote_response':
                self.raft_consensus.handle_vote_response(
                    voter_id=data.get('voter_id'),
                    term=data.get('term'),
                    vote_granted=data.get('vote_granted')
                )
            
            elif msg_type == 'raft_append_entries':
                self.raft_consensus.handle_append_entries(
                    leader_id=data.get('leader_id'),
                    term=data.get('term'),
                    prev_log_index=data.get('prev_log_index'),
                    prev_log_term=data.get('prev_log_term'),
                    entries=data.get('entries', []),
                    leader_commit=data.get('leader_commit')
                )
            
            elif msg_type == 'raft_append_response':
                self.raft_consensus.handle_append_response(
                    follower_id=data.get('follower_id'),
                    term=data.get('term'),
                    success=data.get('success'),
                    match_index=data.get('match_index')
                )
            
            else:
                self.logger.warning(f"Unknown RAFT message type: {msg_type}")
                
        except Exception as e:
            self.logger.error(f"Error handling RAFT message {msg_type}: {e}")
    
    def get_status(self) -> Dict:
        """Get comprehensive node status."""
        status = {
            'node_id': self.node_id,
            'running': self.running,
            'startup_complete': self.startup_complete,
            'host': self.host,
            'port': self.port,
            'discovery_server': f"{self.discovery_host}:{self.discovery_port}",
        }
        
        if self.raft_node:
            status.update({
                'raft_state': self.raft_node.state.name,
                'raft_term': self.raft_node.current_term,
                'known_nodes': list(self.raft_node.known_nodes) if self.raft_node.known_nodes else [],
                'is_leader': self.raft_node.state == RaftState.LEADER,
                'log_length': len(self.raft_node.log),
                'commit_index': self.raft_node.commit_index,
            })
        
        if self.raft_consensus:
            status['consensus_running'] = self.raft_consensus.is_running
            status['leader_id'] = self.raft_consensus.get_leader_id()
        
        if self.comm_manager:
            cluster_nodes = self.comm_manager.get_cluster_nodes_info()
            status['discovered_nodes'] = list(cluster_nodes.keys())
            status['cluster_size'] = len(cluster_nodes)
            status['bridge_registered'] = self.comm_manager.bridge_registered
        
        if self.service_discovery_bridge:
            status['bridge_stats'] = self.service_discovery_bridge.get_statistics()
        
        return status
    
    def _print_status(self):
        """Print current node status."""
        status = self.get_status()
        self.logger.info("=== RAFT Node Status ===")
        for key, value in status.items():
            self.logger.info(f"  {key}: {value}")
        self.logger.info("========================")
    
    def trigger_election(self):
        """Trigger a RAFT election (for testing)."""
        if self.raft_node:
            self.logger.info("Triggering RAFT election...")
            self.raft_node.start_election()
    
    def add_log_entry(self, entry_data: Dict):
        """Add a log entry (leader only)."""
        if self.raft_consensus and self.raft_node.state == RaftState.LEADER:
            return self.raft_node.add_log_entry(entry_data)
        else:
            self.logger.warning("Cannot add log entry - not a leader")
            return -1


def main():
    """Run a single RAFT node with service discovery integration."""
    parser = argparse.ArgumentParser(description='RAFT Node with Service Discovery Bridge')
    parser.add_argument('--node-id', type=int, required=True, help='Unique node identifier')
    parser.add_argument('--host', default='localhost', help='Local host address')
    parser.add_argument('--port', type=int, help='Local gRPC port (default: 8890 + node_id)')
    parser.add_argument('--discovery-host', default='localhost', help='Service discovery host')
    parser.add_argument('--discovery-port', type=int, default=8090, help='Service discovery port')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Create RAFT node
    node = RaftNodeWithBridge(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        discovery_host=args.discovery_host,
        discovery_port=args.discovery_port,
        log_level=args.log_level
    )
    
    # Signal handler for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        node.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start the node
        node.start()
        
        print(f"RAFT Node {args.node_id} is running!")
        print("Commands:")
        print("  status - Show node status")
        print("  election - Trigger election")
        print("  quit - Stop the node")
        print()
        
        # Interactive mode
        while node.running:
            try:
                cmd = input(f"Node-{args.node_id}> ").strip().lower()
                
                if cmd == 'status':
                    node._print_status()
                
                elif cmd == 'election':
                    node.trigger_election()
                    print("Election triggered")
                
                elif cmd in ['quit', 'exit']:
                    break
                
                elif cmd == 'help':
                    print("Commands: status, election, quit")
                
                elif cmd:
                    print(f"Unknown command: {cmd}")
                    
            except EOFError:
                break
    
    except KeyboardInterrupt:
        pass
    
    finally:
        node.stop()


if __name__ == '__main__':
    main()
