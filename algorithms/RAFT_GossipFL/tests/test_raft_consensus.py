#!/usr/bin/env python3
"""
Pure RAFT Consensus Testing Framework

This module provides comprehensive testing for the RAFT consensus implementation
without dependencies on GossipFL components. It focuses on testing the core
RAFT consensus algorithm with pure message passing.

Tests include:
- Leader election
- Log replication
- Network partitions
- Membership changes  
- Concurrent operations
- Failure recovery
"""

import logging
import threading
import time
import random
import queue
import sys
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from unittest.mock import Mock

# Add parent directory to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Import the RAFT implementation
from raft_node import RaftNode, RaftState
from raft_consensus import RaftConsensus
from raft_messages import RaftMessage


@dataclass
class TestMessage:
    """Represents a message in the test network."""
    sender: int
    receiver: int
    msg_type: int
    content: dict
    timestamp: float = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


class MockWorkerManager:
    """
    Mock worker manager for testing RAFT consensus without GossipFL.
    
    Simulates message passing between nodes in a controlled test environment.
    """
    
    def __init__(self, node_id: int, network: 'TestNetwork'):
        self.node_id = node_id
        self.network = network
        self.consensus = None  # Will be set by the test framework
        
    def set_consensus(self, consensus):
        """Set the consensus instance for message handling."""
        self.consensus = consensus
    
    def send_vote_request(self, target_node_id: int, term: int, last_log_index: int, last_log_term: int):
        """Send a vote request to another node."""
        message = TestMessage(
            sender=self.node_id,
            receiver=target_node_id,
            msg_type=RaftMessage.MSG_TYPE_RAFT_REQUEST_VOTE,
            content={
                RaftMessage.MSG_ARG_TERM: term,
                RaftMessage.MSG_ARG_CANDIDATE_ID: self.node_id,
                RaftMessage.MSG_ARG_LAST_LOG_INDEX: last_log_index,
                RaftMessage.MSG_ARG_LAST_LOG_TERM: last_log_term
            }
        )
        self.network.send_message(message)
    
    def send_vote_response(self, target_node_id: int, term: int, vote_granted: bool):
        """Send a vote response to a candidate."""
        message = TestMessage(
            sender=self.node_id,
            receiver=target_node_id,
            msg_type=RaftMessage.MSG_TYPE_RAFT_VOTE_RESPONSE,
            content={
                RaftMessage.MSG_ARG_TERM: term,
                RaftMessage.MSG_ARG_VOTE_GRANTED: vote_granted
            }
        )
        self.network.send_message(message)
    
    def send_append_entries(self, target_node_id: int, term: int, prev_log_index: int, 
                           prev_log_term: int, entries: List[dict], leader_commit: int):
        """Send append entries RPC to a follower."""
        message = TestMessage(
            sender=self.node_id,
            receiver=target_node_id,
            msg_type=RaftMessage.MSG_TYPE_RAFT_APPEND_ENTRIES,
            content={
                RaftMessage.MSG_ARG_TERM: term,
                RaftMessage.MSG_ARG_PREV_LOG_INDEX: prev_log_index,
                RaftMessage.MSG_ARG_PREV_LOG_TERM: prev_log_term,
                RaftMessage.MSG_ARG_ENTRIES: entries,
                RaftMessage.MSG_ARG_LEADER_COMMIT: leader_commit
            }
        )
        self.network.send_message(message)
    
    def send_append_response(self, target_node_id: int, term: int, success: bool, match_index: int):
        """Send append entries response to the leader."""
        message = TestMessage(
            sender=self.node_id,
            receiver=target_node_id,
            msg_type=RaftMessage.MSG_TYPE_RAFT_APPEND_RESPONSE,
            content={
                RaftMessage.MSG_ARG_TERM: term,
                RaftMessage.MSG_ARG_SUCCESS: success,
                RaftMessage.MSG_ARG_MATCH_INDEX: match_index
            }
        )
        self.network.send_message(message)


class TestNetwork:
    """
    Simulates a network for testing RAFT consensus.
    
    Provides controllable message delivery, network partitions,
    and message delays for comprehensive testing.
    """
    
    def __init__(self):
        self.nodes: Dict[int, MockWorkerManager] = {}
        self.consensus_nodes: Dict[int, RaftConsensus] = {}
        self.message_queue = queue.Queue()
        self.running = False
        self.message_thread = None
        
        # Network controls
        self.partitions: Set[frozenset] = set()  # Set of node groups that can communicate
        self.failed_nodes: Set[int] = set()
        self.message_delay_range = (0.001, 0.01)  # 1-10ms delay
        self.message_loss_rate = 0.0  # 0-1 probability of message loss
        
        # Statistics
        self.messages_sent = 0
        self.messages_delivered = 0
        self.messages_dropped = 0
        
        self.lock = threading.RLock()
    
    def add_node(self, node_id: int, worker_manager: MockWorkerManager, consensus: RaftConsensus):
        """Add a node to the network."""
        with self.lock:
            self.nodes[node_id] = worker_manager
            self.consensus_nodes[node_id] = consensus
            worker_manager.set_consensus(consensus)
    
    def remove_node(self, node_id: int):
        """Remove a node from the network."""
        with self.lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
            if node_id in self.consensus_nodes:
                del self.consensus_nodes[node_id]
            self.failed_nodes.discard(node_id)
    
    def start(self):
        """Start the network message processing."""
        if self.running:
            return
        
        self.running = True
        self.message_thread = threading.Thread(target=self._message_processor, daemon=True)
        self.message_thread.start()
    
    def stop(self):
        """Stop the network message processing."""
        self.running = False
        if self.message_thread:
            self.message_thread.join(timeout=1.0)
    
    def send_message(self, message: TestMessage):
        """Send a message through the network."""
        with self.lock:
            self.messages_sent += 1
            
            # Check if sender or receiver is failed
            if message.sender in self.failed_nodes or message.receiver in self.failed_nodes:
                self.messages_dropped += 1
                return
            
            # Check for network partitions
            if self.partitions and not self._can_communicate(message.sender, message.receiver):
                self.messages_dropped += 1
                return
            
            # Simulate message loss
            if random.random() < self.message_loss_rate:
                self.messages_dropped += 1
                return
            
            # Add delay and queue the message
            delay = random.uniform(*self.message_delay_range)
            message.timestamp = time.time() + delay
            self.message_queue.put(message)
    
    def _can_communicate(self, node1: int, node2: int) -> bool:
        """Check if two nodes can communicate given current partitions."""
        if not self.partitions:
            return True
        
        for partition in self.partitions:
            if node1 in partition and node2 in partition:
                return True
        return False
    
    def _message_processor(self):
        """Process messages with delays."""
        pending_messages = []
        
        while self.running:
            try:
                # Get new messages with timeout
                try:
                    message = self.message_queue.get(timeout=0.01)
                    pending_messages.append(message)
                except queue.Empty:
                    pass
                
                # Process messages whose time has come
                current_time = time.time()
                ready_messages = [msg for msg in pending_messages if msg.timestamp <= current_time]
                pending_messages = [msg for msg in pending_messages if msg.timestamp > current_time]
                
                for message in ready_messages:
                    self._deliver_message(message)
                
            except Exception as e:
                logging.error(f"Error in message processor: {e}")
    
    def _deliver_message(self, message: TestMessage):
        """Deliver a message to its target node."""
        with self.lock:
            if message.receiver not in self.consensus_nodes:
                return
            
            consensus = self.consensus_nodes[message.receiver]
            content = message.content
            
            try:
                if message.msg_type == RaftMessage.MSG_TYPE_RAFT_REQUEST_VOTE:
                    consensus.handle_vote_request(
                        message.sender,
                        content[RaftMessage.MSG_ARG_TERM],
                        content[RaftMessage.MSG_ARG_LAST_LOG_INDEX],
                        content[RaftMessage.MSG_ARG_LAST_LOG_TERM]
                    )
                elif message.msg_type == RaftMessage.MSG_TYPE_RAFT_VOTE_RESPONSE:
                    consensus.handle_vote_response(
                        message.sender,
                        content[RaftMessage.MSG_ARG_TERM],
                        content[RaftMessage.MSG_ARG_VOTE_GRANTED]
                    )
                elif message.msg_type == RaftMessage.MSG_TYPE_RAFT_APPEND_ENTRIES:
                    consensus.handle_append_entries(
                        message.sender,
                        content[RaftMessage.MSG_ARG_TERM],
                        content[RaftMessage.MSG_ARG_PREV_LOG_INDEX],
                        content[RaftMessage.MSG_ARG_PREV_LOG_TERM],
                        content[RaftMessage.MSG_ARG_ENTRIES],
                        content[RaftMessage.MSG_ARG_LEADER_COMMIT]
                    )
                elif message.msg_type == RaftMessage.MSG_TYPE_RAFT_APPEND_RESPONSE:
                    consensus.handle_append_response(
                        message.sender,
                        content[RaftMessage.MSG_ARG_TERM],
                        content[RaftMessage.MSG_ARG_SUCCESS],
                        content[RaftMessage.MSG_ARG_MATCH_INDEX]
                    )
                
                self.messages_delivered += 1
                
            except Exception as e:
                logging.error(f"Error delivering message: {e}")
    
    def create_partition(self, partition1: List[int], partition2: List[int]):
        """Create a network partition between two groups of nodes."""
        with self.lock:
            self.partitions = {frozenset(partition1), frozenset(partition2)}
            logging.info(f"Created partition: {partition1} | {partition2}")
    
    def heal_partition(self):
        """Heal all network partitions."""
        with self.lock:
            self.partitions.clear()
            logging.info("Healed all partitions")
    
    def fail_node(self, node_id: int):
        """Simulate node failure."""
        with self.lock:
            self.failed_nodes.add(node_id)
            logging.info(f"Failed node {node_id}")
    
    def recover_node(self, node_id: int):
        """Recover a failed node."""
        with self.lock:
            self.failed_nodes.discard(node_id)
            logging.info(f"Recovered node {node_id}")
    
    def get_stats(self) -> dict:
        """Get network statistics."""
        with self.lock:
            return {
                'messages_sent': self.messages_sent,
                'messages_delivered': self.messages_delivered,
                'messages_dropped': self.messages_dropped,
                'active_nodes': len(self.nodes) - len(self.failed_nodes),
                'failed_nodes': len(self.failed_nodes),
                'partitions': len(self.partitions)
            }


class RaftTestFramework:
    """
    Main test framework for RAFT consensus testing.
    
    Provides methods to create clusters, run tests, and verify results.
    """
    
    def __init__(self, num_nodes: int = 5):
        self.num_nodes = num_nodes
        self.network = TestNetwork()
        self.nodes: Dict[int, RaftNode] = {}
        self.consensus_nodes: Dict[int, RaftConsensus] = {}
        self.worker_managers: Dict[int, MockWorkerManager] = {}
        
        # Test configuration
        self.test_timeout = 30.0  # 30 seconds timeout for tests
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
        self.setup_cluster()
    
    def setup_cluster(self):
        """Set up a RAFT cluster for testing."""
        # Create mock args
        args = Mock()
        args.client_num_in_total = self.num_nodes
        args.min_election_timeout = 150
        args.max_election_timeout = 300
        args.heartbeat_interval = 50
        
        # Create nodes
        for node_id in range(self.num_nodes):
            # Create RAFT node
            raft_node = RaftNode(node_id, args)
            
            # Create mock worker manager
            worker_manager = MockWorkerManager(node_id, self.network)
            
            # Create consensus
            consensus = RaftConsensus(raft_node, worker_manager, args)
            
            # Store references
            self.nodes[node_id] = raft_node
            self.worker_managers[node_id] = worker_manager
            self.consensus_nodes[node_id] = consensus
            
            # Add to network
            self.network.add_node(node_id, worker_manager, consensus)
        
        # Start network
        self.network.start()
        
        logging.info(f"Created RAFT cluster with {self.num_nodes} nodes")
    
    def start_cluster(self):
        """Start all consensus processes in the cluster."""
        for consensus in self.consensus_nodes.values():
            consensus.start()
    
    def stop_cluster(self):
        """Stop all consensus processes in the cluster."""
        for consensus in self.consensus_nodes.values():
            consensus.stop()
        self.network.stop()
    
    def wait_for_leader_election(self, timeout: float = 10.0) -> Optional[int]:
        """Wait for a leader to be elected."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            leaders = [node_id for node_id, node in self.nodes.items() 
                      if node.state == RaftState.LEADER]
            
            if len(leaders) == 1:
                leader_id = leaders[0]
                logging.info(f"Leader elected: Node {leader_id}")
                return leader_id
            elif len(leaders) > 1:
                logging.warning(f"Multiple leaders detected: {leaders}")
            
            time.sleep(0.1)
        
        logging.error("No leader elected within timeout")
        return None
    
    def get_cluster_state(self) -> dict:
        """Get the current state of the cluster."""
        states = {}
        for node_id, node in self.nodes.items():
            states[node_id] = {
                'state': node.state.name,
                'term': node.current_term,
                'log_length': len(node.log),
                'commit_index': node.commit_index,
                'voted_for': node.voted_for
            }
        return states
    
    def print_cluster_state(self):
        """Print the current state of all nodes."""
        print("\n" + "="*60)
        print("CLUSTER STATE")
        print("="*60)
        
        for node_id, state in self.get_cluster_state().items():
            failed = " (FAILED)" if node_id in self.network.failed_nodes else ""
            print(f"Node {node_id}{failed}: {state['state']} | Term: {state['term']} | "
                  f"Log: {state['log_length']} | Commit: {state['commit_index']}")
        
        network_stats = self.network.get_stats()
        print(f"\nNetwork: Sent: {network_stats['messages_sent']}, "
              f"Delivered: {network_stats['messages_delivered']}, "
              f"Dropped: {network_stats['messages_dropped']}")
        print("="*60)
    
    def verify_log_consistency(self) -> bool:
        """Verify that all nodes have consistent committed logs."""
        committed_logs = {}
        
        for node_id, node in self.nodes.items():
            if node_id in self.network.failed_nodes:
                continue
                
            committed_log = node.log[:node.commit_index]
            committed_logs[node_id] = committed_log
        
        if not committed_logs:
            return True
        
        # Compare all committed logs
        reference_log = list(committed_logs.values())[0]
        
        for node_id, log in committed_logs.items():
            if log != reference_log:
                logging.error(f"Log inconsistency detected on node {node_id}")
                return False
        
        logging.info("All committed logs are consistent")
        return True
    
    def add_log_entries(self, entries: List[dict], leader_id: Optional[int] = None) -> bool:
        """Add log entries through the leader."""
        if leader_id is None:
            leader_id = self.wait_for_leader_election()
            if leader_id is None:
                return False
        
        leader_consensus = self.consensus_nodes[leader_id]
        
        for entry in entries:
            if leader_consensus.raft_node.state != RaftState.LEADER:
                logging.error(f"Node {leader_id} is no longer leader")
                return False
            
            index = leader_consensus.raft_node.add_log_entry(entry)
            if index == -1:
                logging.error(f"Failed to add log entry to leader {leader_id}")
                return False
            
            logging.info(f"Added log entry to leader {leader_id} at index {index}")
        
        return True


def main():
    """Main function to demonstrate RAFT testing."""
    print("RAFT Consensus Testing Framework")
    print("=" * 50)
    
    # Create test framework
    framework = RaftTestFramework(num_nodes=5)
    
    try:
        # Start the cluster
        framework.start_cluster()
        
        # Test 1: Basic Leader Election
        print("\nTest 1: Basic Leader Election")
        print("-" * 30)
        
        # Initialize nodes to followers (simulate startup synchronization)
        for node in framework.nodes.values():
            node.state = RaftState.FOLLOWER
        
        leader_id = framework.wait_for_leader_election(timeout=15.0)
        
        if leader_id is not None:
            print(f"✓ Leader elected: Node {leader_id}")
            framework.print_cluster_state()
        else:
            print("✗ Leader election failed")
            framework.print_cluster_state()
            return
        
        # Test 2: Log Replication
        print("\nTest 2: Log Replication")
        print("-" * 30)
        
        test_entries = [
            {'type': 'test', 'data': 'entry1', 'timestamp': time.time()},
            {'type': 'test', 'data': 'entry2', 'timestamp': time.time()},
            {'type': 'test', 'data': 'entry3', 'timestamp': time.time()}
        ]
        
        if framework.add_log_entries(test_entries, leader_id):
            print("✓ Log entries added to leader")
            
            # Wait for replication
            time.sleep(3.0)
            
            if framework.verify_log_consistency():
                print("✓ Log replication successful")
            else:
                print("✗ Log replication failed")
            
            framework.print_cluster_state()
        else:
            print("✗ Failed to add log entries")
        
        # Test 3: Leader Failure and Re-election
        print("\nTest 3: Leader Failure and Re-election")
        print("-" * 30)
        
        # Fail the current leader
        framework.network.fail_node(leader_id)
        print(f"Failed leader node {leader_id}")
        
        # Wait for new leader election
        new_leader_id = framework.wait_for_leader_election(timeout=15.0)
        
        if new_leader_id is not None and new_leader_id != leader_id:
            print(f"✓ New leader elected: Node {new_leader_id}")
            framework.print_cluster_state()
            
            # Add more entries with new leader
            more_entries = [
                {'type': 'test', 'data': 'entry4', 'timestamp': time.time()},
                {'type': 'test', 'data': 'entry5', 'timestamp': time.time()}
            ]
            
            if framework.add_log_entries(more_entries, new_leader_id):
                print("✓ Log entries added to new leader")
                time.sleep(2.0)
                
                if framework.verify_log_consistency():
                    print("✓ Log consistency maintained after leader change")
                else:
                    print("✗ Log consistency lost after leader change")
            
        else:
            print("✗ New leader election failed")
        
        # Test 4: Network Partition
        print("\nTest 4: Network Partition")
        print("-" * 30)
        
        # Create partition: [0, 1] vs [2, 3, 4]
        partition1 = [0, 1]
        partition2 = [2, 3, 4]
        framework.network.create_partition(partition1, partition2)
        
        print(f"Created partition: {partition1} | {partition2}")
        
        # Wait to see if majority partition elects leader
        time.sleep(5.0)
        
        majority_leaders = [node_id for node_id in partition2 
                          if node_id in framework.nodes and 
                          framework.nodes[node_id].state == RaftState.LEADER]
        
        minority_leaders = [node_id for node_id in partition1 
                          if node_id in framework.nodes and 
                          framework.nodes[node_id].state == RaftState.LEADER]
        
        print(f"Majority partition leaders: {majority_leaders}")
        print(f"Minority partition leaders: {minority_leaders}")
        
        if len(majority_leaders) == 1 and len(minority_leaders) == 0:
            print("✓ Only majority partition elected leader")
        else:
            print("✗ Partition behavior incorrect")
        
        # Heal partition
        framework.network.heal_partition()
        print("Healed partition")
        
        time.sleep(3.0)
        framework.print_cluster_state()
        
        print(f"\nFinal network statistics: {framework.network.get_stats()}")
        
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        framework.stop_cluster()
        print("\nCluster stopped")


if __name__ == "__main__":
    main()
