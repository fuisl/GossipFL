#!/usr/bin/env python3
"""
RAFT Message Interaction Demo

This script demonstrates how RAFT nodes communicate through message passing
to achieve consensus. It shows the actual message flow during leader election
and log replication.
"""

import logging
import time
import random
import sys
import os
from typing import Dict, List
from unittest.mock import Mock

# Add parent directory to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# Import the RAFT implementation
from raft_node import RaftNode, RaftState
from raft_messages import RaftMessage


class MessageLogger:
    """Logs and tracks all messages exchanged between nodes."""
    
    def __init__(self):
        self.messages = []
        self.message_count = 0
    
    def log_message(self, sender: int, receiver: int, msg_type: str, content: dict):
        """Log a message exchange."""
        self.message_count += 1
        message = {
            'id': self.message_count,
            'timestamp': time.time(),
            'sender': sender,
            'receiver': receiver,
            'type': msg_type,
            'content': content
        }
        self.messages.append(message)
        
        print(f"[MSG {self.message_count:03d}] {sender} → {receiver}: {msg_type}")
        for key, value in content.items():
            print(f"    {key}: {value}")
        print()
    
    def get_messages_by_type(self, msg_type: str) -> List[dict]:
        """Get all messages of a specific type."""
        return [msg for msg in self.messages if msg['type'] == msg_type]
    
    def print_summary(self):
        """Print a summary of all messages."""
        print("\n" + "="*60)
        print("MESSAGE SUMMARY")
        print("="*60)
        
        type_counts = {}
        for msg in self.messages:
            msg_type = msg['type']
            type_counts[msg_type] = type_counts.get(msg_type, 0) + 1
        
        for msg_type, count in type_counts.items():
            print(f"{msg_type}: {count}")
        
        print(f"\nTotal messages: {len(self.messages)}")
        print("="*60)


class RaftMessageDemo:
    """Demonstrates RAFT message passing between nodes."""
    
    def __init__(self, num_nodes: int = 3):
        self.num_nodes = num_nodes
        self.nodes: Dict[int, RaftNode] = {}
        self.message_logger = MessageLogger()
        
        # Setup logging
        logging.basicConfig(level=logging.WARNING)  # Reduce log noise
        
        self.setup_nodes()
    
    def setup_nodes(self):
        """Create RAFT nodes for the demo."""
        args = Mock()
        args.client_num_in_total = self.num_nodes
        args.min_election_timeout = 150
        args.max_election_timeout = 300
        args.heartbeat_interval = 50
        
        for node_id in range(self.num_nodes):
            node = RaftNode(node_id, args)
            # Start all nodes as followers (simulate startup synchronization)
            node.state = RaftState.FOLLOWER
            node.current_term = 0
            self.nodes[node_id] = node
        
        print(f"Created {self.num_nodes} RAFT nodes")
        self.print_node_states()
    
    def print_node_states(self):
        """Print current state of all nodes."""
        print("\nNode States:")
        print("-" * 40)
        for node_id, node in self.nodes.items():
            print(f"Node {node_id}: {node.state.name} | Term: {node.current_term} | "
                  f"Voted for: {node.voted_for} | Log: {len(node.log)}")
        print()
    
    def simulate_vote_request(self, candidate_id: int, target_id: int):
        """Simulate a vote request message."""
        candidate = self.nodes[candidate_id]
        target = self.nodes[target_id]
        
        # Get candidate's log info
        last_log_index, last_log_term = candidate.get_last_log_info()
        
        # Log the message
        self.message_logger.log_message(
            candidate_id, target_id, "VOTE_REQUEST",
            {
                'term': candidate.current_term,
                'candidate_id': candidate_id,
                'last_log_index': last_log_index,
                'last_log_term': last_log_term
            }
        )
        
        # Process the vote request
        current_term, vote_granted = target.receive_vote_request(
            candidate_id, candidate.current_term, last_log_index, last_log_term
        )
        
        # Log the response
        self.message_logger.log_message(
            target_id, candidate_id, "VOTE_RESPONSE",
            {
                'term': current_term,
                'vote_granted': vote_granted
            }
        )
        
        # Process the vote response
        became_leader = candidate.receive_vote_response(target_id, current_term, vote_granted)
        
        return vote_granted, became_leader
    
    def simulate_append_entries(self, leader_id: int, follower_id: int, entries: List[dict] = None):
        """Simulate an AppendEntries message."""
        leader = self.nodes[leader_id]
        follower = self.nodes[follower_id]
        
        if entries is None:
            entries = []
        
        # Determine prev_log_index and prev_log_term
        prev_log_index = len(follower.log)
        prev_log_term = 0
        if prev_log_index > 0:
            prev_log_term = follower.log[prev_log_index - 1]['term']
        
        # Log the message
        self.message_logger.log_message(
            leader_id, follower_id, "APPEND_ENTRIES",
            {
                'term': leader.current_term,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': leader.commit_index
            }
        )
        
        # Process the AppendEntries
        current_term, success = follower.append_entries(
            leader_id, leader.current_term, prev_log_index, prev_log_term, 
            entries, leader.commit_index
        )
        
        # Log the response
        match_index = prev_log_index + len(entries) if success else 0
        self.message_logger.log_message(
            follower_id, leader_id, "APPEND_RESPONSE",
            {
                'term': current_term,
                'success': success,
                'match_index': match_index
            }
        )
        
        # Update leader's state if successful
        if success and leader.state == RaftState.LEADER:
            leader.next_index[follower_id] = match_index + 1
            leader.match_index[follower_id] = match_index
        
        return success
    
    def demo_leader_election(self):
        """Demonstrate a complete leader election process."""
        print("\n" + "="*60)
        print("DEMO: Leader Election Process")
        print("="*60)
        
        # Node 0 starts an election
        candidate_id = 0
        candidate = self.nodes[candidate_id]
        
        print(f"Node {candidate_id} starts election...")
        
        # Start election (increment term, vote for self)
        success = candidate.start_election()
        print(f"Election started: {success}")
        self.print_node_states()
        
        # Send vote requests to all other nodes
        votes_received = 1  # Candidate votes for itself
        
        for target_id in range(self.num_nodes):
            if target_id == candidate_id:
                continue
            
            print(f"\nSending vote request to Node {target_id}...")
            vote_granted, became_leader = self.simulate_vote_request(candidate_id, target_id)
            
            if vote_granted:
                votes_received += 1
                print(f"✓ Vote granted! Total votes: {votes_received}")
            else:
                print(f"✗ Vote denied")
            
            if became_leader:
                print(f"🎉 Node {candidate_id} became LEADER!")
                break
        
        self.print_node_states()
        
        # Return the leader ID if elected
        if candidate.state == RaftState.LEADER:
            return candidate_id
        return None
    
    def demo_log_replication(self, leader_id: int):
        """Demonstrate log replication from leader to followers."""
        print("\n" + "="*60)
        print("DEMO: Log Replication Process")
        print("="*60)
        
        leader = self.nodes[leader_id]
        
        # Leader adds some log entries
        test_entries = [
            {'type': 'test', 'data': 'first_entry', 'timestamp': time.time()},
            {'type': 'test', 'data': 'second_entry', 'timestamp': time.time()}
        ]
        
        print(f"Leader {leader_id} adding log entries...")
        for entry in test_entries:
            index = leader.add_log_entry(entry)
            print(f"Added entry at index {index}: {entry}")
        
        self.print_node_states()
        
        # Replicate to all followers
        for follower_id in range(self.num_nodes):
            if follower_id == leader_id:
                continue
            
            print(f"\nReplicating to Node {follower_id}...")
            
            # Prepare entries to send
            follower = self.nodes[follower_id]
            next_index = len(follower.log) + 1
            entries_to_send = leader.log[len(follower.log):]
            
            success = self.simulate_append_entries(leader_id, follower_id, entries_to_send)
            
            if success:
                print(f"✓ Replication successful to Node {follower_id}")
            else:
                print(f"✗ Replication failed to Node {follower_id}")
        
        self.print_node_states()
    
    def demo_heartbeat(self, leader_id: int):
        """Demonstrate heartbeat messages."""
        print("\n" + "="*60)
        print("DEMO: Heartbeat Messages")
        print("="*60)
        
        print(f"Leader {leader_id} sending heartbeats...")
        
        for follower_id in range(self.num_nodes):
            if follower_id == leader_id:
                continue
            
            print(f"\nSending heartbeat to Node {follower_id}...")
            success = self.simulate_append_entries(leader_id, follower_id, [])
            
            if success:
                print(f"✓ Heartbeat acknowledged by Node {follower_id}")
            else:
                print(f"✗ Heartbeat failed to Node {follower_id}")
        
        self.print_node_states()
    
    def demo_log_conflict_resolution(self):
        """Demonstrate how log conflicts are resolved."""
        print("\n" + "="*60)
        print("DEMO: Log Conflict Resolution")
        print("="*60)
        
        # Create a scenario where nodes have different logs
        
        # Node 0 has: [entry1, entry2]
        self.nodes[0].log = [
            {'term': 1, 'command': {'type': 'test', 'data': 'entry1'}, 'index': 1},
            {'term': 1, 'command': {'type': 'test', 'data': 'entry2'}, 'index': 2}
        ]
        
        # Node 1 has: [entry1, entry3] - conflict at index 2
        self.nodes[1].log = [
            {'term': 1, 'command': {'type': 'test', 'data': 'entry1'}, 'index': 1},
            {'term': 1, 'command': {'type': 'test', 'data': 'entry3'}, 'index': 2}
        ]
        
        # Node 0 becomes leader
        self.nodes[0].state = RaftState.LEADER
        self.nodes[0].current_term = 2
        
        print("Initial conflicting logs:")
        for node_id in [0, 1]:
            node = self.nodes[node_id]
            print(f"Node {node_id}: {[entry['command']['data'] for entry in node.log]}")
        
        # Leader tries to replicate its log to follower
        print(f"\nLeader 0 attempting to replicate to Node 1...")
        
        # First attempt - will fail due to conflict
        leader_entry = {'term': 2, 'command': {'type': 'test', 'data': 'entry4'}, 'index': 3}
        success = self.simulate_append_entries(0, 1, [leader_entry])
        
        print(f"First replication attempt: {'Success' if success else 'Failed'}")
        
        # Show final state
        print("\nFinal logs:")
        for node_id in [0, 1]:
            node = self.nodes[node_id]
            print(f"Node {node_id}: {[entry['command']['data'] for entry in node.log]}")
    
    def run_complete_demo(self):
        """Run a complete demonstration of RAFT consensus."""
        print("RAFT Consensus Message Passing Demo")
        print("=" * 60)
        
        # 1. Leader Election
        leader_id = self.demo_leader_election()
        
        if leader_id is None:
            print("❌ No leader elected, stopping demo")
            return
        
        # 2. Log Replication
        self.demo_log_replication(leader_id)
        
        # 3. Heartbeat
        self.demo_heartbeat(leader_id)
        
        # 4. Log Conflict Resolution
        self.demo_log_conflict_resolution()
        
        # 5. Message Summary
        self.message_logger.print_summary()
        
        print("\n🎉 Demo completed successfully!")


def main():
    """Run the RAFT message demonstration."""
    demo = RaftMessageDemo(num_nodes=3)
    demo.run_complete_demo()


if __name__ == "__main__":
    main()
