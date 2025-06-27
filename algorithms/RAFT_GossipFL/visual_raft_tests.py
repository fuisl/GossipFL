#!/usr/bin/env python3
"""
Visual RAFT Test Runner

Enhanced test runner with real-time visualization of RAFT consensus operations.
Shows node states, message flows, and cluster topology during test execution.
"""

import sys
import os
import time
import threading
from unittest.mock import Mock

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from raft_node import RaftNode, RaftState
from raft_visualizer import RaftVisualizer


class VisualRaftNode(RaftNode):
    """
    Enhanced RAFT node with built-in visualization support.
    Automatically logs state changes and operations to the visualizer.
    """
    
    def __init__(self, node_id, args, visualizer=None):
        super().__init__(node_id, args)
        self.visualizer = visualizer
        self._log_state_change()
    
    def _log_state_change(self):
        """Log current state to visualizer."""
        if self.visualizer:
            self.visualizer.log_node_state(
                self.node_id,
                self.state.name,
                self.current_term,
                self.voted_for,
                len(self.log),
                self.commit_index,
                getattr(self, 'votes_received', set())
            )
    
    def start_election(self):
        """Start election with visualization."""
        result = super().start_election()
        self._log_state_change()
        return result
    
    def become_leader(self):
        """Become leader with visualization."""
        result = super().become_leader()
        self._log_state_change()
        return result
    
    def become_follower(self, term):
        """Become follower with visualization."""
        super().become_follower(term)
        self._log_state_change()
    
    def receive_vote_request(self, candidate_id, term, last_log_index, last_log_term):
        """Handle vote request with message logging."""
        if self.visualizer:
            self.visualizer.log_message(
                candidate_id, self.node_id, 'VOTE_REQUEST',
                {
                    'term': term,
                    'candidate_id': candidate_id,
                    'last_log_index': last_log_index,
                    'last_log_term': last_log_term
                }
            )
        
        current_term, vote_granted = super().receive_vote_request(
            candidate_id, term, last_log_index, last_log_term
        )
        
        if self.visualizer:
            self.visualizer.log_message(
                self.node_id, candidate_id, 'VOTE_RESPONSE',
                {
                    'term': current_term,
                    'vote_granted': vote_granted
                },
                success=vote_granted
            )
        
        self._log_state_change()
        return current_term, vote_granted
    
    def receive_vote_response(self, voter_id, term, vote_granted):
        """Handle vote response with visualization."""
        became_leader = super().receive_vote_response(voter_id, term, vote_granted)
        self._log_state_change()
        return became_leader
    
    def append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        """Handle append entries with message logging."""
        if self.visualizer:
            self.visualizer.log_message(
                leader_id, self.node_id, 'APPEND_ENTRIES',
                {
                    'term': term,
                    'prev_log_index': prev_log_index,
                    'prev_log_term': prev_log_term,
                    'entries': len(entries) if entries else 0,
                    'leader_commit': leader_commit
                }
            )
        
        current_term, success = super().append_entries(
            leader_id, term, prev_log_index, prev_log_term, entries, leader_commit
        )
        
        if self.visualizer:
            self.visualizer.log_message(
                self.node_id, leader_id, 'APPEND_RESPONSE',
                {
                    'term': current_term,
                    'success': success
                },
                success=success
            )
        
        self._log_state_change()
        return current_term, success
    
    def add_log_entry(self, command):
        """Add log entry with visualization."""
        index = super().add_log_entry(command)
        self._log_state_change()
        return index


class VisualTestFramework:
    """
    Interactive test framework with real-time visualization.
    """
    
    def __init__(self, num_nodes=3):
        self.num_nodes = num_nodes
        self.visualizer = RaftVisualizer()
        self.nodes = {}
        self.setup_nodes()
    
    def setup_nodes(self):
        """Create visual RAFT nodes."""
        args = Mock()
        args.client_num_in_total = self.num_nodes
        args.min_election_timeout = 150
        args.max_election_timeout = 300
        args.heartbeat_interval = 50
        
        for node_id in range(self.num_nodes):
            node = VisualRaftNode(node_id, args, self.visualizer)
            # Start as followers for testing
            node.state = RaftState.FOLLOWER
            node._log_state_change()
            self.nodes[node_id] = node
        
        print(f"Created {self.num_nodes} visual RAFT nodes")
    
    def run_visual_election_demo(self):
        """Run an interactive election demonstration."""
        print("\n🗳️  VISUAL ELECTION DEMONSTRATION")
        print("=" * 60)
        
        # Clear screen and show initial state
        self.visualizer.print_full_dashboard()
        
        print("\nPress Enter to start election...")
        input()
        
        # Node 0 starts election
        candidate = self.nodes[0]
        print(f"\n📢 Node {candidate.node_id} starting election...")
        candidate.start_election()
        
        self.visualizer.print_cluster_topology()
        
        print("\nPress Enter to send vote requests...")
        input()
        
        # Send vote requests
        last_log_index, last_log_term = candidate.get_last_log_info()
        votes_received = 1  # Self vote
        
        for target_id in range(self.num_nodes):
            if target_id == candidate.node_id:
                continue
            
            target = self.nodes[target_id]
            print(f"\n📨 Sending vote request to Node {target_id}...")
            
            current_term, vote_granted = target.receive_vote_request(
                candidate.node_id, candidate.current_term, last_log_index, last_log_term
            )
            
            if vote_granted:
                votes_received += 1
                became_leader = candidate.receive_vote_response(target_id, current_term, vote_granted)
                
                print(f"✅ Vote granted! Total votes: {votes_received}")
                
                if became_leader:
                    print(f"👑 Node {candidate.node_id} became LEADER!")
                    break
            else:
                print(f"❌ Vote denied")
            
            self.visualizer.print_cluster_topology()
            print("\nPress Enter to continue...")
            input()
        
        # Final state
        print("\n🎉 Election complete!")
        self.visualizer.print_full_dashboard()
    
    def run_visual_log_replication_demo(self):
        """Run log replication demonstration."""
        print("\n📝 VISUAL LOG REPLICATION DEMONSTRATION")
        print("=" * 60)
        
        # Ensure we have a leader
        leader_id = None
        for node_id, node in self.nodes.items():
            if node.state == RaftState.LEADER:
                leader_id = node_id
                break
        
        if leader_id is None:
            print("No leader found. Running election first...")
            self.nodes[0].state = RaftState.FOLLOWER
            self.nodes[0].start_election()
            
            # Quick election for demo
            for target_id in range(1, self.num_nodes):
                target = self.nodes[target_id]
                last_log_index, last_log_term = self.nodes[0].get_last_log_info()
                current_term, vote_granted = target.receive_vote_request(
                    0, self.nodes[0].current_term, last_log_index, last_log_term
                )
                if vote_granted:
                    self.nodes[0].receive_vote_response(target_id, current_term, vote_granted)
                    if self.nodes[0].state == RaftState.LEADER:
                        leader_id = 0
                        break
        
        if leader_id is None:
            print("Could not establish leader for demo")
            return
        
        leader = self.nodes[leader_id]
        print(f"👑 Leader: Node {leader_id}")
        
        self.visualizer.print_cluster_topology()
        
        print("\nPress Enter to add log entries...")
        input()
        
        # Add log entries
        test_entries = [
            {'type': 'test', 'data': 'entry_1', 'value': 100},
            {'type': 'test', 'data': 'entry_2', 'value': 200},
            {'type': 'config', 'data': 'node_config', 'nodes': list(range(self.num_nodes))}
        ]
        
        for i, entry in enumerate(test_entries):
            print(f"\n📝 Leader adding entry {i+1}: {entry}")
            index = leader.add_log_entry(entry)
            print(f"   Added at index {index}")
            
            self.visualizer.print_cluster_topology()
            
            print("\nPress Enter to replicate to followers...")
            input()
            
            # Replicate to followers
            for follower_id in range(self.num_nodes):
                if follower_id == leader_id:
                    continue
                
                follower = self.nodes[follower_id]
                
                # Simple replication (get all entries)
                prev_log_index = len(follower.log)
                prev_log_term = 0
                if prev_log_index > 0:
                    prev_log_term = follower.log[prev_log_index - 1]['term']
                
                entries_to_send = leader.log[len(follower.log):]
                
                print(f"📤 Replicating to Node {follower_id}...")
                current_term, success = follower.append_entries(
                    leader_id, leader.current_term, prev_log_index, prev_log_term,
                    entries_to_send, leader.commit_index
                )
                
                if success:
                    print(f"   ✅ Replication successful")
                else:
                    print(f"   ❌ Replication failed")
            
            self.visualizer.print_cluster_topology()
            print(f"\nLog entry {i+1} replication complete. Press Enter for next entry...")
            input()
        
        print("\n🎉 Log replication demo complete!")
        self.visualizer.print_full_dashboard()
    
    def run_interactive_demo(self):
        """Run full interactive demonstration."""
        print("\n🚀 RAFT CONSENSUS INTERACTIVE VISUAL DEMO")
        print("=" * 60)
        
        while True:
            print("\nChoose a demonstration:")
            print("1. Election Process")
            print("2. Log Replication")
            print("3. Show Full Dashboard")
            print("4. Show Message Timeline")
            print("5. Show State Transitions")
            print("6. Export Timeline Data")
            print("0. Exit")
            
            choice = input("\nEnter choice (0-6): ").strip()
            
            if choice == '0':
                break
            elif choice == '1':
                self.run_visual_election_demo()
            elif choice == '2':
                self.run_visual_log_replication_demo()
            elif choice == '3':
                self.visualizer.print_full_dashboard()
            elif choice == '4':
                n = input("Number of recent messages to show (default 10): ").strip()
                n = int(n) if n.isdigit() else 10
                self.visualizer.print_message_timeline(n)
            elif choice == '5':
                node_id = input("Node ID to analyze (or press Enter for all): ").strip()
                node_id = int(node_id) if node_id.isdigit() else None
                self.visualizer.print_state_transitions(node_id)
            elif choice == '6':
                filename = input("Export filename (default: raft_timeline.json): ").strip()
                filename = filename if filename else "raft_timeline.json"
                self.visualizer.export_timeline_json(filename)
            else:
                print("Invalid choice. Please try again.")
            
            if choice != '0':
                input("\nPress Enter to continue...")


def run_visual_simple_test():
    """Run simple test with visualization."""
    print("\n🧪 VISUAL SIMPLE TEST")
    print("=" * 50)
    
    framework = VisualTestFramework(3)
    
    # Test election
    print("Testing election process...")
    candidate = framework.nodes[0]
    candidate.start_election()
    
    # Quick vote collection
    for target_id in [1, 2]:
        target = framework.nodes[target_id]
        last_log_index, last_log_term = candidate.get_last_log_info()
        current_term, vote_granted = target.receive_vote_request(
            0, candidate.current_term, last_log_index, last_log_term
        )
        if vote_granted:
            candidate.receive_vote_response(target_id, current_term, vote_granted)
            if candidate.state == RaftState.LEADER:
                break
    
    framework.visualizer.print_full_dashboard()
    
    print("\n✅ Visual simple test completed!")


def main():
    """Main function for visual test runner."""
    print("RAFT Consensus Visual Test Runner")
    print("=" * 50)
    
    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        # Interactive mode
        framework = VisualTestFramework(5)
        framework.run_interactive_demo()
    elif len(sys.argv) > 1 and sys.argv[1] == '--simple':
        # Simple visual test
        run_visual_simple_test()
    else:
        # Demo mode
        print("\nRunning demonstration...")
        framework = VisualTestFramework(3)
        
        print("\n1. Initial cluster state:")
        framework.visualizer.print_cluster_topology()
        time.sleep(2)
        
        print("\n2. Running election...")
        framework.run_visual_election_demo()
        
        print("\n3. Running log replication...")
        framework.run_visual_log_replication_demo()
        
        print("\n✨ Demo completed!")
        
        # Show usage
        print("\nFor interactive mode, run:")
        print("python visual_raft_tests.py --interactive")


if __name__ == "__main__":
    main()
