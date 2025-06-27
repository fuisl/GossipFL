#!/usr/bin/env python3
"""
RAFT Consensus Visual Dashboard

This module provides visual representations of RAFT consensus operations
including node state transitions, message flows, and cluster topology.
"""

import time
import threading
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class NodeStateSnapshot:
    """Snapshot of a node's state at a specific time."""
    timestamp: float
    node_id: int
    state: str
    term: int
    voted_for: Optional[int]
    log_length: int
    commit_index: int
    votes_received: set


@dataclass
class MessageEvent:
    """Represents a message event in the system."""
    timestamp: float
    sender: int
    receiver: int
    msg_type: str
    content: dict
    success: Optional[bool] = None


class RaftVisualizer:
    """
    Visualizes RAFT consensus operations in real-time.
    
    Provides multiple visualization modes:
    - Terminal-based ASCII art display
    - Timeline view of events
    - Network topology graph
    - State transition diagram
    """
    
    def __init__(self):
        self.node_states: Dict[int, List[NodeStateSnapshot]] = {}
        self.messages: List[MessageEvent] = []
        self.current_states: Dict[int, NodeStateSnapshot] = {}
        self.lock = threading.RLock()
        
        # Visual settings
        self.colors = {
            'INITIAL': '\033[90m',    # Gray
            'FOLLOWER': '\033[92m',   # Green
            'CANDIDATE': '\033[93m',  # Yellow
            'LEADER': '\033[91m',     # Red
            'RESET': '\033[0m'        # Reset
        }
        
        self.state_symbols = {
            'INITIAL': '●',
            'FOLLOWER': '○',
            'CANDIDATE': '◐',
            'LEADER': '★'
        }
        
        self.message_symbols = {
            'VOTE_REQUEST': '→',
            'VOTE_RESPONSE': '←',
            'APPEND_ENTRIES': '⇒',
            'APPEND_RESPONSE': '⇐'
        }
    
    def log_node_state(self, node_id: int, state: str, term: int, voted_for: Optional[int],
                      log_length: int, commit_index: int, votes_received: set = None):
        """Log a node state change."""
        with self.lock:
            snapshot = NodeStateSnapshot(
                timestamp=time.time(),
                node_id=node_id,
                state=state,
                term=term,
                voted_for=voted_for,
                log_length=log_length,
                commit_index=commit_index,
                votes_received=votes_received or set()
            )
            
            if node_id not in self.node_states:
                self.node_states[node_id] = []
            
            self.node_states[node_id].append(snapshot)
            self.current_states[node_id] = snapshot
    
    def log_message(self, sender: int, receiver: int, msg_type: str, content: dict, success: bool = None):
        """Log a message event."""
        with self.lock:
            message = MessageEvent(
                timestamp=time.time(),
                sender=sender,
                receiver=receiver,
                msg_type=msg_type,
                content=content,
                success=success
            )
            self.messages.append(message)
    
    def print_cluster_topology(self):
        """Print current cluster topology with visual representation."""
        with self.lock:
            if not self.current_states:
                print("No nodes in cluster")
                return
            
            print("\n" + "="*80)
            print("RAFT CLUSTER TOPOLOGY")
            print("="*80)
            
            # Sort nodes by ID
            sorted_nodes = sorted(self.current_states.items())
            
            # Header
            print("Node ID | State      | Term | Voted For | Log | Commit | Visual")
            print("-" * 70)
            
            # Node information
            for node_id, state in sorted_nodes:
                color = self.colors.get(state.state, '')
                symbol = self.state_symbols.get(state.state, '?')
                reset = self.colors['RESET']
                
                voted_str = str(state.voted_for) if state.voted_for is not None else "-"
                
                print(f"   {node_id}    | {color}{state.state:10}{reset} | {state.term:4} | "
                      f"    {voted_str:2}    | {state.log_length:3} |   {state.commit_index:2}   | "
                      f"{color}{symbol:2}{reset}")
            
            print("-" * 70)
            
            # Legend
            print("\nState Legend:")
            for state, symbol in self.state_symbols.items():
                color = self.colors.get(state, '')
                reset = self.colors['RESET']
                print(f"  {color}{symbol} {state}{reset}")
    
    def print_network_diagram(self):
        """Print network diagram showing connections and message flows."""
        with self.lock:
            if not self.current_states:
                return
            
            print("\n" + "="*80)
            print("NETWORK DIAGRAM")
            print("="*80)
            
            nodes = sorted(self.current_states.keys())
            
            # Create a visual network layout
            if len(nodes) <= 5:
                self._print_small_network(nodes)
            else:
                self._print_large_network(nodes)
    
    def _print_small_network(self, nodes: List[int]):
        """Print network diagram for small clusters (≤5 nodes)."""
        # Arrange nodes in a circle
        positions = {
            1: [(40, 2)],
            2: [(30, 2), (50, 2)],
            3: [(40, 1), (25, 4), (55, 4)],
            4: [(25, 1), (55, 1), (25, 5), (55, 5)],
            5: [(40, 0), (20, 2), (60, 2), (30, 5), (50, 5)]
        }
        
        if len(nodes) not in positions:
            self._print_linear_network(nodes)
            return
        
        # Create grid
        grid = [[' ' for _ in range(80)] for _ in range(7)]
        
        # Place nodes
        pos_list = positions[len(nodes)]
        for i, node_id in enumerate(nodes):
            if i < len(pos_list):
                x, y = pos_list[i]
                state = self.current_states[node_id]
                symbol = self.state_symbols.get(state.state, '?')
                
                # Place node symbol
                if x < 80 and y < 7:
                    grid[y][x] = symbol
                    
                    # Add node label
                    label = f"{node_id}"
                    for j, char in enumerate(label):
                        if x + j + 1 < 80:
                            grid[y][x + j + 1] = char
        
        # Print grid
        for row in grid:
            print(''.join(row))
    
    def _print_linear_network(self, nodes: List[int]):
        """Print linear network layout."""
        line = ""
        for i, node_id in enumerate(nodes):
            state = self.current_states[node_id]
            color = self.colors.get(state.state, '')
            symbol = self.state_symbols.get(state.state, '?')
            reset = self.colors['RESET']
            
            line += f"{color}{symbol}{node_id}{reset}"
            if i < len(nodes) - 1:
                line += " --- "
        
        print(line)
    
    def print_message_timeline(self, last_n: int = 10):
        """Print timeline of recent messages."""
        with self.lock:
            if not self.messages:
                print("No messages recorded")
                return
            
            print("\n" + "="*80)
            print(f"MESSAGE TIMELINE (Last {last_n} messages)")
            print("="*80)
            
            recent_messages = self.messages[-last_n:] if len(self.messages) > last_n else self.messages
            
            print("Time     | Sender → Receiver | Type           | Content")
            print("-" * 70)
            
            for msg in recent_messages:
                timestamp = datetime.fromtimestamp(msg.timestamp).strftime("%H:%M:%S")
                symbol = self.message_symbols.get(msg.msg_type, '?')
                
                # Truncate content for display
                content_str = str(msg.content)
                if len(content_str) > 30:
                    content_str = content_str[:27] + "..."
                
                success_indicator = ""
                if msg.success is not None:
                    success_indicator = " ✓" if msg.success else " ✗"
                
                print(f"{timestamp} |   {msg.sender} {symbol} {msg.receiver}     | "
                      f"{msg.msg_type:14} | {content_str}{success_indicator}")
    
    def print_state_transitions(self, node_id: Optional[int] = None):
        """Print state transition history."""
        with self.lock:
            print("\n" + "="*80)
            if node_id is not None:
                print(f"STATE TRANSITIONS - Node {node_id}")
                nodes_to_show = [node_id] if node_id in self.node_states else []
            else:
                print("STATE TRANSITIONS - All Nodes")
                nodes_to_show = sorted(self.node_states.keys())
            print("="*80)
            
            for nid in nodes_to_show:
                if nid not in self.node_states:
                    continue
                
                print(f"\nNode {nid}:")
                print("Time     | State      | Term | Voted For | Action")
                print("-" * 50)
                
                states = self.node_states[nid]
                for i, state in enumerate(states):
                    timestamp = datetime.fromtimestamp(state.timestamp).strftime("%H:%M:%S")
                    color = self.colors.get(state.state, '')
                    symbol = self.state_symbols.get(state.state, '?')
                    reset = self.colors['RESET']
                    
                    voted_str = str(state.voted_for) if state.voted_for is not None else "-"
                    
                    # Determine action
                    action = ""
                    if i > 0:
                        prev_state = states[i-1]
                        if prev_state.state != state.state:
                            action = f"{prev_state.state} → {state.state}"
                        elif prev_state.term != state.term:
                            action = f"Term: {prev_state.term} → {state.term}"
                        elif prev_state.voted_for != state.voted_for:
                            action = f"Voted: {prev_state.voted_for} → {state.voted_for}"
                    else:
                        action = "Initial state"
                    
                    print(f"{timestamp} | {color}{symbol} {state.state:8}{reset} | {state.term:4} | "
                          f"    {voted_str:2}    | {action}")
    
    def print_election_analysis(self):
        """Analyze and display election information."""
        with self.lock:
            print("\n" + "="*80)
            print("ELECTION ANALYSIS")
            print("="*80)
            
            # Find election events
            elections = self._find_elections()
            
            if not elections:
                print("No elections detected")
                return
            
            for i, election in enumerate(elections, 1):
                print(f"\nElection #{i}:")
                print(f"Term: {election['term']}")
                print(f"Candidates: {', '.join(map(str, election['candidates']))}")
                print(f"Winner: {election['winner']}")
                print(f"Vote Distribution:")
                
                for candidate, votes in election['vote_counts'].items():
                    print(f"  Node {candidate}: {votes} votes")
    
    def _find_elections(self):
        """Find election events from state transitions."""
        elections = []
        
        # Group state changes by term
        term_events = {}
        for node_id, states in self.node_states.items():
            for state in states:
                term = state.term
                if term not in term_events:
                    term_events[term] = []
                term_events[term].append((node_id, state))
        
        # Analyze each term for elections
        for term, events in term_events.items():
            if term == 0:  # Skip initial term
                continue
            
            candidates = []
            winner = None
            vote_counts = {}
            
            for node_id, state in events:
                if state.state == 'CANDIDATE':
                    candidates.append(node_id)
                    vote_counts[node_id] = len(state.votes_received)
                elif state.state == 'LEADER':
                    winner = node_id
            
            if candidates:
                elections.append({
                    'term': term,
                    'candidates': candidates,
                    'winner': winner,
                    'vote_counts': vote_counts
                })
        
        return elections
    
    def print_full_dashboard(self):
        """Print complete visual dashboard."""
        print("\033[2J\033[H")  # Clear screen
        print("🚀 RAFT Consensus Visual Dashboard")
        print("Updated:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        
        self.print_cluster_topology()
        self.print_network_diagram()
        self.print_message_timeline(8)
        self.print_election_analysis()
    
    def export_timeline_json(self, filename: str):
        """Export timeline data as JSON for external visualization."""
        with self.lock:
            data = {
                'node_states': {},
                'messages': []
            }
            
            # Convert node states
            for node_id, states in self.node_states.items():
                data['node_states'][str(node_id)] = [
                    {
                        'timestamp': s.timestamp,
                        'state': s.state,
                        'term': s.term,
                        'voted_for': s.voted_for,
                        'log_length': s.log_length,
                        'commit_index': s.commit_index,
                        'votes_received': list(s.votes_received)
                    }
                    for s in states
                ]
            
            # Convert messages
            data['messages'] = [
                {
                    'timestamp': m.timestamp,
                    'sender': m.sender,
                    'receiver': m.receiver,
                    'msg_type': m.msg_type,
                    'content': m.content,
                    'success': m.success
                }
                for m in self.messages
            ]
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"Timeline data exported to {filename}")


def create_visual_test_wrapper():
    """Create a wrapper that adds visualization to existing tests."""
    
    def visual_test_decorator(test_func):
        """Decorator to add visualization to test functions."""
        def wrapper(*args, **kwargs):
            visualizer = RaftVisualizer()
            
            # Monkey patch the test to include visualization
            original_print = print
            
            def enhanced_print(*args, **kwargs):
                # Call original print
                original_print(*args, **kwargs)
                
                # Update dashboard periodically
                if hasattr(wrapper, '_call_count'):
                    wrapper._call_count += 1
                    if wrapper._call_count % 10 == 0:  # Update every 10 prints
                        time.sleep(0.1)  # Brief pause for readability
                else:
                    wrapper._call_count = 1
            
            # Replace print temporarily
            import builtins
            builtins.print = enhanced_print
            
            try:
                result = test_func(*args, **kwargs)
                
                # Show final dashboard
                print("\n")
                visualizer.print_full_dashboard()
                
                return result
            finally:
                # Restore original print
                builtins.print = original_print
        
        return wrapper
    
    return visual_test_decorator


if __name__ == "__main__":
    # Demo visualization
    visualizer = RaftVisualizer()
    
    # Simulate some node states and messages
    visualizer.log_node_state(0, 'FOLLOWER', 0, None, 0, 0)
    visualizer.log_node_state(1, 'FOLLOWER', 0, None, 0, 0)
    visualizer.log_node_state(2, 'FOLLOWER', 0, None, 0, 0)
    
    time.sleep(0.1)
    
    visualizer.log_node_state(0, 'CANDIDATE', 1, 0, 0, 0, {0})
    visualizer.log_message(0, 1, 'VOTE_REQUEST', {'term': 1, 'candidate_id': 0})
    visualizer.log_message(0, 2, 'VOTE_REQUEST', {'term': 1, 'candidate_id': 0})
    
    time.sleep(0.1)
    
    visualizer.log_message(1, 0, 'VOTE_RESPONSE', {'term': 1, 'vote_granted': True}, True)
    visualizer.log_node_state(0, 'LEADER', 1, 0, 1, 0, {0, 1})
    
    # Show dashboard
    visualizer.print_full_dashboard()
