#!/usr/bin/env python3
"""
Simple RAFT Node Testing Script

This script provides simple, focused tests for individual RAFT operations
to help understand and debug the RAFT consensus implementation.
"""

import logging
import time
import sys
import os
from unittest.mock import Mock

# Add current directory to Python path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import the RAFT implementation
from raft_node import RaftNode, RaftState


def setup_logging():
    """Set up logging for the tests."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def create_test_node(node_id: int, total_nodes: int = 3) -> RaftNode:
    """Create a test RAFT node with mock arguments."""
    args = Mock()
    args.client_num_in_total = total_nodes
    args.min_election_timeout = 150  # ms
    args.max_election_timeout = 300  # ms
    args.heartbeat_interval = 50     # ms
    
    return RaftNode(node_id, args)


def test_node_initialization():
    """Test basic node initialization."""
    print("\n" + "="*50)
    print("TEST: Node Initialization")
    print("="*50)
    
    node = create_test_node(0, 3)
    
    print(f"Node ID: {node.node_id}")
    print(f"Initial State: {node.state.name}")
    print(f"Current Term: {node.current_term}")
    print(f"Known Nodes: {node.known_nodes}")
    print(f"Total Nodes: {node.total_nodes}")
    print(f"Majority: {node.majority}")
    
    assert node.state == RaftState.INITIAL
    assert node.current_term == 0
    assert node.voted_for is None
    assert len(node.log) == 0
    assert node.commit_index == 0
    assert node.last_applied == 0
    
    print("✓ Node initialization test passed")


def test_election_start():
    """Test starting an election."""
    print("\n" + "="*50)
    print("TEST: Election Start")
    print("="*50)
    
    node = create_test_node(0, 3)
    
    # Node must be in FOLLOWER state to start election
    node.state = RaftState.FOLLOWER
    
    print(f"Before election - State: {node.state.name}, Term: {node.current_term}")
    
    success = node.start_election()
    
    print(f"After election - State: {node.state.name}, Term: {node.current_term}")
    print(f"Voted for: {node.voted_for}")
    print(f"Votes received: {node.votes_received}")
    print(f"Election start success: {success}")
    
    assert success == True
    assert node.state == RaftState.CANDIDATE
    assert node.current_term == 1
    assert node.voted_for == 0
    assert 0 in node.votes_received
    
    print("✓ Election start test passed")


def test_vote_request_handling():
    """Test handling vote requests."""
    print("\n" + "="*50)
    print("TEST: Vote Request Handling")
    print("="*50)
    
    # Create two nodes
    node1 = create_test_node(1, 3)
    node2 = create_test_node(2, 3)
    
    # Both start as followers
    node1.state = RaftState.FOLLOWER
    node2.state = RaftState.FOLLOWER
    
    print("Initial state:")
    print(f"Node 1: State={node1.state.name}, Term={node1.current_term}")
    print(f"Node 2: State={node2.state.name}, Term={node2.current_term}")
    
    # Node 1 starts election
    node1.start_election()
    last_log_index, last_log_term = node1.get_last_log_info()
    
    print(f"\nNode 1 started election:")
    print(f"Term: {node1.current_term}")
    print(f"Last log index: {last_log_index}, Last log term: {last_log_term}")
    
    # Node 2 receives vote request from Node 1
    current_term, vote_granted = node2.receive_vote_request(
        candidate_id=1,
        term=node1.current_term,
        last_log_index=last_log_index,
        last_log_term=last_log_term
    )
    
    print(f"\nNode 2 processed vote request:")
    print(f"Current term: {current_term}")
    print(f"Vote granted: {vote_granted}")
    print(f"Node 2 voted for: {node2.voted_for}")
    
    assert vote_granted == True
    assert node2.voted_for == 1
    assert current_term == 1
    
    print("✓ Vote request handling test passed")


def test_vote_response_handling():
    """Test handling vote responses."""
    print("\n" + "="*50)
    print("TEST: Vote Response Handling")
    print("="*50)
    
    node = create_test_node(0, 3)
    node.state = RaftState.FOLLOWER
    
    # Start election
    node.start_election()
    
    print(f"After starting election:")
    print(f"State: {node.state.name}, Term: {node.current_term}")
    print(f"Votes received: {node.votes_received}")
    print(f"Need {node.majority} votes to become leader")
    
    # Receive vote from node 1
    became_leader = node.receive_vote_response(
        voter_id=1,
        term=node.current_term,
        vote_granted=True
    )
    
    print(f"\nAfter receiving vote from node 1:")
    print(f"Became leader: {became_leader}")
    print(f"State: {node.state.name}")
    print(f"Votes received: {node.votes_received}")
    
    # Should become leader with 2 votes out of 3 nodes (majority = 2)
    assert became_leader == True
    assert node.state == RaftState.LEADER
    assert len(node.votes_received) == 2  # Self + node 1
    
    print("✓ Vote response handling test passed")


def test_log_entry_addition():
    """Test adding log entries."""
    print("\n" + "="*50)
    print("TEST: Log Entry Addition")
    print("="*50)
    
    node = create_test_node(0, 3)
    
    # Node must be leader to add entries
    node.state = RaftState.LEADER
    node.current_term = 1
    
    print(f"Initial log length: {len(node.log)}")
    
    # Add some test entries
    test_commands = [
        {'type': 'test', 'data': 'first entry'},
        {'type': 'test', 'data': 'second entry'},
        {'type': 'topology', 'data': {'nodes': [0, 1, 2]}}
    ]
    
    for i, command in enumerate(test_commands):
        index = node.add_log_entry(command)
        print(f"Added entry {i+1}: index={index}, command={command}")
        
        assert index == i + 1
        assert len(node.log) == i + 1
        assert node.log[i]['command'] == command
        assert node.log[i]['term'] == node.current_term
        assert node.log[i]['index'] == i + 1
    
    print(f"Final log length: {len(node.log)}")
    print("Log contents:")
    for entry in node.log:
        print(f"  Index {entry['index']}: Term {entry['term']}, Command: {entry['command']}")
    
    print("✓ Log entry addition test passed")


def test_append_entries():
    """Test AppendEntries RPC handling."""
    print("\n" + "="*50)
    print("TEST: AppendEntries RPC")
    print("="*50)
    
    # Create leader and follower
    leader = create_test_node(0, 3)
    follower = create_test_node(1, 3)
    
    # Set up leader
    leader.state = RaftState.LEADER
    leader.current_term = 2
    
    # Set up follower
    follower.state = RaftState.FOLLOWER
    follower.current_term = 1
    
    print("Initial state:")
    print(f"Leader: Term={leader.current_term}, Log length={len(leader.log)}")
    print(f"Follower: Term={follower.current_term}, Log length={len(follower.log)}")
    
    # Leader adds some entries
    entries_to_replicate = [
        {'term': 2, 'command': {'type': 'test', 'data': 'entry1'}, 'index': 1},
        {'term': 2, 'command': {'type': 'test', 'data': 'entry2'}, 'index': 2}
    ]
    
    # Send AppendEntries to follower
    current_term, success = follower.append_entries(
        leader_id=0,
        term=2,
        prev_log_index=0,
        prev_log_term=0,
        entries=entries_to_replicate,
        leader_commit=0
    )
    
    print(f"\nAfter AppendEntries:")
    print(f"Follower current term: {current_term}")
    print(f"Success: {success}")
    print(f"Follower state: {follower.state.name}")
    print(f"Follower log length: {len(follower.log)}")
    
    assert success == True
    assert follower.current_term == 2
    assert follower.state == RaftState.FOLLOWER
    assert len(follower.log) == 2
    
    print("Follower log contents:")
    for entry in follower.log:
        print(f"  Index {entry['index']}: Term {entry['term']}, Command: {entry['command']}")
    
    print("✓ AppendEntries test passed")


def test_log_consistency_check():
    """Test log consistency checking."""
    print("\n" + "="*50)
    print("TEST: Log Consistency Check")
    print("="*50)
    
    node = create_test_node(0, 3)
    node.state = RaftState.FOLLOWER
    
    # Add some entries to the node's log
    node.log = [
        {'term': 1, 'command': {'type': 'test', 'data': 'entry1'}, 'index': 1},
        {'term': 1, 'command': {'type': 'test', 'data': 'entry2'}, 'index': 2},
        {'term': 2, 'command': {'type': 'test', 'data': 'entry3'}, 'index': 3}
    ]
    
    print(f"Node log: {len(node.log)} entries")
    for entry in node.log:
        print(f"  Index {entry['index']}: Term {entry['term']}")
    
    # Test 1: Consistent append
    print("\nTest 1: Consistent append (prev_log_index=3, prev_log_term=2)")
    current_term, success = node.append_entries(
        leader_id=1,
        term=2,
        prev_log_index=3,
        prev_log_term=2,
        entries=[{'term': 2, 'command': {'type': 'test', 'data': 'entry4'}, 'index': 4}],
        leader_commit=3
    )
    
    print(f"Result: success={success}, log length={len(node.log)}")
    assert success == True
    assert len(node.log) == 4
    
    # Test 2: Inconsistent append (wrong prev_log_term)
    print("\nTest 2: Inconsistent append (prev_log_index=4, prev_log_term=1 - wrong term)")
    current_term, success = node.append_entries(
        leader_id=1,
        term=2,
        prev_log_index=4,
        prev_log_term=1,  # Wrong term, should be 2
        entries=[{'term': 2, 'command': {'type': 'test', 'data': 'entry5'}, 'index': 5}],
        leader_commit=3
    )
    
    print(f"Result: success={success}, log length={len(node.log)}")
    assert success == False
    assert len(node.log) == 4  # Log should not change
    
    print("✓ Log consistency check test passed")


def test_commit_and_apply():
    """Test log entry commit and application."""
    print("\n" + "="*50)
    print("TEST: Commit and Apply")
    print("="*50)
    
    node = create_test_node(0, 3)
    node.state = RaftState.FOLLOWER
    
    # Add entries but don't commit yet
    node.log = [
        {'term': 1, 'command': {'type': 'test', 'data': 'entry1'}, 'index': 1},
        {'term': 1, 'command': {'type': 'test', 'data': 'entry2'}, 'index': 2},
        {'term': 2, 'command': {'type': 'test', 'data': 'entry3'}, 'index': 3}
    ]
    
    print(f"Initial state:")
    print(f"Log length: {len(node.log)}")
    print(f"Commit index: {node.commit_index}")
    print(f"Last applied: {node.last_applied}")
    
    # Receive AppendEntries with higher commit index
    current_term, success = node.append_entries(
        leader_id=1,
        term=2,
        prev_log_index=3,
        prev_log_term=2,
        entries=[],  # Heartbeat
        leader_commit=2  # Commit first 2 entries
    )
    
    print(f"\nAfter AppendEntries with leader_commit=2:")
    print(f"Success: {success}")
    print(f"Commit index: {node.commit_index}")
    print(f"Last applied: {node.last_applied}")
    
    assert success == True
    assert node.commit_index == 2
    assert node.last_applied == 2
    
    print("✓ Commit and apply test passed")


def main():
    """Run all simple RAFT tests."""
    setup_logging()
    
    print("RAFT Node Simple Testing")
    print("=" * 50)
    
    try:
        test_node_initialization()
        test_election_start()
        test_vote_request_handling()
        test_vote_response_handling()
        test_log_entry_addition()
        test_append_entries()
        test_log_consistency_check()
        test_commit_and_apply()
        
        print("\n" + "="*50)
        print("ALL TESTS PASSED! ✓")
        print("="*50)
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
