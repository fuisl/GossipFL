#!/usr/bin/env python3

"""
Test Phase 1 RAFT Consensus Integration with Pure Service Discovery

This test verifies that the RAFT consensus now properly integrates with
the pure service discovery system through discovery hints.
"""

import unittest
import time
import logging
import sys
import os
from unittest.mock import Mock, MagicMock

# Add parent directories to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

# Import the updated RAFT consensus
try:
    from raft_consensus import RaftConsensus, ConsensusType
    from raft_node import RaftNode, RaftState
    from discovery_hints import DiscoveryHintSender, RaftState as HintRaftState
except ImportError as e:
    print(f"Import error: {e}")
    print("This test needs to be run from the correct directory with proper Python path")
    sys.exit(1)


class TestRaftConsensusDiscoveryIntegration(unittest.TestCase):
    """Test RAFT consensus integration with discovery hints."""
    
    def setUp(self):
        """Set up test environment."""
        # Create mock objects
        self.mock_worker_manager = Mock()
        self.mock_bandwidth_manager = Mock()
        self.mock_topology_manager = Mock()
        
        # Create mock args with discovery configuration
        self.mock_args = Mock()
        self.mock_args.discovery_host = "localhost"
        self.mock_args.discovery_port = 8090
        self.mock_args.replication_interval = 1.0
        
        # Create a RAFT node
        self.raft_node = RaftNode(
            node_id=1,
            known_nodes={1, 2, 3},
            election_timeout_range=(150, 300),
            heartbeat_interval=50
        )
        
        # Create RAFT consensus with discovery integration
        self.consensus = RaftConsensus(
            raft_node=self.raft_node,
            worker_manager=self.mock_worker_manager,
            args=self.mock_args,
            bandwidth_manager=self.mock_bandwidth_manager,
            topology_manager=self.mock_topology_manager
        )
    
    def tearDown(self):
        """Clean up test environment."""
        if self.consensus.is_running:
            self.consensus.stop()
    
    def test_discovery_hint_sender_initialization(self):
        """Test that discovery hint sender is properly initialized."""
        # Should have discovery hint sender configured
        self.assertIsNotNone(self.consensus.discovery_hint_sender)
        self.assertTrue(self.consensus.is_discovery_enabled())
        
        # Check configuration
        hint_sender = self.consensus.discovery_hint_sender
        self.assertEqual(hint_sender.discovery_host, "localhost")
        self.assertEqual(hint_sender.discovery_port, 8090)
        self.assertEqual(hint_sender.node_id, 1)
    
    def test_discovery_hint_sender_without_config(self):
        """Test initialization without discovery configuration."""
        # Create args without discovery config
        args_no_discovery = Mock()
        args_no_discovery.discovery_host = None
        args_no_discovery.discovery_port = None
        args_no_discovery.replication_interval = 1.0
        
        # Create consensus without discovery
        consensus_no_discovery = RaftConsensus(
            raft_node=self.raft_node,
            worker_manager=self.mock_worker_manager,
            args=args_no_discovery
        )
        
        # Should not have discovery hint sender
        self.assertIsNone(consensus_no_discovery.discovery_hint_sender)
        self.assertFalse(consensus_no_discovery.is_discovery_enabled())
    
    def test_start_stop_discovery_integration(self):
        """Test starting and stopping with discovery integration."""
        # Mock the hint sender to avoid actual network calls
        mock_hint_sender = Mock()
        self.consensus.discovery_hint_sender = mock_hint_sender
        
        # Start consensus
        self.consensus.start()
        self.assertTrue(self.consensus.is_running)
        
        # Should have started the hint sender
        mock_hint_sender.start.assert_called_once()
        
        # Stop consensus
        self.consensus.stop()
        self.assertFalse(self.consensus.is_running)
        
        # Should have stopped the hint sender
        mock_hint_sender.stop.assert_called_once()
    
    def test_state_change_updates_discovery_hints(self):
        """Test that state changes update discovery hints."""
        # Mock the hint sender
        mock_hint_sender = Mock()
        self.consensus.discovery_hint_sender = mock_hint_sender
        
        # Test becoming leader
        self.consensus.handle_state_change(RaftState.LEADER)
        
        # Should update hint sender for leader state
        mock_hint_sender.update_raft_state.assert_called_with(
            new_state=HintRaftState.LEADER,
            term=self.raft_node.current_term,
            known_nodes=self.raft_node.known_nodes
        )
        
        # Reset mock
        mock_hint_sender.reset_mock()
        
        # Test becoming follower
        self.consensus.handle_state_change(RaftState.FOLLOWER)
        
        # Should update hint sender for follower state
        mock_hint_sender.update_raft_state.assert_called_with(
            new_state=HintRaftState.FOLLOWER,
            term=self.raft_node.current_term,
            known_nodes=self.raft_node.known_nodes
        )
    
    def test_membership_change_updates_discovery_hints(self):
        """Test that membership changes update discovery hints."""
        # Mock the hint sender
        mock_hint_sender = Mock()
        self.consensus.discovery_hint_sender = mock_hint_sender
        
        # Set node as leader
        self.raft_node.state = RaftState.LEADER
        
        # Test membership change
        new_nodes = {1, 2, 3, 4}
        self.consensus.on_membership_change(new_nodes, round_num=5)
        
        # Should update hint sender with new membership
        mock_hint_sender.update_raft_state.assert_called_with(
            new_state=HintRaftState.LEADER,
            term=self.raft_node.current_term,
            known_nodes=new_nodes
        )
    
    def test_helper_methods(self):
        """Test discovery integration helper methods."""
        # Test leader status
        self.raft_node.state = RaftState.LEADER
        self.assertTrue(self.consensus.is_leader())
        
        self.raft_node.state = RaftState.FOLLOWER
        self.assertFalse(self.consensus.is_leader())
        
        # Test known nodes
        known_nodes = self.consensus.get_known_nodes()
        self.assertEqual(known_nodes, {1, 2, 3})
        
        # Test current term
        self.raft_node.current_term = 5
        self.assertEqual(self.consensus.get_current_term(), 5)
        
        # Test discovery stats
        mock_hint_sender = Mock()
        mock_hint_sender.get_stats.return_value = {"test": "stats"}
        self.consensus.discovery_hint_sender = mock_hint_sender
        
        stats = self.consensus.get_discovery_stats()
        self.assertEqual(stats, {"test": "stats"})
    
    def test_set_discovery_service(self):
        """Test dynamically setting discovery service."""
        # Start with no discovery
        self.consensus.discovery_hint_sender = None
        self.assertFalse(self.consensus.is_discovery_enabled())
        
        # Set discovery service
        self.consensus.set_discovery_service("example.com", 9090)
        
        # Should now have discovery enabled
        self.assertTrue(self.consensus.is_discovery_enabled())
        self.assertEqual(self.consensus.discovery_hint_sender.discovery_host, "example.com")
        self.assertEqual(self.consensus.discovery_hint_sender.discovery_port, 9090)


if __name__ == '__main__':
    # Configure logging for tests
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Run tests
    unittest.main(verbosity=2)
