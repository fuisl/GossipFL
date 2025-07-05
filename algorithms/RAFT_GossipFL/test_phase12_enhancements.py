#!/usr/bin/env python3

import sys
import os
import unittest
from unittest.mock import Mock, patch, MagicMock
import logging

# Add the RAFT_GossipFL directory to sys.path
sys.path.insert(0, '/workspaces/GossipFL')
sys.path.insert(0, '/workspaces/GossipFL/algorithms/RAFT_GossipFL')

# Now we can import the classes
from raft_node import RaftNode, RaftState

class TestRaftNodePhase12(unittest.TestCase):
    """Test suite for Phase 1.2 RAFT node enhancements"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create mock args
        self.mock_args = Mock()
        self.mock_args.client_num_in_total = 3
        self.mock_args.bootstrap = False
        self.mock_args.min_election_timeout = 150
        self.mock_args.max_election_timeout = 300
        self.mock_args.heartbeat_interval = 50
        self.mock_args.raft_log_compaction_threshold = 100
        
        # Create RAFT node
        self.node = RaftNode(node_id=1, args=self.mock_args)
        
        # Set up logging
        logging.basicConfig(level=logging.DEBUG)
        
    def test_initial_state_transition_requirements(self):
        """Test that INITIAL to FOLLOWER transition requires log sync and model params"""
        # Node should start in INITIAL state
        self.assertEqual(self.node.state, RaftState.INITIAL)
        
        # Should not be able to transition without requirements
        result = self.node.transition_to_follower_from_initial()
        self.assertFalse(result)
        self.assertEqual(self.node.state, RaftState.INITIAL)
        
        # Set up known nodes but missing other requirements
        self.node.known_nodes = {1, 2, 3}
        result = self.node.transition_to_follower_from_initial()
        self.assertFalse(result)
        self.assertEqual(self.node.state, RaftState.INITIAL)
        
    def test_state_sync_completion_flag(self):
        """Test state sync completion flag functionality"""
        # Initially should not be set
        self.assertFalse(self.node._is_log_synchronized())
        
        # Set state sync completed
        self.node.set_state_sync_completed(True)
        self.assertTrue(hasattr(self.node, 'state_sync_completed'))
        self.assertTrue(self.node.state_sync_completed)
        
        # But still missing model params
        self.assertFalse(self.node._has_model_parameters())
        
    def test_model_parameters_setting(self):
        """Test model parameters setting and validation"""
        # Initially should not have model params
        self.assertFalse(self.node._has_model_parameters())
        
        # Set model parameters
        mock_model_state = {'layer1': [1, 2, 3], 'layer2': [4, 5, 6]}
        self.node.set_model_parameters(mock_model_state, model_version=1)
        
        # Should now have model params
        self.assertTrue(self.node._has_model_parameters())
        self.assertEqual(self.node.current_model_version, 1)
        self.assertEqual(self.node.model_state, mock_model_state)
        
    def test_complete_transition_to_follower(self):
        """Test complete transition from INITIAL to FOLLOWER with all requirements"""
        # Set up all requirements
        self.node.known_nodes = {1, 2, 3}
        self.node.total_nodes = 3
        self.node.majority = 2
        
        # Mock state change callback first (before any transitions happen)
        state_change_callback = Mock()
        self.node.on_state_change = state_change_callback
        
        # Verify we start in INITIAL state
        self.assertEqual(self.node.state, RaftState.INITIAL)
        
        # Set state sync completed and proper indices
        self.node.set_state_sync_completed(True)
        self.node.commit_index = 0
        self.node.last_applied = 0
        
        # Verify still in INITIAL state (only log sync requirement met)
        self.assertEqual(self.node.state, RaftState.INITIAL)
        
        # Set model parameters - this should trigger automatic transition
        mock_model_state = {'layer1': [1, 2, 3]}
        self.node.set_model_parameters(mock_model_state, model_version=1)
        
        # Verify transition happened automatically when both requirements were met
        self.assertEqual(self.node.state, RaftState.FOLLOWER)
        
        # Verify callback was called during automatic transition
        state_change_callback.assert_called_with(RaftState.FOLLOWER)
        
        # Verify all requirements are satisfied
        self.assertTrue(self.node._is_log_synchronized())
        self.assertTrue(self.node._has_model_parameters())
        self.assertTrue(getattr(self.node, 'state_sync_completed', False))
        self.assertTrue(getattr(self.node, 'model_params_received', False))
        
    def test_bootstrap_detection_single_node(self):
        """Test bootstrap detection for single node cluster"""
        # Mock state change callback
        state_change_callback = Mock()
        self.node.on_state_change = state_change_callback
        
        # Check initial state
        self.assertEqual(self.node.state, RaftState.INITIAL)
        
        # Clear known nodes first (they get initialized with all nodes)
        self.node.known_nodes = set()
        
        # Single node discovery
        result = self.node.bootstrap_detection({1})
        
        self.assertTrue(result)
        self.assertEqual(self.node.state, RaftState.LEADER)
        self.assertEqual(self.node.current_term, 1)
        
    def test_bootstrap_detection_multi_node(self):
        """Test bootstrap detection for multi-node cluster"""
        # Mock state change callback
        state_change_callback = Mock()
        self.node.on_state_change = state_change_callback
        
        # Mock consensus manager for state sync request
        mock_consensus_manager = Mock()
        self.node.consensus_manager = mock_consensus_manager
        
        # Clear known nodes first (they get initialized with all nodes)
        self.node.known_nodes = set()
        
        # Multi-node discovery
        result = self.node.bootstrap_detection({1, 2, 3})
        self.assertTrue(result)
        self.assertEqual(self.node.state, RaftState.FOLLOWER)
        
        # Should have requested state sync
        mock_consensus_manager.broadcast_state_sync_request.assert_called_once()
        
    def test_initialize_from_discovery(self):
        """Test initialization from service discovery"""
        # Mock connection info
        node_info_map = {
            1: {'ip_address': '192.168.1.1', 'port': 8891},
            2: {'ip_address': '192.168.1.2', 'port': 8892},
            3: {'ip_address': '192.168.1.3', 'port': 8893}
        }
        
        # Initialize from discovery
        result = self.node.initialize_from_discovery({1, 2, 3}, node_info_map)
        self.assertTrue(result)
        
        # Should have stored connection info
        self.assertTrue(hasattr(self.node, 'node_connection_info'))
        self.assertEqual(len(self.node.node_connection_info), 3)
        self.assertEqual(self.node.node_connection_info[1]['ip_address'], '192.168.1.1')
        
    def test_enhanced_add_node_with_connection_info(self):
        """Test enhanced add_node method with connection information"""
        # Become leader first
        self.node.state = RaftState.LEADER
        self.node.current_term = 1
        self.node.known_nodes = {1}
        
        # Mock add_log_entry
        self.node.add_log_entry = Mock(return_value=1)
        
        # Add node with connection info
        node_info = {'ip_address': '192.168.1.4', 'port': 8894}
        result = self.node.enhance_add_node_with_connection_info(4, node_info, round_num=1)
        
        self.assertEqual(result, 1)
        self.assertIn(4, self.node.known_nodes)
        
        # Verify log entry was called with connection info
        self.node.add_log_entry.assert_called_once()
        call_args = self.node.add_log_entry.call_args[0][0]
        self.assertEqual(call_args['type'], 'membership')
        self.assertEqual(call_args['action'], 'add')
        self.assertEqual(call_args['node_id'], 4)
        self.assertEqual(call_args['node_info'], node_info)
        
    def test_membership_change_with_connection_info(self):
        """Test membership change application with connection information"""
        # Set up node connection info storage
        self.node.node_connection_info = {}
        
        # Mock communication manager
        mock_comm_manager = Mock()
        self.node.comm_manager = mock_comm_manager
        
        # Apply membership change with connection info
        command = {
            'action': 'add',
            'node_id': 5,
            'node_info': {'ip_address': '192.168.1.5', 'port': 8895},
            'current_nodes': [1, 5],
            'round': 1
        }
        
        self.node._apply_membership_change(command)
        
        # Verify node was added
        self.assertIn(5, self.node.known_nodes)
        
        # Verify connection info was stored
        self.assertIn(5, self.node.node_connection_info)
        self.assertEqual(self.node.node_connection_info[5]['ip_address'], '192.168.1.5')
        
        # Verify communication manager was notified
        mock_comm_manager.establish_connection.assert_called_once_with(5, command['node_info'])
        
    def test_state_sync_response_handling(self):
        """Test state sync response handling"""
        # Prepare state sync response
        response = {
            'leader_id': 2,
            'term': 1,
            'known_nodes': [1, 2, 3],
            'commit_index': 0,
            'log_entries': []
        }
        
        # Handle state sync response
        result = self.node.handle_state_sync_response(response)
        self.assertTrue(result)
        
        # Verify state sync completion was set
        self.assertTrue(hasattr(self.node, 'state_sync_completed'))
        self.assertTrue(self.node.state_sync_completed)
        
        # Verify term was updated
        self.assertEqual(self.node.current_term, 1)
        
    def test_can_participate_in_elections(self):
        """Test election participation logic"""
        # INITIAL state cannot participate
        self.assertEqual(self.node.state, RaftState.INITIAL)
        self.assertFalse(self.node.can_participate_in_elections())
        
        # FOLLOWER state can participate
        self.node.state = RaftState.FOLLOWER
        self.node.known_nodes = {1, 2, 3}
        self.assertTrue(self.node.can_participate_in_elections())
        
        # But not without known nodes
        self.node.known_nodes = set()
        self.assertFalse(self.node.can_participate_in_elections())
        
    def test_cluster_state_information(self):
        """Test cluster state information retrieval"""
        # Set up cluster state
        self.node.known_nodes = {1, 2, 3}
        self.node.current_term = 2
        self.node.commit_index = 5
        self.node.state = RaftState.FOLLOWER
        
        # Get cluster state
        state = self.node.get_cluster_state()
        
        # Verify state information
        self.assertEqual(state['known_nodes'], [1, 2, 3])
        self.assertEqual(state['current_term'], 2)
        self.assertEqual(state['state'], 'FOLLOWER')
        self.assertEqual(state['commit_index'], 5)
        self.assertEqual(state['total_nodes'], 3)
        
    def test_manual_transition_to_follower(self):
        """Test manual transition from INITIAL to FOLLOWER with all requirements"""
        # Set up all requirements without triggering automatic transition
        self.node.known_nodes = {1, 2, 3}
        self.node.total_nodes = 3
        self.node.majority = 2
        
        # Mock state change callback
        state_change_callback = Mock()
        self.node.on_state_change = state_change_callback
        
        # Manually set the flags without triggering automatic transition
        self.node.state_sync_completed = True
        self.node.commit_index = 0
        self.node.last_applied = 0
        self.node.model_params_received = True
        self.node.current_model_version = 1
        self.node.model_state = {'layer1': [1, 2, 3]}
        
        # Verify we're still in INITIAL state
        self.assertEqual(self.node.state, RaftState.INITIAL)
        
        # Verify all requirements are met
        self.assertTrue(self.node._is_log_synchronized())
        self.assertTrue(self.node._has_model_parameters())
        
        # Now manually call transition
        result = self.node.transition_to_follower_from_initial()
        self.assertTrue(result)
        self.assertEqual(self.node.state, RaftState.FOLLOWER)
        
        # Verify callback was called
        state_change_callback.assert_called_with(RaftState.FOLLOWER)

if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)
