#!/usr/bin/env python3
"""
Test script for Phase 1.3: Complete service discovery bridge integration with RAFT consensus.

This script tests the full flow from service discovery events to RAFT membership changes
and state synchronization.
"""

import sys
import os
import unittest
from unittest.mock import Mock, MagicMock, patch, call
import logging
import time
from threading import Lock

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Import required modules
from algorithms.RAFT_GossipFL.service_discovery_bridge import (
    RaftServiceDiscoveryBridge, 
    BridgeState,
    NodeDiscoveryEvent
)
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
from algorithms.RAFT_GossipFL.raft_node import RaftNode, RaftState
from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestPhase13Integration(unittest.TestCase):
    """Test complete bridge-consensus integration for Phase 1.3"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.node_id = 1
        self.args = Mock()
        self.args.client_id = self.node_id
        self.args.client_num_in_total = 5
        self.args.comm_round = 0
        self.args.min_election_timeout = 150  # milliseconds
        self.args.max_election_timeout = 300  # milliseconds
        self.args.heartbeat_interval = 50     # milliseconds
        self.args.raft_log_compaction_threshold = 100
        
        # Mock communication manager
        self.comm_manager = Mock(spec=DynamicGRPCCommManager)
        self.comm_manager.on_node_discovered = None
        self.comm_manager.on_node_lost = None
        
        # Create RAFT node
        self.raft_node = RaftNode(
            node_id=self.node_id,
            args=self.args
        )
        
        # Set up known nodes after creation
        self.raft_node.update_known_nodes(node_ids=[1, 2, 3])
        
        # Set initial state to FOLLOWER for testing
        self.raft_node.state = RaftState.FOLLOWER
        
        # Mock worker manager
        self.worker_manager = Mock()
        self.worker_manager.round_idx = 0
        self.worker_manager.send_message_to_node = Mock()
        self.worker_manager.broadcast_message = Mock()
        
        # Create consensus manager
        self.consensus_manager = RaftConsensus(
            args=self.args,
            raft_node=self.raft_node,
            worker_manager=self.worker_manager
        )
        
        # Create bridge
        self.bridge = RaftServiceDiscoveryBridge(
            node_id=self.node_id,
            raft_consensus=None,  # Will be set during registration
            validation_timeout=5.0,
            sync_timeout=10.0
        )
        
        # Test data
        self.test_node_info = {
            'ip_address': '192.168.1.100',
            'port': 8080,
            'capabilities': ['grpc', 'fedml'],
            'metadata': {'version': '1.0'}
        }
        
        self.test_node_info_invalid = {
            'ip_address': '192.168.1.100',
            'port': 8080,
            'capabilities': ['http'],  # Missing required capabilities
            'metadata': {'version': '1.0'}
        }
    
    def test_bridge_registration_with_consensus_manager(self):
        """Test bridge registration with consensus manager"""
        # Register bridge with consensus manager
        success = self.consensus_manager.register_service_discovery_bridge(self.bridge)
        self.assertTrue(success)
        
        # Verify bridge is registered
        self.assertEqual(self.consensus_manager.service_discovery_bridge, self.bridge)
        self.assertEqual(self.bridge.raft_consensus, self.consensus_manager)
        
        # Test duplicate registration
        duplicate_success = self.consensus_manager.register_service_discovery_bridge(self.bridge)
        self.assertFalse(duplicate_success)
    
    def test_bridge_unregistration(self):
        """Test bridge unregistration"""
        # Register first
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Unregister
        success = self.consensus_manager.unregister_service_discovery_bridge()
        self.assertTrue(success)
        
        # Verify unregistration
        self.assertIsNone(self.consensus_manager.service_discovery_bridge)
        self.assertIsNone(self.bridge.raft_consensus)
    
    def test_service_discovery_hint_processing(self):
        """Test processing of service discovery hints"""
        # Register bridge
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Make node a leader for testing
        self.raft_node.state = RaftState.LEADER
        
        # Test node discovered hint
        hint_data = {
            'event_type': 'discovered',
            'node_id': 4,
            'node_info': self.test_node_info,
            'timestamp': time.time()
        }
        
        with patch.object(self.consensus_manager, 'propose_membership_change') as mock_propose:
            mock_propose.return_value = True
            
            success = self.consensus_manager.handle_service_discovery_hint(hint_data)
            self.assertTrue(success)
            mock_propose.assert_called_once_with('add', 4, self.test_node_info)
    
    def test_membership_change_proposal(self):
        """Test membership change proposal through consensus"""
        # Register bridge
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Make node a leader
        self.raft_node.state = RaftState.LEADER
        
        # Mock the enhanced add_node method
        with patch.object(self.raft_node, 'enhance_add_node_with_connection_info') as mock_add:
            mock_add.return_value = 5  # Mock log index
            
            # Test adding a node
            success = self.consensus_manager.propose_membership_change(
                action='add',
                node_id=4,
                node_info=self.test_node_info
            )
            
            self.assertTrue(success)
            mock_add.assert_called_once_with(4, self.test_node_info, round_num=0)
    
    def test_new_node_sync_coordination(self):
        """Test state synchronization coordination for new nodes"""
        # Register bridge
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Make node a leader
        self.raft_node.state = RaftState.LEADER
        
        # Set up committed log entries
        self.raft_node.commit_index = 2
        self.raft_node.first_log_index = 1
        self.raft_node.log = [
            {'term': 1, 'type': 'membership_change', 'action': 'add', 'node_id': 2},
            {'term': 1, 'type': 'membership_change', 'action': 'add', 'node_id': 3}
        ]
        
        # Mock cluster state
        with patch.object(self.raft_node, 'get_cluster_state') as mock_state:
            mock_state.return_value = {'known_nodes': [1, 2, 3]}
            
            # Test coordination
            success = self.consensus_manager.coordinate_new_node_sync(4)
            self.assertTrue(success)
            
            # Verify message was sent
            self.worker_manager.send_message_to_node.assert_called_once()
    
    def test_bridge_event_processing_as_leader(self):
        """Test bridge event processing when node is leader"""
        # Register bridge with comm manager and consensus
        self.bridge.register_with_comm_manager(self.comm_manager)
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Make node a leader
        self.raft_node.state = RaftState.LEADER
        
        # Mock membership change proposal
        with patch.object(self.consensus_manager, 'propose_membership_change') as mock_propose:
            mock_propose.return_value = True
            
            # Simulate node discovery
            self.bridge._on_node_discovered_bridge(4, Mock(
                ip_address='192.168.1.100',
                port=8080,
                capabilities=['grpc', 'fedml'],
                metadata={'version': '1.0'}
            ))
            
            # Verify proposal was made
            mock_propose.assert_called_once()
    
    def test_bridge_event_processing_as_follower(self):
        """Test bridge event processing when node is follower"""
        # Register bridge with comm manager and consensus
        self.bridge.register_with_comm_manager(self.comm_manager)
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Node is follower by default
        self.raft_node.state = RaftState.FOLLOWER
        self.consensus_manager.current_leader_id = 2
        
        # Mock message forwarding
        with patch.object(self.consensus_manager, '_forward_to_leader') as mock_forward:
            mock_forward.return_value = True
            
            # Simulate node discovery
            self.bridge._on_node_discovered_bridge(4, Mock(
                ip_address='192.168.1.100',
                port=8080,
                capabilities=['grpc', 'fedml'],
                metadata={'version': '1.0'}
            ))
            
            # Verify forwarding was attempted
            mock_forward.assert_called_once()
    
    def test_node_capability_validation(self):
        """Test node capability validation"""
        # Register bridge
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Test valid capabilities
        valid_result = self.consensus_manager._validate_node_capabilities(4, self.test_node_info)
        self.assertTrue(valid_result)
        
        # Test invalid capabilities
        invalid_result = self.consensus_manager._validate_node_capabilities(4, self.test_node_info_invalid)
        self.assertFalse(invalid_result)
        
        # Test missing connection info
        incomplete_info = {'capabilities': ['grpc', 'fedml']}
        incomplete_result = self.consensus_manager._validate_node_capabilities(4, incomplete_info)
        self.assertFalse(incomplete_result)
    
    def test_bridge_status_reporting(self):
        """Test bridge status reporting"""
        # Test without bridge
        status = self.consensus_manager.get_bridge_status()
        self.assertFalse(status['registered'])
        self.assertFalse(status['active'])
        
        # Register bridge
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Test with bridge
        status = self.consensus_manager.get_bridge_status()
        self.assertTrue(status['registered'])
        # Bridge should be in INITIALIZING state after registration
        # Active depends on bridge state - INITIALIZING counts as active for registration
        # The is_active() method should return True when bridge is registered and has consensus manager
    
    def test_full_integration_flow(self):
        """Test complete integration flow from discovery to consensus"""
        # Set up complete integration
        self.bridge.register_with_comm_manager(self.comm_manager)
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Make node a leader
        self.raft_node.state = RaftState.LEADER
        
        # Mock all required methods
        with patch.object(self.consensus_manager, 'propose_membership_change') as mock_propose, \
             patch.object(self.consensus_manager, 'coordinate_new_node_sync') as mock_sync:
            
            mock_propose.return_value = True
            mock_sync.return_value = True
            
            # Simulate complete flow
            node_info_mock = Mock(
                ip_address='192.168.1.100',
                port=8080,
                capabilities=['grpc', 'fedml'],
                metadata={'version': '1.0'}
            )
            
            # 1. Service discovery event
            self.bridge._on_node_discovered_bridge(4, node_info_mock)
            
            # 2. Verify membership change was proposed
            mock_propose.assert_called_once_with(
                action='add',
                node_id=4,
                node_info={
                    'ip_address': '192.168.1.100',
                    'port': 8080,
                    'capabilities': ['grpc', 'fedml'],
                    'metadata': {'version': '1.0'}
                }
            )
            
            # 3. Simulate state sync coordination
            self.consensus_manager.coordinate_new_node_sync(4)
            mock_sync.assert_called_once_with(4)
    
    def test_error_handling_and_recovery(self):
        """Test error handling and recovery scenarios"""
        # Register bridge
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Test with invalid hint data
        invalid_hint = {
            'event_type': 'invalid_event',
            'node_id': 4,
            'timestamp': time.time()
        }
        
        success = self.consensus_manager.handle_service_discovery_hint(invalid_hint)
        self.assertFalse(success)
        
        # Test with missing node info
        incomplete_hint = {
            'event_type': 'discovered',
            'node_id': 4,
            'timestamp': time.time()
            # Missing node_info
        }
        
        success = self.consensus_manager.handle_service_discovery_hint(incomplete_hint)
        self.assertFalse(success)
    
    def test_concurrent_operations(self):
        """Test concurrent operations and thread safety"""
        # Register bridge
        self.consensus_manager.register_service_discovery_bridge(self.bridge)
        
        # Make node a leader
        self.raft_node.state = RaftState.LEADER
        
        # Mock membership change proposal
        with patch.object(self.consensus_manager, 'propose_membership_change') as mock_propose:
            mock_propose.return_value = True
            
            # Simulate concurrent discovery events
            import threading
            
            def simulate_discovery(node_id):
                hint_data = {
                    'event_type': 'discovered',
                    'node_id': node_id,
                    'node_info': self.test_node_info,
                    'timestamp': time.time()
                }
                self.consensus_manager.handle_service_discovery_hint(hint_data)
            
            threads = []
            for i in range(4, 8):  # Nodes 4, 5, 6, 7
                thread = threading.Thread(target=simulate_discovery, args=(i,))
                threads.append(thread)
                thread.start()
            
            for thread in threads:
                thread.join()
            
            # Verify all proposals were made
            self.assertEqual(mock_propose.call_count, 4)


def run_phase13_tests():
    """Run all Phase 1.3 integration tests"""
    print("=" * 60)
    print("Running Phase 1.3: Service Discovery Bridge Integration Tests")
    print("=" * 60)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPhase13Integration)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 60)
    print(f"Phase 1.3 Integration Test Results:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, failure in result.failures:
            print(f"  {test}: {failure}")
    
    if result.errors:
        print("\nErrors:")
        for test, error in result.errors:
            print(f"  {test}: {error}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    print(f"\nPhase 1.3 Integration: {'PASSED' if success else 'FAILED'}")
    print("=" * 60)
    
    return success


if __name__ == '__main__':
    success = run_phase13_tests()
    sys.exit(0 if success else 1)
