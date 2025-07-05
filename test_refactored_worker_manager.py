#!/usr/bin/env python3
"""
Test script to verify the refactored RaftWorkerManager with service discovery bridge.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import unittest
from unittest.mock import Mock, MagicMock, patch
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Import the refactored worker manager
from algorithms.RAFT_GossipFL.raft_worker_manager import RaftWorkerManager
from algorithms.RAFT_GossipFL.service_discovery_bridge import RaftServiceDiscoveryBridge


class TestRefactoredWorkerManager(unittest.TestCase):
    """Test the refactored RaftWorkerManager."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.args = Mock()
        self.args.join_existing_cluster = False
        self.args.ip_address = 'localhost'
        self.args.port = 8080
        
        self.comm = Mock()
        self.rank = 0
        self.size = 3
        
        self.worker = Mock()
        self.topology_manager = Mock()
        self.topology_manager.get_topology.return_value = None
        self.topology_manager.get_neighbor_list.return_value = set()
        
        self.model_trainer = Mock()
        self.timer = Mock()
        self.metrics = Mock()
        self.bandwidth_manager = Mock()
        self.bandwidth_manager.get_bandwidth.return_value = None
        
        # Mock RAFT consensus
        self.raft_consensus = Mock()
        self.raft_consensus.start.return_value = None
        self.raft_consensus.stop.return_value = None
        self.raft_consensus.get_leader_id.return_value = 1
        self.raft_consensus.get_known_nodes.return_value = {0, 1, 2}
        self.raft_consensus.is_state_synchronized.return_value = True
        self.raft_consensus.register_service_discovery_bridge.return_value = True
        self.raft_consensus.unregister_service_discovery_bridge.return_value = True
        
        # Mock RAFT node
        self.raft_node = Mock()
        self.raft_node.current_term = 1
        self.raft_node.commit_index = 5
        self.raft_node.state = "FOLLOWER"
        self.raft_consensus.raft_node = self.raft_node
    
    def test_initialization_with_bridge(self):
        """Test that the worker manager initializes with the service discovery bridge."""
        with patch('algorithms.RAFT_GossipFL.raft_worker_manager.DecentralizedWorkerManager.__init__'):
            worker_manager = RaftWorkerManager(
                self.args, self.comm, self.rank, self.size, self.worker,
                self.topology_manager, self.model_trainer, self.timer, self.metrics,
                self.raft_consensus, self.bandwidth_manager
            )
            
            # Check that the bridge was initialized
            self.assertIsNotNone(worker_manager.service_discovery_bridge)
            self.assertIsInstance(worker_manager.service_discovery_bridge, RaftServiceDiscoveryBridge)
            
            # Check that the bridge was registered with consensus
            self.raft_consensus.register_service_discovery_bridge.assert_called_once()
            
            # Check that the bridge was registered with the worker manager
            self.assertEqual(worker_manager.service_discovery_bridge.comm_manager, worker_manager)
    
    def test_join_request_handling(self):
        """Test that join requests are handled through the bridge."""
        with patch('algorithms.RAFT_GossipFL.raft_worker_manager.DecentralizedWorkerManager.__init__'):
            worker_manager = RaftWorkerManager(
                self.args, self.comm, self.rank, self.size, self.worker,
                self.topology_manager, self.model_trainer, self.timer, self.metrics,
                self.raft_consensus, self.bandwidth_manager
            )
            
            # Mock the bridge's handle_join_request method
            worker_manager.service_discovery_bridge.handle_join_request = Mock()
            
            # Create a join request message using standard FedML message format
            msg_params = {
                'sender': 3,
                'node_info': {
                    'ip_address': '192.168.1.100',
                    'port': 8081,
                    'capabilities': ['grpc', 'fedml']
                }
            }
            
            # Handle the join request
            worker_manager.handle_join_request(msg_params)
            
            # Verify the bridge was called
            worker_manager.service_discovery_bridge.handle_join_request.assert_called_once_with(
                3, msg_params['node_info']
            )
    
    def test_membership_change_notification(self):
        """Test that membership changes are properly handled."""
        with patch('algorithms.RAFT_GossipFL.raft_worker_manager.DecentralizedWorkerManager.__init__'):
            worker_manager = RaftWorkerManager(
                self.args, self.comm, self.rank, self.size, self.worker,
                self.topology_manager, self.model_trainer, self.timer, self.metrics,
                self.raft_consensus, self.bandwidth_manager
            )
            
            # Mock the bridge's on_membership_change method
            worker_manager.service_discovery_bridge.on_membership_change = Mock()
            
            # Simulate a membership change
            new_nodes = {0, 1, 2, 3}
            worker_manager.on_membership_change(new_nodes, round_num=1)
            
            # Verify the bridge was notified
            worker_manager.service_discovery_bridge.on_membership_change.assert_called_once_with(new_nodes)
    
    def test_cleanup(self):
        """Test that cleanup properly stops the bridge and consensus."""
        with patch('algorithms.RAFT_GossipFL.raft_worker_manager.DecentralizedWorkerManager.__init__'):
            worker_manager = RaftWorkerManager(
                self.args, self.comm, self.rank, self.size, self.worker,
                self.topology_manager, self.model_trainer, self.timer, self.metrics,
                self.raft_consensus, self.bandwidth_manager
            )
            
            # Mock the bridge's stop method
            worker_manager.service_discovery_bridge.stop = Mock()
            
            # Call cleanup
            worker_manager.cleanup()
            
            # Verify cleanup was called
            worker_manager.service_discovery_bridge.stop.assert_called_once()
            self.raft_consensus.unregister_service_discovery_bridge.assert_called_once()
            self.raft_consensus.stop.assert_called_once()
    
    def test_cluster_join_through_bridge(self):
        """Test that cluster joining uses the bridge."""
        with patch('algorithms.RAFT_GossipFL.raft_worker_manager.DecentralizedWorkerManager.__init__'):
            worker_manager = RaftWorkerManager(
                self.args, self.comm, self.rank, self.size, self.worker,
                self.topology_manager, self.model_trainer, self.timer, self.metrics,
                self.raft_consensus, self.bandwidth_manager
            )
            
            # Mock the bridge's handle_node_discovered method
            worker_manager.service_discovery_bridge.handle_node_discovered = Mock()
            
            # Call request_cluster_join
            result = worker_manager.request_cluster_join()
            
            # Verify the bridge was called
            self.assertTrue(result)
            worker_manager.service_discovery_bridge.handle_node_discovered.assert_called_once()
    
    def test_message_forwarding(self):
        """Test that messages can be forwarded through the worker manager."""
        with patch('algorithms.RAFT_GossipFL.raft_worker_manager.DecentralizedWorkerManager.__init__'):
            worker_manager = RaftWorkerManager(
                self.args, self.comm, self.rank, self.size, self.worker,
                self.topology_manager, self.model_trainer, self.timer, self.metrics,
                self.raft_consensus, self.bandwidth_manager
            )
            
            # Mock the send_message method
            worker_manager.send_message = Mock()
            worker_manager.get_sender_id = Mock(return_value=0)
            
            # Test sending a message to a node
            message = {
                'type': 'test_message',
                'data': 'test_data'
            }
            
            worker_manager.send_message_to_node(1, message)
            
            # Verify the message was sent
            worker_manager.send_message.assert_called_once()


if __name__ == '__main__':
    print("Testing refactored RaftWorkerManager...")
    print("=" * 60)
    
    # Run the tests
    unittest.main(verbosity=2)
