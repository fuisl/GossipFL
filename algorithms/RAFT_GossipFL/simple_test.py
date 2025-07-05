#!/usr/bin/env python3
"""
Simple Phase 1.3 integration test
"""

import sys
import os
import unittest
from unittest.mock import Mock

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from algorithms.RAFT_GossipFL.raft_node import RaftNode, RaftState
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
from algorithms.RAFT_GossipFL.service_discovery_bridge import RaftServiceDiscoveryBridge

class SimpleTest(unittest.TestCase):
    def test_basic_setup(self):
        """Test basic setup of components"""
        # Create args
        args = Mock()
        args.client_id = 1
        args.client_num_in_total = 3
        args.comm_round = 0
        args.min_election_timeout = 150
        args.max_election_timeout = 300
        args.heartbeat_interval = 50
        args.raft_log_compaction_threshold = 100
        
        # Create RaftNode
        node = RaftNode(node_id=1, args=args)
        node.update_known_nodes(node_ids=[1, 2, 3])
        node.state = RaftState.FOLLOWER
        
        # Create worker manager mock
        worker_manager = Mock()
        worker_manager.round_idx = 0
        worker_manager.send_message_to_node = Mock()
        worker_manager.broadcast_message = Mock()
        
        # Create consensus manager
        consensus = RaftConsensus(args=args, raft_node=node, worker_manager=worker_manager)
        
        # Create bridge
        bridge = RaftServiceDiscoveryBridge(node_id=1, raft_consensus=None, timeout=5.0)
        
        # Test registration
        success = consensus.register_service_discovery_bridge(bridge)
        self.assertTrue(success)
        
        # Test bridge reference
        self.assertEqual(bridge.raft_consensus, consensus)
        
        print("✓ Basic setup test passed")

if __name__ == '__main__':
    unittest.main()
