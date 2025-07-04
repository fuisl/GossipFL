#!/usr/bin/env python3

"""
Integration test for gRPC-based service discovery with hints.
"""

import unittest
import time
import threading
import logging
from pure_service_discovery import PureServiceDiscoveryServer
from grpc_hint_sender import GRPCHintSender, RaftState


class TestGRPCIntegration(unittest.TestCase):
    """Test gRPC integration with service discovery."""
    
    def setUp(self):
        """Set up test environment."""
        # Start discovery server
        self.server = PureServiceDiscoveryServer(
            host='localhost',
            port=8093,
            max_workers=5,
            node_timeout=60.0
        )
        self.server.start()
        time.sleep(0.5)  # Let server start
        
        # Create hint sender
        self.hint_sender = GRPCHintSender(
            discovery_host='localhost',
            discovery_port=8093,
            node_id=1,
            send_interval=30.0
        )
    
    def tearDown(self):
        """Clean up test environment."""
        self.hint_sender.stop()
        self.server.stop()
    
    def test_grpc_hint_integration(self):
        """Test complete gRPC hint flow."""
        discovery = self.server.get_discovery_service()
        
        # Register nodes
        discovery.register_node(1, "192.168.1.1", 8080, ["raft"])
        discovery.register_node(2, "192.168.1.2", 8080, ["raft"])
        
        # Start hint sender
        self.hint_sender.start()
        
        # Update RAFT state to leader
        self.hint_sender.update_raft_state(
            new_state=RaftState.LEADER,
            term=1,
            known_nodes={1, 2}
        )
        
        # Wait for hint to be sent
        time.sleep(1.0)
        
        # Check that hint was received
        self.assertEqual(discovery.last_known_leader, 1)
        
        # Test hint in discovery response
        nodes = discovery.get_node_list(requesting_node_id=3)  # New node
        bootstrap_info = discovery.get_bootstrap_info()
        
        self.assertEqual(bootstrap_info["last_known_leader"], 1)
        self.assertTrue(bootstrap_info["bootstrap_initiated"])
        
        # Check stats
        stats = self.hint_sender.get_stats()
        self.assertEqual(stats["node_id"], 1)
        self.assertEqual(stats["current_state"], "leader")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main()
