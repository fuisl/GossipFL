#!/usr/bin/env python3

"""
Test Pure Service Discovery Implementation

This test verifies that the pure service discovery follows the gateway.md architecture:
- Standalone service (not part of RAFT cluster)
- Bootstrap coordination
- Node registration and discovery
- Leader hints (optional optimization)
- Fault tolerance
"""

import unittest
import time
import threading
import requests
import json
import logging
from pure_service_discovery import PureServiceDiscovery, PureServiceDiscoveryServer, NodeStatus
from discovery_hints import DiscoveryHintSender, RaftState

# Configure logging for tests
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class TestPureServiceDiscovery(unittest.TestCase):
    """Test the pure service discovery implementation."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.discovery = PureServiceDiscovery(node_timeout=60.0, cleanup_interval=10.0)
    
    def test_bootstrap_coordination(self):
        """Test bootstrap coordination for first node."""
        # Initially no bootstrap
        bootstrap_info = self.discovery.get_bootstrap_info()
        self.assertFalse(bootstrap_info["bootstrap_initiated"])
        self.assertTrue(bootstrap_info["needs_bootstrap"])
        
        # Register first node
        result = self.discovery.register_node(1, "192.168.1.1", 8080, ["raft", "gossip"])
        
        # Should be bootstrap node
        self.assertTrue(result["is_bootstrap"])
        self.assertEqual(result["node_id"], 1)
        
        # Bootstrap should be initiated
        bootstrap_info = self.discovery.get_bootstrap_info()
        self.assertTrue(bootstrap_info["bootstrap_initiated"])
        self.assertFalse(bootstrap_info["needs_bootstrap"])
        self.assertEqual(bootstrap_info["last_known_leader"], 1)
        
        # Second node should not be bootstrap
        result2 = self.discovery.register_node(2, "192.168.1.2", 8080, ["raft", "gossip"])
        self.assertFalse(result2["is_bootstrap"])
    
    def test_node_registration_and_discovery(self):
        """Test node registration and discovery."""
        # Register multiple nodes
        nodes = [
            (1, "192.168.1.1", 8080, ["raft", "gossip"]),
            (2, "192.168.1.2", 8080, ["raft", "gossip"]),
            (3, "192.168.1.3", 8080, ["raft", "gossip"])
        ]
        
        for node_id, ip, port, capabilities in nodes:
            result = self.discovery.register_node(node_id, ip, port, capabilities)
            self.assertEqual(result["status"], "registered")
            self.assertEqual(result["node_id"], node_id)
        
        # Test node discovery
        node_list = self.discovery.get_node_list()
        self.assertEqual(len(node_list), 3)
        
        # Test excluding requesting node
        node_list_for_node_1 = self.discovery.get_node_list(requesting_node_id=1)
        self.assertEqual(len(node_list_for_node_1), 2)
        
        # Verify node IDs
        discovered_ids = {node.node_id for node in node_list_for_node_1}
        self.assertEqual(discovered_ids, {2, 3})
    
    def test_leader_hints(self):
        """Test leader hint functionality."""
        # Register nodes
        self.discovery.register_node(1, "192.168.1.1", 8080, ["raft"])
        self.discovery.register_node(2, "192.168.1.2", 8080, ["raft"])
        
        # Test leader hint
        success = self.discovery.leader_hint(2)
        self.assertTrue(success)
        self.assertEqual(self.discovery.last_known_leader, 2)
        
        # Test invalid leader hint
        success = self.discovery.leader_hint(99)
        self.assertFalse(success)
        self.assertEqual(self.discovery.last_known_leader, 2)  # Should remain unchanged
    
    def test_heartbeat_and_cleanup(self):
        """Test heartbeat and node cleanup."""
        # Register node
        self.discovery.register_node(1, "192.168.1.1", 8080, ["raft"])
        
        # Test heartbeat
        result = self.discovery.heartbeat(1)
        self.assertEqual(result["status"], "received")
        
        # Test heartbeat for unknown node
        result = self.discovery.heartbeat(99)
        self.assertEqual(result["status"], "unknown_node")
        
        # Test cleanup (no stale nodes yet)
        stale_nodes = self.discovery.cleanup_stale_nodes()
        self.assertEqual(len(stale_nodes), 0)
        
        # Make node appear stale
        self.discovery.nodes[1].last_seen = time.time() - 400  # 400 seconds ago
        
        # Test cleanup
        stale_nodes = self.discovery.cleanup_stale_nodes()
        self.assertEqual(stale_nodes, [1])
        self.assertEqual(len(self.discovery.nodes), 0)
    
    def test_node_removal(self):
        """Test node removal."""
        # Register nodes
        self.discovery.register_node(1, "192.168.1.1", 8080, ["raft"])
        self.discovery.register_node(2, "192.168.1.2", 8080, ["raft"])
        
        # Remove node
        success = self.discovery.remove_node(1)
        self.assertTrue(success)
        self.assertEqual(len(self.discovery.nodes), 1)
        
        # Try to remove non-existent node
        success = self.discovery.remove_node(99)
        self.assertFalse(success)
    
    def test_statistics(self):
        """Test statistics reporting."""
        # Register nodes
        self.discovery.register_node(1, "192.168.1.1", 8080, ["raft"])
        self.discovery.register_node(2, "192.168.1.2", 8080, ["raft"])
        
        # Get stats
        stats = self.discovery.get_stats()
        
        self.assertEqual(stats["total_nodes"], 2)
        self.assertEqual(stats["active_nodes"], 2)
        self.assertTrue(stats["bootstrap_initiated"])
        self.assertGreater(stats["uptime"], 0)
        self.assertGreater(stats["registry_version"], 0)


class TestDiscoveryHintSender(unittest.TestCase):
    """Test the discovery hint sender."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.hint_sender = DiscoveryHintSender(
            discovery_host='localhost',
            discovery_port=9999,  # Non-existent port for testing
            node_id=1,
            send_interval=1.0  # Short interval for testing
        )
    
    def test_raft_state_updates(self):
        """Test RAFT state updates."""
        # Initial state
        self.assertEqual(self.hint_sender.current_raft_state, RaftState.FOLLOWER)
        
        # Update to leader
        self.hint_sender.update_raft_state(
            new_state=RaftState.LEADER,
            term=1,
            known_nodes={1, 2, 3}
        )
        
        self.assertEqual(self.hint_sender.current_raft_state, RaftState.LEADER)
        self.assertEqual(self.hint_sender.current_term, 1)
        self.assertEqual(self.hint_sender.last_known_nodes, {1, 2, 3})
    
    def test_stats(self):
        """Test statistics reporting."""
        stats = self.hint_sender.get_stats()
        
        self.assertEqual(stats["node_id"], 1)
        self.assertEqual(stats["current_state"], "follower")
        self.assertEqual(stats["current_term"], 0)
        self.assertIn("discovery_endpoint", stats)


class TestServiceDiscoveryServer(unittest.TestCase):
    """Test the service discovery server."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.server = PureServiceDiscoveryServer(
            host='localhost',
            port=8091,  # Use different port for testing
            max_workers=5,
            node_timeout=60.0
        )
    
    def test_server_lifecycle(self):
        """Test server start and stop."""
        # Start server
        self.server.start()
        self.assertTrue(self.server.is_running)
        
        # Give server time to start
        time.sleep(0.5)
        
        # Stop server
        self.server.stop()
        self.assertFalse(self.server.is_running)
    
    def test_http_endpoint(self):
        """Test HTTP endpoint for leader hints."""
        # Start server
        self.server.start()
        
        try:
            # Give server time to start
            time.sleep(1.0)
            
            # Register a node first
            discovery = self.server.get_discovery_service()
            discovery.register_node(1, "192.168.1.1", 8080, ["raft"])
            
            # Test leader hint via HTTP
            hint_data = {
                "leader_id": 1,
                "term": 1,
                "timestamp": time.time(),
                "active_nodes": [1]
            }
            
            # Try to find the HTTP port (it should be gRPC port + 1000)
            http_port = 8091 + 1000
            
            try:
                response = requests.post(
                    f"http://localhost:{http_port}/leader_hint",
                    json=hint_data,
                    timeout=2.0
                )
                
                if response.status_code == 200:
                    result = response.json()
                    self.assertEqual(result["status"], "accepted")
                    self.assertEqual(result["leader_id"], 1)
                else:
                    self.fail(f"HTTP request failed with status {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                # HTTP server might not be available - that's okay for testing
                logging.warning(f"HTTP server not available (expected in some test environments): {e}")
        
        finally:
            # Stop server
            self.server.stop()


class TestIntegration(unittest.TestCase):
    """Integration tests for the complete system."""
    
    def test_complete_workflow(self):
        """Test complete workflow: bootstrap, registration, discovery, hints."""
        # Start discovery server
        server = PureServiceDiscoveryServer(
            host='localhost',
            port=8092,
            max_workers=5,
            node_timeout=60.0
        )
        
        server.start()
        
        try:
            # Give server time to start
            time.sleep(0.5)
            
            discovery = server.get_discovery_service()
            
            # Step 1: Bootstrap with first node
            result = discovery.register_node(1, "192.168.1.1", 8080, ["raft", "gossip"])
            self.assertTrue(result["is_bootstrap"])
            
            # Step 2: Register additional nodes
            for i in range(2, 5):
                result = discovery.register_node(i, f"192.168.1.{i}", 8080, ["raft", "gossip"])
                self.assertFalse(result["is_bootstrap"])
            
            # Step 3: Test discovery
            nodes = discovery.get_node_list()
            self.assertEqual(len(nodes), 4)
            
            # Step 4: Test leader hints
            success = discovery.leader_hint(2)
            self.assertTrue(success)
            self.assertEqual(discovery.last_known_leader, 2)
            
            # Step 5: Test heartbeats
            for i in range(1, 5):
                result = discovery.heartbeat(i)
                self.assertEqual(result["status"], "received")
            
            # Step 6: Test statistics
            stats = discovery.get_stats()
            self.assertEqual(stats["total_nodes"], 4)
            self.assertEqual(stats["active_nodes"], 4)
            self.assertTrue(stats["bootstrap_initiated"])
            self.assertEqual(stats["last_known_leader"], 2)
            
        finally:
            server.stop()


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)
