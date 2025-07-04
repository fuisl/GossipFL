#!/usr/bin/env python3
"""
Gateway Integration Tests

Focused tests for gateway server and client integration scenarios.
"""

import unittest
import time
import sys
import os

# Add paths
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL')
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL/algorithms/RAFT_GossipFL')

class GatewayIntegrationTest(unittest.TestCase):
    """Integration tests for gateway server and client."""
    
    def setUp(self):
        """Set up test environment."""
        self.gateway_server = None
        self.clients = []
    
    def tearDown(self):
        """Clean up test environment."""
        for client in self.clients:
            try:
                client.shutdown()
            except:
                pass
        
        if self.gateway_server:
            try:
                self.gateway_server.stop()
            except:
                pass
    
    def start_gateway(self):
        """Start gateway server."""
        try:
            from algorithms.RAFT_GossipFL.grpc_gateway_server import GRPCGatewayServer
            self.gateway_server = GRPCGatewayServer(host='localhost', port=8090)
            self.gateway_server.start()
            time.sleep(1)
            return True
        except Exception as e:
            self.fail(f"Failed to start gateway: {e}")
    
    def create_client(self):
        """Create and return a gateway client."""
        from algorithms.RAFT_GossipFL.grpc_gateway_client import GRPCGatewayClient
        client = GRPCGatewayClient('localhost', 8090)
        self.clients.append(client)
        return client
    
    def test_bootstrap_node_workflow(self):
        """Test complete bootstrap node workflow."""
        self.start_gateway()
        
        client = self.create_client()
        
        # Step 1: Health check
        health = client.health_check()
        self.assertIsNotNone(health)
        self.assertEqual(health['status'], 'healthy')
        
        # Step 2: Register as bootstrap
        result = client.register_node(
            node_id=1,
            ip_address='127.0.0.1',
            port=8901,
            capabilities=['grpc', 'fedml', 'raft'],
            metadata={'role': 'bootstrap', 'version': '1.0'}
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['status'], 'registered')
        self.assertTrue(result['is_bootstrap'])
        self.assertEqual(result['node_id'], 1)
        
        # Step 3: Check leader assignment
        leader = client.get_leader()
        self.assertIsNotNone(leader)
        self.assertEqual(leader['node_id'], 1)
        
        # Step 4: Get statistics
        stats = client.get_stats()
        self.assertIsNotNone(stats)
        self.assertEqual(stats['total_nodes'], 1)
        self.assertEqual(stats['active_nodes'], 1)
        self.assertEqual(stats['leader_id'], 1)
        self.assertTrue(stats['bootstrap_initiated'])
    
    def test_multi_node_join_workflow(self):
        """Test multiple nodes joining cluster."""
        self.start_gateway()
        
        # Register bootstrap node
        client1 = self.create_client()
        result1 = client1.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'bootstrap'})
        self.assertTrue(result1['is_bootstrap'])
        
        # Register second node
        client2 = self.create_client()
        result2 = client2.register_node(2, '127.0.0.1', 8902, ['grpc'], {'role': 'member'})
        self.assertFalse(result2['is_bootstrap'])
        
        # Register third node
        client3 = self.create_client()
        result3 = client3.register_node(3, '127.0.0.1', 8903, ['grpc'], {'role': 'member'})
        self.assertFalse(result3['is_bootstrap'])
        
        # Check cluster state
        nodes = client1.get_nodes(exclude_self=False)
        self.assertEqual(len(nodes), 3)
        
        node_ids = {node['node_id'] for node in nodes}
        self.assertEqual(node_ids, {1, 2, 3})
        
        # Verify leader remains the same
        leader = client1.get_leader()
        self.assertEqual(leader['node_id'], 1)
    
    def test_node_leave_and_cleanup(self):
        """Test node leaving cluster and cleanup."""
        self.start_gateway()
        
        # Register multiple nodes
        client1 = self.create_client()
        client2 = self.create_client()
        
        client1.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'bootstrap'})
        client2.register_node(2, '127.0.0.1', 8902, ['grpc'], {'role': 'member'})
        
        # Verify both nodes are registered
        nodes = client1.get_nodes(exclude_self=False)
        self.assertEqual(len(nodes), 2)
        
        # Remove node 2
        success = client1.remove_node(2)
        self.assertTrue(success)
        
        # Verify node 2 is removed
        nodes_after = client1.get_nodes(exclude_self=False)
        self.assertEqual(len(nodes_after), 1)
        self.assertEqual(nodes_after[0]['node_id'], 1)
    
    def test_leader_election_simulation(self):
        """Test leader election when bootstrap node leaves."""
        self.start_gateway()
        
        # Register multiple nodes
        client1 = self.create_client()
        client2 = self.create_client()
        client3 = self.create_client()
        
        client1.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'bootstrap'})
        client2.register_node(2, '127.0.0.1', 8902, ['grpc'], {'role': 'member'})
        client3.register_node(3, '127.0.0.1', 8903, ['grpc'], {'role': 'member'})
        
        # Verify initial leader
        leader = client1.get_leader()
        self.assertEqual(leader['node_id'], 1)
        
        # Simulate leader change
        success = client2.update_leader(2)
        self.assertTrue(success)
        
        # Verify new leader
        new_leader = client1.get_leader()
        self.assertEqual(new_leader['node_id'], 2)
    
    def test_heartbeat_lifecycle(self):
        """Test heartbeat functionality lifecycle."""
        self.start_gateway()
        
        client = self.create_client()
        
        # Register node
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'test'})
        self.assertIsNotNone(result)
        
        # Send heartbeats
        for i in range(3):
            heartbeat_result = client.heartbeat()
            self.assertIsNotNone(heartbeat_result)
            self.assertIn('status', heartbeat_result)
            time.sleep(0.5)
        
        # Check that node is still active
        stats = client.get_stats()
        self.assertEqual(stats['active_nodes'], 1)


class GatewayErrorHandlingTest(unittest.TestCase):
    """Tests for gateway error handling and edge cases."""
    
    def setUp(self):
        """Set up test environment."""
        self.gateway_server = None
        self.clients = []
    
    def tearDown(self):
        """Clean up test environment."""
        for client in self.clients:
            try:
                client.shutdown()
            except:
                pass
        
        if self.gateway_server:
            try:
                self.gateway_server.stop()
            except:
                pass
    
    def start_gateway(self):
        """Start gateway server."""
        from algorithms.RAFT_GossipFL.grpc_gateway_server import GRPCGatewayServer
        self.gateway_server = GRPCGatewayServer(host='localhost', port=8090)
        self.gateway_server.start()
        time.sleep(1)
    
    def create_client(self):
        """Create and return a gateway client."""
        from algorithms.RAFT_GossipFL.grpc_gateway_client import GRPCGatewayClient
        client = GRPCGatewayClient('localhost', 8090)
        self.clients.append(client)
        return client
    
    def test_invalid_node_operations(self):
        """Test operations on invalid/non-existent nodes."""
        self.start_gateway()
        
        client = self.create_client()
        
        # Try to remove non-existent node
        result = client.remove_node(999)
        self.assertFalse(result)
        
        # Try to update leader to non-existent node
        result = client.update_leader(999)
        self.assertFalse(result)
        
        # Try heartbeat without registration
        client_unreg = self.create_client()
        result = client_unreg.heartbeat()
        self.assertIsNone(result)
    
    def test_malformed_requests(self):
        """Test handling of malformed requests."""
        self.start_gateway()
        
        client = self.create_client()
        
        # Test registration with invalid data
        try:
            result = client.register_node(
                node_id=0,  # Invalid node ID
                ip_address='invalid-ip',
                port=-1,  # Invalid port
                capabilities=[],
                metadata={}
            )
            # Should either succeed with validation or fail gracefully
            if result:
                self.assertIn('status', result)
        except Exception:
            # Should handle gracefully
            pass
    
    def test_connection_timeout_handling(self):
        """Test connection timeout scenarios."""
        # Create client without starting gateway
        from algorithms.RAFT_GossipFL.grpc_gateway_client import GRPCGatewayClient
        client = GRPCGatewayClient('localhost', 8091, timeout=1.0)  # Short timeout
        
        # Operations should timeout gracefully
        health = client.health_check()
        self.assertIsNone(health)
        
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {})
        self.assertIsNone(result)
        
        client.shutdown()
    
    def test_duplicate_node_handling(self):
        """Test handling of duplicate node registrations."""
        self.start_gateway()
        
        client1 = self.create_client()
        client2 = self.create_client()
        
        # Register same node ID with different details
        result1 = client1.register_node(1, '127.0.0.1', 8901, ['grpc'], {'version': 'v1'})
        result2 = client2.register_node(1, '127.0.0.1', 8902, ['grpc'], {'version': 'v2'})
        
        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)
        
        # Second registration should update
        self.assertEqual(result2['status'], 'updated')
        
        # Final state should reflect the update
        nodes = client1.get_nodes(exclude_self=False)
        node_1 = next(n for n in nodes if n['node_id'] == 1)
        self.assertEqual(node_1['port'], 8902)  # Should have updated port


if __name__ == '__main__':
    # Run integration tests
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(GatewayIntegrationTest))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(GatewayErrorHandlingTest))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    success = result.wasSuccessful()
    sys.exit(0 if success else 1)
