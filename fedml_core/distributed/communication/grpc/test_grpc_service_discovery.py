#!/usr/bin/env python3
"""
Comprehensive Test Suite for gRPC Service Discovery

This test suite provides comprehensive testing of the gRPC service discovery system
including gateway server, client interactions, dynamic node management, and failure scenarios.
"""

import unittest
import time
import threading
import subprocess
import sys
import os
import tempfile
import signal
import logging
from typing import Dict, List, Optional
from unittest.mock import Mock, patch

# Add paths
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL')
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL/algorithms/RAFT_GossipFL')

# Configure logging for tests
logging.basicConfig(level=logging.WARNING)  # Reduce noise during tests

class ServiceDiscoveryTestCase(unittest.TestCase):
    """Base test case with common setup for service discovery tests."""
    
    def setUp(self):
        """Set up test environment."""
        self.gateway_process = None
        self.gateway_host = 'localhost'
        self.gateway_port = 8090
        self.test_nodes = []
        self.temp_files = []
        
    def tearDown(self):
        """Clean up test environment."""
        # Clean up nodes
        for node in self.test_nodes:
            try:
                if hasattr(node, 'cleanup'):
                    node.cleanup()
                if hasattr(node, 'stop_receive_message'):
                    node.stop_receive_message()
                if hasattr(node, 'shutdown_gateway_discovery'):
                    node.shutdown_gateway_discovery()
            except:
                pass
        
        # Stop gateway
        if self.gateway_process:
            try:
                self.gateway_process.terminate()
                self.gateway_process.wait(timeout=5)
            except:
                try:
                    self.gateway_process.kill()
                    self.gateway_process.wait()
                except:
                    pass
        
        # Clean up temp files
        for temp_file in self.temp_files:
            try:
                os.unlink(temp_file)
            except:
                pass
    
    def start_gateway_server(self, host='localhost', port=8090):
        """Start gateway server for testing."""
        try:
            from algorithms.RAFT_GossipFL.grpc_gateway_server import GRPCGatewayServer
            
            # Start in-process gateway
            self.gateway_server = GRPCGatewayServer(host=host, port=port)
            self.gateway_server.start()
            time.sleep(1)  # Allow startup
            return True
        except Exception as e:
            self.fail(f"Failed to start gateway server: {e}")
    
    def stop_gateway_server(self):
        """Stop gateway server."""
        if hasattr(self, 'gateway_server'):
            try:
                self.gateway_server.stop()
            except:
                pass
    
    def create_gateway_client(self, host='localhost', port=8090):
        """Create a gateway client for testing."""
        from algorithms.RAFT_GossipFL.grpc_gateway_client import GRPCGatewayClient
        return GRPCGatewayClient(host, port)
    
    def create_dynamic_comm_manager(self, node_id, port, **kwargs):
        """Create a dynamic communication manager for testing."""
        from fedml_core.distributed.communication.grpc.dynamic_grpc_comm_manager import DynamicGRPCCommManager
        
        default_kwargs = {
            'host': '127.0.0.1',
            'client_num': 10,
            'gateway_host': self.gateway_host,
            'gateway_port': self.gateway_port,
            'capabilities': ['grpc', 'fedml', 'test'],
            'metadata': {'test': 'true', 'created_at': str(time.time())}
        }
        default_kwargs.update(kwargs)
        
        node = DynamicGRPCCommManager(
            port=port,
            node_id=node_id,
            **default_kwargs
        )
        self.test_nodes.append(node)
        return node


class TestGatewayServer(ServiceDiscoveryTestCase):
    """Test cases for the gateway server functionality."""
    
    def test_gateway_startup_shutdown(self):
        """Test gateway server startup and shutdown."""
        self.start_gateway_server()
        self.assertTrue(hasattr(self, 'gateway_server'))
        
        # Test health check
        client = self.create_gateway_client()
        health = client.health_check()
        self.assertIsNotNone(health)
        self.assertEqual(health['status'], 'healthy')
        
        client.shutdown()
        self.stop_gateway_server()
    
    def test_gateway_stats(self):
        """Test gateway statistics functionality."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        stats = client.get_stats()
        self.assertIsNotNone(stats)
        self.assertIn('total_nodes', stats)
        self.assertIn('active_nodes', stats)
        self.assertIn('registry_version', stats)
        
        client.shutdown()
        self.stop_gateway_server()


class TestGatewayClient(ServiceDiscoveryTestCase):
    """Test cases for the gateway client functionality."""
    
    def test_client_initialization(self):
        """Test gateway client initialization."""
        client = self.create_gateway_client()
        self.assertEqual(client.gateway_host, 'localhost')
        self.assertEqual(client.gateway_port, 8090)
        self.assertFalse(client.is_registered)
        client.shutdown()
    
    def test_client_health_check(self):
        """Test client health check functionality."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        health = client.health_check()
        self.assertIsNotNone(health)
        self.assertEqual(health['status'], 'healthy')
        
        client.shutdown()
        self.stop_gateway_server()
    
    def test_client_registration(self):
        """Test node registration with gateway."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Test registration
        result = client.register_node(
            node_id=1,
            ip_address='127.0.0.1',
            port=8901,
            capabilities=['grpc', 'fedml'],
            metadata={'type': 'test'}
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['node_id'], 1)
        self.assertEqual(result['status'], 'registered')
        self.assertTrue(result['is_bootstrap'])
        self.assertTrue(client.is_registered)
        
        client.shutdown()
        self.stop_gateway_server()
    
    def test_metadata_type_conversion(self):
        """Test that metadata values are properly converted to strings."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Test with mixed metadata types
        result = client.register_node(
            node_id=1,
            ip_address='127.0.0.1',
            port=8901,
            capabilities=['grpc', 'fedml'],
            metadata={
                'string': 'value',
                'integer': 123,
                'float': 45.67,
                'boolean': True,
                'timestamp': time.time()
            }
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['status'], 'registered')
        
        client.shutdown()
        self.stop_gateway_server()
    
    def test_node_discovery(self):
        """Test node discovery functionality."""
        self.start_gateway_server()
        
        client1 = self.create_gateway_client()
        client2 = self.create_gateway_client()
        
        # Register first node
        result1 = client1.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'node1'})
        self.assertIsNotNone(result1)
        
        # Register second node
        result2 = client2.register_node(2, '127.0.0.1', 8902, ['grpc'], {'role': 'node2'})
        self.assertIsNotNone(result2)
        
        # Test getting nodes
        nodes = client1.get_nodes()
        self.assertIsNotNone(nodes)
        self.assertEqual(len(nodes), 1)  # Excluding self
        
        # Test getting all nodes
        all_nodes = client1.get_nodes(exclude_self=False)
        self.assertIsNotNone(all_nodes)
        self.assertGreaterEqual(len(all_nodes), 2)
        
        client1.shutdown()
        client2.shutdown()
        self.stop_gateway_server()
    
    def test_leader_management(self):
        """Test leader election and management."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Register node
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'leader'})
        self.assertIsNotNone(result)
        
        # Check leader
        leader = client.get_leader()
        self.assertIsNotNone(leader)
        self.assertEqual(leader['node_id'], 1)
        
        # Update leader
        success = client.update_leader(1)
        self.assertTrue(success)
        
        client.shutdown()
        self.stop_gateway_server()
    
    def test_heartbeat_functionality(self):
        """Test heartbeat functionality."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Register node
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'test'})
        self.assertIsNotNone(result)
        
        # Test heartbeat
        heartbeat_result = client.heartbeat()
        self.assertIsNotNone(heartbeat_result)
        self.assertIn('status', heartbeat_result)
        
        client.shutdown()
        self.stop_gateway_server()


class TestDynamicCommunicationManager(ServiceDiscoveryTestCase):
    """Test cases for the dynamic communication manager."""
    
    def test_single_node_bootstrap(self):
        """Test single node bootstrap functionality."""
        self.start_gateway_server()
        
        node = self.create_dynamic_comm_manager(node_id=1, port=8901)
        time.sleep(2)  # Allow initialization
        
        # Check bootstrap status
        self.assertTrue(node.is_bootstrap_node_local())
        
        # Check cluster nodes
        nodes = node.get_cluster_nodes_info()
        self.assertEqual(len(nodes), 1)
        self.assertIn(1, nodes)
        
        self.stop_gateway_server()
    
    def test_multi_node_cluster(self):
        """Test multi-node cluster formation."""
        self.start_gateway_server()
        
        # Create first node (bootstrap)
        node1 = self.create_dynamic_comm_manager(node_id=1, port=8901)
        time.sleep(2)
        
        # Create second node (join)
        node2 = self.create_dynamic_comm_manager(node_id=2, port=8902)
        time.sleep(3)  # Allow registry refresh
        
        # Check cluster formation
        nodes1 = node1.get_cluster_nodes_info()
        nodes2 = node2.get_cluster_nodes_info()
        
        self.assertEqual(len(nodes1), 2)
        self.assertEqual(len(nodes2), 2)
        self.assertIn(1, nodes1)
        self.assertIn(2, nodes1)
        self.assertIn(1, nodes2)
        self.assertIn(2, nodes2)
        
        # Check bootstrap status
        self.assertTrue(node1.is_bootstrap_node_local())
        self.assertFalse(node2.is_bootstrap_node_local())
        
        self.stop_gateway_server()
    
    def test_messaging_between_nodes(self):
        """Test messaging between dynamic nodes."""
        self.start_gateway_server()
        
        # Create nodes
        node1 = self.create_dynamic_comm_manager(node_id=1, port=8901)
        node2 = self.create_dynamic_comm_manager(node_id=2, port=8902)
        time.sleep(3)  # Allow cluster formation
        
        # Test messaging
        from fedml_core.distributed.communication.message import Message
        
        message = Message(type=1, sender_id=1, receiver_id=2)
        message.set_content("Test message from node 1")
        
        try:
            node1.send_message(message)
            success = True
        except Exception as e:
            success = False
            print(f"Messaging failed: {e}")
        
        self.assertTrue(success, "Message sending should succeed")
        
        self.stop_gateway_server()
    
    def test_node_registry_refresh(self):
        """Test automatic node registry refresh functionality."""
        self.start_gateway_server()
        
        # Create first node
        node1 = self.create_dynamic_comm_manager(node_id=1, port=8901)
        time.sleep(2)
        
        initial_nodes = node1.get_cluster_nodes_info()
        self.assertEqual(len(initial_nodes), 1)
        
        # Add second node
        node2 = self.create_dynamic_comm_manager(node_id=2, port=8902)
        time.sleep(4)  # Wait for registry refresh (interval is 2s)
        
        # Check that node1 discovered node2
        updated_nodes = node1.get_cluster_nodes_info()
        self.assertEqual(len(updated_nodes), 2)
        self.assertIn(2, updated_nodes)
        
        self.stop_gateway_server()


class TestFailureScenarios(ServiceDiscoveryTestCase):
    """Test cases for failure scenarios and error handling."""
    
    def test_gateway_unavailable(self):
        """Test behavior when gateway is unavailable."""
        # Don't start gateway server
        
        # Create node - should fallback to standalone mode
        node = self.create_dynamic_comm_manager(node_id=1, port=8901)
        time.sleep(2)
        
        # Should still initialize but in standalone mode
        nodes = node.get_cluster_nodes_info()
        self.assertEqual(len(nodes), 1)
        self.assertIn(1, nodes)
    
    def test_client_connection_retry(self):
        """Test client connection retry logic."""
        client = self.create_gateway_client()
        
        # Try to connect to non-existent gateway
        health = client.health_check()
        self.assertIsNone(health)  # Should fail gracefully
        
        client.shutdown()
    
    def test_duplicate_node_registration(self):
        """Test handling of duplicate node registrations."""
        self.start_gateway_server()
        
        client1 = self.create_gateway_client()
        client2 = self.create_gateway_client()
        
        # Register same node ID twice
        result1 = client1.register_node(1, '127.0.0.1', 8901, ['grpc'], {'instance': '1'})
        result2 = client2.register_node(1, '127.0.0.1', 8902, ['grpc'], {'instance': '2'})
        
        self.assertIsNotNone(result1)
        self.assertIsNotNone(result2)
        
        # Second registration should update the first
        self.assertEqual(result2['status'], 'updated')
        
        client1.shutdown()
        client2.shutdown()
        self.stop_gateway_server()
    
    def test_invalid_node_removal(self):
        """Test removal of non-existent nodes."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Try to remove non-existent node
        result = client.remove_node(999)
        self.assertFalse(result)  # Should fail gracefully
        
        client.shutdown()
        self.stop_gateway_server()


class TestPerformanceAndLoad(ServiceDiscoveryTestCase):
    """Test cases for performance and load testing."""
    
    def test_multiple_rapid_registrations(self):
        """Test multiple rapid node registrations."""
        self.start_gateway_server()
        
        clients = []
        results = []
        
        # Create multiple clients and register rapidly
        for i in range(5):
            client = self.create_gateway_client()
            result = client.register_node(
                i + 1, '127.0.0.1', 8901 + i, ['grpc'], {'node': str(i + 1)}
            )
            clients.append(client)
            results.append(result)
        
        # Check all registrations succeeded
        for result in results:
            self.assertIsNotNone(result)
        
        # Check final state
        stats = clients[0].get_stats()
        self.assertGreaterEqual(stats['total_nodes'], 5)
        
        for client in clients:
            client.shutdown()
        
        self.stop_gateway_server()
    
    def test_concurrent_operations(self):
        """Test concurrent gateway operations."""
        self.start_gateway_server()
        
        def register_node(node_id):
            client = self.create_gateway_client()
            result = client.register_node(
                node_id, '127.0.0.1', 8900 + node_id, ['grpc'], {'thread': str(node_id)}
            )
            client.shutdown()
            return result
        
        # Run concurrent registrations
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(register_node, i) for i in range(1, 4)]
            results = [future.result() for future in futures]
        
        # Check all succeeded
        for result in results:
            self.assertIsNotNone(result)
        
        self.stop_gateway_server()


class TestCommManagerFactory(ServiceDiscoveryTestCase):
    """Test cases for the communication manager factory."""
    
    def test_factory_creation(self):
        """Test factory-based communication manager creation."""
        self.start_gateway_server()
        
        from fedml_core.distributed.communication.grpc.comm_manager_factory import CommManagerFactory
        
        # Create manager via factory
        manager = CommManagerFactory.create_dynamic_grpc_manager(
            host='127.0.0.1',
            port=8901,
            node_id=1,
            gateway_host=self.gateway_host,
            gateway_port=self.gateway_port
        )
        
        self.assertIsNotNone(manager)
        self.test_nodes.append(manager)
        
        time.sleep(2)  # Allow initialization
        
        # Test basic functionality
        nodes = manager.get_cluster_nodes_info()
        self.assertEqual(len(nodes), 1)
        
        self.stop_gateway_server()


def run_test_suite():
    """Run the complete test suite."""
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_classes = [
        TestGatewayServer,
        TestGatewayClient,
        TestDynamicCommunicationManager,
        TestFailureScenarios,
        TestPerformanceAndLoad,
        TestCommManagerFactory
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    print("=" * 80)
    print("🧪 Comprehensive gRPC Service Discovery Test Suite")
    print("=" * 80)
    
    success = run_test_suite()
    
    print("\n" + "=" * 80)
    if success:
        print("🎉 All tests passed! Service discovery system is working correctly.")
    else:
        print("❌ Some tests failed! Check the output above for details.")
    print("=" * 80)
    
    sys.exit(0 if success else 1)
