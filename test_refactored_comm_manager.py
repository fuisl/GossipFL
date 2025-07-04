#!/usr/bin/env python3

"""
Test suite for the refactored Dynamic gRPC Communication Manager

This test suite verifies the new architecture that:
1. Eliminates polling mechanisms
2. Uses direct gRPC calls to service discovery 
3. Provides event-driven registration and discovery
4. Maintains robust fault tolerance
"""

import sys
import os
import unittest
import threading
import time
import logging
from unittest.mock import Mock, patch, MagicMock

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from fedml_core.distributed.communication.grpc.refactored_dynamic_grpc_comm_manager import (
    RefactoredDynamicGRPCCommManager,
    ServiceDiscoveryClient,
    NodeInfo
)
from fedml_core.distributed.communication.message import Message

# Configure logging for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestServiceDiscoveryClient(unittest.TestCase):
    """Test the ServiceDiscoveryClient component."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.client = ServiceDiscoveryClient("localhost", 8090)
    
    def test_initialization(self):
        """Test client initialization."""
        self.assertEqual(self.client.service_host, "localhost")
        self.assertEqual(self.client.service_port, 8090)
        self.assertEqual(self.client.service_address, "localhost:8090")
    
    def test_connection_management(self):
        """Test gRPC connection management."""
        # Initially no connection
        self.assertIsNone(self.client._channel)
        self.assertIsNone(self.client._stub)
        
        # Connection cleanup should not fail
        self.client._close_connection()
        
        # Multiple close calls should not fail
        self.client._close_connection()
        self.client._close_connection()
    
    def test_close(self):
        """Test client cleanup."""
        self.client.close()
        self.assertIsNone(self.client._channel)
        self.assertIsNone(self.client._stub)


class TestNodeInfo(unittest.TestCase):
    """Test the NodeInfo data structure."""
    
    def test_initialization(self):
        """Test NodeInfo initialization."""
        node = NodeInfo(
            node_id=1,
            ip_address="192.168.1.100",
            port=8891,
            capabilities=["grpc", "fedml"],
            metadata={"type": "client"}
        )
        
        self.assertEqual(node.node_id, 1)
        self.assertEqual(node.ip_address, "192.168.1.100")
        self.assertEqual(node.port, 8891)
        self.assertEqual(node.capabilities, ["grpc", "fedml"])
        self.assertEqual(node.metadata, {"type": "client"})
        self.assertTrue(node.is_reachable)
    
    def test_reachability_update(self):
        """Test node reachability updates."""
        node = NodeInfo(1, "192.168.1.100", 8891)
        
        # Initially reachable
        self.assertTrue(node.is_reachable)
        
        # Mark as unreachable
        node.update_reachability(False)
        self.assertFalse(node.is_reachable)
        
        # Mark as reachable again
        node.update_reachability(True)
        self.assertTrue(node.is_reachable)
    
    def test_to_dict(self):
        """Test NodeInfo to dictionary conversion."""
        node = NodeInfo(
            node_id=1,
            ip_address="192.168.1.100",
            port=8891,
            capabilities=["grpc", "fedml"],
            metadata={"type": "client"}
        )
        
        node_dict = node.to_dict()
        
        self.assertEqual(node_dict["node_id"], 1)
        self.assertEqual(node_dict["ip_address"], "192.168.1.100")
        self.assertEqual(node_dict["port"], 8891)
        self.assertEqual(node_dict["capabilities"], ["grpc", "fedml"])
        self.assertEqual(node_dict["metadata"], {"type": "client"})
        self.assertIn("last_seen", node_dict)
        self.assertIn("is_reachable", node_dict)


class TestRefactoredDynamicGRPCCommManager(unittest.TestCase):
    """Test the RefactoredDynamicGRPCCommManager."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.manager = None
        self.test_port = 8891
        self.test_node_id = 1
        
    def tearDown(self):
        """Clean up test fixtures."""
        if self.manager:
            self.manager.cleanup()
    
    def test_initialization_without_service_discovery(self):
        """Test initialization without service discovery."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        self.assertEqual(self.manager.host, "localhost")
        self.assertEqual(self.manager.port, str(self.test_port))
        self.assertEqual(self.manager.node_id, self.test_node_id)
        self.assertEqual(self.manager.node_type, "client")
        self.assertFalse(self.manager.use_service_discovery)
        self.assertIsNone(self.manager.service_discovery_client)
    
    def test_initialization_with_static_config(self):
        """Test initialization with static configuration."""
        # Create a temporary static config file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("0,localhost\n")
            f.write("1,192.168.1.100\n")
            f.write("2,192.168.1.101\n")
            static_config_path = f.name
        
        try:
            self.manager = RefactoredDynamicGRPCCommManager(
                host="localhost",
                port=self.test_port,
                node_id=self.test_node_id,
                client_num=3,
                use_service_discovery=False,
                ip_config_path=static_config_path
            )
            
            # Check that static config was loaded
            self.assertGreater(len(self.manager.ip_config), 0)
            self.assertEqual(self.manager.ip_config["0"], "localhost")
            self.assertEqual(self.manager.ip_config["1"], "192.168.1.100")
            self.assertEqual(self.manager.ip_config["2"], "192.168.1.101")
            
        finally:
            os.unlink(static_config_path)
    
    def test_node_registry_management(self):
        """Test node registry management."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        # Add nodes to registry
        node2 = NodeInfo(2, "192.168.1.100", 8892)
        node3 = NodeInfo(3, "192.168.1.101", 8893)
        
        self.manager.node_registry[2] = node2
        self.manager.node_registry[3] = node3
        
        # Test cluster info retrieval
        cluster_info = self.manager.get_cluster_nodes_info()
        self.assertIn(2, cluster_info)
        self.assertIn(3, cluster_info)
        
        # Test reachable nodes
        reachable_nodes = self.manager.get_reachable_nodes()
        self.assertIn(2, reachable_nodes)
        self.assertIn(3, reachable_nodes)
        
        # Test node reachability update
        self.manager._update_node_reachability(2, False)
        reachable_nodes = self.manager.get_reachable_nodes()
        self.assertNotIn(2, reachable_nodes)
        self.assertIn(3, reachable_nodes)
    
    def test_observer_pattern(self):
        """Test observer pattern for message handling."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        # Create mock observer
        observer = Mock()
        observer.receive_message = Mock()
        
        # Add observer
        self.manager.add_observer(observer)
        self.assertIn(observer, self.manager._observers)
        
        # Remove observer
        self.manager.remove_observer(observer)
        self.assertNotIn(observer, self.manager._observers)
    
    def test_get_node_ip(self):
        """Test IP address retrieval for nodes."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        # Add node to registry
        node2 = NodeInfo(2, "192.168.1.100", 8892)
        self.manager.node_registry[2] = node2
        
        # Add to IP config
        self.manager.ip_config["3"] = "192.168.1.101"
        
        # Test retrieval from registry
        ip = self.manager._get_node_ip(2)
        self.assertEqual(ip, "192.168.1.100")
        
        # Test retrieval from IP config
        ip = self.manager._get_node_ip(3)
        self.assertEqual(ip, "192.168.1.101")
        
        # Test non-existent node
        ip = self.manager._get_node_ip(999)
        self.assertIsNone(ip)
    
    def test_cluster_size_and_info(self):
        """Test cluster size and information methods."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        # Initially should have just this node
        self.assertEqual(self.manager.get_cluster_size(), 0)
        
        # Add some nodes
        node2 = NodeInfo(2, "192.168.1.100", 8892)
        node3 = NodeInfo(3, "192.168.1.101", 8893)
        
        self.manager.node_registry[2] = node2
        self.manager.node_registry[3] = node3
        
        # Test cluster size
        self.assertEqual(self.manager.get_cluster_size(), 2)
        
        # Test node info retrieval
        info = self.manager.get_node_info(2)
        self.assertIsNotNone(info)
        self.assertEqual(info.ip_address, "192.168.1.100")
        
        # Test non-existent node
        info = self.manager.get_node_info(999)
        self.assertIsNone(info)
    
    def test_node_reachability_check(self):
        """Test node reachability checking."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        # Add node
        node2 = NodeInfo(2, "192.168.1.100", 8892)
        self.manager.node_registry[2] = node2
        
        # Initially reachable
        self.assertTrue(self.manager.is_node_reachable(2))
        
        # Mark as unreachable
        self.manager._update_node_reachability(2, False)
        self.assertFalse(self.manager.is_node_reachable(2))
        
        # Test non-existent node
        self.assertFalse(self.manager.is_node_reachable(999))
    
    def test_callbacks(self):
        """Test event callbacks."""
        node_discovered_calls = []
        node_lost_calls = []
        
        def on_node_discovered(node_id, node_info):
            node_discovered_calls.append((node_id, node_info))
        
        def on_node_lost(node_id):
            node_lost_calls.append(node_id)
        
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False,
            on_node_discovered=on_node_discovered,
            on_node_lost=on_node_lost
        )
        
        # Simulate node discovery
        node2 = NodeInfo(2, "192.168.1.100", 8892)
        self.manager.node_registry[2] = node2
        
        # Simulate discovery update (this would normally trigger callbacks)
        cluster_info = {
            'nodes': [
                {'node_id': 2, 'ip_address': '192.168.1.100', 'port': 8892, 'capabilities': [], 'metadata': {}},
                {'node_id': 3, 'ip_address': '192.168.1.101', 'port': 8893, 'capabilities': [], 'metadata': {}}
            ]
        }
        
        self.manager._update_node_registry_from_discovery(cluster_info)
        
        # Check that callbacks were called
        self.assertEqual(len(node_discovered_calls), 1)
        self.assertEqual(node_discovered_calls[0][0], 3)  # Node 3 was discovered
    
    def test_bootstrap_node_status(self):
        """Test bootstrap node status management."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        # Default bootstrap status
        self.assertTrue(self.manager.is_bootstrap_node())
        
        # Change bootstrap status
        self.manager.is_bootstrap_node_flag = False
        self.assertFalse(self.manager.is_bootstrap_node())
    
    def test_cleanup(self):
        """Test cleanup functionality."""
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=False
        )
        
        # Should be running initially
        self.assertTrue(self.manager.is_running)
        
        # Cleanup
        self.manager.cleanup()
        
        # Should be stopped
        self.assertFalse(self.manager.is_running)
        
        # Cleanup should be idempotent
        self.manager.cleanup()
        self.assertFalse(self.manager.is_running)


class TestIntegrationWithServiceDiscovery(unittest.TestCase):
    """Integration tests with service discovery (mocked)."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.manager = None
        self.test_port = 8891
        self.test_node_id = 1
    
    def tearDown(self):
        """Clean up test fixtures."""
        if self.manager:
            self.manager.cleanup()
    
    @patch('fedml_core.distributed.communication.grpc.refactored_dynamic_grpc_comm_manager.ServiceDiscoveryClient')
    def test_successful_service_discovery_initialization(self, mock_client_class):
        """Test successful service discovery initialization."""
        # Mock the service discovery client
        mock_client = Mock()
        mock_client.discover_cluster.return_value = (
            True,  # is_bootstrap
            {
                'nodes': [
                    {'node_id': 1, 'ip_address': 'localhost', 'port': 8891, 'capabilities': ['grpc'], 'metadata': {}}
                ],
                'total_nodes': 1,
                'bootstrap_node': 1
            }
        )
        mock_client.register_node.return_value = True
        mock_client_class.return_value = mock_client
        
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=True
        )
        
        # Verify service discovery was initialized
        mock_client_class.assert_called_once()
        mock_client.discover_cluster.assert_called_once()
        mock_client.register_node.assert_called_once()
        
        # Verify manager state
        self.assertTrue(self.manager.is_bootstrap_node())
        self.assertIn(self.test_node_id, self.manager.node_registry)
    
    @patch('fedml_core.distributed.communication.grpc.refactored_dynamic_grpc_comm_manager.ServiceDiscoveryClient')
    def test_service_discovery_failure_fallback(self, mock_client_class):
        """Test fallback behavior when service discovery fails."""
        # Mock the service discovery client to fail
        mock_client = Mock()
        mock_client.discover_cluster.side_effect = Exception("Service discovery unavailable")
        mock_client_class.return_value = mock_client
        
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=True
        )
        
        # Should fall back to standalone mode
        self.assertTrue(self.manager.is_bootstrap_node())
        self.assertIn(self.test_node_id, self.manager.node_registry)
        
        # Service discovery client should still be created
        mock_client_class.assert_called_once()
    
    @patch('fedml_core.distributed.communication.grpc.refactored_dynamic_grpc_comm_manager.ServiceDiscoveryClient')
    def test_registry_refresh(self, mock_client_class):
        """Test manual registry refresh."""
        # Mock the service discovery client
        mock_client = Mock()
        mock_client.discover_cluster.return_value = (False, {'nodes': [], 'total_nodes': 0})
        mock_client.register_node.return_value = True
        mock_client.get_nodes.return_value = [
            {'node_id': 2, 'ip_address': '192.168.1.100', 'port': 8892, 'capabilities': ['grpc'], 'metadata': {}},
            {'node_id': 3, 'ip_address': '192.168.1.101', 'port': 8893, 'capabilities': ['grpc'], 'metadata': {}}
        ]
        mock_client_class.return_value = mock_client
        
        self.manager = RefactoredDynamicGRPCCommManager(
            host="localhost",
            port=self.test_port,
            node_id=self.test_node_id,
            client_num=1,
            use_service_discovery=True
        )
        
        # Initially should only have this node
        initial_size = self.manager.get_cluster_size()
        
        # Refresh registry
        self.manager.refresh_node_registry()
        
        # Should now have more nodes
        new_size = self.manager.get_cluster_size()
        self.assertGreater(new_size, initial_size)
        
        # Verify nodes were added
        self.assertIn(2, self.manager.node_registry)
        self.assertIn(3, self.manager.node_registry)


def run_tests():
    """Run all tests."""
    logging.basicConfig(level=logging.INFO)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTests(loader.loadTestsFromTestCase(TestServiceDiscoveryClient))
    suite.addTests(loader.loadTestsFromTestCase(TestNodeInfo))
    suite.addTests(loader.loadTestsFromTestCase(TestRefactoredDynamicGRPCCommManager))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegrationWithServiceDiscovery))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
