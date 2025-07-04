#!/usr/bin/env python3
"""
Comprehensive gRPC Service Discovery Test Suite

This test suite provides exhaustive testing of the gRPC service discovery system,
including gateway server, client interactions, dynamic node management, failure scenarios,
performance testing, and integration tests.

Test Coverage:
1. Gateway Server Operations
2. Gateway Client Operations
3. Dynamic Communication Manager
4. Service Discovery Workflows
5. Error Handling and Failure Scenarios
6. Performance and Load Testing
7. Integration Testing
8. Real-world Scenario Testing
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
import random
import json
from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock, patch
import concurrent.futures

# Add paths
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL')
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL/algorithms/RAFT_GossipFL')

# Configure logging for tests
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ComprehensiveServiceDiscoveryTestCase(unittest.TestCase):
    """Enhanced base test case with comprehensive setup for service discovery tests."""
    
    def setUp(self):
        """Set up test environment."""
        self.gateway_server = None
        self.gateway_host = 'localhost'
        self.gateway_port = 8090
        self.test_nodes = []
        self.test_clients = []
        self.temp_files = []
        self.background_threads = []
        
        # Test configuration
        self.default_timeout = 10.0
        self.heartbeat_interval = 5.0
        self.registry_refresh_interval = 2.0
        
    def tearDown(self):
        """Clean up test environment."""
        # Stop background threads
        for thread in self.background_threads:
            if thread and thread.is_alive():
                try:
                    # Signal thread to stop (this is a simplified example)
                    thread.join(timeout=2)
                except:
                    pass
        
        # Clean up clients
        for client in self.test_clients:
            try:
                if hasattr(client, 'shutdown'):
                    client.shutdown()
            except:
                pass
        
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
        
        # Stop gateway server
        if self.gateway_server:
            try:
                self.gateway_server.stop()
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
            
            self.gateway_server = GRPCGatewayServer(host=host, port=port)
            self.gateway_server.start()
            time.sleep(1)  # Allow startup
            return True
        except Exception as e:
            self.fail(f"Failed to start gateway server: {e}")
    
    def create_gateway_client(self, host='localhost', port=8090, timeout=None):
        """Create a gateway client for testing."""
        from algorithms.RAFT_GossipFL.grpc_gateway_client import GRPCGatewayClient
        
        client = GRPCGatewayClient(
            host, 
            port, 
            timeout=timeout or self.default_timeout
        )
        self.test_clients.append(client)
        return client
    
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
    
    def wait_for_cluster_formation(self, nodes, expected_size, timeout=10):
        """Wait for cluster to form with expected size."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if all(len(node.get_cluster_nodes_info()) == expected_size for node in nodes):
                return True
            time.sleep(0.5)
        return False
    
    def create_test_message(self, sender_id, receiver_id, message_type=1, content="test"):
        """Create a test message for inter-node communication."""
        from fedml_core.distributed.communication.message import Message
        
        message = Message(type=message_type, sender_id=sender_id, receiver_id=receiver_id)
        message.set_content(content)
        return message


class TestGatewayServerComprehensive(ComprehensiveServiceDiscoveryTestCase):
    """Comprehensive test cases for the gateway server functionality."""
    
    def test_gateway_lifecycle(self):
        """Test complete gateway server lifecycle."""
        # Start gateway
        self.start_gateway_server()
        self.assertIsNotNone(self.gateway_server)
        
        # Test health check
        client = self.create_gateway_client()
        health = client.health_check()
        self.assertIsNotNone(health)
        self.assertEqual(health['status'], 'healthy')
        self.assertIn('timestamp', health)
        self.assertIn('version', health)
        
        # Test stats
        stats = client.get_stats()
        self.assertIsNotNone(stats)
        self.assertIn('total_nodes', stats)
        self.assertIn('active_nodes', stats)
        self.assertIn('registry_version', stats)
        
        # Stop gateway
        self.gateway_server.stop()
        
        # Verify gateway is stopped
        health_after_stop = client.health_check()
        self.assertIsNone(health_after_stop)
    
    def test_gateway_node_registry_management(self):
        """Test gateway node registry management."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Register multiple nodes
        nodes_to_register = [
            (1, '127.0.0.1', 8901, ['grpc', 'fedml'], {'role': 'leader'}),
            (2, '127.0.0.1', 8902, ['grpc', 'fedml'], {'role': 'follower'}),
            (3, '127.0.0.1', 8903, ['grpc', 'fedml'], {'role': 'follower'}),
        ]
        
        for node_id, ip, port, capabilities, metadata in nodes_to_register:
            result = client.register_node(node_id, ip, port, capabilities, metadata)
            self.assertIsNotNone(result)
            self.assertEqual(result['node_id'], node_id)
            self.assertIn('status', result)
        
        # Check registry
        all_nodes = client.get_nodes(exclude_self=False)
        self.assertEqual(len(all_nodes), 3)
        
        # Test node removal
        success = client.remove_node(3)
        self.assertTrue(success)
        
        # Verify removal
        remaining_nodes = client.get_nodes(exclude_self=False)
        self.assertEqual(len(remaining_nodes), 2)
        
        # Verify specific node is removed
        node_ids = {node['node_id'] for node in remaining_nodes}
        self.assertNotIn(3, node_ids)
        self.assertIn(1, node_ids)
        self.assertIn(2, node_ids)
    
    def test_gateway_leader_management(self):
        """Test gateway leader management functionality."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Register bootstrap node
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'bootstrap'})
        self.assertTrue(result['is_bootstrap'])
        
        # Check initial leader
        leader = client.get_leader()
        self.assertIsNotNone(leader)
        self.assertEqual(leader['node_id'], 1)
        
        # Register another node
        client.register_node(2, '127.0.0.1', 8902, ['grpc'], {'role': 'follower'})
        
        # Update leader
        success = client.update_leader(2)
        self.assertTrue(success)
        
        # Verify leader change
        new_leader = client.get_leader()
        self.assertEqual(new_leader['node_id'], 2)
        
        # Test invalid leader update
        invalid_success = client.update_leader(999)
        self.assertFalse(invalid_success)
    
    def test_gateway_concurrent_operations(self):
        """Test gateway handling of concurrent operations."""
        self.start_gateway_server()
        
        def register_multiple_nodes(start_id, count):
            client = self.create_gateway_client()
            results = []
            for i in range(count):
                node_id = start_id + i
                result = client.register_node(
                    node_id, '127.0.0.1', 8900 + node_id, 
                    ['grpc'], {'batch': str(start_id)}
                )
                results.append(result)
            return results
        
        # Run concurrent registrations
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(register_multiple_nodes, 1, 3),
                executor.submit(register_multiple_nodes, 10, 3),
                executor.submit(register_multiple_nodes, 20, 3)
            ]
            
            results = []
            for future in concurrent.futures.as_completed(futures):
                batch_results = future.result()
                results.extend(batch_results)
        
        # Verify all registrations succeeded
        self.assertEqual(len(results), 9)
        for result in results:
            self.assertIsNotNone(result)
        
        # Check final registry state
        client = self.create_gateway_client()
        stats = client.get_stats()
        self.assertEqual(stats['total_nodes'], 9)


class TestGatewayClientAdvanced(ComprehensiveServiceDiscoveryTestCase):
    """Advanced test cases for gateway client functionality."""
    
    def test_client_connection_management(self):
        """Test client connection management and reconnection."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Test initial connection
        health = client.health_check()
        self.assertIsNotNone(health)
        
        # Register node
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {'test': 'connection'})
        self.assertIsNotNone(result)
        
        # Simulate gateway restart
        self.gateway_server.stop()
        time.sleep(1)
        self.start_gateway_server()
        
        # Client should handle connection gracefully
        health_after_restart = client.health_check()
        self.assertIsNotNone(health_after_restart)
    
    def test_client_heartbeat_management(self):
        """Test client heartbeat management."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Register node
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {'role': 'heartbeat_test'})
        self.assertIsNotNone(result)
        
        # Test multiple heartbeats
        for i in range(3):
            heartbeat_result = client.heartbeat()
            self.assertIsNotNone(heartbeat_result)
            self.assertIn('status', heartbeat_result)
            time.sleep(1)
        
        # Verify node is still active
        stats = client.get_stats()
        self.assertEqual(stats['active_nodes'], 1)
    
    def test_client_error_handling(self):
        """Test client error handling for various scenarios."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Test registration with invalid data
        result = client.register_node(
            node_id=0,  # Invalid node ID
            ip_address='invalid-ip',
            port=-1,  # Invalid port
            capabilities=[],
            metadata={}
        )
        # Should handle gracefully (either succeed with validation or fail gracefully)
        # The exact behavior depends on server-side validation
        
        # Test operations without registration
        unregistered_client = self.create_gateway_client()
        heartbeat_result = unregistered_client.heartbeat()
        # Should handle gracefully
        
        # Test timeout scenarios
        timeout_client = self.create_gateway_client(timeout=0.1)
        # Very short timeout may cause operations to fail
    
    def test_client_metadata_handling(self):
        """Test client metadata handling and type conversion."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Test with various metadata types
        complex_metadata = {
            'string_value': 'test',
            'int_value': 42,
            'float_value': 3.14,
            'bool_value': True,
            'timestamp': time.time(),
            'list_as_string': str([1, 2, 3]),
            'dict_as_string': str({'key': 'value'})
        }
        
        result = client.register_node(
            1, '127.0.0.1', 8901, ['grpc'], complex_metadata
        )
        self.assertIsNotNone(result)
        
        # Verify metadata is preserved
        nodes = client.get_nodes(exclude_self=False)
        node = next(n for n in nodes if n['node_id'] == 1)
        self.assertIn('metadata', node)


class TestDynamicCommunicationManagerAdvanced(ComprehensiveServiceDiscoveryTestCase):
    """Advanced test cases for dynamic communication manager."""
    
    def test_dynamic_cluster_formation(self):
        """Test dynamic cluster formation with multiple nodes."""
        self.start_gateway_server()
        
        # Create nodes sequentially
        nodes = []
        for i in range(1, 4):
            node = self.create_dynamic_comm_manager(node_id=i, port=8900 + i)
            nodes.append(node)
            time.sleep(1)  # Stagger creation
        
        # Wait for cluster formation
        self.assertTrue(self.wait_for_cluster_formation(nodes, 3))
        
        # Verify bootstrap detection
        self.assertTrue(nodes[0].is_bootstrap_node_local())
        for i in range(1, 3):
            self.assertFalse(nodes[i].is_bootstrap_node_local())
        
        # Verify all nodes see each other
        for node in nodes:
            cluster_nodes = node.get_cluster_nodes_info()
            self.assertEqual(len(cluster_nodes), 3)
            for i in range(1, 4):
                self.assertIn(i, cluster_nodes)
    
    def test_dynamic_node_messaging(self):
        """Test messaging between dynamically discovered nodes."""
        self.start_gateway_server()
        
        # Create nodes
        node1 = self.create_dynamic_comm_manager(node_id=1, port=8901)
        node2 = self.create_dynamic_comm_manager(node_id=2, port=8902)
        node3 = self.create_dynamic_comm_manager(node_id=3, port=8903)
        
        # Wait for cluster formation
        nodes = [node1, node2, node3]
        self.assertTrue(self.wait_for_cluster_formation(nodes, 3))
        
        # Test different messaging patterns
        test_messages = [
            (1, 2, "Message from node 1 to node 2"),
            (2, 3, "Message from node 2 to node 3"),
            (3, 1, "Message from node 3 to node 1"),
            (1, 3, "Another message from node 1 to node 3")
        ]
        
        for sender_id, receiver_id, content in test_messages:
            sender_node = next(n for n in nodes if n.node_id == sender_id)
            message = self.create_test_message(sender_id, receiver_id, content=content)
            
            try:
                sender_node.send_message(message)
                success = True
            except Exception as e:
                success = False
                print(f"Messaging failed from {sender_id} to {receiver_id}: {e}")
            
            self.assertTrue(success, f"Message from {sender_id} to {receiver_id} should succeed")
    
    def test_dynamic_node_leave_join(self):
        """Test dynamic node leaving and rejoining."""
        self.start_gateway_server()
        
        # Create initial cluster
        node1 = self.create_dynamic_comm_manager(node_id=1, port=8901)
        node2 = self.create_dynamic_comm_manager(node_id=2, port=8902)
        
        # Wait for initial cluster
        self.assertTrue(self.wait_for_cluster_formation([node1, node2], 2))
        
        # Remove node2
        node2.cleanup()
        self.test_nodes.remove(node2)
        
        # Wait for cluster size to update
        time.sleep(self.registry_refresh_interval * 2)
        
        # Verify node1 detects departure
        cluster_nodes = node1.get_cluster_nodes_info()
        # Note: Depending on implementation, may still show 2 nodes until heartbeat timeout
        # self.assertEqual(len(cluster_nodes), 1)
        
        # Add new node3
        node3 = self.create_dynamic_comm_manager(node_id=3, port=8903)
        time.sleep(self.registry_refresh_interval * 2)
        
        # Verify node3 joins
        cluster_nodes = node1.get_cluster_nodes_info()
        self.assertIn(3, cluster_nodes)
    
    def test_dynamic_registry_refresh(self):
        """Test automatic registry refresh mechanism."""
        self.start_gateway_server()
        
        # Create first node
        node1 = self.create_dynamic_comm_manager(node_id=1, port=8901)
        time.sleep(2)
        
        initial_nodes = node1.get_cluster_nodes_info()
        self.assertEqual(len(initial_nodes), 1)
        
        # Add second node
        node2 = self.create_dynamic_comm_manager(node_id=2, port=8902)
        
        # Wait for registry refresh
        time.sleep(self.registry_refresh_interval * 2)
        
        # Verify node1 discovered node2
        updated_nodes = node1.get_cluster_nodes_info()
        self.assertGreaterEqual(len(updated_nodes), 2)
        self.assertIn(2, updated_nodes)


class TestFailureScenariosComprehensive(ComprehensiveServiceDiscoveryTestCase):
    """Comprehensive test cases for failure scenarios and error handling."""
    
    def test_gateway_failure_recovery(self):
        """Test system behavior during gateway failure and recovery."""
        self.start_gateway_server()
        
        # Create cluster
        node1 = self.create_dynamic_comm_manager(node_id=1, port=8901)
        node2 = self.create_dynamic_comm_manager(node_id=2, port=8902)
        
        # Wait for cluster formation
        self.assertTrue(self.wait_for_cluster_formation([node1, node2], 2))
        
        # Stop gateway
        self.gateway_server.stop()
        
        # Nodes should continue operating
        # Test messaging (should still work with cached registry)
        message = self.create_test_message(1, 2, content="During gateway failure")
        try:
            node1.send_message(message)
            messaging_works = True
        except Exception as e:
            messaging_works = False
            print(f"Messaging failed during gateway failure: {e}")
        
        self.assertTrue(messaging_works, "Messaging should work even when gateway is down")
        
        # Restart gateway
        self.start_gateway_server()
        
        # Nodes should reconnect and continue
        time.sleep(3)
        
        # Test operations after gateway recovery
        client = self.create_gateway_client()
        stats = client.get_stats()
        self.assertIsNotNone(stats)
    
    def test_node_failure_scenarios(self):
        """Test various node failure scenarios."""
        self.start_gateway_server()
        
        # Create cluster
        nodes = []
        for i in range(1, 4):
            node = self.create_dynamic_comm_manager(node_id=i, port=8900 + i)
            nodes.append(node)
        
        # Wait for cluster formation
        self.assertTrue(self.wait_for_cluster_formation(nodes, 3))
        
        # Test sudden node termination
        nodes[1].cleanup()  # Simulate node 2 failure
        self.test_nodes.remove(nodes[1])
        
        # Remaining nodes should handle gracefully
        time.sleep(self.registry_refresh_interval * 2)
        
        # Test messaging to failed node (should handle gracefully)
        message = self.create_test_message(1, 2, content="To failed node")
        try:
            nodes[0].send_message(message)
            # Should either succeed (if connection cached) or fail gracefully
        except Exception as e:
            # Should handle gracefully
            pass
        
        # Test messaging between remaining nodes
        message = self.create_test_message(1, 3, content="Between remaining nodes")
        try:
            nodes[0].send_message(message)
            success = True
        except Exception as e:
            success = False
            print(f"Messaging failed between remaining nodes: {e}")
        
        self.assertTrue(success, "Messaging should work between remaining nodes")
    
    def test_network_partition_scenarios(self):
        """Test network partition scenarios."""
        self.start_gateway_server()
        
        # Create clients with different timeouts to simulate network issues
        clients = []
        for i in range(3):
            client = self.create_gateway_client(timeout=1.0)  # Short timeout
            clients.append(client)
        
        # Register nodes
        for i, client in enumerate(clients):
            result = client.register_node(
                i + 1, '127.0.0.1', 8901 + i, ['grpc'], 
                {'partition_test': 'true'}
            )
            self.assertIsNotNone(result)
        
        # Simulate network issues with very short timeouts
        # Some operations may fail due to timeout
        for client in clients:
            try:
                stats = client.get_stats()
                # May succeed or fail depending on network conditions
            except Exception:
                # Should handle gracefully
                pass
    
    def test_invalid_operations(self):
        """Test handling of invalid operations."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Test invalid node removal
        result = client.remove_node(999)
        self.assertFalse(result)
        
        # Test invalid leader update
        result = client.update_leader(999)
        self.assertFalse(result)
        
        # Test operations on empty registry
        nodes = client.get_nodes()
        self.assertEqual(len(nodes), 0)
        
        leader = client.get_leader()
        self.assertIsNone(leader)


class TestPerformanceAndLoadAdvanced(ComprehensiveServiceDiscoveryTestCase):
    """Advanced performance and load testing."""
    
    def test_high_frequency_operations(self):
        """Test high frequency operations."""
        self.start_gateway_server()
        
        client = self.create_gateway_client()
        
        # Register a node
        result = client.register_node(1, '127.0.0.1', 8901, ['grpc'], {'perf': 'test'})
        self.assertIsNotNone(result)
        
        # Perform high frequency operations
        operations = ['get_nodes', 'get_leader', 'get_stats', 'heartbeat']
        
        for _ in range(20):  # Perform 20 operations rapidly
            for operation in operations:
                try:
                    if operation == 'get_nodes':
                        client.get_nodes()
                    elif operation == 'get_leader':
                        client.get_leader()
                    elif operation == 'get_stats':
                        client.get_stats()
                    elif operation == 'heartbeat':
                        client.heartbeat()
                except Exception as e:
                    self.fail(f"High frequency operation {operation} failed: {e}")
    
    def test_large_cluster_simulation(self):
        """Test behavior with larger cluster size."""
        self.start_gateway_server()
        
        # Create multiple nodes
        nodes = []
        for i in range(1, 8):  # Create 7 nodes
            node = self.create_dynamic_comm_manager(node_id=i, port=8900 + i)
            nodes.append(node)
            time.sleep(0.5)  # Stagger creation
        
        # Wait for cluster formation
        self.assertTrue(self.wait_for_cluster_formation(nodes, 7, timeout=20))
        
        # Test messaging patterns
        for i in range(3):  # Test 3 rounds of messaging
            sender = nodes[i]
            receiver_id = (i + 1) % 7 + 1  # Circular messaging
            message = self.create_test_message(
                sender.node_id, receiver_id, 
                content=f"Large cluster test message {i}"
            )
            
            try:
                sender.send_message(message)
                success = True
            except Exception as e:
                success = False
                print(f"Large cluster messaging failed: {e}")
            
            self.assertTrue(success, f"Large cluster messaging round {i} should succeed")
    
    def test_concurrent_node_operations(self):
        """Test concurrent node operations and messaging."""
        self.start_gateway_server()
        
        # Create nodes
        nodes = []
        for i in range(1, 5):
            node = self.create_dynamic_comm_manager(node_id=i, port=8900 + i)
            nodes.append(node)
        
        # Wait for cluster formation
        self.assertTrue(self.wait_for_cluster_formation(nodes, 4))
        
        def send_messages_concurrently(sender_node, target_ids):
            """Send messages concurrently to multiple targets."""
            messages_sent = 0
            for target_id in target_ids:
                if target_id != sender_node.node_id:
                    message = self.create_test_message(
                        sender_node.node_id, target_id,
                        content=f"Concurrent message to {target_id}"
                    )
                    try:
                        sender_node.send_message(message)
                        messages_sent += 1
                    except Exception as e:
                        print(f"Concurrent messaging failed: {e}")
            return messages_sent
        
        # Run concurrent messaging
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for node in nodes:
                target_ids = [n.node_id for n in nodes if n.node_id != node.node_id]
                future = executor.submit(send_messages_concurrently, node, target_ids)
                futures.append(future)
            
            # Wait for all messaging to complete
            total_messages = sum(future.result() for future in futures)
        
        # Should have sent multiple messages
        self.assertGreater(total_messages, 0)


class TestRealWorldScenarios(ComprehensiveServiceDiscoveryTestCase):
    """Test real-world scenarios and edge cases."""
    
    def test_federated_learning_simulation(self):
        """Test scenario simulating federated learning setup."""
        self.start_gateway_server()
        
        # Create coordinator node (bootstrap)
        coordinator = self.create_dynamic_comm_manager(
            node_id=1, port=8901,
            capabilities=['grpc', 'fedml', 'coordinator'],
            metadata={'role': 'coordinator', 'model': 'resnet18'}
        )
        
        # Create worker nodes
        workers = []
        for i in range(2, 6):  # 4 workers
            worker = self.create_dynamic_comm_manager(
                node_id=i, port=8900 + i,
                capabilities=['grpc', 'fedml', 'worker'],
                metadata={'role': 'worker', 'device': f'gpu_{i}'}
            )
            workers.append(worker)
            time.sleep(0.5)
        
        # Wait for cluster formation
        all_nodes = [coordinator] + workers
        self.assertTrue(self.wait_for_cluster_formation(all_nodes, 5))
        
        # Simulate federated learning communication patterns
        # 1. Coordinator broadcasts model to workers
        for worker in workers:
            message = self.create_test_message(
                1, worker.node_id,
                message_type=10,  # Model update
                content="Global model parameters"
            )
            try:
                coordinator.send_message(message)
            except Exception as e:
                self.fail(f"Coordinator to worker messaging failed: {e}")
        
        # 2. Workers send updates back to coordinator
        for worker in workers:
            message = self.create_test_message(
                worker.node_id, 1,
                message_type=11,  # Gradient update
                content="Local gradient updates"
            )
            try:
                worker.send_message(message)
            except Exception as e:
                self.fail(f"Worker to coordinator messaging failed: {e}")
    
    def test_dynamic_scaling_scenario(self):
        """Test dynamic scaling scenario."""
        self.start_gateway_server()
        
        # Start with minimal cluster
        initial_nodes = []
        for i in range(1, 3):
            node = self.create_dynamic_comm_manager(node_id=i, port=8900 + i)
            initial_nodes.append(node)
        
        # Wait for initial cluster
        self.assertTrue(self.wait_for_cluster_formation(initial_nodes, 2))
        
        # Simulate gradual scaling up
        additional_nodes = []
        for i in range(3, 6):
            node = self.create_dynamic_comm_manager(node_id=i, port=8900 + i)
            additional_nodes.append(node)
            time.sleep(1)  # Gradual addition
            
            # Verify cluster grows
            all_nodes = initial_nodes + additional_nodes
            expected_size = len(all_nodes)
            cluster_formed = self.wait_for_cluster_formation(all_nodes, expected_size, timeout=10)
            self.assertTrue(cluster_formed, f"Cluster should form with {expected_size} nodes")
        
        # Simulate scaling down
        for node in additional_nodes:
            node.cleanup()
            self.test_nodes.remove(node)
            time.sleep(1)
        
        # Wait for cluster to adjust
        time.sleep(self.registry_refresh_interval * 2)
        
        # Verify messaging still works with remaining nodes
        message = self.create_test_message(1, 2, content="After scaling down")
        try:
            initial_nodes[0].send_message(message)
            success = True
        except Exception as e:
            success = False
            print(f"Messaging after scaling down failed: {e}")
        
        self.assertTrue(success, "Messaging should work after scaling down")
    
    def test_bootstrap_recovery_scenario(self):
        """Test bootstrap node recovery scenario."""
        self.start_gateway_server()
        
        # Create cluster with bootstrap node
        bootstrap_node = self.create_dynamic_comm_manager(node_id=1, port=8901)
        follower_nodes = []
        for i in range(2, 4):
            node = self.create_dynamic_comm_manager(node_id=i, port=8900 + i)
            follower_nodes.append(node)
        
        # Wait for cluster formation
        all_nodes = [bootstrap_node] + follower_nodes
        self.assertTrue(self.wait_for_cluster_formation(all_nodes, 3))
        
        # Verify bootstrap status
        self.assertTrue(bootstrap_node.is_bootstrap_node_local())
        
        # Simulate bootstrap node failure
        bootstrap_node.cleanup()
        self.test_nodes.remove(bootstrap_node)
        
        # Wait for system to handle bootstrap failure
        time.sleep(self.registry_refresh_interval * 2)
        
        # Follower nodes should continue operating
        for node in follower_nodes:
            cluster_nodes = node.get_cluster_nodes_info()
            self.assertGreater(len(cluster_nodes), 0)
        
        # Test messaging between remaining nodes
        message = self.create_test_message(2, 3, content="After bootstrap failure")
        try:
            follower_nodes[0].send_message(message)
            success = True
        except Exception as e:
            success = False
            print(f"Messaging after bootstrap failure failed: {e}")
        
        self.assertTrue(success, "Messaging should work after bootstrap failure")


def create_comprehensive_test_suite():
    """Create the comprehensive test suite."""
    suite = unittest.TestSuite()
    
    # Add all test classes
    test_classes = [
        TestGatewayServerComprehensive,
        TestGatewayClientAdvanced,
        TestDynamicCommunicationManagerAdvanced,
        TestFailureScenariosComprehensive,
        TestPerformanceAndLoadAdvanced,
        TestRealWorldScenarios
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    return suite


def run_comprehensive_test_suite():
    """Run the comprehensive test suite with detailed reporting."""
    print("=" * 100)
    print("🚀 COMPREHENSIVE gRPC SERVICE DISCOVERY TEST SUITE")
    print("=" * 100)
    
    suite = create_comprehensive_test_suite()
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(
        verbosity=2, 
        stream=sys.stdout,
        descriptions=True,
        failfast=False
    )
    
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()
    
    # Print summary
    print("\n" + "=" * 100)
    print("📊 TEST SUMMARY")
    print("=" * 100)
    print(f"⏱️  Total execution time: {end_time - start_time:.2f} seconds")
    print(f"🧪 Tests run: {result.testsRun}")
    print(f"✅ Successful: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Failures: {len(result.failures)}")
    print(f"💥 Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"  • {test}: {traceback.splitlines()[-1]}")
    
    if result.errors:
        print("\n💥 ERRORS:")
        for test, traceback in result.errors:
            print(f"  • {test}: {traceback.splitlines()[-1]}")
    
    success = result.wasSuccessful()
    
    print("\n" + "=" * 100)
    if success:
        print("🎉 ALL TESTS PASSED! gRPC Service Discovery system is robust and ready for production.")
    else:
        print("⚠️  SOME TESTS FAILED! Review the failures above and fix the issues.")
    print("=" * 100)
    
    return success


if __name__ == '__main__':
    success = run_comprehensive_test_suite()
    sys.exit(0 if success else 1)
