#!/usr/bin/env python3
"""
Test script for the gRPC Gateway Server and Client implementation.

This script tests the integration between the gRPC gateway server and client
components for node discovery and registration.
"""

import logging
import time
import threading
import sys
import subprocess
from typing import List

from grpc_gateway_server import GRPCGatewayServer
from grpc_gateway_client import GRPCGatewayClient, GRPCGatewayDiscoveryMixin


def test_grpc_gateway_server():
    """Test the gRPC gateway server functionality."""
    print("=" * 60)
    print("Testing gRPC Gateway Server")
    print("=" * 60)
    
    # Start gateway server
    gateway = GRPCGatewayServer(host='localhost', port=8090)
    gateway.start()
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Create client
        client = GRPCGatewayClient('localhost', 8090)
        
        # Test health check
        print("\n1. Testing health check...")
        health = client.health_check()
        print(f"Health check response: {health}")
        assert health is not None, "Health check failed"
        
        # Test node registration
        print("\n2. Testing node registration...")
        reg_result = client.register_node(
            node_id=1,
            ip_address='127.0.0.1',
            port=5001,
            capabilities=['raft', 'gossip'],
            metadata={'test': 'true'}
        )
        print(f"Registration result: {reg_result}")
        assert reg_result is not None, "Registration failed"
        assert reg_result['status'] in ['registered', 'updated'], "Registration status invalid"
        
        # Test getting nodes
        print("\n3. Testing get nodes...")
        nodes = client.get_nodes()
        print(f"Nodes: {nodes}")
        assert isinstance(nodes, list), "Nodes should be a list"
        
        # Test getting leader
        print("\n4. Testing get leader...")
        leader = client.get_leader()
        print(f"Leader: {leader}")
        # Leader might be None if no bootstrap node
        
        # Test heartbeat
        print("\n5. Testing heartbeat...")
        heartbeat_result = client.heartbeat()
        print(f"Heartbeat result: {heartbeat_result}")
        assert heartbeat_result is not None, "Heartbeat failed"
        
        # Test stats
        print("\n6. Testing stats...")
        stats = client.get_stats()
        print(f"Stats: {stats}")
        assert stats is not None, "Stats request failed"
        
        # Register second node
        print("\n7. Testing second node registration...")
        client2 = GRPCGatewayClient('localhost', 8090)
        reg_result2 = client2.register_node(
            node_id=2,
            ip_address='127.0.0.1',
            port=5002,
            capabilities=['raft', 'gossip'],
            metadata={'test': 'true'}
        )
        print(f"Second registration result: {reg_result2}")
        assert reg_result2 is not None, "Second registration failed"
        
        # Test getting nodes again
        print("\n8. Testing get nodes after second registration...")
        nodes = client.get_nodes()
        print(f"Nodes after second registration: {len(nodes)} nodes")
        
        print("\n✓ gRPC Gateway server tests completed successfully!")
        
        client2.shutdown()
        client.shutdown()
        return True
        
    except Exception as e:
        print(f"✗ gRPC Gateway server test failed: {e}")
        return False
    finally:
        gateway.stop()


def test_grpc_gateway_client():
    """Test the gRPC gateway client functionality."""
    print("=" * 60)
    print("Testing gRPC Gateway Client")
    print("=" * 60)
    
    # Start gateway server
    gateway = GRPCGatewayServer(host='localhost', port=8091)
    gateway.start()
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Test client discovery
        print("\n1. Testing client discovery...")
        client1 = GRPCGatewayClient('localhost', 8091)
        
        is_bootstrap, cluster_info = client1.discover_cluster(
            node_id=1,
            ip_address='127.0.0.1',
            port=5001,
            capabilities=['raft', 'gossip', 'test'],
            metadata={'client': 'test1'}
        )
        
        print(f"Client 1 - Bootstrap: {is_bootstrap}")
        print(f"Client 1 - Cluster info: {cluster_info}")
        assert is_bootstrap == True, "First node should be bootstrap"
        
        # Test second client
        print("\n2. Testing second client discovery...")
        client2 = GRPCGatewayClient('localhost', 8091)
        
        is_bootstrap2, cluster_info2 = client2.discover_cluster(
            node_id=2,
            ip_address='127.0.0.1',
            port=5002,
            capabilities=['raft', 'gossip', 'test'],
            metadata={'client': 'test2'}
        )
        
        print(f"Client 2 - Bootstrap: {is_bootstrap2}")
        print(f"Client 2 - Cluster info: {cluster_info2}")
        assert is_bootstrap2 == False, "Second node should not be bootstrap"
        assert len(cluster_info2['nodes']) >= 1, "Should find existing nodes"
        
        # Test client operations
        print("\n3. Testing client operations...")
        
        # Get nodes
        nodes = client1.get_nodes()
        print(f"Client 1 nodes: {len(nodes)} nodes")
        
        # Get leader
        leader = client1.get_leader()
        print(f"Client 1 leader: {leader}")
        
        # Update leader
        success = client1.update_leader(2)
        print(f"Leader update success: {success}")
        
        # Get stats
        stats = client1.get_stats()
        print(f"Gateway stats: {stats}")
        
        # Test heartbeat (should be automatic)
        print("\n4. Testing heartbeat...")
        time.sleep(3)  # Let heartbeat run
        
        print("\n✓ gRPC Gateway client tests completed successfully!")
        
        client1.shutdown()
        client2.shutdown()
        return True
        
    except Exception as e:
        print(f"✗ gRPC Gateway client test failed: {e}")
        return False
    finally:
        gateway.stop()


def test_discovery_mixin():
    """Test the discovery mixin functionality."""
    print("=" * 60)
    print("Testing Gateway Discovery Mixin")
    print("=" * 60)
    
    # Start gateway server
    gateway = GRPCGatewayServer(host='localhost', port=8092)
    gateway.start()
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Create a test class using the mixin
        class TestNode(GRPCGatewayDiscoveryMixin):
            def __init__(self, node_id, ip, port):
                super().__init__()
                self.node_id = node_id
                self.ip = ip
                self.port = port
        
        # Test first node (bootstrap)
        print("\n1. Testing first node with mixin...")
        node1 = TestNode(1, '127.0.0.1', 5001)
        
        success = node1.initialize_gateway_discovery(
            gateway_host='localhost',
            gateway_port=8092,
            node_id=node1.node_id,
            ip_address=node1.ip,
            port=node1.port,
            capabilities=['raft', 'gossip'],
            metadata={'role': 'test_node'}
        )
        
        assert success, "Gateway discovery initialization failed"
        assert node1.is_bootstrap_node(), "First node should be bootstrap"
        
        print(f"Node 1 - Bootstrap: {node1.is_bootstrap_node()}")
        print(f"Node 1 - Discovery info: {node1.get_discovery_info()}")
        
        # Test second node
        print("\n2. Testing second node with mixin...")
        node2 = TestNode(2, '127.0.0.1', 5002)
        
        success2 = node2.initialize_gateway_discovery(
            gateway_host='localhost',
            gateway_port=8092,
            node_id=node2.node_id,
            ip_address=node2.ip,
            port=node2.port,
            capabilities=['raft', 'gossip'],
            metadata={'role': 'test_node'}
        )
        
        assert success2, "Second node gateway discovery initialization failed"
        assert not node2.is_bootstrap_node(), "Second node should not be bootstrap"
        
        print(f"Node 2 - Bootstrap: {node2.is_bootstrap_node()}")
        print(f"Node 2 - Cluster nodes: {len(node2.get_cluster_nodes())} nodes")
        
        # Test mixin operations
        print("\n3. Testing mixin operations...")
        
        # Get cluster nodes
        cluster_nodes = node2.get_cluster_nodes()
        print(f"Cluster nodes from mixin: {len(cluster_nodes)} nodes")
        
        # Get cluster leader
        leader = node2.get_cluster_leader()
        print(f"Cluster leader from mixin: {leader}")
        
        # Update leader
        leader_update_success = node2.update_gateway_leader(2)
        print(f"Leader update through mixin: {leader_update_success}")
        
        print("\n✓ Gateway Discovery Mixin tests completed successfully!")
        
        node1.shutdown_gateway_discovery()
        node2.shutdown_gateway_discovery()
        return True
        
    except Exception as e:
        print(f"✗ Gateway Discovery Mixin test failed: {e}")
        return False
    finally:
        gateway.stop()


def test_bootstrap_scenario():
    """Test bootstrap scenario detection."""
    print("=" * 60)
    print("Testing Bootstrap Scenario")
    print("=" * 60)
    
    # Start gateway server
    gateway = GRPCGatewayServer(host='localhost', port=8093)
    gateway.start()
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Test first node (should be bootstrap)
        print("\n1. Testing first node (bootstrap)...")
        client1 = GRPCGatewayClient('localhost', 8093)
        
        is_bootstrap, cluster_info = client1.discover_cluster(
            node_id=1,
            ip_address='127.0.0.1',
            port=5001,
            capabilities=['raft', 'gossip'],
            metadata={'role': 'first'}
        )
        
        print(f"First node - Bootstrap: {is_bootstrap}")
        print(f"First node - Should be True: {is_bootstrap == True}")
        print(f"First node - Cluster size: {len(cluster_info['nodes'])}")
        assert is_bootstrap == True, "First node should be bootstrap"
        
        # Test second node (should not be bootstrap)
        print("\n2. Testing second node (not bootstrap)...")
        client2 = GRPCGatewayClient('localhost', 8093)
        
        is_bootstrap2, cluster_info2 = client2.discover_cluster(
            node_id=2,
            ip_address='127.0.0.1',
            port=5002,
            capabilities=['raft', 'gossip'],
            metadata={'role': 'second'}
        )
        
        print(f"Second node - Bootstrap: {is_bootstrap2}")
        print(f"Second node - Should be False: {is_bootstrap2 == False}")
        print(f"Second node - Cluster size: {len(cluster_info2['nodes'])}")
        assert is_bootstrap2 == False, "Second node should not be bootstrap"
        
        # Verify leader assignment
        leader = client2.get_leader()
        print(f"Leader after two nodes: {leader}")
        
        print("\n✓ Bootstrap scenario tests completed successfully!")
        
        client1.shutdown()
        client2.shutdown()
        return True
        
    except Exception as e:
        print(f"✗ Bootstrap scenario test failed: {e}")
        return False
    finally:
        gateway.stop()


def run_all_tests():
    """Run all test scenarios."""
    print("Starting gRPC Gateway Implementation Tests")
    print("=" * 80)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    tests = [
        ("gRPC Gateway Server", test_grpc_gateway_server),
        ("gRPC Gateway Client", test_grpc_gateway_client),
        ("Gateway Discovery Mixin", test_discovery_mixin),
        ("Bootstrap Scenario", test_bootstrap_scenario)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'='*80}")
        print(f"Running {test_name} Test")
        print(f"{'='*80}")
        
        try:
            result = test_func()
            results.append((test_name, result))
            
            if result:
                print(f"✓ {test_name} test PASSED")
            else:
                print(f"✗ {test_name} test FAILED")
                
        except Exception as e:
            print(f"✗ {test_name} test FAILED with exception: {e}")
            results.append((test_name, False))
        
        # Small delay between tests
        time.sleep(2)
    
    # Print final results
    print("\n" + "=" * 80)
    print("FINAL TEST RESULTS")
    print("=" * 80)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        print(f"{test_name:<30} : {status}")
        if result:
            passed += 1
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print("❌ Some tests failed!")
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
