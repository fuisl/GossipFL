#!/usr/bin/env python3
"""
Test script for the Gateway Server and Client implementation.

This script tests the basic functionality of the gateway server and client
components for node discovery and registration.
"""

import json
import logging
import time
import threading
import subprocess
import sys
from typing import List
import requests

from gateway_server import GatewayServer
from gateway_client import GatewayClient


def test_gateway_server():
    """Test the gateway server functionality."""
    print("=" * 60)
    print("Testing Gateway Server")
    print("=" * 60)
    
    # Start gateway server
    gateway = GatewayServer(host='localhost', port=8080)
    gateway.start()
    
    # Wait for server to start
    time.sleep(1)
    
    try:
        # Test health check
        print("\n1. Testing health check...")
        response = requests.get('http://localhost:8080/health')
        print(f"Health check response: {response.status_code}")
        print(f"Response data: {response.json()}")
        
        # Test node registration
        print("\n2. Testing node registration...")
        node_data = {
            'node_id': 1,
            'ip_address': '127.0.0.1',
            'port': 5001,
            'capabilities': ['raft', 'gossip'],
            'metadata': {'test': True}
        }
        response = requests.post('http://localhost:8080/register', json=node_data)
        print(f"Registration response: {response.status_code}")
        reg_data = response.json()
        print(f"Registration data: {reg_data}")
        
        # Test getting nodes
        print("\n3. Testing get nodes...")
        response = requests.get('http://localhost:8080/nodes')
        print(f"Get nodes response: {response.status_code}")
        print(f"Nodes data: {response.json()}")
        
        # Test getting leader
        print("\n4. Testing get leader...")
        response = requests.get('http://localhost:8080/leader')
        print(f"Get leader response: {response.status_code}")
        print(f"Leader data: {response.json()}")
        
        # Test heartbeat
        print("\n5. Testing heartbeat...")
        heartbeat_data = {'node_id': 1}
        response = requests.post('http://localhost:8080/heartbeat', json=heartbeat_data)
        print(f"Heartbeat response: {response.status_code}")
        print(f"Heartbeat data: {response.json()}")
        
        # Test stats
        print("\n6. Testing stats...")
        response = requests.get('http://localhost:8080/stats')
        print(f"Stats response: {response.status_code}")
        print(f"Stats data: {response.json()}")
        
        # Register second node
        print("\n7. Testing second node registration...")
        node_data = {
            'node_id': 2,
            'ip_address': '127.0.0.1',
            'port': 5002,
            'capabilities': ['raft', 'gossip'],
            'metadata': {'test': True}
        }
        response = requests.post('http://localhost:8080/register', json=node_data)
        print(f"Second registration response: {response.status_code}")
        reg_data = response.json()
        print(f"Second registration data: {reg_data}")
        
        # Test getting nodes again
        print("\n8. Testing get nodes after second registration...")
        response = requests.get('http://localhost:8080/nodes')
        print(f"Get nodes response: {response.status_code}")
        print(f"Nodes data: {response.json()}")
        
        print("\n✓ Gateway server tests completed successfully!")
        
    except Exception as e:
        print(f"✗ Gateway server test failed: {e}")
        return False
    finally:
        gateway.stop()
    
    return True


def test_gateway_client():
    """Test the gateway client functionality."""
    print("=" * 60)
    print("Testing Gateway Client")
    print("=" * 60)
    
    # Start gateway server
    gateway = GatewayServer(host='localhost', port=8081)
    gateway.start()
    
    # Wait for server to start
    time.sleep(1)
    
    try:
        # Test client discovery
        print("\n1. Testing client discovery...")
        client1 = GatewayClient('localhost', 8081)
        
        is_bootstrap, cluster_info = client1.discover_cluster(
            node_id=1,
            ip_address='127.0.0.1',
            port=5001,
            capabilities=['raft', 'gossip', 'test'],
            metadata={'client': 'test1'}
        )
        
        print(f"Client 1 - Bootstrap: {is_bootstrap}")
        print(f"Client 1 - Cluster info: {cluster_info}")
        
        # Test second client
        print("\n2. Testing second client discovery...")
        client2 = GatewayClient('localhost', 8081)
        
        is_bootstrap2, cluster_info2 = client2.discover_cluster(
            node_id=2,
            ip_address='127.0.0.1',
            port=5002,
            capabilities=['raft', 'gossip', 'test'],
            metadata={'client': 'test2'}
        )
        
        print(f"Client 2 - Bootstrap: {is_bootstrap2}")
        print(f"Client 2 - Cluster info: {cluster_info2}")
        
        # Test client operations
        print("\n3. Testing client operations...")
        
        # Get nodes
        nodes = client1.get_nodes()
        print(f"Client 1 nodes: {nodes}")
        
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
        time.sleep(2)  # Let heartbeat run
        
        print("\n✓ Gateway client tests completed successfully!")
        
    except Exception as e:
        print(f"✗ Gateway client test failed: {e}")
        return False
    finally:
        client1.shutdown()
        client2.shutdown()
        gateway.stop()
    
    return True


def test_multi_node_scenario():
    """Test multi-node scenario with gateway."""
    print("=" * 60)
    print("Testing Multi-Node Scenario")
    print("=" * 60)
    
    # Start gateway server
    gateway = GatewayServer(host='localhost', port=8082)
    gateway.start()
    
    # Wait for server to start
    time.sleep(1)
    
    clients = []
    
    try:
        # Create multiple clients
        print("\n1. Creating multiple clients...")
        for i in range(5):
            client = GatewayClient('localhost', 8082)
            
            is_bootstrap, cluster_info = client.discover_cluster(
                node_id=i + 1,
                ip_address='127.0.0.1',
                port=5000 + i + 1,
                capabilities=['raft', 'gossip'],
                metadata={'node_index': i}
            )
            
            clients.append(client)
            
            print(f"Node {i+1} - Bootstrap: {is_bootstrap}, Nodes in cluster: {len(cluster_info['nodes'])}")
            
            # Small delay between registrations
            time.sleep(0.5)
        
        # Test final state
        print("\n2. Testing final cluster state...")
        final_nodes = clients[0].get_nodes()
        final_leader = clients[0].get_leader()
        final_stats = clients[0].get_stats()
        
        print(f"Final nodes count: {len(final_nodes)}")
        print(f"Final leader: {final_leader}")
        print(f"Final stats: {final_stats}")
        
        # Test heartbeat functionality
        print("\n3. Testing heartbeat functionality...")
        time.sleep(5)  # Let heartbeats run
        
        # Check stats again
        stats_after_heartbeat = clients[0].get_stats()
        print(f"Stats after heartbeat: {stats_after_heartbeat}")
        
        print("\n✓ Multi-node scenario tests completed successfully!")
        
    except Exception as e:
        print(f"✗ Multi-node scenario test failed: {e}")
        return False
    finally:
        for client in clients:
            client.shutdown()
        gateway.stop()
    
    return True


def test_bootstrap_scenario():
    """Test bootstrap scenario detection."""
    print("=" * 60)
    print("Testing Bootstrap Scenario")
    print("=" * 60)
    
    # Start gateway server
    gateway = GatewayServer(host='localhost', port=8083)
    gateway.start()
    
    # Wait for server to start
    time.sleep(1)
    
    try:
        # Test first node (should be bootstrap)
        print("\n1. Testing first node (bootstrap)...")
        client1 = GatewayClient('localhost', 8083)
        
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
        
        # Test second node (should not be bootstrap)
        print("\n2. Testing second node (not bootstrap)...")
        client2 = GatewayClient('localhost', 8083)
        
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
        
        # Verify leader assignment
        leader = client2.get_leader()
        print(f"Leader after two nodes: {leader}")
        
        print("\n✓ Bootstrap scenario tests completed successfully!")
        
    except Exception as e:
        print(f"✗ Bootstrap scenario test failed: {e}")
        return False
    finally:
        client1.shutdown()
        client2.shutdown()
        gateway.stop()
    
    return True


def run_all_tests():
    """Run all test scenarios."""
    print("Starting Gateway Implementation Tests")
    print("=" * 80)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    tests = [
        ("Gateway Server", test_gateway_server),
        ("Gateway Client", test_gateway_client),
        ("Multi-Node Scenario", test_multi_node_scenario),
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
        print(f"{test_name:<25} : {status}")
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
