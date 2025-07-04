#!/usr/bin/env python3
"""
Stress Tests and Performance Benchmarks for gRPC Service Discovery

This module provides stress tests and performance benchmarks for the gRPC service discovery
system to ensure it can handle real-world loads and edge cases.
"""

import unittest
import time
import threading
import sys
import os
import statistics
import concurrent.futures
from typing import List, Dict, Any, Tuple
import random

# Add paths
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL')
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL/algorithms/RAFT_GossipFL')

class StressTestCase(unittest.TestCase):
    """Base class for stress tests."""
    
    def setUp(self):
        """Set up stress test environment."""
        self.gateway_server = None
        self.test_clients = []
        self.test_nodes = []
        self.performance_metrics = {}
        
    def tearDown(self):
        """Clean up stress test environment."""
        for client in self.test_clients:
            try:
                client.shutdown()
            except:
                pass
        
        for node in self.test_nodes:
            try:
                if hasattr(node, 'cleanup'):
                    node.cleanup()
            except:
                pass
        
        if self.gateway_server:
            try:
                self.gateway_server.stop()
            except:
                pass
    
    def start_gateway_server(self):
        """Start gateway server for stress testing."""
        from algorithms.RAFT_GossipFL.grpc_gateway_server import GRPCGatewayServer
        
        self.gateway_server = GRPCGatewayServer(host='localhost', port=8090)
        self.gateway_server.start()
        time.sleep(1)
    
    def create_client(self):
        """Create a test client."""
        from algorithms.RAFT_GossipFL.grpc_gateway_client import GRPCGatewayClient
        
        client = GRPCGatewayClient('localhost', 8090)
        self.test_clients.append(client)
        return client
    
    def measure_operation_time(self, operation_func, *args, **kwargs):
        """Measure time for an operation."""
        start_time = time.time()
        result = operation_func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time
    
    def collect_performance_metrics(self, operation_name: str, times: List[float]):
        """Collect performance metrics for an operation."""
        if times:
            self.performance_metrics[operation_name] = {
                'count': len(times),
                'min': min(times),
                'max': max(times),
                'avg': statistics.mean(times),
                'median': statistics.median(times),
                'p95': statistics.quantiles(times, n=20)[18] if len(times) > 20 else max(times),
                'p99': statistics.quantiles(times, n=100)[98] if len(times) > 100 else max(times)
            }


class TestGatewayStressLoad(StressTestCase):
    """Stress tests for gateway server load handling."""
    
    def test_high_registration_load(self):
        """Test gateway under high registration load."""
        self.start_gateway_server()
        
        NUM_NODES = 50
        CONCURRENT_THREADS = 10
        
        def register_batch(start_id, count):
            """Register a batch of nodes."""
            client = self.create_client()
            times = []
            
            for i in range(count):
                node_id = start_id + i
                _, duration = self.measure_operation_time(
                    client.register_node,
                    node_id, '127.0.0.1', 8900 + node_id, 
                    ['grpc', 'stress'], {'batch': str(start_id)}
                )
                times.append(duration)
            
            return times
        
        # Run concurrent registrations
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
            batch_size = NUM_NODES // CONCURRENT_THREADS
            futures = []
            
            for i in range(CONCURRENT_THREADS):
                start_id = i * batch_size + 1
                future = executor.submit(register_batch, start_id, batch_size)
                futures.append(future)
            
            # Collect results
            all_times = []
            for future in concurrent.futures.as_completed(futures):
                batch_times = future.result()
                all_times.extend(batch_times)
        
        # Analyze performance
        self.collect_performance_metrics('high_registration_load', all_times)
        
        # Verify all registrations succeeded
        self.assertEqual(len(all_times), NUM_NODES)
        
        # Performance assertions
        avg_time = statistics.mean(all_times)
        max_time = max(all_times)
        
        self.assertLess(avg_time, 1.0, "Average registration time should be under 1 second")
        self.assertLess(max_time, 5.0, "Maximum registration time should be under 5 seconds")
        
        # Verify final state
        client = self.create_client()
        stats = client.get_stats()
        self.assertEqual(stats['total_nodes'], NUM_NODES)
    
    def test_high_query_load(self):
        """Test gateway under high query load."""
        self.start_gateway_server()
        
        # Register some nodes first
        client = self.create_client()
        for i in range(1, 11):
            client.register_node(i, '127.0.0.1', 8900 + i, ['grpc'], {'id': str(i)})
        
        NUM_QUERIES = 200
        CONCURRENT_THREADS = 20
        
        def query_batch(query_count):
            """Perform a batch of queries."""
            client = self.create_client()
            times = []
            
            operations = [
                ('get_nodes', lambda: client.get_nodes()),
                ('get_leader', lambda: client.get_leader()),
                ('get_stats', lambda: client.get_stats()),
                ('health_check', lambda: client.health_check())
            ]
            
            for i in range(query_count):
                op_name, op_func = random.choice(operations)
                _, duration = self.measure_operation_time(op_func)
                times.append(duration)
            
            return times
        
        # Run concurrent queries
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
            queries_per_thread = NUM_QUERIES // CONCURRENT_THREADS
            futures = []
            
            for i in range(CONCURRENT_THREADS):
                future = executor.submit(query_batch, queries_per_thread)
                futures.append(future)
            
            # Collect results
            all_times = []
            for future in concurrent.futures.as_completed(futures):
                batch_times = future.result()
                all_times.extend(batch_times)
        
        # Analyze performance
        self.collect_performance_metrics('high_query_load', all_times)
        
        # Performance assertions
        avg_time = statistics.mean(all_times)
        p95_time = statistics.quantiles(all_times, n=20)[18] if len(all_times) > 20 else max(all_times)
        
        self.assertLess(avg_time, 0.1, "Average query time should be under 100ms")
        self.assertLess(p95_time, 0.5, "95th percentile query time should be under 500ms")
    
    def test_rapid_node_churn(self):
        """Test rapid node joining and leaving."""
        self.start_gateway_server()
        
        NUM_CYCLES = 20
        NODES_PER_CYCLE = 5
        
        churn_times = []
        
        for cycle in range(NUM_CYCLES):
            cycle_start = time.time()
            
            # Register nodes
            clients = []
            for i in range(NODES_PER_CYCLE):
                client = self.create_client()
                clients.append(client)
                
                node_id = cycle * NODES_PER_CYCLE + i + 1
                client.register_node(
                    node_id, '127.0.0.1', 8900 + node_id, 
                    ['grpc', 'churn'], {'cycle': str(cycle)}
                )
            
            # Remove nodes
            for i, client in enumerate(clients):
                node_id = cycle * NODES_PER_CYCLE + i + 1
                client.remove_node(node_id)
                client.shutdown()
            
            cycle_end = time.time()
            churn_times.append(cycle_end - cycle_start)
        
        # Analyze churn performance
        self.collect_performance_metrics('rapid_node_churn', churn_times)
        
        # Verify system stability
        client = self.create_client()
        stats = client.get_stats()
        self.assertEqual(stats['total_nodes'], 0)  # All nodes should be removed
        
        # Performance assertions
        avg_churn_time = statistics.mean(churn_times)
        self.assertLess(avg_churn_time, 5.0, "Average churn cycle should be under 5 seconds")


class TestMessagingStressLoad(StressTestCase):
    """Stress tests for messaging performance."""
    
    def test_high_volume_messaging(self):
        """Test high volume messaging between nodes."""
        self.start_gateway_server()
        
        # Create dynamic communication managers
        from fedml_core.distributed.communication.grpc.dynamic_grpc_comm_manager import DynamicGRPCCommManager
        from fedml_core.distributed.communication.message import Message
        
        NUM_NODES = 5
        MESSAGES_PER_NODE = 20
        
        nodes = []
        for i in range(1, NUM_NODES + 1):
            node = DynamicGRPCCommManager(
                port=8900 + i,
                node_id=i,
                host='127.0.0.1',
                client_num=10,
                gateway_host='localhost',
                gateway_port=8090,
                capabilities=['grpc', 'messaging', 'stress']
            )
            nodes.append(node)
            self.test_nodes.append(node)
        
        # Wait for cluster formation
        time.sleep(5)
        
        # Verify cluster formation
        for node in nodes:
            cluster_nodes = node.get_cluster_nodes_info()
            self.assertEqual(len(cluster_nodes), NUM_NODES)
        
        def send_messages(sender_node, message_count):
            """Send messages from one node to others."""
            times = []
            
            for i in range(message_count):
                # Pick random target (excluding self)
                target_nodes = [n for n in nodes if n.node_id != sender_node.node_id]
                target_node = random.choice(target_nodes)
                
                message = Message(
                    type=1,
                    sender_id=sender_node.node_id,
                    receiver_id=target_node.node_id
                )
                message.set_content(f"Stress test message {i}")
                
                start_time = time.time()
                try:
                    sender_node.send_message(message)
                    end_time = time.time()
                    times.append(end_time - start_time)
                except Exception as e:
                    print(f"Message send failed: {e}")
            
            return times
        
        # Send messages concurrently from all nodes
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_NODES) as executor:
            futures = []
            
            for node in nodes:
                future = executor.submit(send_messages, node, MESSAGES_PER_NODE)
                futures.append(future)
            
            # Collect results
            all_times = []
            for future in concurrent.futures.as_completed(futures):
                node_times = future.result()
                all_times.extend(node_times)
        
        # Analyze messaging performance
        self.collect_performance_metrics('high_volume_messaging', all_times)
        
        # Performance assertions
        if all_times:
            avg_time = statistics.mean(all_times)
            max_time = max(all_times)
            
            self.assertLess(avg_time, 1.0, "Average message time should be under 1 second")
            self.assertLess(max_time, 5.0, "Maximum message time should be under 5 seconds")
    
    def test_concurrent_cluster_operations(self):
        """Test concurrent cluster operations."""
        self.start_gateway_server()
        
        # Create multiple clients
        clients = []
        for i in range(10):
            client = self.create_client()
            clients.append(client)
        
        def perform_operations(client, node_id_offset):
            """Perform various operations concurrently."""
            operation_times = []
            
            # Register node
            _, reg_time = self.measure_operation_time(
                client.register_node,
                node_id_offset, '127.0.0.1', 8900 + node_id_offset,
                ['grpc', 'concurrent'], {'offset': str(node_id_offset)}
            )
            operation_times.append(('register', reg_time))
            
            # Perform queries
            for _ in range(5):
                _, query_time = self.measure_operation_time(client.get_nodes)
                operation_times.append(('query', query_time))
            
            # Heartbeat
            _, hb_time = self.measure_operation_time(client.heartbeat)
            operation_times.append(('heartbeat', hb_time))
            
            return operation_times
        
        # Run concurrent operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            
            for i, client in enumerate(clients):
                future = executor.submit(perform_operations, client, i + 1)
                futures.append(future)
            
            # Collect results
            all_operations = []
            for future in concurrent.futures.as_completed(futures):
                client_operations = future.result()
                all_operations.extend(client_operations)
        
        # Analyze by operation type
        by_operation = {}
        for op_type, duration in all_operations:
            if op_type not in by_operation:
                by_operation[op_type] = []
            by_operation[op_type].append(duration)
        
        # Collect metrics for each operation type
        for op_type, times in by_operation.items():
            self.collect_performance_metrics(f'concurrent_{op_type}', times)
        
        # Verify final state
        client = self.create_client()
        stats = client.get_stats()
        self.assertEqual(stats['total_nodes'], 10)


class TestFailureStressScenarios(StressTestCase):
    """Stress tests for failure scenarios."""
    
    def test_gateway_restart_during_load(self):
        """Test gateway restart during high load."""
        self.start_gateway_server()
        
        # Register initial nodes
        client = self.create_client()
        for i in range(1, 6):
            client.register_node(i, '127.0.0.1', 8900 + i, ['grpc'], {'initial': 'true'})
        
        def continuous_operations():
            """Perform continuous operations."""
            client = self.create_client()
            operation_count = 0
            error_count = 0
            
            start_time = time.time()
            while time.time() - start_time < 20:  # Run for 20 seconds
                try:
                    client.get_nodes()
                    operation_count += 1
                except Exception:
                    error_count += 1
                time.sleep(0.1)
            
            return operation_count, error_count
        
        # Start continuous operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future = executor.submit(continuous_operations)
            
            # Restart gateway during operations
            time.sleep(5)
            self.gateway_server.stop()
            time.sleep(2)
            self.start_gateway_server()
            
            # Wait for operations to complete
            operation_count, error_count = future.result()
        
        # Verify system recovery
        client = self.create_client()
        health = client.health_check()
        self.assertIsNotNone(health)
        
        # Some operations should have succeeded
        self.assertGreater(operation_count, 0)
        
        # Error rate should be reasonable
        total_operations = operation_count + error_count
        error_rate = error_count / total_operations if total_operations > 0 else 0
        self.assertLess(error_rate, 0.5, "Error rate should be under 50% during restart")


class TestLongRunningStability(StressTestCase):
    """Long-running stability tests."""
    
    def test_long_running_cluster_stability(self):
        """Test cluster stability over extended period."""
        self.start_gateway_server()
        
        # Create initial cluster
        from fedml_core.distributed.communication.grpc.dynamic_grpc_comm_manager import DynamicGRPCCommManager
        
        nodes = []
        for i in range(1, 4):
            node = DynamicGRPCCommManager(
                port=8900 + i,
                node_id=i,
                host='127.0.0.1',
                client_num=10,
                gateway_host='localhost',
                gateway_port=8090,
                capabilities=['grpc', 'stability']
            )
            nodes.append(node)
            self.test_nodes.append(node)
        
        # Wait for cluster formation
        time.sleep(3)
        
        # Run stability test for 30 seconds
        start_time = time.time()
        stability_checks = []
        
        while time.time() - start_time < 30:
            # Check cluster consistency
            cluster_sizes = []
            for node in nodes:
                try:
                    cluster_nodes = node.get_cluster_nodes_info()
                    cluster_sizes.append(len(cluster_nodes))
                except Exception:
                    cluster_sizes.append(0)
            
            # All nodes should see same cluster size
            if len(set(cluster_sizes)) == 1:
                stability_checks.append(True)
            else:
                stability_checks.append(False)
            
            time.sleep(1)
        
        # Analyze stability
        stability_rate = sum(stability_checks) / len(stability_checks)
        self.assertGreater(stability_rate, 0.9, "Cluster should be stable >90% of the time")


def print_performance_report(test_results):
    """Print detailed performance report."""
    print("\n" + "="*100)
    print("🚀 PERFORMANCE BENCHMARK REPORT")
    print("="*100)
    
    for test_case, metrics in test_results.items():
        print(f"\n📊 {test_case}:")
        print("-" * 60)
        
        for operation, stats in metrics.items():
            print(f"  {operation}:")
            print(f"    Count: {stats['count']}")
            print(f"    Average: {stats['avg']:.3f}s")
            print(f"    Median: {stats['median']:.3f}s")
            print(f"    Min: {stats['min']:.3f}s")
            print(f"    Max: {stats['max']:.3f}s")
            print(f"    P95: {stats['p95']:.3f}s")
            print(f"    P99: {stats['p99']:.3f}s")
            print()


def run_stress_tests():
    """Run all stress tests."""
    print("🔥 STRESS TESTS AND PERFORMANCE BENCHMARKS")
    print("=" * 100)
    
    # Create test suite
    suite = unittest.TestSuite()
    
    test_classes = [
        TestGatewayStressLoad,
        TestMessagingStressLoad,
        TestFailureStressScenarios,
        TestLongRunningStability
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()
    
    # Collect performance metrics from test cases
    performance_results = {}
    for test_class in test_classes:
        # This is a simplified approach - in practice, you'd need to
        # collect metrics from actual test instances
        class_name = test_class.__name__
        performance_results[class_name] = {}
    
    print(f"\n⏱️  Total stress test time: {end_time - start_time:.2f} seconds")
    print(f"🧪 Tests run: {result.testsRun}")
    print(f"✅ Successful: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"❌ Failures: {len(result.failures)}")
    print(f"💥 Errors: {len(result.errors)}")
    
    success = result.wasSuccessful()
    
    if success:
        print("\n🎉 ALL STRESS TESTS PASSED! System is ready for production load.")
    else:
        print("\n⚠️  SOME STRESS TESTS FAILED! Review performance and stability issues.")
    
    return success


if __name__ == '__main__':
    success = run_stress_tests()
    sys.exit(0 if success else 1)
