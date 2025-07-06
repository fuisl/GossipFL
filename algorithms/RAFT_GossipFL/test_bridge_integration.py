#!/usr/bin/env python3

"""
Test Service Discovery Bridge Integration

This script tests the integration between the service discovery bridge
and the communication manager to ensure Phase 1.1 refactoring works correctly.
"""

import sys
import os
import logging
import time
import threading

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, project_root)
print(f"Added to Python path: {project_root}")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_bridge_integration():
    """Test the service discovery bridge integration."""
    
    print("=== Testing Service Discovery Bridge Integration ===")
    
    try:
        # Import required modules
        from algorithms.RAFT_GossipFL.service_discovery_bridge import RaftServiceDiscoveryBridge
        from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager
        
        print("✓ Successfully imported bridge and communication manager")
        
        # Create a mock RAFT consensus for testing
        class MockRaftConsensus:
            def __init__(self):
                self.is_leader_flag = True
                self.proposals = []
            
            def is_leader(self):
                return self.is_leader_flag
            
            def propose_membership_change(self, action, node_id, node_info=None):
                proposal = {
                    'action': action,
                    'node_id': node_id,
                    'node_info': node_info,
                    'timestamp': time.time()
                }
                self.proposals.append(proposal)
                print(f"✓ RAFT proposal recorded: {action} node {node_id}")
                return True
            
            def coordinate_new_node_sync(self, node_id, node_info):
                print(f"✓ State sync coordinated for node {node_id}")
                return True
        
        # Create bridge
        bridge = RaftServiceDiscoveryBridge(node_id=1)
        mock_raft = MockRaftConsensus()
        bridge.set_raft_consensus(mock_raft)
        
        print("✓ Bridge created and RAFT consensus set")
        
        # Create communication manager with minimal configuration
        comm_manager = DynamicGRPCCommManager(
            host="localhost",
            port=8891,
            node_id=1,
            client_num=5,
            use_service_discovery=False  # Disable for testing
        )
        
        print("✓ Communication manager created")
        
        # Register bridge with communication manager
        success = comm_manager.register_service_discovery_bridge(bridge)
        if success:
            print("✓ Bridge successfully registered with communication manager")
        else:
            print("✗ Failed to register bridge")
            return False
        
        # Test bridge statistics
        stats = comm_manager.get_bridge_statistics()
        print(f"✓ Bridge statistics: {stats}")
        
        # Test mock node discovery event
        from fedml_core.distributed.communication.grpc.grpc_comm_manager import NodeInfo
        
        mock_node_info = NodeInfo(
            node_id=2,
            ip_address="192.168.1.100",
            port=8892,
            capabilities=['grpc', 'fedml'],
            metadata={'type': 'test_node'}
        )
        
        # Simulate node discovered event
        print("\n--- Simulating Node Discovery Event ---")
        if comm_manager.on_node_discovered:
            comm_manager.on_node_discovered(2, mock_node_info)
            print("✓ Node discovery event processed")
        
        # Check if RAFT proposal was made
        time.sleep(0.1)  # Allow processing
        if mock_raft.proposals:
            proposal = mock_raft.proposals[-1]
            print(f"✓ RAFT membership proposal created: {proposal}")
        else:
            print("✗ No RAFT proposal was made")
        
        # Simulate node lost event
        print("\n--- Simulating Node Loss Event ---")
        if comm_manager.on_node_lost:
            comm_manager.on_node_lost(2)
            print("✓ Node loss event processed")
        
        # Check final statistics
        final_stats = comm_manager.get_bridge_statistics()
        print(f"✓ Final bridge statistics: {final_stats}")
        
        # Cleanup
        comm_manager.cleanup()
        print("✓ Cleanup completed")
        
        print("\n=== Bridge Integration Test PASSED ===")
        return True
        
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_bridge_callback_replacement():
    """Test that bridge correctly replaces communication manager callbacks."""
    
    print("\n=== Testing Callback Replacement ===")
    
    try:
        from service_discovery_bridge import RaftServiceDiscoveryBridge
        from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager
        
        # Track original callback calls
        original_discovered_calls = []
        original_lost_calls = []
        
        def original_on_discovered(node_id, node_info):
            original_discovered_calls.append((node_id, node_info))
            print(f"✓ Original discovered callback called for node {node_id}")
        
        def original_on_lost(node_id):
            original_lost_calls.append(node_id)
            print(f"✓ Original lost callback called for node {node_id}")
        
        # Create communication manager with original callbacks
        comm_manager = DynamicGRPCCommManager(
            host="localhost",
            port=8893,
            node_id=3,
            client_num=5,
            use_service_discovery=False,
            on_node_discovered=original_on_discovered,
            on_node_lost=original_on_lost
        )
        
        # Verify original callbacks are set
        assert comm_manager.on_node_discovered == original_on_discovered
        assert comm_manager.on_node_lost == original_on_lost
        print("✓ Original callbacks verified")
        
        # Create and register bridge
        bridge = RaftServiceDiscoveryBridge(node_id=3)
        success = comm_manager.register_service_discovery_bridge(bridge)
        assert success
        
        # Verify callbacks were replaced
        assert comm_manager.on_node_discovered != original_on_discovered
        assert comm_manager.on_node_lost != original_on_lost
        print("✓ Callbacks successfully replaced by bridge")
        
        # Verify bridge stored original callbacks
        assert hasattr(bridge, '_original_on_node_discovered')
        assert hasattr(bridge, '_original_on_node_lost')
        assert bridge._original_on_node_discovered == original_on_discovered
        assert bridge._original_on_node_lost == original_on_lost
        print("✓ Original callbacks preserved in bridge")
        
        # Cleanup
        comm_manager.cleanup()
        print("✓ Callback replacement test PASSED")
        return True
        
    except Exception as e:
        print(f"✗ Callback replacement test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting Service Discovery Bridge Integration Tests...\n")
    
    test1_passed = test_bridge_integration()
    test2_passed = test_bridge_callback_replacement()
    
    print(f"\n=== Test Results ===")
    print(f"Bridge Integration Test: {'PASSED' if test1_passed else 'FAILED'}")
    print(f"Callback Replacement Test: {'PASSED' if test2_passed else 'FAILED'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 All Phase 1.1 tests PASSED!")
        sys.exit(0)
    else:
        print("\n❌ Some tests FAILED!")
        sys.exit(1)
