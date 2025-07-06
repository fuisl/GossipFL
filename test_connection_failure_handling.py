#!/usr/bin/env python3
"""
Test script for connection failure handling in the gRPC communication manager.

This script tests the new connection failure detection and automatic node removal
functionality that was added to handle the StatusCode.UNAVAILABLE gRPC errors.
"""

import sys
import os
import logging
import time
import threading
from unittest.mock import Mock, MagicMock
import grpc

# Add the project root to the path
sys.path.insert(0, '/workspaces/GossipFL')

from fedml_core.distributed.communication.grpc.grpc_comm_manager import (
    DynamicGRPCCommManager, 
    ConnectionFailureHandler,
    NodeInfo
)
from fedml_core.distributed.communication.message import Message

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MockServiceDiscoveryBridge:
    """Mock service discovery bridge for testing."""
    
    def __init__(self, is_leader=True):
        self.is_leader_flag = is_leader
        self.proposed_changes = []
        
    def propose_membership_change(self, action, node_id, reason=None):
        """Mock propose membership change."""
        change = {
            'action': action,
            'node_id': node_id,
            'reason': reason,
            'timestamp': time.time()
        }
        self.proposed_changes.append(change)
        logger.info(f"Mock: Proposed membership change: {change}")
        return True
        
    def is_leader(self):
        return self.is_leader_flag
        
    def get_leader_id(self):
        return 1 if self.is_leader_flag else None


def create_grpc_unavailable_error():
    """Create a mock gRPC unavailable error similar to the one in the log."""
    # Create a mock gRPC error that looks like the real one
    error = grpc.RpcError()
    error.code = lambda: grpc.StatusCode.UNAVAILABLE
    error.details = lambda: "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:9003: Failed to connect to remote host: Connection refused"
    return error


def test_connection_failure_detection():
    """Test that connection failures are properly detected."""
    logger.info("=" * 60)
    logger.info("Testing Connection Failure Detection")
    logger.info("=" * 60)
    
    # Create a mock communication manager
    mock_comm_manager = Mock()
    mock_comm_manager.node_id = 1
    mock_comm_manager.bridge_registered = True
    mock_comm_manager.service_discovery_bridge = MockServiceDiscoveryBridge(is_leader=True)
    mock_comm_manager._remove_node_from_registry = Mock()
    
    # Create the failure handler
    failure_handler = ConnectionFailureHandler(mock_comm_manager)
    
    # Test 1: Non-connection error should not trigger removal
    logger.info("Test 1: Non-connection errors should not trigger removal")
    regular_error = Exception("Some other error")
    is_conn_failure = failure_handler.is_connection_failure(regular_error)
    logger.info(f"Regular error detected as connection failure: {is_conn_failure}")
    assert not is_conn_failure, "Regular errors should not be detected as connection failures"
    
    # Test 2: gRPC UNAVAILABLE error should trigger detection
    logger.info("Test 2: gRPC UNAVAILABLE errors should be detected")
    grpc_error = create_grpc_unavailable_error()
    is_conn_failure = failure_handler.is_connection_failure(grpc_error)
    logger.info(f"gRPC UNAVAILABLE error detected as connection failure: {is_conn_failure}")
    assert is_conn_failure, "gRPC UNAVAILABLE errors should be detected as connection failures"
    
    # Test 3: Multiple failures should trigger removal
    logger.info("Test 3: Multiple failures should trigger node removal")
    node_id = 3
    
    # First two failures should not remove the node
    for i in range(1, 3):
        removed = failure_handler.handle_connection_failure(node_id, grpc_error)
        logger.info(f"Failure {i}: Node removed = {removed}")
        assert not removed, f"Node should not be removed after {i} failures"
    
    # Third failure should trigger removal
    removed = failure_handler.handle_connection_failure(node_id, grpc_error)
    logger.info(f"Failure 3: Node removed = {removed}")
    assert removed, "Node should be removed after 3 failures"
    
    # Verify the bridge was called
    bridge = mock_comm_manager.service_discovery_bridge
    assert len(bridge.proposed_changes) == 1, "Should have proposed one membership change"
    change = bridge.proposed_changes[0]
    assert change['action'] == 'remove', "Should propose removal"
    assert change['node_id'] == node_id, "Should remove the correct node"
    
    logger.info("✓ Connection failure detection tests passed!")


def test_successful_connection_resets_failures():
    """Test that successful connections reset failure counts."""
    logger.info("=" * 60)
    logger.info("Testing Failure Count Reset")
    logger.info("=" * 60)
    
    # Create a mock communication manager
    mock_comm_manager = Mock()
    mock_comm_manager.node_id = 1
    mock_comm_manager.bridge_registered = True
    mock_comm_manager.service_discovery_bridge = MockServiceDiscoveryBridge(is_leader=True)
    mock_comm_manager._remove_node_from_registry = Mock()
    
    failure_handler = ConnectionFailureHandler(mock_comm_manager)
    node_id = 3
    grpc_error = create_grpc_unavailable_error()
    
    # Generate 2 failures
    for i in range(2):
        removed = failure_handler.handle_connection_failure(node_id, grpc_error)
        assert not removed, f"Node should not be removed after {i+1} failures"
    
    # Reset failure count (simulating successful connection)
    failure_handler.reset_failure_count(node_id)
    logger.info("Reset failure count for node")
    
    # Generate 2 more failures - should not remove since count was reset
    for i in range(2):
        removed = failure_handler.handle_connection_failure(node_id, grpc_error)
        assert not removed, f"Node should not be removed after reset + {i+1} failures"
    
    # Now the 3rd failure (after reset) should remove
    removed = failure_handler.handle_connection_failure(node_id, grpc_error)
    assert removed, "Node should be removed after 3 failures following reset"
    
    logger.info("✓ Failure count reset tests passed!")


def test_non_leader_behavior():
    """Test behavior when the node is not a leader."""
    logger.info("=" * 60)
    logger.info("Testing Non-Leader Behavior")
    logger.info("=" * 60)
    
    # Create a mock communication manager for non-leader
    mock_comm_manager = Mock()
    mock_comm_manager.node_id = 2  # Not the leader
    mock_comm_manager.bridge_registered = True
    mock_comm_manager.service_discovery_bridge = MockServiceDiscoveryBridge(is_leader=False)
    mock_comm_manager._remove_node_from_registry = Mock()
    
    failure_handler = ConnectionFailureHandler(mock_comm_manager)
    node_id = 3
    grpc_error = create_grpc_unavailable_error()
    
    # Generate enough failures to trigger removal attempt
    for i in range(3):
        removed = failure_handler.handle_connection_failure(node_id, grpc_error)
    
    # Should still remove locally and return True even if not leader
    assert removed, "Non-leader should still remove node locally"
    
    # Bridge should still be called (though it might fail as non-leader)
    bridge = mock_comm_manager.service_discovery_bridge
    assert len(bridge.proposed_changes) == 1, "Should attempt to propose membership change"
    
    logger.info("✓ Non-leader behavior tests passed!")


def run_all_tests():
    """Run all connection failure handling tests."""
    logger.info("Starting Connection Failure Handling Tests")
    logger.info("=" * 80)
    
    try:
        test_connection_failure_detection()
        test_successful_connection_resets_failures()
        test_non_leader_behavior()
        
        logger.info("=" * 80)
        logger.info("🎉 All tests passed successfully!")
        logger.info("Connection failure handling is working correctly.")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    return True


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
