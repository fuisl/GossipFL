#!/usr/bin/env python3
"""
Simple test to debug the Phase 1.3 integration issue
"""

import sys
import os
import logging

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

print("Testing basic imports...")

try:
    from algorithms.RAFT_GossipFL.raft_node import RaftNode, RaftState
    print("✓ RaftNode imported successfully")
except Exception as e:
    print(f"✗ Error importing RaftNode: {e}")
    sys.exit(1)

try:
    from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
    print("✓ RaftConsensus imported successfully")
except Exception as e:
    print(f"✗ Error importing RaftConsensus: {e}")
    sys.exit(1)

try:
    from algorithms.RAFT_GossipFL.service_discovery_bridge import RaftServiceDiscoveryBridge
    print("✓ RaftServiceDiscoveryBridge imported successfully")
except Exception as e:
    print(f"✗ Error importing RaftServiceDiscoveryBridge: {e}")
    sys.exit(1)

# Test creating RaftNode
print("\nTesting RaftNode creation...")
try:
    from unittest.mock import Mock
    
    args = Mock()
    args.client_id = 1
    args.client_num_in_total = 3
    args.comm_round = 0
    args.min_election_timeout = 150
    args.max_election_timeout = 300
    args.heartbeat_interval = 50
    args.raft_log_compaction_threshold = 100
    
    node = RaftNode(node_id=1, args=args)
    print(f"✓ RaftNode created with state: {node.state}")
    
    # Test updating known nodes
    node.update_known_nodes(node_ids=[1, 2, 3])
    print(f"✓ Known nodes updated: {node.known_nodes}")
    
except Exception as e:
    print(f"✗ Error creating RaftNode: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\nAll basic tests passed!")
