"""
RAFT GossipFL Algorithm Package

This package implements RAFT consensus algorithm integrated with GossipFL
for dynamic federated learning with consensus-based coordination.

Key Components:
- raft_node: Core RAFT node implementation
- raft_consensus: RAFT consensus coordination
- raft_worker_manager: Integration with GossipFL training
- service_discovery_bridge: Bridge between service discovery and RAFT
- discovery_hints: RAFT leader notifications to service discovery
"""

from .raft_node import RaftNode, RaftState
from .raft_consensus import RaftConsensus, ConsensusType
from .raft_worker_manager import RaftWorkerManager
from .service_discovery_bridge import RaftServiceDiscoveryBridge, BridgeState
from .discovery_hints import DiscoveryHintSender

__all__ = [
    'RaftNode',
    'RaftState', 
    'RaftConsensus',
    'ConsensusType',
    'RaftWorkerManager',
    'RaftServiceDiscoveryBridge',
    'BridgeState',
    'DiscoveryHintSender'
]