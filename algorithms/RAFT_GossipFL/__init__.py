"""
RAFT_GossipFL: A decentralized federated learning framework that integrates 
the RAFT consensus algorithm with GossipFL for improved fault-tolerance and scalability.
"""

# Core RAFT components
from .raft_node import RaftNode, RaftState
from .raft_consensus import RaftConsensus
from .raft_messages import RaftMessage
from .raft_worker_manager import RaftWorkerManager
from .raft_topology_manager import RaftTopologyManager
from .raft_bandwidth_manager import RaftBandwidthManager

# Gateway components for dynamic node discovery (gRPC-based)
from .grpc_gateway_server import GRPCGatewayServer
from .grpc_gateway_client import GRPCGatewayClient, GRPCGatewayDiscoveryMixin

# API
# from .RAFT_GossipFL_API import RAFT_GossipFL_API

__all__ = [
    # Core RAFT
    'RaftNode',
    'RaftState', 
    'RaftConsensus',
    'RaftMessage',
    'RaftWorkerManager',
    'RaftTopologyManager',
    'RaftBandwidthManager',
    
    # Gateway (gRPC-based)
    'GRPCGatewayServer',
    'GRPCGatewayClient',
    'GRPCGatewayDiscoveryMixin',
    
    # API
    # 'RAFT_GossipFL_API'
]
