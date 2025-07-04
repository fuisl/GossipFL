"""
gRPC Communication Module

This module provides gRPC-based communication managers for federated learning,
including both static and dynamic membership support.
"""

from .grpc_comm_manager import GRPCCommManager

# Import dynamic components - use try/except for robustness
try:
    from .dynamic_grpc_comm_manager import DynamicGRPCCommManager, NodeInfo
    from .comm_manager_factory import (
        CommManagerFactory,
        AutoCommManager,
        create_communication_manager
    )
    
    __all__ = [
        "GRPCCommManager",
        "DynamicGRPCCommManager",
        "NodeInfo",
        "CommManagerFactory",
        "AutoCommManager",
        "create_communication_manager"
    ]
except ImportError as e:
    # If dynamic components can't be imported, fall back to basic components
    __all__ = ["GRPCCommManager"]