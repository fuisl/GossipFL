import logging
import numpy as np
from typing import Set, Dict, List, Optional

from fedml_core.distributed.topology.symmetric_topology_manager import SymmetricTopologyManager
from algorithms.SAPS_FL.minmax_commuication_cost import SAPS_gossip
from algorithms.SAPS_FL.utils import generate_bandwidth


class DynamicSAPSTopologyManager(SymmetricTopologyManager):
    """
    A dynamic version of SAPS topology manager that handles node join/leave events.
    
    This manager maintains a dynamic set of nodes and updates the topology accordingly
    when nodes join or leave the network.
    """

    def __init__(self, args=None, raft_consensus=None):
        # Initialize with default values, will be updated dynamically
        super().__init__(n=0, neighbor_num=1)
        
        self.args = args
        self.raft_consensus = raft_consensus
        self.node_id = args.node_id  # Current node's ID
        
        # Track active nodes (IDs) - will be updated dynamically
        self.active_nodes = set()
        # Map from node ID to index in matrices
        self.node_id_to_index = {}
        # Map from matrix index to node ID
        self.index_to_node_id = {}
        
        # Will be initialized when nodes are added
        self.bandwidth = None
        self.SAPS_gossip_match = None
        self.real_bandwidth_threshold = None
        
        # Initialize with empty topology
        self.topology = np.zeros((0, 0))
        
        logging.info(f"DynamicSAPSTopologyManager initialized for node {self.node_id}")
    
    def update_nodes(self, node_set: Set[int]):
        """
        Update the set of active nodes and regenerate topology.
        
        Args:
            node_set: Set of active node IDs
        """
        # If no change in nodes, return early
        if set(self.active_nodes) == set(node_set):
            return
            
        logging.info(f"Updating topology with nodes: {node_set}")
        
        # Update active nodes
        self.active_nodes = set(node_set)
        self.n = len(self.active_nodes)
        
        # Rebuild node ID mappings
        self.node_id_to_index = {node_id: i for i, node_id in enumerate(sorted(self.active_nodes))}
        self.index_to_node_id = {i: node_id for node_id, i in self.node_id_to_index.items()}
        
        # Regenerate bandwidth matrix for active nodes
        self._regenerate_bandwidth()
        
        # Reinitialize SAPS gossip matcher
        self.SAPS_gossip_match = SAPS_gossip(
            self.bandwidth, 
            self.args.B_thres if hasattr(self.args, 'B_thres') else 0, 
            self.args.T_thres if hasattr(self.args, 'T_thres') else 0
        )
        
        # Generate initial topology (at time 0)
        self.generate_topology(0)
        
        logging.info(f"Topology updated with {self.n} nodes")
    
    def _regenerate_bandwidth(self):
        """Regenerate bandwidth matrix for current active nodes."""
        # For testing, create a simple bandwidth matrix where all nodes have equal bandwidth
        if self.n == 0:
            self.bandwidth = np.zeros((0, 0))
            return
            
        # Simple bandwidth matrix - in a real implementation, you'd use measured bandwidths
        self.bandwidth = np.ones((self.n, self.n))
        np.fill_diagonal(self.bandwidth, 0)  # Zero bandwidth to self
        
        # If you have a way to get actual bandwidth between nodes, use it here
        # For example, if you have a bandwidth manager:
        # if hasattr(self.args, 'bandwidth_manager') and self.args.bandwidth_manager:
        #     for i, node_i in self.index_to_node_id.items():
        #         for j, node_j in self.index_to_node_id.items():
        #             if i != j:
        #                 self.bandwidth[i][j] = self.args.bandwidth_manager.get_bandwidth(node_i, node_j)
    
    # override
    def generate_topology(self, t):
        """
        Generate a new topology for the current time step.
        
        Args:
            t: Current time step
        """
        if self.n == 0 or self.SAPS_gossip_match is None:
            self.topology = np.zeros((0, 0))
            return
            
        # Generate matches using SAPS gossip algorithm
        match, self.real_bandwidth_threshold = self.SAPS_gossip_match.generate_match(t)
        logging.debug(f"Match at time {t}: {match}")
        
        # Create new topology matrix
        self.topology = np.zeros([self.n, self.n])
        for i in range(self.n):
            # Each node connects to itself
            self.topology[i][i] = 1/2
            # And to its matched node
            self.topology[i][match[i]] = 1/2
    
    def get_in_neighbor_idx_list(self, node_idx):
        """
        Get incoming neighbors for a node by its index.
        
        Args:
            node_idx: Index of the node
            
        Returns:
            List of neighbor indices
        """
        if self.n == 0 or node_idx >= self.n:
            return []
            
        neighbor_indices = []
        for i in range(self.n):
            if self.topology[i][node_idx] > 0 and i != node_idx:
                neighbor_indices.append(i)
        return neighbor_indices
    
    def get_out_neighbor_idx_list(self, node_idx):
        """
        Get outgoing neighbors for a node by its index.
        
        Args:
            node_idx: Index of the node
            
        Returns:
            List of neighbor indices
        """
        if self.n == 0 or node_idx >= self.n:
            return []
            
        neighbor_indices = []
        for i in range(self.n):
            if self.topology[node_idx][i] > 0 and i != node_idx:
                neighbor_indices.append(i)
        return neighbor_indices
    
    def get_neighbor_list(self):
        """
        Get the list of neighbor node IDs for the current node.
        
        Returns:
            List of neighbor node IDs
        """
        if self.node_id not in self.node_id_to_index:
            return []
            
        node_idx = self.node_id_to_index[self.node_id]
        neighbor_indices = self.get_out_neighbor_idx_list(node_idx)
        
        # Convert indices back to node IDs
        return [self.index_to_node_id[idx] for idx in neighbor_indices]
    
    def get_topology(self):
        """Get the current topology matrix."""
        return self.topology
    
    def get_worker_index(self, node_id):
        """
        Convert from node ID to matrix index.
        
        Args:
            node_id: Node ID
            
        Returns:
            Index in topology matrix, or None if node not found
        """
        return self.node_id_to_index.get(node_id)
    
    def __getitem__(self, index):
        """Handle index access for the topology property."""
        # If index is node_id, convert to matrix index
        if isinstance(index, int) and index in self.node_id_to_index:
            matrix_index = self.node_id_to_index[index]
            return self.topology[matrix_index]
        return self.topology[index]