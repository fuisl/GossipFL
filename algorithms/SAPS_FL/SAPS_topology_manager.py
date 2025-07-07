import logging

import numpy as np

from fedml_core.distributed.topology.symmetric_topology_manager import SymmetricTopologyManager
from .minmax_commuication_cost import SAPS_gossip
from .utils import generate_bandwidth


class SAPSTopologyManager(SymmetricTopologyManager):
    """
    """

    def __init__(self, args=None):
        super().__init__(n=args.client_num_in_total, neighbor_num=1)
        # super:
        # self.n = n
        # self.neighbor_num = neighbor_num
        # self.topology = []
        self.bandwidth = generate_bandwidth(args)
        self.SAPS_gossip_match = SAPS_gossip(self.bandwidth, args.B_thres, args.T_thres)

    # override
    def generate_topology(self, t):
        match, self.real_bandwidth_threshold = self.SAPS_gossip_match.generate_match(t)
        logging.debug("match: %s" % match)
        self.topology = np.zeros([self.n, self.n])
        for i in range(self.n):
            self.topology[i][i] = 1/2
            self.topology[i][match[i]] = 1/2
    
    def get_neighbor_list(self):
        """Get list of neighbor nodes for the current topology."""
        if not hasattr(self, 'topology') or self.topology is None:
            return set()
        
        # Return all nodes that have connections in the topology
        neighbors = set()
        for i in range(self.n):
            for j in range(self.n):
                if i != j and self.topology[i][j] > 0:
                    neighbors.add(j)
        return neighbors

    def get_topology(self):
        """Get the current topology matrix."""
        if hasattr(self, 'topology'):
            return self.topology
        return None
    
    def update_nodes(self, new_nodes):
        """Update the topology manager with new set of nodes."""
        logging.info(f"Updating topology manager with nodes: {new_nodes}")
        
        # Update the number of nodes
        old_n = self.n
        self.n = len(new_nodes)
        
        # If the number of nodes changed, regenerate topology
        if old_n != self.n:
            logging.info(f"Node count changed from {old_n} to {self.n}, regenerating topology")
            # Regenerate bandwidth for new node count
            # Note: This is a simplified approach - in practice you might want to preserve existing bandwidth info
            try:
                # Try to regenerate the topology with current round (assume round 0 if not tracking)
                current_round = getattr(self, '_current_round', 0)
                self.generate_topology(current_round)
            except Exception as e:
                logging.warning(f"Failed to regenerate topology: {e}")
                # Create fallback topology
                self.topology = np.zeros([self.n, self.n])
                for i in range(self.n):
                    self.topology[i][i] = 1.0  # Self-connection
        else:
            logging.info("Node count unchanged, keeping existing topology")