import logging
import numpy as np

class RaftTopologyManager():
    """    
    This manager integrates with RAFT to ensure all nodes have a consistent
    view of the network topology. Topology changes are proposed through RAFT
    and applied once consensus is reached.
    """
    
    def __init__(self, args, raft_consensus):
        """
        Initialize the RAFT topology manager.
        
        Args:
            args: Configuration parameters
            raft_consensus: The RAFT consensus manager
        """
        self.raft_consensus = raft_consensus
        self.latest_topology_round = -1
        self.topology_cache = {}  # Cache for topologies by round number
        self.match = None  # Store the gossip match list
        
        # Initialize with a basic identity topology to avoid index errors
        # This will be replaced when generate_topology() is called
        if len(self.topology) == 0:
            self.topology = np.zeros([self.n, self.n])
            for i in range(self.n):
                self.topology[i][i] = 1.0  # Self-connections as initial fallback
        
        logging.info(f"RaftTopologyManager initialized")

    # --- Reimplementing the base class ---
    def get_in_neighbor_weights(self, node_index):
        if node_index >= self.n:
            return []
        return self.topology[node_index]

    def get_out_neighbor_weights(self, node_index):
        if node_index >= self.n:
            return []
        return self.topology[node_index]

    def get_in_neighbor_idx_list(self, node_index):
        neighbor_in_idx_list = []
        neighbor_weights = self.get_in_neighbor_weights(node_index)
        for idx, neighbor_w in enumerate(neighbor_weights):
            if neighbor_w > 0 and node_index != idx:
                neighbor_in_idx_list.append(idx)
        return neighbor_in_idx_list

    def get_out_neighbor_idx_list(self, node_index):
        neighbor_out_idx_list = []
        neighbor_weights = self.get_out_neighbor_weights(node_index)
        for idx, neighbor_w in enumerate(neighbor_weights):
            if neighbor_w > 0 and node_index != idx:
                neighbor_out_idx_list.append(idx)
        return neighbor_out_idx_list

    
    def generate_topology(self, t):
        """
        Generate or retrieve a topology for round t.
        
        If a topology for this round already exists in the consensus log,
        use that. Otherwise, generate a new topology and propose it through
        RAFT.
        
        Args:
            t (int): The round number
        """
        # Check if we have a committed topology for this round in our cache
        if t in self.topology_cache:
            logging.debug(f"Using cached topology for round {t}")
            self.topology = self.topology_cache[t]
            return
        
        # If this node is not the leader, wait for the leader to propose a topology
        if not self.raft_consensus.is_leader():
            logging.debug(f"Not a leader, waiting for topology for round {t}")
            # Try to get the topology from the committed log entries
            topology_data = self.get_topology_from_committed_log(t)
            if topology_data is not None:
                self.topology = topology_data
                self.topology_cache[t] = topology_data
                return
            
            # No topology in log yet, use the last known topology as a fallback
            if self.topology is not None and len(self.topology) > 0:
                logging.debug(f"Using previous topology for round {t} temporarily")
                return
            
            # If no previous topology, wait for leader's proposal
            # In a real implementation, we might want to add a timeout here
            # and proceed with a default topology if no consensus is reached
            logging.warning(f"No topology available for round {t}, using empty topology")
            self.topology = np.zeros([self.n, self.n])
            for i in range(self.n):
                self.topology[i][i] = 1.0  # Self-connections only as fallback
            return
        
        # As leader, generate a new topology and propose it through RAFT
        logging.debug(f"Leader generating topology for round {t}")
        match, self.real_bandwidth_threshold = self.SAPS_gossip_match.generate_match(t)
        self.topology = np.zeros([self.n, self.n])
        for i in range(self.n):
            self.topology[i][i] = 1/2
            self.topology[i][match[i]] = 1/2
        
        # Cache the topology
        self.topology_cache[t] = self.topology.copy()
        
        # Propose the topology through RAFT
        self.propose_topology_update(self.topology, t)
        
        # Update the latest round
        self.latest_topology_round = t
    
    def get_topology_from_committed_log(self, round_number):
        """
        Retrieve a topology for a specific round from the committed log entries.
        
        Args:
            round_number (int): The round number
            
        Returns:
            numpy.ndarray or None: The topology matrix if found, None otherwise
        """
        
        # Ask the raft_consensus for the topology for this round
        topology_data = self.raft_consensus.get_topology_for_round(round_number)
        return topology_data
    
    def propose_topology_update(self, topology_matrix, round_number):
        """
        Propose a topology update through RAFT.
        
        Args:
            topology_matrix (numpy.ndarray): The topology matrix
            round_number (int): The round number
            
        Returns:
            bool: True if the proposal was accepted, False otherwise
        """
        # Convert the topology matrix to a format suitable for RAFT
        topology_data = {
            'round': round_number,
            'matrix': topology_matrix.tolist(),  # Convert to list for JSON serialization
            'timestamp': self.raft_consensus.raft_node.get_current_timestamp()
        }
        
        # Add the topology update to the RAFT log
        log_index = self.raft_consensus.add_topology_update(topology_data)
        
        return log_index > 0
    
    def apply_topology_update(self, topology_data):
        """
        Apply a topology update from a committed log entry.
        
        This method is called by the RAFT node when a topology update is committed.
        
        Args:
            topology_data (dict): The topology data from the log entry
            
        Returns:
            bool: True if the update was successfully applied
        """
        round_number = topology_data.get('round', -1)
        matrix = topology_data.get('matrix', None)
        
        if matrix is None:
            logging.error(f"Invalid topology data: {topology_data}")
            return False
        
        # Convert the list back to a numpy array
        topology_matrix = np.array(matrix)
        
        # Update the cache
        self.topology_cache[round_number] = topology_matrix
        
        # If this is for the current or future round, update the current topology
        if round_number >= self.latest_topology_round:
            self.topology = topology_matrix
            self.latest_topology_round = round_number
            logging.info(f"Applied topology update for round {round_number}")
        
        return True

    def update_match(self, match, round_number=None):
        """Update the stored match list and rebuild the topology matrix."""
        if match is None:
            return

        self.match = list(match)

        self.topology = np.zeros([self.n, self.n])
        for i in range(self.n):
            self.topology[i][i] = 1 / 2
            partner = self.match[i]
            if partner is not None and 0 <= partner < self.n:
                self.topology[i][partner] = 1 / 2

        if round_number is not None:
            self.topology_cache[round_number] = self.topology.copy()
            if round_number > self.latest_topology_round:
                self.latest_topology_round = round_number

    def update_topology_matrix(self, topology_matrix, round_number=None):
        """Replace the current topology matrix and update caches."""
        if topology_matrix is None:
            return

        topology_matrix = np.array(topology_matrix)
        self.topology = topology_matrix

        # Derive match list from the topology matrix
        match = []
        for i in range(self.n):
            neighbors = np.where(topology_matrix[i] > 0)[0]
            neighbors = [j for j in neighbors if j != i]
            match.append(neighbors[0] if neighbors else i)
        self.match = match

        if round_number is not None:
            self.topology_cache[round_number] = topology_matrix
            if round_number > self.latest_topology_round:
                self.latest_topology_round = round_number

    def apply_match_changes(self, changes, base_version=None):
        """Apply incremental changes to the match list and topology."""
        if changes is None:
            return

        if self.match is None:
            # Initialize with identity if no previous match
            self.match = [i for i in range(self.n)]

        for node_idx, new_match in changes:
            if 0 <= node_idx < self.n:
                self.match[node_idx] = new_match

        # Rebuild topology matrix from updated match
        self.topology = np.zeros([self.n, self.n])
        for i in range(self.n):
            self.topology[i][i] = 1 / 2
            partner = self.match[i]
            if partner is not None and 0 <= partner < self.n:
                self.topology[i][partner] = 1 / 2

        if base_version is not None:
            self.topology_cache[base_version] = self.topology.copy()
            if base_version > self.latest_topology_round:
                self.latest_topology_round = base_version

    def get_topology(self):
        """Return the current topology matrix."""
        return self.topology
