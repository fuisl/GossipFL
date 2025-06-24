import logging
import numpy as np

from algorithms.SAPS_FL.utils import generate_bandwidth


class RaftBandwidthManager:
    """
    Manages bandwidth information using RAFT for consensus.
    
    This manager ensures all nodes have a consistent view of the network
    bandwidth. Bandwidth changes are proposed through RAFT and applied
    once consensus is reached.
    """
    
    def __init__(self, args, raft_consensus):
        """
        Initialize the RAFT bandwidth manager.
        
        Args:
            args: Configuration parameters
            raft_consensus: The RAFT consensus manager
        """
        self.args = args
        self.raft_consensus = raft_consensus
        self.bandwidth = generate_bandwidth(args)
        self.bandwidth_cache = {}  # Cache for bandwidth matrices by timestamp
        self.latest_bandwidth_timestamp = 0
        
        logging.info(f"RaftBandwidthManager initialized")
    
    def get_bandwidth(self):
        """
        Get the current bandwidth matrix.
        
        Returns:
            numpy.ndarray: The current bandwidth matrix
        """
        return self.bandwidth
    
    def update_bandwidth(self, new_bandwidth=None):
        """
        Update the bandwidth matrix and propose the change through RAFT.
        
        Args:
            new_bandwidth (numpy.ndarray, optional): The new bandwidth matrix.
                If None, generate a new one.
                
        Returns:
            bool: True if the proposal was accepted, False otherwise
        """
        # If no bandwidth provided, generate a new one
        if new_bandwidth is None:
            new_bandwidth = generate_bandwidth(self.args)
        
        # If this node is not the leader, it can't propose changes
        if not self.raft_consensus.is_leader():
            logging.warning("Only the leader can propose bandwidth updates")
            return False
        
        # Propose the bandwidth update through RAFT
        return self.propose_bandwidth_update(new_bandwidth)
    
    def propose_bandwidth_update(self, bandwidth_matrix):
        """
        Propose a bandwidth update through RAFT.
        
        Args:
            bandwidth_matrix (numpy.ndarray): The bandwidth matrix
            
        Returns:
            bool: True if the proposal was accepted, False otherwise
        """
        # Convert the bandwidth matrix to a format suitable for RAFT
        timestamp = self.raft_consensus.raft_node.get_current_timestamp()
        bandwidth_data = {
            'timestamp': timestamp,
            'matrix': bandwidth_matrix.tolist()  # Convert to list for JSON serialization
        }
        
        # Add the bandwidth update to the RAFT log
        log_index = self.raft_consensus.add_bandwidth_update(bandwidth_data)
        
        return log_index > 0
    
    def apply_bandwidth_update(self, bandwidth_data):
        """
        Apply a bandwidth update from a committed log entry.
        
        This method is called by the RAFT node when a bandwidth update is committed.
        
        Args:
            bandwidth_data (dict): The bandwidth data from the log entry
            
        Returns:
            bool: True if the update was successfully applied
        """
        timestamp = bandwidth_data.get('timestamp', 0)
        matrix = bandwidth_data.get('matrix', None)
        
        if matrix is None:
            logging.error(f"Invalid bandwidth data: {bandwidth_data}")
            return False
        
        # Convert the list back to a numpy array
        bandwidth_matrix = np.array(matrix)
        
        # Update the cache
        self.bandwidth_cache[timestamp] = bandwidth_matrix
        
        # If this is newer than our current bandwidth, update it
        if timestamp > self.latest_bandwidth_timestamp:
            self.bandwidth = bandwidth_matrix
            self.latest_bandwidth_timestamp = timestamp
            logging.info(f"Applied bandwidth update from timestamp {timestamp}")
        
        return True
    
    def get_bandwidth_from_committed_log(self):
        """
        Retrieve the latest bandwidth from the committed log entries.
        
        Returns:
            numpy.ndarray or None: The bandwidth matrix if found, None otherwise
        """
        # This is a simplified implementation. In practice, we would search
        # through the committed log entries to find the latest bandwidth.
        # For now, we'll rely on the raft_consensus to provide this functionality.
        
        # Ask the raft_consensus for the latest bandwidth
        bandwidth_data = self.raft_consensus.get_latest_bandwidth()
        
        if bandwidth_data is None:
            return None
        
        return np.array(bandwidth_data.get('matrix', None))
