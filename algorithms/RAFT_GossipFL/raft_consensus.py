import logging
import threading
import time
from enum import Enum

from .raft_node import RaftState


class ConsensusType(Enum):
    """
    Types of consensus operations supported by RAFT consensus.
    """
    TOPOLOGY = 1     # Consensus on network topology
    BANDWIDTH = 2    # Consensus on bandwidth matrix
    MEMBERSHIP = 3   # Consensus on cluster membership changes
    COORDINATOR = 4  # Consensus on coordinator selection


class RaftConsensus:
    """
    Implements RAFT consensus logic for GossipFL.
    
    This class coordinates the RAFT consensus operations, manages message
    exchange between nodes, and provides a high-level interface for the
    worker manager to interact with the RAFT consensus algorithm.
    
    It handles:
    - Leader election and state management
    - Log replication and consistency
    - Topology updates consensus
    - Bandwidth updates consensus
    - Membership changes consensus
    - Coordinator selection
    
    The consensus manager runs multiple threads to handle different aspects
    of the RAFT protocol:
    - Election timeout monitoring
    - Heartbeat sending (leaders only)
    - Log replication (leaders only)
    
    Each thread has error handling and can recover from failures.
    """
    
    def __init__(self, raft_node, worker_manager, args, bandwidth_manager=None, topology_manager=None):
        """
        Initialize the RAFT consensus manager.
        
        Args:
            raft_node (RaftNode): The local RAFT node
            worker_manager: The worker manager that handles message passing
            args: Configuration parameters
            bandwidth_manager: The bandwidth manager for handling bandwidth updates
            topology_manager: The topology manager for handling topology updates
        """
        self.raft_node = raft_node
        self.worker_manager = worker_manager
        self.args = args
        self.bandwidth_manager = bandwidth_manager
        self.topology_manager = topology_manager
        
        # Set manager references in the RAFT node
        if bandwidth_manager:
            self.raft_node.bandwidth_manager = bandwidth_manager
        if topology_manager:
            self.raft_node.topology_manager = topology_manager
        
        # Set callback for state changes
        self.raft_node.on_state_change = self.handle_state_change
        
        # Heartbeat thread for leaders
        self.heartbeat_thread = None
        self.heartbeat_stop_event = threading.Event()
        self.heartbeat_failed_event = threading.Event()

        # Election thread for all nodes
        self.election_thread = None
        self.election_stop_event = threading.Event()
        self.election_failed_event = threading.Event()

        # Log replication thread for leaders
        self.replication_thread = None
        self.replication_stop_event = threading.Event()
        self.replication_failed_event = threading.Event()

        # Supervisor thread to monitor worker threads
        self.supervisor_thread = None
        self.supervisor_stop_event = threading.Event()

        # Lock to protect thread operations
        self.thread_lock = threading.RLock()
        
        # Callback for leadership changes
        self.on_leadership_change = None
        
        # Current leader ID (None if unknown)
        self.current_leader_id = None
        
        # Initialize election timeout checking
        self.start_election_thread()

        # Start supervisor thread to monitor other threads
        self.supervisor_stop_event.clear()
        self.supervisor_thread = threading.Thread(
            target=self._supervisor_thread_func,
            name=f"raft-supervisor-{self.raft_node.node_id}",
            daemon=True
        )
        self.supervisor_thread.start()

        logging.info(f"RAFT Consensus initialized for node {raft_node.node_id}")
    
    def start(self):
        """Start the consensus operations."""
        pass  # Election thread already started in __init__
    
    def stop(self):
        """Stop all consensus operations."""
        logging.info(f"Node {self.raft_node.node_id}: Stopping all consensus operations")
        with self.thread_lock:
            self.supervisor_stop_event.set()
            self.election_stop_event.set()
            self.heartbeat_stop_event.set()
            self.replication_stop_event.set()
            
            threads_to_join = []
            
            if self.supervisor_thread and self.supervisor_thread.is_alive():
                threads_to_join.append((self.supervisor_thread, "supervisor"))

            if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                threads_to_join.append((self.heartbeat_thread, "heartbeat"))
            
            if self.election_thread and self.election_thread.is_alive():
                threads_to_join.append((self.election_thread, "election"))
            
            if self.replication_thread and self.replication_thread.is_alive():
                threads_to_join.append((self.replication_thread, "replication"))
            
            # Join all threads with timeout
            for thread, name in threads_to_join:
                thread.join(timeout=1.0)
                if thread.is_alive():
                    logging.warning(f"Node {self.raft_node.node_id}: {name} thread did not terminate gracefully")
            
            # Reset thread references
            # [NOTE]: This might lose the reference to running threads.
            self.heartbeat_thread = None
            self.election_thread = None
            self.replication_thread = None
            self.supervisor_thread = None
            
            logging.info(f"Node {self.raft_node.node_id}: All consensus operations stopped")
    
    def handle_state_change(self, new_state):
        """
        Handle RAFT node state changes.
        
        Args:
            new_state (RaftState): The new state of the node
        """
        if new_state == RaftState.LEADER:
            self.start_heartbeat_thread()
            self.start_replication_thread()
            self.current_leader_id = self.raft_node.node_id
            
            # Notify leadership change
            if self.on_leadership_change:
                self.on_leadership_change(self.raft_node.node_id)
        
        elif new_state == RaftState.FOLLOWER or new_state == RaftState.CANDIDATE:
            self.stop_heartbeat_thread()
            self.stop_replication_thread()
    
    def start_election_thread(self):
        """Start the election timeout monitoring thread."""
        with self.thread_lock:
            if self.election_thread and self.election_thread.is_alive():
                return

            self.election_stop_event.clear()
            self.election_failed_event.clear()
            self.election_thread = threading.Thread(
                target=self._election_thread_func,
                name=f"raft-election-{self.raft_node.node_id}",
                daemon=True
            )
            self.election_thread.start()
    
    def stop_election_thread(self):
        """Stop the election timeout monitoring thread."""
        with self.thread_lock:
            if not self.election_thread or not self.election_thread.is_alive():
                return
            
            self.election_stop_event.set()
            self.election_thread.join(timeout=1.0)
            self.election_thread = None
    
    def start_heartbeat_thread(self):
        """Start the heartbeat thread (leaders only)."""
        with self.thread_lock:
            if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                return

            self.heartbeat_stop_event.clear()
            self.heartbeat_failed_event.clear()
            self.heartbeat_thread = threading.Thread(
                target=self._heartbeat_thread_func,
                name=f"raft-heartbeat-{self.raft_node.node_id}",
                daemon=True
            )
            self.heartbeat_thread.start()
    
    def stop_heartbeat_thread(self):
        """Stop the heartbeat thread."""
        with self.thread_lock:
            if not self.heartbeat_thread or not self.heartbeat_thread.is_alive():
                return
            
            self.heartbeat_stop_event.set()
            self.heartbeat_thread.join(timeout=1.0)
            self.heartbeat_thread = None
    
    def start_replication_thread(self):
        """Start the log replication thread (leaders only)."""
        with self.thread_lock:
            if self.replication_thread and self.replication_thread.is_alive():
                return

            self.replication_stop_event.clear()
            self.replication_failed_event.clear()
            self.replication_thread = threading.Thread(
                target=self._replication_thread_func,
                name=f"raft-replication-{self.raft_node.node_id}",
                daemon=True
            )
            self.replication_thread.start()
    
    def stop_replication_thread(self):
        """Stop the log replication thread."""
        with self.thread_lock:
            if not self.replication_thread or not self.replication_thread.is_alive():
                return
            
            self.replication_stop_event.set()
            self.replication_thread.join(timeout=1.0)
            self.replication_thread = None
    
    def _election_thread_func(self):
        """Thread function to monitor election timeouts."""
        try:
            while not self.election_stop_event.is_set():
                # Leaders don't have election timeouts
                if self.raft_node.state == RaftState.LEADER:
                    time.sleep(0.1)
                    continue
                
                # Check for election timeout
                if self.raft_node.is_election_timeout():
                    logging.info(f"Node {self.raft_node.node_id}: Election timeout detected")
                    
                    # Start a new election
                    if self.raft_node.start_election():
                        # Send vote requests to all other nodes
                        self.request_votes_from_all()
                
                # Sleep for a short time before checking again
                time.sleep(0.01)  # 10ms check interval
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in election thread: {str(e)}")
            if not self.election_stop_event.is_set():
                self.election_failed_event.set()
    
    def _heartbeat_thread_func(self):
        """Thread function to send heartbeats (leaders only)."""
        try:
            while not self.heartbeat_stop_event.is_set():
                if self.raft_node.state != RaftState.LEADER:
                    break
                
                # Send heartbeats to all followers
                self.send_heartbeats()
                
                # Sleep for heartbeat interval
                time.sleep(self.raft_node.heartbeat_interval)
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in heartbeat thread: {str(e)}")
            if not self.heartbeat_stop_event.is_set() and self.raft_node.state == RaftState.LEADER:
                self.heartbeat_failed_event.set()
    
    def _replication_thread_func(self):
        """Thread function to replicate logs (leaders only)."""
        try:
            while not self.replication_stop_event.is_set():
                if self.raft_node.state != RaftState.LEADER:
                    break
                
                # Replicate any pending log entries
                self.replicate_logs()
                
                # Update commit index based on match indices
                self.raft_node.update_commit_index()
                
                # Sleep for a short time before checking again
                time.sleep(0.05)  # 50ms check interval
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in replication thread: {str(e)}")
            if not self.replication_stop_event.is_set() and self.raft_node.state == RaftState.LEADER:
                self.replication_failed_event.set()

    def _supervisor_thread_func(self):
        """Monitor worker threads and restart them on failure."""
        try:
            while not self.supervisor_stop_event.is_set():
                if not self.election_stop_event.is_set():
                    if (self.election_failed_event.is_set() or
                        not (self.election_thread and self.election_thread.is_alive())):
                        self.election_failed_event.clear()
                        self.start_election_thread()

                if (not self.heartbeat_stop_event.is_set() and
                        self.raft_node.state == RaftState.LEADER):
                    if (self.heartbeat_failed_event.is_set() or
                        not (self.heartbeat_thread and self.heartbeat_thread.is_alive())):
                        self.heartbeat_failed_event.clear()
                        self.start_heartbeat_thread()

                if (not self.replication_stop_event.is_set() and
                        self.raft_node.state == RaftState.LEADER):
                    if (self.replication_failed_event.is_set() or
                        not (self.replication_thread and self.replication_thread.is_alive())):
                        self.replication_failed_event.clear()
                        self.start_replication_thread()

                time.sleep(0.1)
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Supervisor thread error: {str(e)}")
    
    def request_votes_from_all(self):
        """Send vote requests to all other nodes."""
        if self.raft_node.state != RaftState.CANDIDATE:
            return
        
        last_log_index, last_log_term = self.raft_node.get_last_log_info()
        
        for node_id in self.raft_node.known_nodes:
            if node_id == self.raft_node.node_id:
                continue  # Skip self
            
            # Send vote request to node
            self.worker_manager.send_vote_request(
                node_id,
                self.raft_node.current_term,
                last_log_index,
                last_log_term
            )
    
    def handle_vote_request(self, candidate_id, term, last_log_index, last_log_term):
        """
        Handle a vote request from a candidate.
        
        Args:
            candidate_id (int): ID of the candidate
            term (int): Candidate's term
            last_log_index (int): Index of candidate's last log entry
            last_log_term (int): Term of candidate's last log entry
        """
        # Process the vote request in the RAFT node
        current_term, vote_granted = self.raft_node.receive_vote_request(
            candidate_id, term, last_log_index, last_log_term)
        
        # Send response back to the candidate
        self.worker_manager.send_vote_response(
            candidate_id, current_term, vote_granted)
    
    def handle_vote_response(self, voter_id, term, vote_granted):
        """
        Handle a vote response from another node.
        
        Args:
            voter_id (int): ID of the voting node
            term (int): Current term in the vote response
            vote_granted (bool): Whether the vote was granted
        """
        # Process the vote response in the RAFT node
        became_leader = self.raft_node.receive_vote_response(voter_id, term, vote_granted)
        
        # If became leader, take leadership actions
        if became_leader:
            logging.info(f"Node {self.raft_node.node_id} became leader for term {self.raft_node.current_term}")
            
            # Start heartbeat and replication threads
            self.start_heartbeat_thread()
            self.start_replication_thread()
            
            # Set current leader
            self.current_leader_id = self.raft_node.node_id
            
            # Notify leadership change
            if self.on_leadership_change:
                self.on_leadership_change(self.raft_node.node_id)
            
            # Add a no-op entry to the log
            self.add_no_op_entry()
    
    def add_no_op_entry(self):
        """Add a no-op entry to the log to commit previous entries."""
        if self.raft_node.state != RaftState.LEADER:
            return
        
        self.raft_node.add_log_entry({
            'type': 'no-op',
            'timestamp': time.time()
        })
    
    def send_heartbeats(self):
        """Send heartbeats to all followers."""
        if self.raft_node.state != RaftState.LEADER:
            return
        
        for node_id in self.raft_node.known_nodes:
            if node_id == self.raft_node.node_id:
                continue  # Skip self
            
            # Get next index for this follower
            next_idx = self.raft_node.next_index.get(node_id, 1)
            prev_log_index = next_idx - 1
            prev_log_term = 0
            
            if prev_log_index > 0 and prev_log_index <= len(self.raft_node.log):
                prev_log_term = self.raft_node.log[prev_log_index - 1]['term']
            
            # Empty entries list for heartbeat
            entries = []
            
            # Send AppendEntries RPC
            self.worker_manager.send_append_entries(
                node_id,
                self.raft_node.current_term,
                prev_log_index,
                prev_log_term,
                entries,
                self.raft_node.commit_index
            )
    
    def replicate_logs(self):
        """Replicate logs to all followers."""
        if self.raft_node.state != RaftState.LEADER:
            return
        
        for node_id in self.raft_node.known_nodes:
            if node_id == self.raft_node.node_id:
                continue  # Skip self
            
            # Get next index for this follower
            next_idx = self.raft_node.next_index.get(node_id, 1)
            
            # If there are entries to send
            if next_idx <= len(self.raft_node.log):
                prev_log_index = next_idx - 1
                prev_log_term = 0
                
                if prev_log_index > 0:
                    prev_log_term = self.raft_node.log[prev_log_index - 1]['term']
                
                # Get entries to send
                entries = self.raft_node.log[prev_log_index:]
                
                # Send AppendEntries RPC
                self.worker_manager.send_append_entries(
                    node_id,
                    self.raft_node.current_term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    self.raft_node.commit_index
                )
    
    def handle_append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        """
        Handle an AppendEntries RPC from the leader.
        
        Args:
            leader_id (int): ID of the leader
            term (int): Leader's term
            prev_log_index (int): Index of log entry immediately preceding new ones
            prev_log_term (int): Term of prev_log_index entry
            entries (list): List of log entries to append
            leader_commit (int): Leader's commit index
            
        Returns:
            tuple: (current_term, success)
        """
        try:
            # Update current leader if this is a valid AppendEntries
            if term >= self.raft_node.current_term:
                self.current_leader_id = leader_id
                logging.debug(f"Node {self.raft_node.node_id}: Updated leader to {leader_id} for term {term}")
            
            # Process the AppendEntries in the RAFT node
            current_term, success = self.raft_node.append_entries(
                leader_id, term, prev_log_index, prev_log_term, entries, leader_commit)
            
            # Send response back to the leader
            self.worker_manager.send_append_response(
                leader_id, current_term, success, prev_log_index + len(entries) if success else 0)
            
            return current_term, success
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error handling append entries: {str(e)}")
            return self.raft_node.current_term, False
    
    def handle_append_response(self, follower_id, term, success, match_index):
        """
        Handle an AppendEntries response from a follower.
        
        Args:
            follower_id (int): ID of the follower
            term (int): Follower's current term
            success (bool): Whether the append was successful
            match_index (int): Index of highest log entry known to be replicated on the follower
        """
        with self.raft_node.state_lock:
            # If term > currentTerm, convert to follower
            if term > self.raft_node.current_term:
                self.raft_node.become_follower(term)
                return
            
            # Ignore if not a leader
            if self.raft_node.state != RaftState.LEADER:
                return
            
            # If success, update nextIndex and matchIndex for follower
            if success:
                self.raft_node.next_index[follower_id] = match_index + 1
                self.raft_node.match_index[follower_id] = match_index
                
                # Update commit index
                self.raft_node.update_commit_index()
            else:
                # If AppendEntries fails because of log inconsistency, decrement nextIndex and retry
                if follower_id in self.raft_node.next_index:
                    self.raft_node.next_index[follower_id] = max(1, self.raft_node.next_index[follower_id] - 1)
    
    def add_topology_update(self, topology_data):
        """
        Add a topology update to the log (leaders only).
        
        Args:
            topology_data (dict): Topology data to add
            
        Returns:
            int: Index of the new log entry, or -1 if not leader
        """
        if self.raft_node.state != RaftState.LEADER:
            logging.warning(f"Node {self.raft_node.node_id}: Cannot add topology update - not a leader")
            return -1
        
        if not self.topology_manager:
            logging.error(f"Node {self.raft_node.node_id}: Cannot add topology update - topology manager not set")
            return -1
        
        # Create the command for the log
        command = {
            'type': 'topology',
            'data': topology_data,
            'timestamp': time.time()
        }
        
        # Add to the log and get the index
        log_index = self.raft_node.add_log_entry(command)
        
        if log_index > 0:
            logging.info(f"Node {self.raft_node.node_id}: Added topology update to log at index {log_index}")
        
        return log_index
    
    def add_bandwidth_update(self, bandwidth_data):
        """
        Add a bandwidth matrix update to the log (leaders only).
        
        Args:
            bandwidth_data (dict): Bandwidth data to add
            
        Returns:
            int: Index of the new log entry, or -1 if not leader
        """
        if self.raft_node.state != RaftState.LEADER:
            logging.warning(f"Node {self.raft_node.node_id}: Cannot add bandwidth update - not a leader")
            return -1
        
        if not self.bandwidth_manager:
            logging.error(f"Node {self.raft_node.node_id}: Cannot add bandwidth update - bandwidth manager not set")
            return -1
        
        # Create the command for the log
        command = {
            'type': 'bandwidth',
            'data': bandwidth_data,
            'timestamp': time.time()
        }
        
        # Add to the log and get the index
        log_index = self.raft_node.add_log_entry(command)
        
        if log_index > 0:
            logging.info(f"Node {self.raft_node.node_id}: Added bandwidth update to log at index {log_index}")
        
        return log_index
    
    def add_membership_change(self, action, node_id):
        """
        Add a membership change to the log (leaders only).
        
        Args:
            action (str): 'add' or 'remove'
            node_id (int): ID of the node to add/remove
            
        Returns:
            int: Index of the new log entry, or -1 if not leader
        """
        if self.raft_node.state != RaftState.LEADER:
            logging.warning(f"Node {self.raft_node.node_id}: Cannot add membership change - not a leader")
            return -1
        
        if action not in ['add', 'remove']:
            logging.error(f"Node {self.raft_node.node_id}: Invalid membership action: {action}")
            return -1
        
        try:
            if action == 'add':
                # First add the node to known nodes
                if not self.raft_node.add_node(node_id):
                    logging.warning(f"Node {self.raft_node.node_id}: Failed to add node {node_id} to known nodes")
                    return -1
                logging.info(f"Node {self.raft_node.node_id}: Added node {node_id} to known nodes")
            
            # Create the command for the log
            command = {
                'type': 'membership',
                'action': action,
                'node_id': node_id,
                'timestamp': time.time()
            }
            
            # Add to the log and get the index
            log_index = self.raft_node.add_log_entry(command)
            
            if log_index > 0:
                logging.info(f"Node {self.raft_node.node_id}: Added membership change ({action} node {node_id}) to log at index {log_index}")
            
            return log_index
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error adding membership change: {str(e)}")
            return -1
    
    def add_coordinator_update(self, coordinator_id):
        """
        Add a coordinator update to the log (leaders only).
        
        Args:
            coordinator_id (int): ID of the new coordinator
            
        Returns:
            int: Index of the new log entry, or -1 if not leader
        """
        if self.raft_node.state != RaftState.LEADER:
            return -1
        
        command = {
            'type': 'coordinator',
            'coordinator_id': coordinator_id,
            'timestamp': time.time()
        }
        
        return self.raft_node.add_log_entry(command)
    
    def get_coordinator_id(self):
        """
        Get the current coordinator ID.
        
        Returns:
            int: ID of the current coordinator (same as leader)
        """
        return self.current_leader_id
    
    def is_state_synchronized(self):
        """
        Check if the node's state is properly synchronized with the cluster.
        
        A node is considered synchronized if:
        - It has a valid state (not INITIAL)
        - It has received and applied some log entries
        - Its commit index is reasonable compared to its log
        
        Returns:
            bool: True if the node is synchronized, False otherwise
        """
        with self.raft_node.state_lock:
            # Must not be in initial state
            if self.raft_node.state == RaftState.INITIAL:
                return False
            
            # Must have some committed entries (unless we're starting fresh)
            if len(self.raft_node.log) > 0 and self.raft_node.commit_index == 0:
                return False
            
            # If we have log entries, we should have applied them
            if self.raft_node.commit_index > self.raft_node.last_applied + 5:
                return False  # Too many unapplied entries
            
            return True
    
    def get_latest_committed_state(self):
        """
        Get the latest committed state from the RAFT log.
        
        This searches through committed log entries to find the most recent
        state information including model parameters, topology, and bandwidth.
        
        Returns:
            dict or None: The latest committed state, or None if no state found
        """
        with self.raft_node.state_lock:
            if self.raft_node.commit_index == 0:
                return None
            
            # Start with empty state
            committed_state = {
                'model_params': None,
                'topology': None,
                'bandwidth': None,
                'round_idx': 0,
                'raft_term': self.raft_node.current_term,
                'raft_commit_index': self.raft_node.commit_index
            }
            
            # Search through committed log entries from most recent to oldest
            for idx in range(min(self.raft_node.commit_index, len(self.raft_node.log)) - 1, -1, -1):
                entry = self.raft_node.log[idx]
                entry_type = entry.get('type')
                entry_data = entry.get('data', {})
                
                # Update state components as we find them
                if entry_type == 'model_params' and committed_state['model_params'] is None:
                    committed_state['model_params'] = entry_data.get('params')
                    committed_state['round_idx'] = entry_data.get('round', committed_state['round_idx'])
                
                if entry_type == 'topology' and committed_state['topology'] is None:
                    committed_state['topology'] = entry_data.get('matrix')
                
                if entry_type == 'bandwidth' and committed_state['bandwidth'] is None:
                    committed_state['bandwidth'] = entry_data.get('matrix')
                
                # If we have all components, we can stop searching
                if (committed_state['model_params'] is not None and
                    committed_state['topology'] is not None and
                    committed_state['bandwidth'] is not None):
                    break
            
            return committed_state
    
    def handle_node_join_request(self, node_id):
        """
        Handle a request from a new node to join the cluster.
        
        This method uses the RAFT protocol to ensure the joining node
        gets a consistent view of the cluster state.
        
        Args:
            node_id (int): ID of the node wanting to join
            
        Returns:
            bool: True if the join was handled successfully, False otherwise
        """
        if not self.is_leader():
            logging.info(f"Node {self.raft_node.node_id}: Not leader, cannot handle join request from {node_id}")
            return False
        
        logging.info(f"Node {self.raft_node.node_id}: Handling join request from node {node_id}")
        
        # Add the node to our known nodes if not already present
        if node_id not in self.raft_node.known_nodes:
            self.raft_node.known_nodes.add(node_id)
            
            # Initialize next and match indices for the new node
            self.raft_node.next_index[node_id] = len(self.raft_node.log) + 1
            self.raft_node.match_index[node_id] = 0
            
            logging.info(f"Node {self.raft_node.node_id}: Added node {node_id} to cluster")
        
        # Send the current state to the joining node
        # This will be handled by the worker manager's enhanced state snapshot method
        return True
    
    def send_state_to_new_node(self, new_node_id):
        """
        Send the current state to a new node.
        
        Args:
            new_node_id (int): ID of the new node
        """
        # Send the complete log
        self.worker_manager.send_state_snapshot(
            new_node_id,
            self.raft_node.current_term,
            self.raft_node.log,
            self.raft_node.commit_index
        )
        # Also send the latest model parameters for faster start
        if hasattr(self.worker_manager.worker, "model_trainer"):
            params = self.worker_manager.worker.model_trainer.get_model_params()
            self.worker_manager.send_model_params(new_node_id, params)
    
    def handle_state_snapshot(self, term, log, commit_index):
        """
        Handle a state snapshot from the leader.
        
        Args:
            term (int): Leader's term
            log (list): Complete log from leader
            commit_index (int): Leader's commit index
            
        Returns:
            bool: True if snapshot was applied, False otherwise
        """
        with self.raft_node.state_lock:
            # Update term if needed
            if term > self.raft_node.current_term:
                self.raft_node.become_follower(term)
            elif self.raft_node.state == RaftState.INITIAL:
                self.raft_node.become_follower(term)
            
            # Replace log with snapshot
            self.raft_node.log = log
            
            # Update commit index and apply committed entries
            if commit_index > self.raft_node.commit_index:
                self.raft_node.commit_index = commit_index
                self.raft_node.apply_committed_entries()
            
            return True
    
    def is_leader(self):
        """
        Check if this node is the leader.
        
        Returns:
            bool: True if this node is the leader
        """
        return self.raft_node.state == RaftState.LEADER
    
    def get_leader_id(self):
        """
        Get the ID of the current leader.
        
        Returns:
            int: ID of the current leader, or None if unknown
        """
        return self.current_leader_id
    
    def get_current_leader(self):
        """
        Alias for get_leader_id() for compatibility.
        
        Returns:
            int: ID of the current leader, or None if unknown
        """
        return self.get_leader_id()
    
    def get_topology_for_round(self, round_number):
        """
        Get the topology for a specific round from the committed log entries.
        
        Args:
            round_number (int): The round number
            
        Returns:
            numpy.ndarray or None: The topology matrix if found, None otherwise
        """
        with self.raft_node.state_lock:
            # Search through committed log entries
            for idx in range(self.raft_node.commit_index):
                entry = self.raft_node.log[idx]
                if entry.get('type') == 'topology':
                    data = entry.get('data', {})
                    if data.get('round') == round_number:
                        # Return the topology matrix
                        return data.get('matrix')
        
        return None
    
    def get_latest_bandwidth(self):
        """
        Get the latest bandwidth from the committed log entries.
        
        Returns:
            dict or None: The bandwidth data if found, None otherwise
        """
        latest_timestamp = 0
        latest_bandwidth = None
        
        with self.raft_node.state_lock:
            # Search through committed log entries
            for idx in range(self.raft_node.commit_index):
                entry = self.raft_node.log[idx]
                if entry.get('type') == 'bandwidth':
                    data = entry.get('data', {})
                    timestamp = data.get('timestamp', 0)
                    if timestamp > latest_timestamp:
                        latest_timestamp = timestamp
                        latest_bandwidth = data
        
        return latest_bandwidth
    
    def get_status(self):
        """
        Get the current status of the RAFT node.
        
        Returns:
            dict: Status information including state, term, leader, etc.
        """
        with self.raft_node.state_lock:
            status = {
                'node_id': self.raft_node.node_id,
                'state': self.raft_node.state.name,
                'current_term': self.raft_node.current_term,
                'voted_for': self.raft_node.voted_for,
                'current_leader': self.current_leader_id,
                'log_length': len(self.raft_node.log),
                'commit_index': self.raft_node.commit_index,
                'last_applied': self.raft_node.last_applied,
                'known_nodes': list(self.raft_node.known_nodes),
            }
            
            if self.raft_node.state == RaftState.LEADER:
                status['next_indices'] = dict(self.raft_node.next_index)
                status['match_indices'] = dict(self.raft_node.match_index)
            
            return status
