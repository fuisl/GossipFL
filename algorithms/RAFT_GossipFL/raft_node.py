import logging
import random
import time
import threading
from enum import Enum

class RaftState(Enum):
    """Represents the possible states of a RAFT node."""

    INITIAL = 0
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class RaftNode:
    """
    Implementation of a RAFT consensus node.
    
    This class encapsulates the state and behavior of a node in the RAFT consensus
    algorithm. It manages state transitions, voting, and term progression.
    """
    
    def __init__(self, node_id, args):
        """
        Initialize a RAFT node.
        
        Args:
            node_id (int): Unique identifier for this node
            args: Configuration parameters
        """
        self.node_id = node_id
        self.args = args
        
        # RAFT state
        # Newly started nodes are in INITIAL state until synchronized
        # [BUG]: This is a temporary fix to set the initial state problem
        if self.node_id == 0:
            self.state = RaftState.FOLLOWER
        else:
            self.state = RaftState.INITIAL
        self.current_term = 0
        self.voted_for = None
        
        # Log entries
        self.log = []  # Format: [{'term': term, 'command': command, 'index': index}, ...]
        self.commit_index = 0
        self.last_applied = 0
        
        # Leader state (initialized when becoming leader)
        self.next_index = {}  # {node_id: next_log_index, ...}
        self.match_index = {}  # {node_id: match_index, ...}
        
        # Election state
        self.votes_received = set()  # Set of nodes that voted for this node in current term
        self.last_heartbeat_time = time.time()
        
        # Election timeout (randomized to prevent split votes)
        self.min_election_timeout = getattr(args, 'min_election_timeout', 150) / 1000.0  # default 150ms
        self.max_election_timeout = getattr(args, 'max_election_timeout', 300) / 1000.0  # default 300ms
        self.reset_election_timeout()
        
        # Heartbeat interval (for leaders)
        self.heartbeat_interval = getattr(args, 'heartbeat_interval', 50) / 1000.0  # default 50ms
        
        # Lock for thread safety
        self.state_lock = threading.RLock()
        
        # Callback for state changes (to be set by the manager)
        self.on_state_change = None
        
        # Track known nodes
        self.known_nodes = set()
        self.update_known_nodes(args.client_num_in_total)
        
        logging.info(
            f"RAFT Node {self.node_id} initialized in {self.state.name} state"
        )
    
    def update_known_nodes(self, total_nodes=None, node_ids=None):
        """
        Update the set of known nodes.
        
        Args:
            total_nodes (int, optional): Total number of nodes in the network
            node_ids (list, optional): Explicit list of node IDs to use
        """
        with self.state_lock:
            # Update the known_nodes set based on provided information
            if node_ids is not None:
                # Use explicitly provided node IDs
                self.known_nodes = set(node_ids)
            elif total_nodes is not None and not self.known_nodes:
                # Only use range if we don't have any nodes yet and no explicit IDs provided
                # This maintains backward compatibility with existing code
                self.known_nodes = set(range(total_nodes))
                logging.debug(f"Node {self.node_id}: Initializing with sequential IDs (0-{total_nodes-1})")
            # Otherwise, keep the existing known_nodes
                
            # Update node count and majority threshold
            self.total_nodes = len(self.known_nodes)
            self.majority = self.total_nodes // 2 + 1
            
            # Update leader state if we're the leader
            if self.state == RaftState.LEADER:
                # Update next_index and match_index for any new nodes
                for node_id in self.known_nodes:
                    if node_id != self.node_id and node_id not in self.next_index:
                        self.next_index[node_id] = len(self.log) + 1
                        self.match_index[node_id] = 0
                
                # Remove any nodes that are no longer in the cluster
                for node_id in list(self.next_index.keys()):
                    if node_id not in self.known_nodes:
                        del self.next_index[node_id]
                        if node_id in self.match_index:
                            del self.match_index[node_id]
            
            logging.info(f"Node {self.node_id}: Known nodes updated, total={self.total_nodes}, majority={self.majority}, nodes={sorted(self.known_nodes)}")
    
    def reset_election_timeout(self):
        """Reset the election timeout with a random value."""
        self.election_timeout = random.uniform(self.min_election_timeout, self.max_election_timeout)
        logging.debug(f"Node {self.node_id}: Reset election timeout to {self.election_timeout*1000:.2f}ms")
    
    def update_heartbeat(self):
        """
        Update the last heartbeat time.
        
        This method records when the last heartbeat was received, which is used
        to detect leader failures and trigger elections.
        """
        with self.state_lock:
            # Update the heartbeat timestamp
            try:
                self.last_heartbeat_time = time.time()
                logging.debug(f"Node {self.node_id}: Updated heartbeat time, election timeout in {self.election_timeout*1000:.2f}ms")
                
                # If we're in INITIAL state and receiving heartbeats, we may need to transition to FOLLOWER
                if self.state == RaftState.INITIAL:
                    logging.debug(f"Node {self.node_id}: Received heartbeat while in INITIAL state")
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error updating heartbeat time: {e}")
    
    def is_election_timeout(self):
        """
        Check if election timeout has occurred.
        
        This method determines whether enough time has passed since the last
        heartbeat to trigger an election. This is a key part of leader detection
        in the RAFT algorithm.
        
        Returns:
            bool: True if timeout has occurred, False otherwise
        """
        with self.state_lock:
            try:
                # Leaders don't check for election timeouts
                if self.state == RaftState.LEADER:
                    return False
                
                # Nodes in INITIAL state don't participate in elections
                if self.state == RaftState.INITIAL:
                    return False
                
                # Calculate time since last heartbeat
                elapsed = time.time() - self.last_heartbeat_time
                is_timeout = elapsed > self.election_timeout
                
                # Log timeout events for debugging
                if is_timeout:
                    logging.debug(f"Node {self.node_id}: Election timeout detected after {elapsed*1000:.2f}ms " +
                                 f"(timeout was {self.election_timeout*1000:.2f}ms)")
                
                return is_timeout
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error checking election timeout: {e}")
                # Return False on error to avoid unnecessary elections
                return False
    
    def start_election(self):
        """
        Start a new election (transition to CANDIDATE state).
        
        This method initiates a leader election by incrementing the current term,
        voting for itself, and transitioning to the CANDIDATE state. It follows the
        RAFT algorithm's election process.
        
        Returns:
            bool: True if successfully started election, False otherwise
        """
        with self.state_lock:
            try:
                # Check current state
                if self.state == RaftState.LEADER:
                    logging.debug(f"Node {self.node_id}: Cannot start election as LEADER")
                    return False
                
                # Newly initialized nodes should not start elections until synchronized
                if self.state == RaftState.INITIAL:
                    logging.debug(f"Node {self.node_id}: Cannot start election in INITIAL state")
                    return False
                
                # Check if we have enough known nodes to potentially win an election
                if len(self.known_nodes) < self.majority:
                    logging.warning(f"Node {self.node_id}: Not enough known nodes ({len(self.known_nodes)}) " +
                                  f"to reach majority ({self.majority}), skipping election")
                    return False
                
                # Increment term, vote for self
                old_term = self.current_term
                self.current_term += 1
                self.voted_for = self.node_id
                self.state = RaftState.CANDIDATE
                self.votes_received = {self.node_id}  # Vote for self
                
                # Reset election timeout
                self.reset_election_timeout()
                self.update_heartbeat()
                
                # Notify state change
                if self.on_state_change:
                    self.on_state_change(RaftState.CANDIDATE)
                
                logging.info(f"Node {self.node_id}: Starting election for term {self.current_term} " +
                            f"(previous term: {old_term}, known nodes: {len(self.known_nodes)})")
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error starting election: {e}")
                return False
    
    def receive_vote_request(self, candidate_id, term, last_log_index, last_log_term):
        """
        Handle a vote request from a candidate.
        
        Args:
            candidate_id (int): ID of the candidate requesting the vote
            term (int): Candidate's term
            last_log_index (int): Index of candidate's last log entry
            last_log_term (int): Term of candidate's last log entry
            
        Returns:
            tuple: (term, vote_granted)
        """
        with self.state_lock:
            try:
                # Validate the candidate
                if candidate_id not in self.known_nodes:
                    logging.warning(f"Node {self.node_id}: Received vote request from unknown node {candidate_id}")
                    return self.current_term, False
                
                # Do not participate in elections before initialization
                if self.state == RaftState.INITIAL:
                    logging.debug(f"Node {self.node_id}: Cannot vote in INITIAL state, request from {candidate_id} for term {term}")
                    return self.current_term, False
                
                # If term < currentTerm, reject immediately
                if term < self.current_term:
                    logging.debug(f"Node {self.node_id}: Rejected vote for {candidate_id}, term {term} < current term {self.current_term}")
                    return self.current_term, False

                # If term > currentTerm, convert to follower
                if term > self.current_term:
                    logging.debug(f"Node {self.node_id}: Converting to follower due to higher term from {candidate_id}: {term} > {self.current_term}")
                    self.become_follower(term)
                    # Note: voted_for is reset in become_follower(), so we continue to evaluate the vote request
                
                # Leaders should not grant votes (this is a safeguard)
                if self.state == RaftState.LEADER and term == self.current_term:
                    logging.warning(f"Node {self.node_id}: Leader for term {self.current_term} refusing to vote for {candidate_id}")
                    return self.current_term, False
                
                # Check if vote can be granted:
                # 1. Our term matches the candidate's term
                # 2. We haven't voted for anyone else this term (or we already voted for this candidate)
                # 3. The candidate's log is at least as up-to-date as ours
                vote_granted = False
                if (term == self.current_term and 
                    (self.voted_for is None or self.voted_for == candidate_id) and
                    self.is_log_up_to_date(last_log_index, last_log_term)):
                    
                    # Grant vote
                    self.voted_for = candidate_id
                    vote_granted = True
                    
                    # Reset election timeout when granting vote
                    self.update_heartbeat()
                    
                    logging.info(f"Node {self.node_id}: Granted vote to {candidate_id} for term {term}")
                else:
                    # Log the reason for rejection
                    if term != self.current_term:
                        reason = f"term mismatch: candidate term {term} != current term {self.current_term}"
                    elif self.voted_for is not None and self.voted_for != candidate_id:
                        reason = f"already voted for {self.voted_for} this term"
                    else:
                        reason = "candidate's log is not up-to-date"
                    
                    logging.debug(f"Node {self.node_id}: Rejected vote for {candidate_id} - {reason}")
                
                return self.current_term, vote_granted
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error processing vote request from {candidate_id}: {e}")
                return self.current_term, False
    
    def receive_vote_response(self, voter_id, term, vote_granted):
        """
        Handle vote response from another node.
        
        Args:
            voter_id (int): ID of the voting node
            term (int): Current term in the vote response
            vote_granted (bool): Whether the vote was granted
            
        Returns:
            bool: True if node becomes leader, False otherwise
        """
        with self.state_lock:
            try:
                # Validate the voter is a known node
                if voter_id not in self.known_nodes:
                    logging.warning(f"Node {self.node_id}: Received vote response from unknown node {voter_id}")
                    return False
                
                # If term > currentTerm, convert to follower
                if term > self.current_term:
                    logging.debug(f"Node {self.node_id}: Converting to follower due to higher term from {voter_id}: {term} > {self.current_term}")
                    self.become_follower(term)
                    return False
                
                # Ignore if not a candidate or from a previous term
                if self.state != RaftState.CANDIDATE:
                    logging.debug(f"Node {self.node_id}: Ignoring vote from {voter_id} as we are not a candidate (state={self.state.name})")
                    return False
                    
                if term < self.current_term:
                    logging.debug(f"Node {self.node_id}: Ignoring vote from {voter_id} for outdated term {term} (current={self.current_term})")
                    return False
                
                # Count the vote
                if vote_granted:
                    self.votes_received.add(voter_id)
                    logging.info(f"Node {self.node_id}: Received vote from {voter_id}, " +
                                f"total votes: {len(self.votes_received)}/{self.majority}")
                    
                    # Check if we have majority
                    if len(self.votes_received) >= self.majority:
                        success = self.become_leader()
                        if success:
                            logging.info(f"Node {self.node_id}: Won election with {len(self.votes_received)} votes")
                        return success
                else:
                    logging.debug(f"Node {self.node_id}: Vote denied by {voter_id} for term {term}")
                
                return False
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error processing vote response from {voter_id}: {e}")
                return False
    
    def become_follower(self, term):
        """
        Transition to FOLLOWER state.
        
        Args:
            term (int): The new current term
        """
        with self.state_lock:
            try:
                old_state = self.state
                
                # Store transition information for logging
                from_state = old_state.name
                
                # Set the state to FOLLOWER
                self.state = RaftState.FOLLOWER
                
                # Update term if it's higher than our current term
                if term > self.current_term:
                    self.current_term = term
                    # According to RAFT, voted_for should be reset when term changes
                    self.voted_for = None
                elif term == self.current_term:
                    # When term is the same, we keep voted_for as is
                    pass
                else:
                    # This should not happen, but just in case
                    logging.warning(f"Node {self.node_id}: Becoming follower with term {term} lower than current term {self.current_term}")
                
                # Reset election state
                if old_state == RaftState.CANDIDATE:
                    self.votes_received = set()  # Clear received votes when stepping down from candidate
                
                # Reset election timeout to ensure randomized election timing
                self.reset_election_timeout()
                self.update_heartbeat()
                
                # Clear leader state if transitioning from leader
                if old_state == RaftState.LEADER:
                    # No need to clear next_index and match_index completely,
                    # but any operations on them should check if still leader
                    logging.info(f"Node {self.node_id}: Stepping down from leader for term {self.current_term}")
                
                # Notify state change if state actually changed and callback is set
                if old_state != RaftState.FOLLOWER and self.on_state_change:
                    self.on_state_change(RaftState.FOLLOWER)
                
                if old_state != RaftState.FOLLOWER:
                    logging.info(f"Node {self.node_id}: State changed from {from_state} to FOLLOWER for term {term}")
                else:
                    logging.debug(f"Node {self.node_id}: Updated follower state for term {term}")
                    
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error transitioning to follower state: {e}")
                # Ensure we're in a safe state even if there's an error
                self.state = RaftState.FOLLOWER
    
    def become_leader(self):
        """
        Transition to LEADER state.
        
        This is called when a candidate has received votes from a majority
        of the servers and becomes the leader for the current term.
        
        Returns:
            bool: True if successfully became leader, False otherwise
        """
        with self.state_lock:
            try:
                # Only candidates can become leaders
                if self.state != RaftState.CANDIDATE:
                    logging.warning(f"Node {self.node_id}: Cannot become leader from {self.state.name} state")
                    return False
                
                # Transition to leader state
                self.state = RaftState.LEADER
                
                # Reset election state
                self.votes_received = set()
                
                # Initialize leader state for each follower
                last_log_index = len(self.log)
                self.next_index = {node_id: last_log_index + 1 for node_id in self.known_nodes if node_id != self.node_id}
                self.match_index = {node_id: 0 for node_id in self.known_nodes if node_id != self.node_id}
                
                # RAFT recommends appending a no-op entry immediately upon becoming leader
                # This helps commit entries from previous terms more quickly
                no_op_entry = {
                    'type': 'no-op',
                    'timestamp': self.get_current_timestamp()
                }
                self.add_log_entry(no_op_entry)
                
                # Notify state change via callback if set
                if self.on_state_change:
                    self.on_state_change(RaftState.LEADER)
                
                logging.info(f"Node {self.node_id}: Became LEADER for term {self.current_term} with {len(self.known_nodes)-1} followers")
                
                # Note: Heartbeats will be sent by the manager's heartbeat thread
                # which regularly calls send_heartbeats for the leader
                
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error transitioning to leader state: {e}")
                return False
    
    def is_log_up_to_date(self, last_log_index, last_log_term):
        """
        Check if candidate's log is at least as up-to-date as this node's log.
        
        According to RAFT paper section 5.4.1, this method implements the following logic:
        1. If the logs have last entries with different terms, then the log with the later term is more up-to-date
        2. If the logs end with the same term, then the longer log is more up-to-date
        
        Args:
            last_log_index (int): Index of candidate's last log entry
            last_log_term (int): Term of candidate's last log entry
            
        Returns:
            bool: True if candidate's log is at least as up-to-date
        """
        with self.state_lock:
            try:
                # Get this node's last log entry details
                my_last_log_index = len(self.log)
                
                # Handle empty log case
                if my_last_log_index == 0:
                    my_last_log_term = 0
                    logging.debug(f"Node {self.node_id}: Log is empty, considering any non-empty log more up-to-date")
                else:
                    my_last_log_term = self.log[my_last_log_index - 1]['term']
                
                # Implement the RAFT log comparison logic:
                # 1. Compare terms of the last log entries
                if last_log_term > my_last_log_term:
                    result = True
                    reason = f"candidate's last term ({last_log_term}) > our last term ({my_last_log_term})"
                elif last_log_term < my_last_log_term:
                    result = False
                    reason = f"candidate's last term ({last_log_term}) < our last term ({my_last_log_term})"
                # 2. If terms are equal, compare log lengths
                elif last_log_index >= my_last_log_index:
                    result = True
                    reason = f"same terms but candidate's log length ({last_log_index}) >= our log length ({my_last_log_index})"
                else:
                    result = False
                    reason = f"same terms but candidate's log length ({last_log_index}) < our log length ({my_last_log_index})"
                
                # Log the decision with details for debugging
                log_level = logging.DEBUG
                if not result:
                    # Use higher log level for rejections to make them more visible
                    log_level = logging.INFO
                
                logging.log(log_level, f"Node {self.node_id}: Log comparison result: {result}, {reason}")
                
                return result
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error comparing logs: {e}")
                # In case of error, reject the vote by returning False
                # This is safer than potentially allowing an invalid leader
                return False
    
    def append_entries(self, leader_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        """
        Handle AppendEntries RPC from leader.
        
        Args:
            leader_id (int): ID of the leader
            term (int): Leader's term
            prev_log_index (int): Index of log entry immediately preceding new ones
            prev_log_term (int): Term of prev_log_index entry
            entries (list): List of log entries to append
            leader_commit (int): Leader's commit index
            
        Returns:
            tuple: (term, success)
        """
        with self.state_lock:
            # If the node is uninitialized, accept the leader and convert to follower
            if self.state == RaftState.INITIAL:
                self.become_follower(term)

            # If term < currentTerm, reject
            if term < self.current_term:
                return self.current_term, False
            
            # If term > currentTerm, convert to follower
            if term > self.current_term:
                self.become_follower(term)
            
            # This is a valid AppendEntries from current leader, so reset election timeout
            self.update_heartbeat()
            
            # If we were a candidate, step down
            if self.state == RaftState.CANDIDATE:
                self.become_follower(term)
            
            # Log consistency check
            log_ok = (prev_log_index == 0 or
                     (prev_log_index <= len(self.log) and
                      (prev_log_index == 0 or self.log[prev_log_index - 1]['term'] == prev_log_term)))
            
            if not log_ok:
                return self.current_term, False
            
            # Process entries
            # Checking if entries is not None and not empty
            if entries:
                # Handle conflicts
                for i, entry in enumerate(entries):
                    idx = prev_log_index + i + 1
                    
                    # If new entry, append
                    if idx > len(self.log):
                        self.log.append(entry)
                    # If conflict, truncate log and append new entry
                    elif self.log[idx - 1]['term'] != entry['term']:
                        self.log = self.log[:idx - 1]
                        self.log.append(entry)
                    # Otherwise entry already exists
              # Update commit index
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self.apply_committed_entries()
            
            return self.current_term, True
    
    def apply_committed_entries(self):
        """Apply committed but not yet applied log entries."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            self.apply_log_entry(entry)
            
    def apply_log_entry(self, entry):
        """
        Apply a log entry to the state machine.
        
        Args:
            entry (dict): Log entry to apply
        """
        # Implement specific command execution here
        command = entry.get('command', {})
        command_type = command.get('type')
        
        if command_type == 'topology':
            # Apply full topology update
            logging.info(f"Node {self.node_id}: Applying topology update from log entry {entry['index']}")
            self._apply_topology_update(command)
        elif command_type == 'topology_delta':
            # Apply incremental topology update
            logging.info(f"Node {self.node_id}: Applying incremental topology update from log entry {entry['index']}")
            self._apply_topology_delta(command)
        elif command_type == 'bandwidth':
            # Apply full bandwidth matrix update
            logging.info(f"Node {self.node_id}: Applying bandwidth update from log entry {entry['index']}")
            self._apply_bandwidth_update(command)
        elif command_type == 'bandwidth_delta':
            # Apply incremental bandwidth update
            logging.info(f"Node {self.node_id}: Applying bandwidth delta from log entry {entry['index']}")
            self._apply_bandwidth_delta(command)
        elif command_type == 'membership':
            # Apply membership change
            logging.info(f"Node {self.node_id}: Applying membership change from log entry {entry['index']}")
            self._apply_membership_change(command)
        elif command_type == 'coordinator':
            # Apply coordinator change
            logging.info(f"Node {self.node_id}: Applying coordinator change from log entry {entry['index']}")
            self._apply_coordinator_change(command)
        elif command_type == 'batched_updates':
            # Apply batched updates            
            logging.info(f"Node {self.node_id}: Applying batched updates from log entry {entry['index']}")
            for update in command.get('updates', []):
                # Create a new command entry for each update in the batch
                self.apply_log_entry({'term': entry['term'], 'index': entry['index'], 'command': update})
        elif command_type == 'no-op':
            # No-op entry, just acknowledge
            logging.debug(f"Node {self.node_id}: Processed no-op entry {entry['index']}")
        else:
            logging.warning(f"Node {self.node_id}: Unknown command type in log entry: {command_type}")
    
    def _apply_topology_update(self, command):
        """
        Apply a full topology update command.
        
        Args:
            command (dict): The topology update command
        """
        data = command.get('data', {})
        
        # Extract topology information
        match = data.get('match')
        topology_matrix = data.get('topology_matrix')
        round_num = data.get('round')
        
        if hasattr(self, 'topology_manager') and self.topology_manager is not None:
            # Update the topology manager with the new topology
            if match is not None:
                self.topology_manager.update_match(match, round_num)
            if topology_matrix is not None:
                self.topology_manager.update_topology_matrix(topology_matrix, round_num)
        else:
            # Store the topology information for later use
            if not hasattr(self, 'pending_topology'):
                self.pending_topology = {}
            self.pending_topology['match'] = match
            self.pending_topology['topology_matrix'] = topology_matrix
            self.pending_topology['round'] = round_num
            
        logging.info(f"Node {self.node_id}: Applied topology update for round {round_num}")
    
    def _apply_topology_delta(self, command):
        """
        Apply an incremental topology update command.
        
        Args:
            command (dict): The topology delta command
        """
        base_version = command.get('base_version')
        changes = command.get('changes', [])
        
        if hasattr(self, 'topology_manager') and self.topology_manager is not None:
            # Apply changes to the topology manager
            self.topology_manager.apply_match_changes(changes, base_version)
        else:
            # Store changes for later application
            if not hasattr(self, 'pending_topology_changes'):                
                self.pending_topology_changes = []
            self.pending_topology_changes.append((base_version, changes))
            
        logging.info(f"Node {self.node_id}: Applied {len(changes)} topology changes from base version {base_version}")

    def _apply_bandwidth_update(self, command):
        """
        Apply a full bandwidth matrix update command.
        
        Args:
            command (dict): The bandwidth update command
        """
        data = command.get('data', {})
        
        if hasattr(self, 'bandwidth_manager') and self.bandwidth_manager is not None:
            # Call the bandwidth manager to apply the update
            self.bandwidth_manager.apply_bandwidth_update(data)
            logging.info(f"Node {self.node_id}: Applied bandwidth update via bandwidth manager")
        else:
            # Store the bandwidth information for later use
            if not hasattr(self, 'pending_bandwidth'):
                self.pending_bandwidth = []
            self.pending_bandwidth.append(data)
            logging.warning(f"Node {self.node_id}: No bandwidth manager available, storing update for later use")
    
    def _apply_bandwidth_delta(self, command):
        """
        Apply an incremental bandwidth update command.
        
        Args:
            command (dict): The bandwidth delta command
        """
        base_version = command.get('base_version')
        changes = command.get('changes', {})
        
        if hasattr(self, 'bandwidth_manager') and self.bandwidth_manager is not None:
            # Apply changes to the bandwidth manager
            self.bandwidth_manager.apply_bandwidth_changes(changes, base_version)
        else:
            # Store changes for later application
            if not hasattr(self, 'pending_bandwidth_changes'):
                self.pending_bandwidth_changes = []
            self.pending_bandwidth_changes.append((base_version, changes))
            
        logging.info(f"Node {self.node_id}: Applied {len(changes)} bandwidth changes from base version {base_version}")
    
    def _apply_membership_change(self, command):
        """
        Apply a membership change command.
        
        Args:
            command (dict): The membership change command
        """
        action = command.get('action')
        node_id = command.get('node_id')
        current_nodes = command.get('current_nodes')
        
        if action == 'add':
            # Add the node to known nodes if not already present
            if node_id not in self.known_nodes:
                self.known_nodes.add(node_id)
                self.update_known_nodes(len(self.known_nodes))
                logging.info(f"Node {self.node_id}: Added node {node_id} to known nodes")
        elif action == 'remove':
            # Remove the node from known nodes if present
            if node_id in self.known_nodes:
                self.known_nodes.remove(node_id)
                self.update_known_nodes(len(self.known_nodes))
                logging.info(f"Node {self.node_id}: Removed node {node_id} from known nodes")
        
        # If current_nodes is provided, use it to update known nodes
        if current_nodes is not None:
            self.known_nodes = set(current_nodes)
            self.update_known_nodes(len(self.known_nodes))
            logging.info(f"Node {self.node_id}: Updated known nodes to {current_nodes}")
    
    def _apply_coordinator_change(self, command):
        """
        Apply a coordinator change command.
        
        Args:
            command (dict): The coordinator change command
        """
        coordinator_id = command.get('coordinator_id')
        previous_coordinator_id = command.get('previous_coordinator_id')
        round_num = command.get('round')
        
        # Update the coordinator information
        if hasattr(self, 'current_coordinator_id'):
            self.previous_coordinator_id = self.current_coordinator_id
        self.current_coordinator_id = coordinator_id
        
        logging.info(f"Node {self.node_id}: Updated coordinator from {previous_coordinator_id} to {coordinator_id} at round {round_num}")
    
    def add_log_entry(self, command):
        """
        Add a new log entry (leaders only).
        
        Args:
            command (dict): The command to add to the log
            
        Returns:
            int: Index of the new log entry, or -1 if not leader
        """
        with self.state_lock:
            if self.state != RaftState.LEADER:
                return -1
                
            # Create and append the log entry
            # [FIXME]: This breaks if we use log compaction
            index = len(self.log) + 1
            entry = {
                'term': self.current_term,
                'command': command,
                'index': index
            }
            self.log.append(entry)
            
            logging.info(f"Leader {self.node_id}: Added new log entry at index {index}")
            return index
    
    def get_last_log_info(self):
        """
        Get information about the last log entry.
        
        Returns:
            tuple: (last_log_index, last_log_term)
        """
        last_log_index = len(self.log)
        last_log_term = self.log[last_log_index - 1]['term'] if last_log_index > 0 else 0
        return last_log_index, last_log_term
    
    def add_node(self, new_node_id):
        """
        Add a new node to the cluster (leaders only).
        
        Args:
            new_node_id (int): ID of the new node
            
        Returns:
            bool: True if successful, False otherwise
        """
        with self.state_lock:
            if self.state != RaftState.LEADER:
                return False
                
            if new_node_id in self.known_nodes:
                return True  # Node already known
                
            # Add node to known nodes
            self.known_nodes.add(new_node_id)
            self.update_known_nodes(len(self.known_nodes))
            
            # Initialize leader state for the new node
            self.next_index[new_node_id] = len(self.log) + 1
            self.match_index[new_node_id] = 0
            
            # Add a log entry for the membership change
            self.add_log_entry({
                'type': 'membership',
                'action': 'add',
                'node_id': new_node_id
            })
            
            logging.info(f"Leader {self.node_id}: Added new node {new_node_id} to cluster")
            return True
    
    def remove_node(self, node_id):
        """
        Remove a node from the cluster (leaders only).
        
        Args:
            node_id (int): ID of the node to remove
            
        Returns:
            bool: True if successful, False otherwise
        """
        with self.state_lock:
            if self.state != RaftState.LEADER:
                return False
                
            if node_id not in self.known_nodes:
                return True  # Node already removed
                
            # Remove node from known nodes
            self.known_nodes.remove(node_id)
            self.update_known_nodes(len(self.known_nodes))
            
            # Remove from leader state
            if node_id in self.next_index:
                del self.next_index[node_id]
            if node_id in self.match_index:
                del self.match_index[node_id]
            
            # Add a log entry for the membership change
            self.add_log_entry({
                'type': 'membership',
                'action': 'remove',
                'node_id': node_id
            })
            
            logging.info(f"Leader {self.node_id}: Removed node {node_id} from cluster")
            return True
    
    def update_commit_index(self):
        """
        Update commit index based on match_index (leaders only).
        """
        if self.state != RaftState.LEADER:
            return
            
        # Find the highest index that is replicated on a majority of servers
        for n in range(len(self.log), 0, -1):
            # Only commit entries from current term
            if self.log[n - 1]['term'] != self.current_term:
                continue
                
            # Count replications
            count = 1  # Leader itself
            for node_id in self.match_index:
                if self.match_index[node_id] >= n:
                    count += 1
            
            # If majority, update commit index
            if count >= self.majority and n > self.commit_index:
                self.commit_index = n
                self.apply_committed_entries()
                logging.info(f"Leader {self.node_id}: Updated commit index to {n}")
                break
    
    def get_current_timestamp(self):
        """Get the current timestamp."""
        return time.time()
