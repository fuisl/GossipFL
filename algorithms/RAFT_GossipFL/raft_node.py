import logging
import random
import time
import threading
from enum import Enum

class RaftState(Enum):
    """Represents the possible states of a RAFT node."""

    INITIAL = 0
    FOLLOWER = 1
    PREVOTE = 2
    CANDIDATE = 3
    LEADER = 4


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
        # All newly started nodes are in INITIAL state until properly synchronized
        # They will transition based on service discovery and cluster state
        self.state = RaftState.INITIAL
        
        # Bootstrap mode means this node is starting a new cluster
        # It will become leader immediately when it discovers it's alone
        self.is_bootstrap_node = getattr(args, "bootstrap", False)
        
        self.current_term = 0
        self.voted_for = None
        
        # Log entries
        self.log = []  # Format: [{'term': term, 'command': command, 'index': index}, ...]
        self.commit_index = 0
        self.last_applied = 0
        
        # Log compaction state
        self.log_compaction_threshold = getattr(args, 'raft_log_compaction_threshold', 100)  # default 100 entries
        self.first_log_index = 1  # Index of the first entry in log (changes with compaction)
        self.last_snapshot_index = 0  # Index of last entry in the snapshot
        self.last_snapshot_term = 0  # Term of last entry in the snapshot
        
        # Leader state (initialized when becoming leader)
        self.next_index = {}  # {node_id: next_log_index, ...}
        self.match_index = {}  # {node_id: match_index, ...}
        
        # Election state
        self.votes_received = set()  # Set of nodes that voted for this node in current term
        self.last_heartbeat_time = time.time()
        
        # PreVote state
        self.prevote_requested = False  # Whether this node has requested prevotes for a potential election
        self.prevotes_received = set()  # Set of nodes that granted prevotes for this node
        self.prevote_term = 0  # The term for which prevotes are being collected (current_term + 1)
        
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
        
        # Track known nodes - start empty for dynamic discovery
        self.known_nodes = set()
        
        # For backward compatibility, if a fixed cluster size is provided, use it
        # But in dynamic mode, we'll update this through service discovery
        if hasattr(args, 'client_num_in_total') and args.client_num_in_total is not None:
            # Static cluster mode - initialize with sequential IDs
            self.update_known_nodes(args.client_num_in_total)
            logging.info(f"Node {self.node_id}: Initialized in static cluster mode with {args.client_num_in_total} nodes")
        else:
            # Dynamic cluster mode - wait for service discovery
            self.total_nodes = 0
            self.majority = 1  # Will be updated when nodes are discovered
            logging.info(f"Node {self.node_id}: Initialized in dynamic cluster mode")
        
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
        """Reset the election timeout with a random value and clear prevote state."""
        self.election_timeout = random.uniform(self.min_election_timeout, self.max_election_timeout)
        # Reset prevote state
        self.prevote_requested = False
        self.prevotes_received.clear()
        self.prevote_term = 0
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
                
                # If we're already in PREVOTE state, don't start another prevote
                if self.state == RaftState.PREVOTE and self.prevote_requested:
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
    
    def start_prevote(self):
        """
        Start the prevote phase before initiating an election.
        
        The prevote phase is a preliminary step before a full election to prevent
        disruptions from temporarily disconnected nodes or network partitions.
        
        Returns:
            bool: True if successfully started prevote, False otherwise
        """
        with self.state_lock:
            try:
                # Check current state - only followers initiate prevotes
                if self.state not in [RaftState.FOLLOWER]:
                    logging.debug(f"Node {self.node_id}: Cannot start prevote in {self.state} state")
                    return False
                
                # Check if we have enough known nodes to potentially win an election
                if not hasattr(self, 'known_nodes') or not hasattr(self, 'majority'):
                    logging.warning(f"Node {self.node_id}: Cannot determine majority, skipping prevote")
                    return False
                
                if len(self.known_nodes) < self.majority:
                    logging.warning(f"Node {self.node_id}: Not enough known nodes ({len(self.known_nodes)}) " +
                                  f"to reach majority ({self.majority}), skipping prevote")
                    return False
                
                # Set prevote state
                self.state = RaftState.PREVOTE
                self.prevote_requested = True
                self.prevote_term = self.current_term + 1  # The term we would use if election succeeds
                self.prevotes_received = {self.node_id}  # Count our own vote
                
                # Notify state change if callback exists
                if hasattr(self, 'on_state_change') and callable(self.on_state_change):
                    self.on_state_change(RaftState.PREVOTE)
                
                # Get last log info for vote requests
                last_log_index, last_log_term = self.get_last_log_info()
                
                logging.info(f"Node {self.node_id}: Starting prevote for potential term {self.prevote_term} " +
                            f"(current term: {self.current_term}, known nodes: {len(self.known_nodes)})")
                
                # Request prevotes from all other nodes
                # Note: This would actually send RPCs to other nodes in a real implementation
                # For the implementation here, we need to call the consensus manager
                if hasattr(self, 'consensus_manager') and self.consensus_manager is not None:
                    self.consensus_manager.broadcast_prevote_request(
                        candidate_id=self.node_id,
                        term=self.prevote_term,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term
                    )
                
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error starting prevote: {e}")
                # Reset state on error
                if self.state == RaftState.PREVOTE:
                    self.state = RaftState.FOLLOWER
                self.prevote_requested = False
                return False
    
    def start_election(self):
        """
        Start a new election (transition to CANDIDATE state).
        
        This method initiates a leader election by incrementing the current term,
        voting for itself, and transitioning to the CANDIDATE state. It follows the
        RAFT algorithm's election process with the prevote enhancement.
        
        Returns:
            bool: True if successfully started election, False otherwise
        """
        # # [TODO] Pretend we already got unanimous PreVotes:
        # self.prevotes_received = set(self.known_nodes)
        # self.prevote_requested = True
        # self.prevote_term = self.current_term + 1

        with self.state_lock:
            try:
                # Check if we're already in CANDIDATE state
                if self.state == RaftState.CANDIDATE:
                    logging.debug(f"Node {self.node_id}: Already in CANDIDATE state for term {self.current_term}")
                    return False
                
                # Check current state
                if self.state == RaftState.LEADER:
                    logging.debug(f"Node {self.node_id}: Cannot start election as LEADER")
                    return False
                
                # Newly initialized nodes should not start elections until synchronized
                if self.state == RaftState.INITIAL:
                    logging.debug(f"Node {self.node_id}: Cannot start election in INITIAL state")
                    return False
                
                # If we're not in PREVOTE state or didn't get majority prevotes, start prevote instead
                if self.state != RaftState.PREVOTE or len(self.prevotes_received) < self.majority:
                    logging.debug(f"Node {self.node_id}: Starting prevote phase instead of full election")
                    return self.start_prevote()
                
                # We have majority prevotes, proceed with real election
                # Increment term, vote for self
                old_term = self.current_term
                self.current_term = self.prevote_term  # Use the term we got prevotes for
                self.voted_for = self.node_id
                self.state = RaftState.CANDIDATE
                self.votes_received = {self.node_id}  # Vote for self
                
                # Reset election timeout
                self.reset_election_timeout()
                self.update_heartbeat()
                
                # Notify state change if callback exists
                if hasattr(self, 'on_state_change') and callable(self.on_state_change):
                    self.on_state_change(RaftState.CANDIDATE)
                
                logging.info(f"Node {self.node_id}: Starting election for term {self.current_term} " +
                            f"(previous term: {old_term}, known nodes: {len(self.known_nodes)})")
                
                # Request votes from all other nodes
                # In a real implementation, this would send RPCs to other nodes
                if hasattr(self, 'consensus_manager') and self.consensus_manager is not None:
                    last_log_index, last_log_term = self.get_last_log_info()
                    self.consensus_manager.request_votes_from_all()
                
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
                            logging.info(f"Node {self.node_id}: Won election.")
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
                
                logging.info(f"Node {self.node_id}: Became LEADER for term {self.current_term}")
                
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
                # Get this node's last log entry details (accounting for compaction)
                my_last_log_index, my_last_log_term = self.get_last_log_info()
                
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
        
        This is the core mechanism for log replication in the RAFT consensus algorithm.
        It handles both heartbeats (empty entries) and actual log replication.
        
        Args:
            leader_id (int): ID of the leader sending the request
            term (int): Leader's current term
            prev_log_index (int): Index of log entry immediately preceding new ones
            prev_log_term (int): Term of prev_log_index entry
            entries (list): List of log entries to append (empty for heartbeats)
            leader_commit (int): Leader's commit index
            
        Returns:
            tuple: (current_term, success) - current_term is for the leader to update its term
                  if needed, success indicates whether the append was successful
        """
        with self.state_lock:
            try:
                # Log heartbeat/append request at appropriate level
                is_heartbeat = not entries
                log_level = logging.DEBUG if is_heartbeat else logging.INFO
                logging.log(log_level, 
                           f"Node {self.node_id}: Received {'heartbeat' if is_heartbeat else 'AppendEntries'} " +
                           f"from leader {leader_id} for term {term} " +
                           f"(prev_idx={prev_log_index}, prev_term={prev_log_term}, " +
                           f"entries={len(entries) if entries else 0}, leader_commit={leader_commit})")
                
                # --- Step 1: State validation and updates ---
                
                # Nodes in INITIAL state should not accept AppendEntries unless they're ready
                if self.state == RaftState.INITIAL:
                    # Only accept AppendEntries if:
                    # 1. We have proper cluster information, AND
                    # 2. We have been synchronized (or this is a bootstrap scenario)
                    if not self._can_accept_leader_messages():
                        logging.debug(f"Node {self.node_id}: Rejecting AppendEntries in INITIAL state - not ready")
                        return self.current_term, False
                    
                    # We're ready to accept leadership - transition to follower
                    logging.info(f"Node {self.node_id}: Converting from INITIAL to FOLLOWER on receiving AppendEntries from leader {leader_id}")
                    self.become_follower(term)

                # If term < currentTerm, reject
                if term < self.current_term:
                    logging.info(f"Node {self.node_id}: Rejecting AppendEntries with term {term} < current term {self.current_term}")
                    return self.current_term, False
                
                # If term > currentTerm, convert to follower
                if term > self.current_term:
                    logging.info(f"Node {self.node_id}: Converting to follower due to higher term from leader {leader_id}")
                    self.become_follower(term)
                
                # This is a valid AppendEntries from current leader, reset election timeout
                self.update_heartbeat()
                
                # If we were a candidate, step down
                if self.state == RaftState.CANDIDATE:
                    logging.info(f"Node {self.node_id}: Stepping down from candidate due to valid leader for term {term}")
                    self.become_follower(term)
                
                # --- Step 2: Log consistency check ---
                
                # Convert prev_log_index (absolute) to array index considering first_log_index
                array_idx = prev_log_index - self.first_log_index

                # Check if our log has the entry at prev_log_index with term prev_log_term
                # Special case: prev_log_index = 0 means we're starting from the beginning
                if prev_log_index == 0:
                    log_ok = True
                    reason = "starting from beginning of log"
                elif array_idx >= len(self.log) or array_idx < 0:
                    log_ok = False
                    reason = f"prev_log_index ({prev_log_index}) outside log range (first_log_index={self.first_log_index}, log length={len(self.log)})"
                elif self.log[array_idx]['term'] != prev_log_term:
                    log_ok = False
                    reason = f"term mismatch at prev_log_index: expected {prev_log_term}, found {self.log[array_idx]['term']}"
                else:
                    log_ok = True
                    reason = "log consistency check passed"
                
                logging.log(logging.INFO if not log_ok else logging.DEBUG, 
                           f"Node {self.node_id}: Log consistency check: {log_ok}, {reason}")
                
                if not log_ok:
                    # For future log compaction support, we might return nextIndex hint here
                    return self.current_term, False
                
                # --- Step 3: Process entries ---
                
                # Only process entries if we received any
                if entries:
                    # Track if log was modified for logging purposes
                    log_modified = False
                    truncated = False
                    appended = 0
                    
                    # Process each entry
                    for i, entry in enumerate(entries):
                        # Calculate the absolute index for this entry
                        idx = prev_log_index + i + 1
                        array_idx = idx - self.first_log_index

                        # Case 1: This entry goes beyond our log, so append it
                        if array_idx >= len(self.log):
                            self.log.append(entry)
                            log_modified = True
                            appended += 1
                        # Case 2: This entry conflicts with our log, truncate and append
                        elif array_idx < 0 or self.log[array_idx]['term'] != entry['term']:
                            self.log = self.log[:array_idx]  # Truncate log up to this point
                            self.log.append(entry)
                            log_modified = True
                            truncated = True
                            appended = 1
                            # We've truncated, so all further entries will be appends
                        # Case 3: Entry already exists in our log with the same term
                        # No action needed, entry already exists
                    
                    # Log what happened for debugging
                    if log_modified:
                        log_msg = f"Node {self.node_id}: Log updated: "
                        if truncated:
                            log_msg += f"truncated to index {prev_log_index}, "
                        log_msg += f"appended {appended} entries"
                        logging.info(log_msg)
                
                # --- Step 4: Update commit index ---
                
                # Update commit index if leader's commit index is higher
                if leader_commit > self.commit_index:
                    old_commit_index = self.commit_index
                    self.commit_index = min(leader_commit, len(self.log))
                    
                    if self.commit_index > old_commit_index:
                        logging.info(f"Node {self.node_id}: Updated commit index from {old_commit_index} to {self.commit_index}")
                        self.apply_committed_entries()
                
                return self.current_term, True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error processing AppendEntries: {e}", exc_info=True)
                # On error, it's safer to reject the append
                return self.current_term, False
    
    def compact_log(self, up_to_index=None):
        """
        Compact the log by creating a snapshot up to the given index.
        
        This method implements log compaction according to section 7 of the RAFT paper.
        It removes entries up to the specified index (or commit_index if not specified)
        and updates state variables to reflect the new log beginning.
        
        Args:
            up_to_index (int, optional): Last log index to include in the snapshot.
                                        If not provided, uses commit_index.
        
        Returns:
            bool: True if compaction was performed, False otherwise
        """
        with self.state_lock:
            try:
                # Determine compaction index (never compact beyond commit_index for safety)
                if up_to_index is None:
                    # Default to commit_index to ensure we don't lose uncommitted entries
                    up_to_index = self.commit_index
                else:
                    # Don't compact beyond commit_index
                    up_to_index = min(up_to_index, self.commit_index)
                
                # Don't compact if there's nothing to compact
                if up_to_index <= self.last_snapshot_index:
                    logging.debug(f"Node {self.node_id}: Skipping log compaction, no new entries to compact " +
                                 f"(up_to_index={up_to_index}, last_snapshot_index={self.last_snapshot_index})")
                    return False
                
                # Don't compact if the log isn't long enough yet
                current_log_size = len(self.log)
                if current_log_size < self.log_compaction_threshold:
                    logging.debug(f"Node {self.node_id}: Skipping log compaction, log not full yet " +
                                 f"({current_log_size}/{self.log_compaction_threshold} entries)")
                    return False
                
                # Adjust for real index in log array
                array_index = up_to_index - self.first_log_index
                
                if array_index < 0 or array_index >= len(self.log):
                    logging.warning(f"Node {self.node_id}: Invalid compaction index: {up_to_index} " +
                                  f"(first_log_index={self.first_log_index}, log length={len(self.log)})")
                    return False
                
                # Save the term of the last included entry
                last_included_term = self.log[array_index]['term']
                
                # In a real implementation, we would serialize the state machine up to this point
                # For this implementation, we'll just note what would be in the snapshot
                
                logging.info(f"Node {self.node_id}: Compacting log up to index {up_to_index} (term {last_included_term}), " +
                           f"removing {array_index + 1} entries")
                
                # Update snapshot metadata
                self.last_snapshot_index = up_to_index
                self.last_snapshot_term = last_included_term
                
                # Remove compacted entries from log
                self.log = self.log[array_index + 1:]
                
                # Update first_log_index to reflect the new beginning of the log
                self.first_log_index = up_to_index + 1
                
                logging.debug(f"Node {self.node_id}: Log compaction complete. New log size: {len(self.log)}, " +
                            f"first_log_index: {self.first_log_index}")
                
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error during log compaction: {e}", exc_info=True)
                return False
    
    def check_and_compact_log(self):
        """
        Check if log needs compaction and perform it if necessary.
        
        This method is intended to be called periodically to keep the log size manageable.
        """
        # Only compact if log exceeds the threshold
        if len(self.log) >= self.log_compaction_threshold:
            # Compact up to last_applied to ensure we keep all data needed for catching up
            return self.compact_log(self.last_applied)
        return False
    
    def apply_committed_entries(self):
        """
        Apply committed but not yet applied log entries.
        
        This method iterates through all committed but not yet applied entries
        and applies them to the state machine. It also checks if log compaction
        should be performed after applying entries.
        """
        entries_applied = 0
        
        try:
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                
                # Calculate actual index in the log array
                log_array_index = self.last_applied - self.first_log_index
                
                # Skip if this entry is in the snapshot (already applied)
                if self.last_applied <= self.last_snapshot_index:
                    logging.debug(f"Node {self.node_id}: Skipping entry {self.last_applied} (in snapshot)")
                    continue
                    
                # Check if entry exists in current log
                if log_array_index < 0 or log_array_index >= len(self.log):
                    logging.error(f"Node {self.node_id}: Cannot apply entry at index {self.last_applied}, " +
                                f"array index {log_array_index} is out of bounds (log size {len(self.log)}, " +
                                f"first_log_index {self.first_log_index})")
                    # Reset last_applied to prevent repeated errors
                    self.last_applied -= 1
                    break
                
                # Get and apply the entry
                entry = self.log[log_array_index]
                self.apply_log_entry(entry)
                entries_applied += 1
            
            # Consider compaction after applying entries
            if entries_applied > 0:
                logging.debug(f"Node {self.node_id}: Applied {entries_applied} committed entries, " + 
                            f"now at index {self.last_applied}")
                
                # Check if we should compact the log
                if len(self.log) >= self.log_compaction_threshold:
                    self.check_and_compact_log()
        
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error applying committed entries: {e}", exc_info=True)
            # On error, roll back the last_applied counter to avoid skipping entries
            if entries_applied > 0:
                self.last_applied -= 1
            
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
        
        # Check if the base version is in a compacted part of the log
        if base_version < self.last_snapshot_index:
            logging.warning(f"Node {self.node_id}: Cannot apply topology delta with base_version {base_version} - " +
                          f"version predates last snapshot at index {self.last_snapshot_index}")
            
            # Request a full topology update instead
            if hasattr(self, 'request_topology_update') and callable(self.request_topology_update):
                logging.info(f"Node {self.node_id}: Requesting full topology update due to compaction")
                self.request_topology_update()
            return
        
        if hasattr(self, 'topology_manager') and self.topology_manager is not None:
            # Apply changes to the topology manager
            try:
                self.topology_manager.apply_match_changes(changes, base_version)
                logging.info(f"Node {self.node_id}: Applied {len(changes)} topology changes from base version {base_version}")
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error applying topology delta: {e}")
                # If error occurs, it might be due to missing base version - request full update
                if hasattr(self, 'request_topology_update') and callable(self.request_topology_update):
                    logging.info(f"Node {self.node_id}: Requesting full topology update due to error")
                    self.request_topology_update()
        else:
            # Store changes for later application
            if not hasattr(self, 'pending_topology_changes'):                
                self.pending_topology_changes = []
            self.pending_topology_changes.append((base_version, changes))
            logging.debug(f"Node {self.node_id}: Stored {len(changes)} topology changes for later application")

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
        
        # Check if the base version is in a compacted part of the log
        if base_version < self.last_snapshot_index:
            logging.warning(f"Node {self.node_id}: Cannot apply bandwidth delta with base_version {base_version} - " +
                          f"version predates last snapshot at index {self.last_snapshot_index}")
            
            # Request a full bandwidth update instead
            if hasattr(self, 'request_bandwidth_update') and callable(self.request_bandwidth_update):
                logging.info(f"Node {self.node_id}: Requesting full bandwidth update due to compaction")
                self.request_bandwidth_update()
            return
        
        if hasattr(self, 'bandwidth_manager') and self.bandwidth_manager is not None:
            # Apply changes to the bandwidth manager
            try:
                self.bandwidth_manager.apply_bandwidth_changes(changes, base_version)
                logging.info(f"Node {self.node_id}: Applied {len(changes)} bandwidth changes from base version {base_version}")
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error applying bandwidth delta: {e}")
                # If error occurs, it might be due to missing base version - request full update
                if hasattr(self, 'request_bandwidth_update') and callable(self.request_bandwidth_update):
                    logging.info(f"Node {self.node_id}: Requesting full bandwidth update due to error")
                    self.request_bandwidth_update()
        else:
            # Store changes for later application
            if not hasattr(self, 'pending_bandwidth_changes'):
                self.pending_bandwidth_changes = []
            self.pending_bandwidth_changes.append((base_version, changes))
            logging.debug(f"Node {self.node_id}: Stored {len(changes)} bandwidth changes for later application")
    
    def _apply_membership_change(self, command):
        """
        Apply a membership change command with connection information.
        
        Args:
            command (dict): The membership change command
        """
        with self.state_lock:
            try:
                action = command.get('action')
                node_id = command.get('node_id')
                node_info = command.get('node_info', {})
                current_nodes = command.get('current_nodes')
                current_nodes_info = command.get('current_nodes_info', {})
                round_num = command.get('round', 0)
                
                old_known_nodes = set(self.known_nodes)  # Copy for change detection
                
                if action == 'add':
                    # Add the node to known nodes if not already present
                    if node_id not in self.known_nodes:
                        self.known_nodes.add(node_id)
                        logging.info(f"Node {self.node_id}: Added node {node_id} to known nodes at round {round_num}")
                        
                        # Store connection information
                        if node_info and ('ip_address' in node_info or 'port' in node_info):
                            if not hasattr(self, 'node_connection_info'):
                                self.node_connection_info = {}
                            self.node_connection_info[node_id] = node_info
                            logging.debug(f"Node {self.node_id}: Stored connection info for node {node_id}")
                            
                elif action == 'remove':
                    # Remove the node from known nodes if present
                    if node_id in self.known_nodes:
                        self.known_nodes.remove(node_id)
                        logging.info(f"Node {self.node_id}: Removed node {node_id} from known nodes at round {round_num}")
                        
                        # Clean up connection information
                        if hasattr(self, 'node_connection_info') and node_id in self.node_connection_info:
                            del self.node_connection_info[node_id]
                            logging.debug(f"Node {self.node_id}: Removed connection info for node {node_id}")
                
                # If current_nodes is provided, use it to update known nodes
                if current_nodes is not None:
                    self.known_nodes = set(current_nodes)
                    logging.info(f"Node {self.node_id}: Updated known nodes to {current_nodes} at round {round_num}")
                
                # Update connection info for all nodes if provided
                if current_nodes_info:
                    if not hasattr(self, 'node_connection_info'):
                        self.node_connection_info = {}
                    self.node_connection_info.update(current_nodes_info)
                    logging.debug(f"Node {self.node_id}: Updated connection info for {len(current_nodes_info)} nodes")
                
                # Only call update if there was actually a change
                if old_known_nodes != self.known_nodes:
                    # Update known nodes count and notify any monitoring components
                    self.update_known_nodes(node_ids=list(self.known_nodes))
                    
                    # If we have a manager, notify it about membership change
                    if hasattr(self, 'consensus_manager') and self.consensus_manager is not None:
                        try:
                            self.consensus_manager.on_membership_change(self.known_nodes, round_num)
                            logging.debug(f"Node {self.node_id}: Notified consensus manager of membership change")
                        except Exception as e:
                            logging.error(f"Node {self.node_id}: Error notifying consensus manager of membership change: {e}")
                    
                    # Notify communication manager to establish/close connections
                    if hasattr(self, 'comm_manager') and self.comm_manager is not None:
                        try:
                            if action == 'add' and node_id in self.node_connection_info:
                                self.comm_manager.establish_connection(node_id, self.node_connection_info[node_id])
                            elif action == 'remove':
                                self.comm_manager.close_connection(node_id)
                        except Exception as e:
                            logging.error(f"Node {self.node_id}: Error updating connections: {e}")
                            
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error applying membership change: {e}", exc_info=True)
    
    def _apply_coordinator_change(self, command):
        """
        Apply a coordinator change command.
        
        Args:
            command (dict): The coordinator change command containing:
                - coordinator_id: ID of the new coordinator
                - previous_coordinator_id: ID of the previous coordinator (optional)
                - round: Training round number (optional)
                - reason: Reason for coordinator change (optional)
        """
        with self.state_lock:
            try:
                coordinator_id = command.get('coordinator_id')
                previous_coordinator_id = command.get('previous_coordinator_id')
                round_num = command.get('round', 0)
                reason = command.get('reason', 'unspecified')
                
                if coordinator_id is None:
                    logging.error(f"Node {self.node_id}: Received coordinator change with null coordinator_id")
                    return
                
                # Track if coordinator actually changed
                coordinator_changed = False
                old_coordinator = None
                
                # Update the coordinator information
                if hasattr(self, 'current_coordinator_id'):
                    old_coordinator = self.current_coordinator_id
                    if self.current_coordinator_id != coordinator_id:
                        self.previous_coordinator_id = self.current_coordinator_id
                        self.current_coordinator_id = coordinator_id
                        coordinator_changed = True
                else:
                    # First time setting coordinator
                    self.current_coordinator_id = coordinator_id
                    self.previous_coordinator_id = previous_coordinator_id
                    coordinator_changed = True
                
                if coordinator_changed:
                    logging.info(f"Node {self.node_id}: Updated coordinator from {old_coordinator} to {coordinator_id} at round {round_num} (reason: {reason})")
                    
                    # Notify training components of coordinator change if available
                    if hasattr(self, 'consensus_manager') and self.consensus_manager is not None:
                        try:
                            # Notify consensus manager of coordinator change
                            self.consensus_manager.on_coordinator_change(
                                new_coordinator=coordinator_id,
                                old_coordinator=old_coordinator,
                                round_num=round_num,
                                reason=reason
                            )
                            logging.debug(f"Node {self.node_id}: Notified consensus manager of coordinator change")
                            
                            # If this node is the new coordinator, it should initiate training
                            if coordinator_id == self.node_id:
                                logging.info(f"Node {self.node_id}: I am the new coordinator for round {round_num}")
                                self.consensus_manager.on_become_coordinator(round_num)
                        except Exception as e:
                            logging.error(f"Node {self.node_id}: Error notifying of coordinator change: {e}", exc_info=True)
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error applying coordinator change: {e}", exc_info=True)
    
    def add_log_entry(self, command):
        """
        Add a new log entry (leaders only).
        
        Args:
            command (dict): The command to add to the log
            
        Returns:
            int: Index of the new log entry, or -1 if not leader
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    return -1
                    
                # Calculate the next log index considering log compaction
                # We need to account for gaps in the log due to compaction
                next_log_index = self.first_log_index + len(self.log)
                
                # Create and append the log entry
                entry = {
                    'term': self.current_term,
                    'command': command,
                    'index': next_log_index
                }
                self.log.append(entry)
                
                logging.info(f"Leader {self.node_id}: Added new log entry at index {next_log_index}")
                
                # Check if we should compact the log after adding a new entry
                if len(self.log) > self.log_compaction_threshold:
                    # Only initiate compaction if enough entries are committed
                    if self.commit_index - self.last_snapshot_index > self.log_compaction_threshold / 2:
                        logging.debug(f"Leader {self.node_id}: Initiating log compaction check after adding entry")
                        self.check_and_compact_log()
                
                return next_log_index
                
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error adding log entry: {e}", exc_info=True)
                return -1
    
    def get_last_log_info(self):
        """
        Get information about the last log entry.
        
        This method accounts for log compaction and correctly returns
        the absolute log index and term of the last entry.
        
        Returns:
            tuple: (last_log_index, last_log_term)
        """
        with self.state_lock:
            try:
                # If log is empty, check if we have a snapshot
                if len(self.log) == 0:
                    return self.last_snapshot_index, self.last_snapshot_term
                
                # Calculate the actual log index considering compaction
                last_log_index = self.first_log_index + len(self.log) - 1
                last_log_term = self.log[-1]['term']
                
                return last_log_index, last_log_term
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error getting last log info: {e}", exc_info=True)
                # Return safe defaults in case of error
                if len(self.log) > 0:
                    return len(self.log), self.log[-1]['term']
                else:
                    return self.last_snapshot_index, self.last_snapshot_term
        return last_log_index, last_log_term

    def get_term_at_index(self, index):
        """Get the term of the log entry at the given absolute index."""
        with self.state_lock:
            if index == self.last_snapshot_index:
                return self.last_snapshot_term

            if index < self.first_log_index:
                return 0

            array_idx = index - self.first_log_index
            if array_idx < 0 or array_idx >= len(self.log):
                return 0

            return self.log[array_idx]["term"]
    
    def add_node(self, new_node_id, round_num=0):
        """
        Add a new node to the cluster (leaders only).
        
        Args:
            new_node_id (int): ID of the new node
            round_num (int, optional): Current training round number
            
        Returns:
            int: Log index of the membership entry, or -1 on failure
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    logging.warning(
                        f"Node {self.node_id}: Cannot add node {new_node_id} - not a leader"
                    )
                    return -1
                    
                if new_node_id in self.known_nodes:
                    logging.debug(
                        f"Leader {self.node_id}: Node {new_node_id} already in cluster, no action needed"
                    )
                    return -1  # Already known, nothing appended
                
                # Make a copy of current nodes for the log entry (for consistency)
                current_nodes = set(self.known_nodes)
                current_nodes.add(new_node_id)
                
                # Initialize leader state for the new node (before applying changes)
                # This allows us to start sending AppendEntries to the new node right away
                last_log_index = self.first_log_index + len(self.log) - 1 if len(self.log) > 0 else 0
                self.next_index[new_node_id] = last_log_index + 1
                self.match_index[new_node_id] = 0
                
                # Add a log entry for the membership change
                # We include current_nodes to ensure all nodes have the same view
                log_index = self.add_log_entry({
                    'type': 'membership',
                    'action': 'add',
                    'node_id': new_node_id,
                    'current_nodes': list(current_nodes),  # Convert set to list for JSON serialization
                    'round': round_num
                })
                
                if log_index == -1:
                    logging.error(f"Leader {self.node_id}: Failed to add log entry for node addition")
                    # Clean up our premature leader state updates
                    if new_node_id in self.next_index:
                        del self.next_index[new_node_id]
                    if new_node_id in self.match_index:
                        del self.match_index[new_node_id]
                    return -1
                
                # Only update known_nodes after successful log entry creation
                # The actual change will be applied when the entry is committed
                self.known_nodes.add(new_node_id)
                self.update_known_nodes(len(self.known_nodes))
                
                logging.info(
                    f"Leader {self.node_id}: Added new node {new_node_id} to cluster at round {round_num}"
                )
                return log_index
                
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error adding node {new_node_id}: {e}", exc_info=True)
                # Clean up if we failed after initializing leader state
                if new_node_id in self.next_index:
                    del self.next_index[new_node_id]
                if new_node_id in self.match_index:
                    del self.match_index[new_node_id]
                return -1
    
    def remove_node(self, node_id, round_num=0, reason="unspecified"):
        """
        Remove a node from the cluster (leaders only).
        
        Args:
            node_id (int): ID of the node to remove
            round_num (int, optional): Current training round number
            reason (str, optional): Reason for node removal (e.g., "timeout", "failure", "voluntary")
            
        Returns:
            int: Log index of the membership entry, or -1 on failure
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    logging.warning(
                        f"Node {self.node_id}: Cannot remove node {node_id} - not a leader"
                    )
                    return -1
                    
                if node_id not in self.known_nodes:
                    logging.debug(
                        f"Leader {self.node_id}: Node {node_id} not in cluster, no action needed"
                    )
                    return -1  # Already removed
                
                # Make a copy of current nodes for the log entry (for consistency)
                current_nodes = set(self.known_nodes)
                current_nodes.remove(node_id)
                
                # Add a log entry for the membership change first
                # We include current_nodes to ensure all nodes have the same view
                log_index = self.add_log_entry({
                    'type': 'membership',
                    'action': 'remove',
                    'node_id': node_id,
                    'current_nodes': list(current_nodes),  # Convert set to list for JSON serialization
                    'round': round_num,
                    'reason': reason
                })
                
                if log_index == -1:
                    logging.error(
                        f"Leader {self.node_id}: Failed to add log entry for node removal"
                    )
                    return -1
                
                # Only update state after successful log entry creation
                # The actual change will be applied when the entry is committed
                self.known_nodes.remove(node_id)
                self.update_known_nodes(len(self.known_nodes))
                
                # Clean up leader state for the removed node
                if node_id in self.next_index:
                    del self.next_index[node_id]
                if node_id in self.match_index:
                    del self.match_index[node_id]
                
                # If we're removing the coordinator, we may need to select a new one
                if hasattr(self, 'current_coordinator_id') and self.current_coordinator_id == node_id:
                    logging.warning(f"Leader {self.node_id}: Removed node {node_id} was the coordinator, will need new coordinator")
                    # The actual coordinator change would typically be handled elsewhere
                    # but we note it here for visibility
                
                logging.info(
                    f"Leader {self.node_id}: Removed node {node_id} from cluster at round {round_num} (reason: {reason})"
                )
                return log_index
                
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error removing node {node_id}: {e}", exc_info=True)
                return -1
    
    def update_commit_index(self):
        """
        Update commit index based on match_index (leaders only).
        
        According to the RAFT paper (section 5.3 and 5.4.2), the leader:
        1. Maintains a matchIndex for each follower, indicating the highest
           log entry known to be replicated on that follower
        2. Uses this information to advance the commitIndex when an entry
           is replicated on a majority of servers
        3. Only commits entries from the current term
        
        This method implements these rules and also handles log compaction.
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    return
                
                # If no followers, we still commit our own entries
                if len(self.known_nodes) == 1:  # Only the leader itself
                    # Calculate how many entries can be committed
                    last_log_index, _ = self.get_last_log_info()
                    if last_log_index > self.commit_index:
                        old_commit_index = self.commit_index
                        self.commit_index = last_log_index
                        logging.info(f"Single-node leader {self.node_id}: Updated commit index from {old_commit_index} to {last_log_index}")
                        self.apply_committed_entries()
                    return
                
                # Calculate majority needed for commitment
                majority = (len(self.known_nodes) // 2) + 1
                
                # Instead of iterating through all entries, start from the highest match index
                # among followers and work backwards
                potential_commit_indices = sorted([idx for idx in self.match_index.values()], reverse=True)
                # Add leader's own last index
                last_log_index, _ = self.get_last_log_info()
                potential_commit_indices.insert(0, last_log_index)
                
                # Keep track of highest index that can be committed
                highest_committable_index = self.commit_index
                
                # Check each potential commit index
                for potential_index in potential_commit_indices:
                    # Skip indices we've already committed or those below what we're considering
                    if potential_index <= self.commit_index or potential_index <= highest_committable_index:
                        continue
                    
                    # Skip indices that aren't in our log (could happen after compaction)
                    log_array_idx = potential_index - self.first_log_index
                    if log_array_idx < 0 or log_array_idx >= len(self.log):
                        continue
                    
                    # Only commit entries from current term (RAFT safety property)
                    if self.log[log_array_idx]['term'] != self.current_term:
                        continue
                    
                    # Count replications (including leader itself)
                    count = 1  # Leader itself
                    for follower_id, match_idx in self.match_index.items():
                        if match_idx >= potential_index:
                            count += 1
                    
                    # If majority, this index can be committed
                    if count >= majority:
                        highest_committable_index = potential_index
                        break  # Found highest committable index
                
                # Update commit index if we found a higher committable index
                if highest_committable_index > self.commit_index:
                    old_commit_index = self.commit_index
                    self.commit_index = highest_committable_index
                    
                    # Apply the newly committed entries
                    logging.info(f"Leader {self.node_id}: Updated commit index from {old_commit_index} to {highest_committable_index} (replicated on {count}/{len(self.known_nodes)} nodes)")
                    self.apply_committed_entries()
            
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error updating commit index: {e}", exc_info=True)
    
    def get_current_timestamp(self):
        """Get the current timestamp."""
        return time.time()
        
    def request_topology_update(self):
        """
        Request a full topology update from the leader.
        
        This method is called when an incremental update cannot be applied,
        typically due to log compaction removing the base version.
        """
        if not hasattr(self, 'consensus_manager') or self.consensus_manager is None:
            logging.warning(f"Node {self.node_id}: Cannot request topology update - no consensus manager available")
            return
            
        try:
            logging.info(f"Node {self.node_id}: Requesting full topology update from leader")
            # The actual implementation depends on the consensus manager interface
            # This could involve sending a message to the leader or triggering an update protocol
            self.consensus_manager.request_full_topology_update()
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error requesting topology update: {e}")
            
    def request_bandwidth_update(self):
        """
        Request a full bandwidth update from the leader.
        
        This method is called when an incremental update cannot be applied,
        typically due to log compaction removing the base version.
        """
        if not hasattr(self, 'consensus_manager') or self.consensus_manager is None:
            logging.warning(f"Node {self.node_id}: Cannot request bandwidth update - no consensus manager available")
            return
            
        try:
            logging.info(f"Node {self.node_id}: Requesting full bandwidth update from leader")
            # The actual implementation depends on the consensus manager interface
            # This could involve sending a message to the leader or triggering an update protocol
            self.consensus_manager.request_full_bandwidth_update()
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error requesting bandwidth update: {e}")
    
    def receive_prevote_request(self, candidate_id, term, last_log_index, last_log_term):
        """
        Handle a prevote request from a potential candidate.
        
        A prevote is granted if:
        1. The candidate's term is at least as up-to-date as our current term
        2. The candidate's log is at least as up-to-date as ours
        3. We haven't heard from a valid leader recently (election timeout is close)
        
        Args:
            candidate_id (int): ID of the potential candidate requesting the prevote
            term (int): Candidate's proposed term (usually current_term + 1)
            last_log_index (int): Index of candidate's last log entry
            last_log_term (int): Term of candidate's last log entry
            
        Returns:
            tuple: (current_term, prevote_granted)
        """
        with self.state_lock:
            try:
                logging.debug(f"Node {self.node_id}: Received prevote request from {candidate_id} for term {term}")
                
                # Always reject prevotes if we are a leader with an active lease
                if self.state == RaftState.LEADER:
                    logging.debug(f"Node {self.node_id}: Rejecting prevote as LEADER")
                    return self.current_term, False
                
                # The candidate's term should be future term (current + 1)
                if term < self.current_term:
                    logging.debug(f"Node {self.node_id}: Rejecting prevote - term {term} < current term {self.current_term}")
                    return self.current_term, False
                
                # Only grant prevote if we haven't heard from the leader recently
                # This is a key optimization - if we're getting heartbeats, don't disrupt the leader
                time_since_heartbeat = time.time() - self.last_heartbeat_time
                if time_since_heartbeat < 0.8 * self.election_timeout:  # Still getting regular heartbeats
                    logging.debug(f"Node {self.node_id}: Rejecting prevote - still receiving heartbeats " +
                                 f"({time_since_heartbeat*1000:.2f}ms < {0.8*self.election_timeout*1000:.2f}ms)")
                    return self.current_term, False
                
                # Check if the candidate's log is up-to-date with ours
                if not self.is_log_up_to_date(last_log_index, last_log_term):
                    logging.debug(f"Node {self.node_id}: Rejecting prevote - candidate log not up-to-date")
                    return self.current_term, False
                
                # Grant the prevote
                logging.info(f"Node {self.node_id}: Granting prevote to node {candidate_id} for term {term}")
                return self.current_term, True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error processing prevote request: {e}")
                # Safer to reject on error
                return self.current_term, False
    
    def receive_prevote_response(self, voter_id, term, prevote_granted):
        """
        Handle prevote response from another node.
        
        Args:
            voter_id (int): ID of the voting node
            term (int): Current term in the prevote response
            prevote_granted (bool): Whether the prevote was granted
            
        Returns:
            bool: True if node should start actual election, False otherwise
        """
        with self.state_lock:
            try:
                # Ignore responses if no longer in PREVOTE state
                if self.state != RaftState.PREVOTE:
                    logging.debug(f"Node {self.node_id}: Ignoring prevote response from {voter_id} - no longer in PREVOTE state")
                    return False
                
                # # Ignore responses for wrong term
                # if term != self.prevote_term:
                #     logging.debug(f"Node {self.node_id}: Ignoring prevote response from {voter_id} - " +
                #                  f"term mismatch (got {term}, expected {self.prevote_term})")
                #     return False
                
                # If we see a higher term, become follower
                if term > self.current_term:
                    logging.info(f"Node {self.node_id}: Discovered higher term {term} in prevote response from {voter_id}")
                    self.become_follower(term)
                    return False
                
                # Process the vote
                if prevote_granted:
                    self.prevotes_received.add(voter_id)
                    votes_count = len(self.prevotes_received)
                    majority_count = self.majority
                    
                    logging.info(f"Node {self.node_id}: Received prevote from {voter_id} " +
                                f"({votes_count}/{len(self.known_nodes)} votes, need {majority_count})")
                    
                    # If we have a majority of prevotes, start actual election
                    if votes_count >= majority_count:
                        logging.info(f"Node {self.node_id}: Received majority of prevotes " +
                                    f"({votes_count}/{len(self.known_nodes)}), can proceed to election")
                        return True
                else:
                    logging.debug(f"Node {self.node_id}: Node {voter_id} rejected prevote request")
                
                return False
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error processing prevote response: {e}")
                return False
    
    def get_cluster_state(self):
        """
        Get the current cluster state information.
        
        Returns:
            dict: Current cluster state including known nodes, current term, and leader info
        """
        with self.state_lock:
            return {
                'known_nodes': sorted(list(self.known_nodes)),
                'current_term': self.current_term,
                'leader_id': self.get_leader_id(),
                'state': self.state.name,
                'commit_index': self.commit_index,
                'last_log_index': self.first_log_index + len(self.log) - 1 if len(self.log) > 0 else 0,
                'total_nodes': self.total_nodes
            }

    def bootstrap_detection(self, discovered_nodes):
        """
        Handle bootstrap detection from service discovery.
        
        This method is called when service discovery provides initial cluster information.
        It determines whether this node should transition from INITIAL to FOLLOWER or LEADER.
        
        Args:
            discovered_nodes (set): Set of discovered node IDs
            
        Returns:
            bool: True if state transition occurred, False otherwise
        """
        with self.state_lock:
            try:
                # Only process bootstrap detection if we're in INITIAL state
                if self.state != RaftState.INITIAL:
                    logging.debug(f"Node {self.node_id}: Ignoring bootstrap detection - not in INITIAL state")
                    return False
                
                if not discovered_nodes:
                    logging.debug(f"Node {self.node_id}: No nodes discovered for bootstrap")
                    return False
                
                # Update known nodes based on discovery
                old_known_nodes = set(self.known_nodes)
                self.known_nodes.update(discovered_nodes)
                
                # Only add ourselves if we're not already in the known nodes
                if self.node_id not in self.known_nodes:
                    self.known_nodes.add(self.node_id)
                
                # Update node count and majority threshold
                self.total_nodes = len(self.known_nodes)
                self.majority = self.total_nodes // 2 + 1
                
                logging.info(f"Node {self.node_id}: Bootstrap detection - known nodes: {sorted(self.known_nodes)}, "
                           f"total: {self.total_nodes}, majority: {self.majority}")
                
                # Determine state transition based on cluster composition
                if len(self.known_nodes) == 1 and self.node_id in self.known_nodes:
                    # Single node cluster - become leader immediately
                    logging.info(f"Node {self.node_id}: Single node cluster detected - transitioning to LEADER")
                    
                    # Initialize as leader directly (bootstrap case)
                    self.current_term = 1
                    self.voted_for = self.node_id
                    self.state = RaftState.LEADER  # Direct transition for bootstrap
                    
                    # Initialize leader state (no followers for single node)
                    self.next_index = {}
                    self.match_index = {}
                    
                    # Reset election state
                    self.votes_received = set()
                    
                    # Add no-op entry for bootstrap (now that we're leader)
                    no_op_entry = {
                        'type': 'no-op',
                        'timestamp': self.get_current_timestamp()
                    }
                    # Direct append since we're the only node
                    entry = {
                        'term': self.current_term,
                        'command': no_op_entry,
                        'index': 1
                    }
                    self.log.append(entry)
                    
                    # Notify state change
                    if self.on_state_change:
                        self.on_state_change(RaftState.LEADER)
                    
                    logging.info(f"Node {self.node_id}: Became bootstrap LEADER for term {self.current_term}")
                    return True
                    
                elif len(self.known_nodes) > 1:
                    # Multi-node cluster - become follower and wait for leader
                    logging.info(f"Node {self.node_id}: Multi-node cluster detected - transitioning to FOLLOWER")
                    self.state = RaftState.FOLLOWER
                    self.reset_election_timeout()
                    self.update_heartbeat()
                    
                    if self.on_state_change:
                        self.on_state_change(RaftState.FOLLOWER)
                    
                    # If we're joining an existing cluster, request state synchronization
                    if old_known_nodes != self.known_nodes:
                        self.request_state_sync()
                    
                    return True
                
                return False
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error in bootstrap detection: {e}")
                return False

    def request_state_sync(self):
        """
        Request state synchronization from the cluster leader.
        
        This method is called when a node joins an existing cluster and needs
        to synchronize its state with the current cluster state.
        
        Returns:
            bool: True if state sync request was sent, False otherwise
        """
        with self.state_lock:
            try:
                # Only request state sync if we're in FOLLOWER state
                if self.state != RaftState.FOLLOWER:
                    logging.debug(f"Node {self.node_id}: Cannot request state sync - not in FOLLOWER state")
                    return False
                
                # Send state sync request via consensus manager if available
                if hasattr(self, 'consensus_manager') and self.consensus_manager is not None:
                    logging.info(f"Node {self.node_id}: Requesting state synchronization from cluster")
                    
                    # Create state sync request message
                    state_sync_request = {
                        'type': 'state_sync_request',
                        'node_id': self.node_id,
                        'current_term': self.current_term,
                        'timestamp': self.get_current_timestamp()
                    }
                    
                    # Send to consensus manager for broadcasting
                    self.consensus_manager.broadcast_state_sync_request(state_sync_request)
                    return True
                else:
                    logging.warning(f"Node {self.node_id}: No consensus manager available for state sync request")
                    return False
                    
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error requesting state sync: {e}")
                return False

    def handle_state_sync_response(self, response):
        """
        Handle state synchronization response from the leader.
        
        Args:
            response (dict): State synchronization response containing cluster state
            
        Returns:
            bool: True if state was successfully synchronized, False otherwise
        """
        with self.state_lock:
            try:
                # Validate response
                if not isinstance(response, dict):
                    logging.error(f"Node {self.node_id}: Invalid state sync response format")
                    return False
                
                leader_id = response.get('leader_id')
                response_term = response.get('term', 0)
                known_nodes = response.get('known_nodes', [])
                commit_index = response.get('commit_index', 0)
                log_entries = response.get('log_entries', [])
                
                logging.info(f"Node {self.node_id}: Received state sync response from leader {leader_id}, "
                           f"term: {response_term}, known_nodes: {known_nodes}, commit_index: {commit_index}")
                
                # Update term if necessary
                if response_term > self.current_term:
                    self.current_term = response_term
                    self.voted_for = None
                
                # Update known nodes
                if known_nodes:
                    old_known_nodes = set(self.known_nodes)
                    self.known_nodes = set(known_nodes)
                    
                    if old_known_nodes != self.known_nodes:
                        self.update_known_nodes(node_ids=list(self.known_nodes))
                        logging.info(f"Node {self.node_id}: Updated known nodes from state sync: {sorted(self.known_nodes)}")
                
                # Update log entries if provided
                if log_entries:
                    # For simplicity, we'll append the entries (in production, this would be more sophisticated)
                    for entry in log_entries:
                        if entry not in self.log:
                            self.log.append(entry)
                    
                    logging.info(f"Node {self.node_id}: Updated log from state sync, new length: {len(self.log)}")
                
                # Update commit index
                if commit_index > self.commit_index:
                    self.commit_index = commit_index
                    
                    # Apply committed entries
                    while self.last_applied < self.commit_index:
                        self.last_applied += 1
                        if self.last_applied <= len(self.log):
                            entry = self.log[self.last_applied - 1]
                            self.apply_log_entry(entry)
                
                # Mark state synchronization as completed
                self.set_state_sync_completed(True)
                
                # Try to transition to FOLLOWER if we're still in INITIAL state
                if self.state == RaftState.INITIAL:
                    self.transition_to_follower_from_initial()
                
                logging.info(f"Node {self.node_id}: State synchronization completed successfully")
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error handling state sync response: {e}")
                return False

    def transition_to_follower_from_initial(self):
        """
        Transition from INITIAL state to FOLLOWER state.
        
        This method handles the specific transition from INITIAL to FOLLOWER
        when a node has received enough information to participate in the cluster.
        
        Requirements for transition:
        1. Log is replicated (synchronized with leader)
        2. Model parameters have been received from leader successfully
        3. Basic cluster information is available
        
        Returns:
            bool: True if transition was successful, False otherwise
        """
        with self.state_lock:
            try:
                if self.state != RaftState.INITIAL:
                    logging.debug(f"Node {self.node_id}: Not in INITIAL state for transition to FOLLOWER")
                    return False
                
                # Requirement 1: Ensure we have enough cluster information
                if not hasattr(self, 'known_nodes') or len(self.known_nodes) == 0:
                    logging.warning(f"Node {self.node_id}: Cannot transition to FOLLOWER - no known nodes")
                    return False
                
                # Requirement 2: Check if log is properly replicated
                if not self._is_log_synchronized():
                    logging.warning(f"Node {self.node_id}: Cannot transition to FOLLOWER - log not synchronized")
                    return False
                
                # Requirement 3: Check if model parameters have been received
                if not self._has_model_parameters():
                    logging.warning(f"Node {self.node_id}: Cannot transition to FOLLOWER - model parameters not received")
                    return False
                
                # All requirements met - transition to FOLLOWER
                self.state = RaftState.FOLLOWER
                self.reset_election_timeout()
                self.update_heartbeat()
                
                # Notify state change
                if self.on_state_change:
                    self.on_state_change(RaftState.FOLLOWER)
                
                logging.info(f"Node {self.node_id}: Transitioned from INITIAL to FOLLOWER (log synchronized, model parameters received)")
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error transitioning from INITIAL to FOLLOWER: {e}")
                return False

    def can_participate_in_elections(self):
        """
        Check if this node can participate in elections.
        
        Returns:
            bool: True if node can participate in elections, False otherwise
        """
        with self.state_lock:
            # Nodes in INITIAL state cannot participate in elections
            if self.state == RaftState.INITIAL:
                return False
            
            # Must have valid cluster information
            if not hasattr(self, 'known_nodes') or len(self.known_nodes) == 0:
                return False
            
            # Must be part of the known nodes
            if self.node_id not in self.known_nodes:
                return False
            
            return True

    def get_leader_id(self):
        """
        Get the current leader ID if known.
        
        Returns:
            int or None: Leader ID if known, None otherwise
        """
        with self.state_lock:
            if self.state == RaftState.LEADER:
                return self.node_id
            
            # For followers, we don't explicitly track leader ID in basic RAFT
            # But we could track it based on heartbeats received
            if hasattr(self, 'current_leader_id'):
                return self.current_leader_id
            
            return None

    def set_current_leader(self, leader_id):
        """
        Set the current leader ID.
        
        Args:
            leader_id (int): ID of the current leader
        """
        with self.state_lock:
            self.current_leader_id = leader_id
            logging.debug(f"Node {self.node_id}: Set current leader to {leader_id}")

    def get_current_timestamp(self):
        """
        Get current timestamp for log entries.
        
        Returns:
            float: Current timestamp
        """
        return time.time()

    # Enhanced method to end the class
    def _is_log_synchronized(self):
        """
        Check if the log is properly synchronized with the leader.
        
        For a node joining an existing cluster, this means:
        1. The log has been replicated from the leader
        2. The commit index is up-to-date
        3. All committed entries have been applied
        
        Returns:
            bool: True if log is synchronized, False otherwise
        """
        with self.state_lock:
            try:
                # For new nodes, we need to have received some log entries
                # and have our commit index properly set
                
                # Check if we have received state synchronization
                if not hasattr(self, 'state_sync_completed') or not self.state_sync_completed:
                    logging.debug(f"Node {self.node_id}: State sync not completed")
                    return False
                
                # Check if we have a reasonable commit index
                if self.commit_index < 0:
                    logging.debug(f"Node {self.node_id}: Invalid commit index: {self.commit_index}")
                    return False
                
                # Check if last_applied is caught up with commit_index
                if self.last_applied < self.commit_index:
                    logging.debug(f"Node {self.node_id}: Last applied ({self.last_applied}) behind commit index ({self.commit_index})")
                    return False
                
                # For joining nodes, we should have received at least the current membership
                if len(self.log) == 0 and self.commit_index > 0:
                    logging.debug(f"Node {self.node_id}: Empty log but commit index is {self.commit_index}")
                    return False
                
                logging.debug(f"Node {self.node_id}: Log synchronization check passed")
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error checking log synchronization: {e}")
                return False

    def _has_model_parameters(self):
        """
        Check if model parameters have been received from the leader.
        
        For federated learning, a node joining the cluster needs to have:
        1. Received the current model state from the leader
        2. Model parameters are valid and ready for training
        3. Model version is synchronized with the cluster
        
        Returns:
            bool: True if model parameters are available, False otherwise
        """
        with self.state_lock:
            try:
                # Check if we have received model parameters
                if not hasattr(self, 'model_params_received') or not self.model_params_received:
                    logging.debug(f"Node {self.node_id}: Model parameters not received")
                    return False
                
                # Check if model parameters are valid
                if not hasattr(self, 'current_model_version') or self.current_model_version is None:
                    logging.debug(f"Node {self.node_id}: No current model version")
                    return False
                
                # Check if we have the actual model state
                if not hasattr(self, 'model_state') or self.model_state is None:
                    logging.debug(f"Node {self.node_id}: No model state available")
                    return False
                
                # Verify model state is not empty
                if isinstance(self.model_state, dict) and len(self.model_state) == 0:
                    logging.debug(f"Node {self.node_id}: Model state is empty")
                    return False
                
                logging.debug(f"Node {self.node_id}: Model parameters check passed - version: {self.current_model_version}")
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error checking model parameters: {e}")
                return False

    def set_state_sync_completed(self, completed=True):
        """
        Mark state synchronization as completed.
        
        Args:
            completed (bool): Whether state sync is completed
        """
        with self.state_lock:
            self.state_sync_completed = completed
            logging.debug(f"Node {self.node_id}: State sync completed set to {completed}")

    def set_model_parameters(self, model_state, model_version):
        """
        Set the model parameters received from the leader.
        
        Args:
            model_state (dict): The model state parameters
            model_version (int): The model version/round number
        """
        with self.state_lock:
            self.model_state = model_state
            self.current_model_version = model_version
            self.model_params_received = True
            logging.info(f"Node {self.node_id}: Received model parameters for version {model_version}")
            
            # Check if we can now transition to FOLLOWER (only if log is also synchronized)
            if self.state == RaftState.INITIAL and self._is_log_synchronized():
                self.transition_to_follower_from_initial()

    def initialize_from_discovery(self, discovered_nodes, node_info_map=None):
        """
        Initialize the RAFT node based on service discovery information.
        
        This method is called by the service discovery bridge to provide
        initial cluster information and trigger appropriate state transitions.
        
        Args:
            discovered_nodes (set): Set of discovered node IDs
            node_info_map (dict): Optional mapping of node IDs to connection info
            
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        with self.state_lock:
            try:
                # Only initialize if we're in INITIAL state
                if self.state != RaftState.INITIAL:
                    logging.debug(f"Node {self.node_id}: Not in INITIAL state for discovery initialization")
                    return False
                
                logging.info(f"Node {self.node_id}: Initializing from service discovery - discovered nodes: {sorted(discovered_nodes)}")
                
                # Store node connection information if provided
                if node_info_map:
                    if not hasattr(self, 'node_connection_info'):
                        self.node_connection_info = {}
                    self.node_connection_info.update(node_info_map)
                    logging.debug(f"Node {self.node_id}: Stored connection info for {len(node_info_map)} nodes")
                
                # Use the bootstrap detection method to handle state transitions
                return self.bootstrap_detection(discovered_nodes)
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error initializing from discovery: {e}")
                return False

    def enhance_add_node_with_connection_info(self, new_node_id, node_info, round_num=0):
        """
        Enhanced version of add_node that includes connection information.
        
        Args:
            new_node_id (int): ID of the new node
            node_info (dict): Connection information including IP, port, capabilities
            round_num (int): Current training round number
            
        Returns:
            int: Log index of the membership entry, or -1 on failure
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    logging.warning(f"Node {self.node_id}: Cannot add node {new_node_id} - not a leader")
                    return -1
                
                # Validate node_info
                if not node_info or 'ip_address' not in node_info or 'port' not in node_info:
                    logging.error(f"Leader {self.node_id}: Cannot add node {new_node_id} - missing connection info")
                    return -1
                
                if new_node_id in self.known_nodes:
                    logging.debug(f"Leader {self.node_id}: Node {new_node_id} already in cluster")
                    return -1
                
                # Store connection information
                if not hasattr(self, 'node_connection_info'):
                    self.node_connection_info = {}
                self.node_connection_info[new_node_id] = node_info
                
                # Create enhanced membership log entry
                current_nodes = set(self.known_nodes)
                current_nodes.add(new_node_id)
                
                # Get current nodes with their connection info
                current_nodes_info = {}
                for node_id in current_nodes:
                    if node_id in self.node_connection_info:
                        current_nodes_info[node_id] = self.node_connection_info[node_id]
                
                # Add log entry with complete node information
                log_index = self.add_log_entry({
                    'type': 'membership',
                    'action': 'add',
                    'node_id': new_node_id,
                    'node_info': node_info,  # Complete connection info
                    'current_nodes': list(current_nodes),
                    'current_nodes_info': current_nodes_info,  # All nodes with connection info
                    'round': round_num
                })
                
                if log_index != -1:
                    # Initialize leader state for the new node
                    last_log_index = self.first_log_index + len(self.log) - 1 if len(self.log) > 0 else 0
                    self.next_index[new_node_id] = last_log_index + 1
                    self.match_index[new_node_id] = 0
                    
                    # Update known nodes
                    self.known_nodes.add(new_node_id)
                    self.update_known_nodes(node_ids=list(self.known_nodes))
                    
                    logging.info(f"Leader {self.node_id}: Added node {new_node_id} with connection info at {node_info['ip_address']}:{node_info['port']}")
                
                return log_index
                
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error adding node {new_node_id} with connection info: {e}")
                return -1

    def _can_accept_leader_messages(self):
        """
        Check if a node in INITIAL state can accept leader messages.
        
        A node can accept leader messages if:
        1. It has cluster information (knows about other nodes), OR
        2. It's in bootstrap mode and ready to accept leadership, OR
        3. It has been properly synchronized through service discovery
        
        Returns:
            bool: True if node can accept leader messages, False otherwise
        """
        with self.state_lock:
            try:
                # Bootstrap nodes can always accept leadership
                if hasattr(self, 'is_bootstrap_node') and self.is_bootstrap_node:
                    logging.debug(f"Node {self.node_id}: Bootstrap node can accept leader messages")
                    return True
                
                # Nodes that have been discovered through service discovery can accept
                if hasattr(self, 'discovered_through_service_discovery') and self.discovered_through_service_discovery:
                    logging.debug(f"Node {self.node_id}: Node discovered through service discovery can accept leader messages")
                    return True
                
                # Nodes with cluster information can accept
                if len(self.known_nodes) > 0:
                    logging.debug(f"Node {self.node_id}: Node with cluster info can accept leader messages")
                    return True
                
                logging.debug(f"Node {self.node_id}: Node not ready to accept leader messages")
                return False
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error checking if can accept leader messages: {e}")
                return False

    def mark_discovered_through_service_discovery(self):
        """
        Mark this node as having been discovered through service discovery.
        This allows it to accept leader messages even without full cluster info.
        """
        with self.state_lock:
            self.discovered_through_service_discovery = True
            logging.debug(f"Node {self.node_id}: Marked as discovered through service discovery")
