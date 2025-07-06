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
    
    **Pure State Machine Design:**
    This class has been refactored to be a pure RAFT state machine with no 
    application-specific logic. All external interactions happen via callbacks:
    
    - `on_state_change(new_state)`: Called when node state changes
    - `on_commit(entry)`: Called when log entries are committed
    - `on_membership_change(nodes)`: Called when cluster membership changes
    - `on_send_prevote(...)`: Called to send prevote requests
    - `on_send_vote(...)`: Called to send vote requests  
    - `on_send_append(...)`: Called to send append entries
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
        
        # Lock for thread safety
        self.state_lock = threading.RLock()
        
        # Election timeout (randomized to prevent split votes)
        self.min_election_timeout = getattr(args, 'min_election_timeout', 150) / 1000.0  # default 150ms
        self.max_election_timeout = getattr(args, 'max_election_timeout', 300) / 1000.0  # default 300ms
        self.reset_election_timeout()
        
        # Heartbeat interval (for leaders)
        self.heartbeat_interval = getattr(args, 'heartbeat_interval', 50) / 1000.0  # default 50ms
        
        # Callbacks for external interactions (to be set by the manager)
        self.on_state_change = None
        self.on_commit = None
        self.on_membership_change = None
        self.on_send_prevote = None
        self.on_send_vote = None
        self.on_send_append = None
        self.on_install_snapshot = None
        self.on_send_snapshot = None
        
        # Track known nodes - start empty for dynamic discovery
        self.known_nodes = set()
        self.current_leader_id = None  # Track current leader ID
        
        # But in dynamic mode, we'll update this through service discovery
        self.total_nodes = 0
        self.majority = 1  # Will be updated when nodes are discovered
        logging.info(f"Node {self.node_id}: Initialized in dynamic cluster mode")
        
        logging.info(
            f"RAFT Node {self.node_id} initialized in {self.state.name} state"
        )
    
    def update_known_nodes(self, node_ids):
        """Update the known nodes set and notify components."""
        try:
            old_known_nodes = set(self.known_nodes)
            new_known_nodes = set(node_ids)
            self.total_nodes = len(new_known_nodes)
            
            logging.debug(f"Node {self.node_id}: update_known_nodes called with {node_ids}")
            logging.debug(f"Node {self.node_id}: old_known_nodes={old_known_nodes}, new_known_nodes={new_known_nodes}")
            
            # Update the known nodes
            self.known_nodes = new_known_nodes
            
            # Update majority calculation
            self.majority = len(self.known_nodes) // 2 + 1
            
            # Log the change
            added_nodes = new_known_nodes - old_known_nodes
            removed_nodes = old_known_nodes - new_known_nodes
            
            if added_nodes or removed_nodes:
                logging.info(f"Node {self.node_id}: Known nodes updated, total={len(self.known_nodes)}, "
                            f"majority={self.majority}, nodes={sorted(list(self.known_nodes))}")
                
                if added_nodes:
                    logging.info(f"Node {self.node_id}: Added nodes: {added_nodes}")
                if removed_nodes:
                    logging.info(f"Node {self.node_id}: Removed nodes: {removed_nodes}")
            
            # If this is the leader, initialize state for new nodes
            if self.state == RaftState.LEADER:
                for node_id in added_nodes:
                    if node_id != self.node_id:
                        self.next_index[node_id] = self.first_log_index + len(self.log)
                        self.match_index[node_id] = 0
                        logging.debug(f"Node {self.node_id}: Initialized leader state for new node {node_id}")
            
            # Notify callback if set
            if callable(self.on_membership_change):
                self.on_membership_change(new_known_nodes)
                
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error updating known nodes: {e}")
    
    def reset_election_timeout(self):
        """Reset the election timeout with a random value and clear prevote state."""
        with self.state_lock:
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
                if callable(self.on_send_prevote):
                    self.on_send_prevote(
                        candidate_id=self.node_id,
                        term=self.prevote_term,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term
                    )
                
                # Check if we already have majority prevotes (for single-node cluster)
                if len(self.prevotes_received) >= self.majority:
                    logging.info(f"Node {self.node_id}: Already have majority prevotes ({len(self.prevotes_received)}/{self.majority}), proceeding to election")
                    # Proceed directly to election since we have majority
                    return self._proceed_to_election_from_prevote()
                
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
                if callable(self.on_send_vote):
                    last_log_index, last_log_term = self.get_last_log_info()
                    self.on_send_vote(
                        candidate_id=self.node_id,
                        term=self.current_term,
                        last_log_index=last_log_index,
                        last_log_term=last_log_term
                    )
                
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
                
                # Check if vote can be granted
                vote_granted = False
                if (term == self.current_term and 
                    (self.voted_for is None or self.voted_for == candidate_id) and
                    self.is_log_up_to_date(last_log_index, last_log_term)):
                    
                    # Grant vote
                    self.voted_for = candidate_id
                    vote_granted = True
                    
                    # Reset election timeout when granting vote
                    self.update_heartbeat()
                    
                    # Clear current leader since we're voting for a new candidate
                    if hasattr(self, 'current_leader_id'):
                        self.current_leader_id = None
                    
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
                last_log_index, _ = self.get_last_log_info()
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
                logging.info(f"Node {self.node_id}: Initialized leader state for nodes: {list(self.known_nodes)}")
                
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
            tuple: (current_term, success) where:
                - current_term: follower's current term for leader to update if needed
                - success: True if append was successful, False otherwise
        """
        with self.state_lock:
            try:
                # Log request
                is_heartbeat = not entries
                log_level = logging.DEBUG if is_heartbeat else logging.INFO
                logging.log(log_level, 
                        f"Node {self.node_id}: Received {'heartbeat' if is_heartbeat else 'AppendEntries'} " +
                        f"from leader {leader_id} for term {term}")
                
                # Nodes in INITIAL state can accept AppendEntries if they're ready
                if self.state == RaftState.INITIAL:
                    if not self._is_ready_for_cluster_participation():
                        logging.debug(f"Node {self.node_id}: Rejecting AppendEntries in INITIAL state - not ready")
                        return self.current_term, False
                    
                    # Ready to accept leadership - transition to follower
                    logging.info(f"Node {self.node_id}: Converting from INITIAL to FOLLOWER on receiving AppendEntries")
                    self.become_follower(term)
                
                # If term < currentTerm, reject
                if term < self.current_term:
                    logging.info(f"Node {self.node_id}: Rejecting AppendEntries with term {term} < current term {self.current_term}")
                    return self.current_term, False
                
                # If term > currentTerm, convert to follower
                if term > self.current_term:
                    logging.info(f"Node {self.node_id}: Converting to follower due to higher term from leader {leader_id}")
                    self.become_follower(term)
                
                # Track current leader for client redirects
                if hasattr(self, 'current_leader_id') and self.current_leader_id != leader_id:
                    self.current_leader_id = leader_id
                    logging.debug(f"Node {self.node_id}: Updated current leader to {leader_id}")
                elif not hasattr(self, 'current_leader_id'):
                    self.current_leader_id = leader_id
                    logging.debug(f"Node {self.node_id}: Set current leader to {leader_id}")
                
                # This is a valid AppendEntries from current leader, reset election timeout
                self.update_heartbeat()
                
                # If we were a candidate, step down
                if self.state == RaftState.CANDIDATE:
                    logging.info(f"Node {self.node_id}: Stepping down from candidate due to valid leader for term {term}")
                    self.become_follower(term)
                
                # --- Step 2: Log consistency check ---
                
                # Check if prev_log_index is in our snapshot
                if prev_log_index > 0 and prev_log_index <= self.last_snapshot_index:
                    # Check if the term matches our snapshot
                    if prev_log_index == self.last_snapshot_index and prev_log_term == self.last_snapshot_term:
                        log_ok = True
                        reason = "prev_log_index matches snapshot"
                    else:
                        log_ok = False
                        reason = f"prev_log_index ({prev_log_index}) in snapshot but term mismatch"
                else:
                    # Standard log consistency check
                    array_idx = prev_log_index - self.first_log_index
                    
                    if prev_log_index == 0:
                        log_ok = True
                        reason = "starting from beginning of log"
                    elif array_idx >= len(self.log) or array_idx < 0:
                        log_ok = False
                        reason = f"prev_log_index ({prev_log_index}) outside log range"
                        logging.info(f"Node {self.node_id}: Log consistency check failed: {reason}")
                        return self.current_term, False
                    elif self.log[array_idx]['term'] != prev_log_term:
                        log_ok = False
                        reason = f"term mismatch at prev_log_index {prev_log_index}: expected term {prev_log_term}, got {self.log[array_idx]['term']}"
                        logging.info(f"Node {self.node_id}: Log consistency check failed: {reason}")
                        return self.current_term, False
                    else:
                        log_ok = True
                        reason = "log consistency check passed"
                
                logging.log(logging.INFO if not log_ok else logging.DEBUG, 
                        f"Node {self.node_id}: Log consistency check: {log_ok}, {reason}")
                
                if not log_ok:
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
                    # Fix: Use proper index calculation for commit index
                    max_log_index = self.first_log_index + len(self.log) - 1 if len(self.log) > 0 else self.last_snapshot_index
                    self.commit_index = min(leader_commit, max_log_index)
                    
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
        Apply a log entry to the state machine using the callback approach.

        Args:
            entry (dict): Log entry to apply, of the form:
                {
                'term': …,
                'index': …,
                'command': {
                    'type': …,
                    …  # payload fields
                }
                }
        """
        try:
            # Extract the payload where we actually put 'type', 'callback', etc.
            payload = entry.get('command', {})
            entry_type = payload.get('type', 'command')
            idx = entry.get('index', 'unknown')

            if entry_type == 'no-op':
                logging.debug(f"Node {self.node_id}: Applied no-op entry at index {idx}")
                return

            if entry_type == 'read-barrier':
                callback = payload.get('callback')
                if callable(callback):
                    callback()
                logging.debug(f"Node {self.node_id}: Applied read barrier at index {idx}")
                return

            if entry_type == 'membership':
                action = payload.get('action', 'unknown')
                node_id = payload.get('node_id', 'unknown')
                logging.info(f"Node {self.node_id}: Applied membership change: {action} node {node_id}")
                # fall through to on_commit so external components can update cluster state

            # Delegate everything else (including membership) to on_commit
            if callable(self.on_commit):
                logging.debug(f"Node {self.node_id}: Applying log entry at index {idx} with type {entry_type}")
                self.on_commit(entry)
            else:
                logging.warning(
                    f"Node {self.node_id}: No on_commit handler set for log entry at index {idx}"
                )

        except Exception as e:
            logging.error(
                f"Node {self.node_id}: Error applying log entry at index {entry.get('index')}: {e}",
                exc_info=True
            )

    
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
                return self.last_snapshot_index, self.last_snapshot_term

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
    
    def add_node(self, new_node_info, round_num=0):
        """
        Add a new node to the cluster with connection information.
        
        Args:
            new_node_info: Can be either:
                - int: Just the node ID (for backward compatibility)
                - dict: Dictionary containing node_id, ip_address, port, etc.
            round_num (int): Current training round number
                
        Returns:
            int: Log index of the membership entry, or -1 on failure
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    logging.warning(f"Node {self.node_id}: Cannot add node - not a leader")
                    return -1
                
                # if node already known or already pending, skip
                node_id, _ = self._extract_node_info(new_node_info)
                if node_id in self.known_nodes or (hasattr(self, 'pending_node_connection_info') and node_id in self.pending_node_connection_info):
                    logging.debug(f"Leader {self.node_id}: Node {node_id} already known or pending, skipping add")
                    return -1
                
                # Extract and validate node information
                node_id, connection_info = self._extract_node_info(new_node_info)
                if node_id is None:
                    return -1
                
                # Check if node already exists
                if node_id in self.known_nodes:
                    logging.debug(f"Leader {self.node_id}: Node {node_id} already in cluster")
                    return -1
                
                # Create membership log entry with complete connection info
                # DO NOT update known_nodes immediately - wait for commit
                log_entry = self._create_membership_log_entry(
                    action='add',
                    node_id=node_id,
                    connection_info=connection_info,
                    round_num=round_num
                )
                
                log_index = self.add_log_entry(log_entry)
                
                if log_index == -1:
                    logging.error(f"Leader {self.node_id}: Failed to add log entry for node addition")
                    return -1
                
                # Store connection info for immediate leader use (replication)
                # but don't update known_nodes until commit
                if not hasattr(self, 'pending_node_connection_info'):
                    self.pending_node_connection_info = {}
                self.pending_node_connection_info[node_id] = connection_info
                
                # Initialize leader state for the new node (for replication)
                last_log_index = self.first_log_index + len(self.log) - 1 if len(self.log) > 0 else 0
                self.next_index[node_id] = last_log_index + 1
                self.match_index[node_id] = 0
                
                logging.info(f"Leader {self.node_id}: Proposed adding node {node_id} with connection info " +
                            f"{connection_info['ip_address']}:{connection_info['port']} at round {round_num}")
                
                return log_index
                
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error adding node: {e}", exc_info=True)
                return -1
    
    def remove_node(self, node_id, round_num=0, reason="unspecified"):
        """
        Remove a node from the cluster (leaders only).
        
        Args:
            node_id (int): ID of the node to remove
            round_num (int, optional): Current training round number
            reason (str, optional): Reason for node removal
            
        Returns:
            int: Log index of the membership entry, or -1 on failure
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    logging.warning(f"Node {self.node_id}: Cannot remove node {node_id} - not a leader")
                    return -1
                    
                if node_id not in self.known_nodes:
                    logging.debug(f"Leader {self.node_id}: Node {node_id} not in cluster, no action needed")
                    return -1
                
                log_entry = self._create_membership_log_entry(
                    action='remove',
                    node_id=node_id,
                    connection_info=None,
                    round_num=round_num,
                    reason=reason
                )
                
                log_index = self.add_log_entry(log_entry)
                
                if log_index == -1:
                    logging.error(f"Leader {self.node_id}: Failed to add log entry for node removal")
                    return -1
                
                logging.info(f"Leader {self.node_id}: Proposed removing node {node_id} from cluster at round {round_num} (reason: {reason})")
                
                return log_index
                
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error removing node {node_id}: {e}", exc_info=True)
                return -1
    
    def _extract_node_info(self, new_node_info):
        """
        Extract node ID and connection info from input parameter.
        
        Args:
            new_node_info: Either int (node ID) or dict (node info)
            
        Returns:
            tuple: (node_id, connection_info_dict) or (None, None) on error
        """
        try:
            if isinstance(new_node_info, dict):
                node_id = new_node_info.get('node_id')
                if node_id is None:
                    logging.error(f"Leader {self.node_id}: No node_id found in node info")
                    return None, None
                
                # Standardized connection info structure
                connection_info = {
                    'ip_address': new_node_info.get('ip_address', 'localhost'),
                    'port': new_node_info.get('port', 9000 + node_id),
                    'capabilities': new_node_info.get('capabilities', ['grpc', 'fedml']),
                    'timestamp': new_node_info.get('timestamp', time.time())
                }
            else:
                # Backward compatibility - just node ID
                node_id = new_node_info
                connection_info = {
                    'ip_address': 'localhost',
                    'port': 9000 + node_id,
                    'capabilities': ['grpc', 'fedml'],
                    'timestamp': time.time()
                }
            
            return node_id, connection_info
            
        except Exception as e:
            logging.error(f"Leader {self.node_id}: Error extracting node info: {e}")
            return None, None
    
    def _create_membership_log_entry(self, action, node_id, connection_info, round_num, reason=None):
        """
        Create a standardized membership log entry.
        
        Args:
            action (str): 'add' or 'remove'
            node_id (int): Node ID
            connection_info (dict): Connection information (None for remove)
            round_num (int): Training round number
            reason (str, optional): Reason for change
            
        Returns:
            dict: Log entry command
        """
        try:
            # Create base log entry
            log_entry = {
                'type': 'membership',
                'action': action,
                'node_id': node_id,
                'round': round_num,
                'timestamp': time.time()
            }
            
            # Add connection info for add operations
            if action == 'add' and connection_info:
                log_entry['node_info'] = connection_info
            
            # Add reason for remove operations
            if action == 'remove' and reason:
                log_entry['reason'] = reason
            
            # Add current cluster state for consistency
            log_entry['current_nodes'] = list(self.known_nodes)
            
            # Add connection info for all current nodes
            if hasattr(self, 'node_connection_info'):
                log_entry['current_nodes_info'] = dict(self.node_connection_info)
            
            return log_entry
            
        except Exception as e:
            logging.error(f"Leader {self.node_id}: Error creating membership log entry: {e}")
            return {}
    
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
        
    # These methods have been removed as they are application-specific.
    # The application layer should handle topology and bandwidth updates directly.
    
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

    def _proceed_to_election_from_prevote(self):
        """
        Proceed to actual election after getting majority prevotes.
        
        This method transitions from PREVOTE to CANDIDATE state and starts
        the actual election process.
        
        Returns:
            bool: True if successfully transitioned to election, False otherwise
        """
        try:
            # We should be in PREVOTE state with majority prevotes
            if self.state != RaftState.PREVOTE:
                logging.error(f"Node {self.node_id}: Cannot proceed to election from {self.state} state")
                return False
            
            if len(self.prevotes_received) < self.majority:
                logging.error(f"Node {self.node_id}: Insufficient prevotes {len(self.prevotes_received)}/{self.majority}")
                return False
            
            # Transition to CANDIDATE state
            old_term = self.current_term
            self.current_term = self.prevote_term  # Use the term we got prevotes for
            self.voted_for = self.node_id
            self.state = RaftState.CANDIDATE
            
            # Reset votes for actual election - vote for ourselves
            self.votes_received = {self.node_id}
            
            # Clear prevote state AFTER setting up candidate state
            self.prevote_requested = False
            self.prevotes_received.clear()
            self.prevote_term = 0
            
            # Notify state change
            if hasattr(self, 'on_state_change') and callable(self.on_state_change):
                self.on_state_change(RaftState.CANDIDATE)
            
            logging.info(f"Node {self.node_id}: Transitioned to CANDIDATE for term {self.current_term} " +
                        f"(previous term: {old_term})")
            
            # For single-node cluster, immediately become leader
            if len(self.known_nodes) == 1 and self.node_id in self.known_nodes:
                logging.info(f"Node {self.node_id}: Single-node cluster detected, becoming leader immediately")
                return self.become_leader()
            
            # For multi-node cluster, send vote requests
            last_log_index, last_log_term = self.get_last_log_info()
            
            # Start election timeout for this election
            self.reset_election_timeout()
            
            # Send vote requests to other nodes
            if callable(self.on_send_vote):
                self.on_send_vote(
                    candidate_id=self.node_id,
                    term=self.current_term,
                    last_log_index=last_log_index,
                    last_log_term=last_log_term
                )
            
            return True
      
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error proceeding to election from prevote: {e}")
            # Reset to FOLLOWER state on error
            self.state = RaftState.FOLLOWER
            self.prevote_requested = False
            self.prevotes_received.clear()
            self.prevote_term = 0
            return False

    def install_snapshot(self, leader_id, term, last_included_index, last_included_term, snapshot_data):
        """
        Install a snapshot from the leader.
        
        This method handles the InstallSnapshot RPC according to the RAFT paper.
        
        Args:
            leader_id (int): ID of the leader sending the snapshot
            term (int): Leader's current term
            last_included_index (int): Index of last entry in snapshot
            last_included_term (int): Term of last entry in snapshot
            snapshot_data (bytes): Snapshot data
            
        Returns:
            tuple: (current_term, success)
        """
        with self.state_lock:
            try:
                logging.info(f"Node {self.node_id}: Received snapshot from leader {leader_id} " +
                            f"for term {term} (last_included: {last_included_index})")
                
                # If term < currentTerm, reject
                if term < self.current_term:
                    logging.info(f"Node {self.node_id}: Rejecting snapshot with term {term} < current term {self.current_term}")
                    return self.current_term, False
                
                # If term > currentTerm, convert to follower
                if term > self.current_term:
                    logging.info(f"Node {self.node_id}: Converting to follower due to higher term from leader {leader_id}")
                    self.become_follower(term)
                
                # Update heartbeat to prevent election timeout
                self.update_heartbeat()
                
                # If we already have this snapshot or a more recent one, ignore
                if last_included_index <= self.last_snapshot_index:
                    logging.debug(f"Node {self.node_id}: Snapshot already installed or more recent")
                    return self.current_term, True
                
                # Save snapshot metadata
                self.last_snapshot_index = last_included_index
                self.last_snapshot_term = last_included_term
                
                # Discard log entries covered by snapshot
                # Keep entries after the snapshot if they exist
                if len(self.log) > 0:
                    # Find entries that come after the snapshot
                    snapshot_end_array_idx = last_included_index - self.first_log_index + 1
                    if snapshot_end_array_idx < len(self.log):
                        # Keep entries after the snapshot
                        self.log = self.log[snapshot_end_array_idx:]
                        self.first_log_index = last_included_index + 1
                    else:
                        # Snapshot covers all our log entries
                        self.log = []
                        self.first_log_index = last_included_index + 1
                else:
                    # No log entries, just update first_log_index
                    self.first_log_index = last_included_index + 1
                
                # Update commit and apply indices
                if last_included_index > self.commit_index:
                    self.commit_index = last_included_index
                if last_included_index > self.last_applied:
                    self.last_applied = last_included_index
                
                # Apply snapshot to state machine via callback
                if hasattr(self, 'on_install_snapshot') and callable(self.on_install_snapshot):
                    self.on_install_snapshot(snapshot_data, last_included_index, last_included_term)
                
                logging.info(f"Node {self.node_id}: Installed snapshot up to index {last_included_index}")
                
                return self.current_term, True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error installing snapshot: {e}", exc_info=True)
                return self.current_term, False
            
    def _is_ready_for_cluster_participation(self):
        """
        Check if this node is ready to participate in the cluster.
        
        Returns:
            bool: True if ready, False otherwise
        """
        with self.state_lock:
            try:
                # Must have cluster information
                if not hasattr(self, 'known_nodes') or len(self.known_nodes) == 0:
                    logging.debug(f"Node {self.node_id}: Not ready - no known nodes")
                    return False
                
                # Bootstrap nodes are always ready
                if hasattr(self, 'is_bootstrap_node') and self.is_bootstrap_node:
                    logging.debug(f"Node {self.node_id}: Ready - bootstrap node")
                    return True
                
                # Check if discovered through service discovery
                if hasattr(self, 'discovered_through_service_discovery') and self.discovered_through_service_discovery:
                    logging.debug(f"Node {self.node_id}: Ready - discovered through service discovery")
                    return True
                
                # For regular nodes, check if we have sufficient state
                # (This is where FL-specific checks would go in the application layer)
                return True
                
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error checking cluster readiness: {e}")
                return False
    
    def read_index_barrier(self, callback=None):
        """
        Implement read-index barrier for linearizable reads.
        
        This method ensures that reads see all writes that were committed before
        the read was initiated, according to the RAFT paper section 8.
        
        Args:
            callback: Optional callback to invoke when barrier is committed
            
        Returns:
            int: Log index of the barrier entry, or -1 if not leader
        """
        with self.state_lock:
            try:
                if self.state != RaftState.LEADER:
                    logging.debug(f"Node {self.node_id}: Cannot create read barrier - not leader")
                    return -1
                
                # Create a read barrier entry
                barrier_entry = {
                    'type': 'read-barrier',
                    'timestamp': self.get_current_timestamp(),
                    'callback': callback  # Store callback for when this gets committed
                }
                
                log_index = self.add_log_entry(barrier_entry)
                
                if log_index != -1:
                    logging.debug(f"Leader {self.node_id}: Created read barrier at index {log_index}")
                
                return log_index
                
            except Exception as e:
                logging.error(f"Leader {self.node_id}: Error creating read barrier: {e}")
                return -1
    
    def send_heartbeats(self):
        """Send heartbeat messages to all followers via callbacks."""
        with self.state_lock:
            if self.state != RaftState.LEADER:
                return
            
            for node_id in self.known_nodes:
                if node_id == self.node_id:
                    continue  # Skip self
                
                try:
                    next_index = self.next_index.get(
                        node_id, self.first_log_index + len(self.log)
                    )
                    
                    if next_index <= self.last_snapshot_index:
                        # Follower is too far behind, send snapshot
                        if callable(self.on_send_snapshot):
                            self.on_send_snapshot(
                                node_id, 
                                self.current_term, 
                                self.node_id, 
                                self.last_snapshot_index, 
                                self.last_snapshot_term, 
                                self.log  # snapshot data
                            )
                        continue
                    
                    prev_log_index = next_index - 1
                    prev_log_term = self.get_term_at_index(prev_log_index)
                    
                    # Send heartbeat (empty append entries)
                    if callable(self.on_send_append):
                        self.on_send_append(
                            node_id,
                            self.current_term,
                            self.node_id,
                            prev_log_index,
                            prev_log_term,
                            [],  # Empty entries for heartbeat
                            self.commit_index
                        )
                        
                except Exception as e:
                    logging.error(f"Node {self.node_id}: Error sending heartbeat to {node_id}: {e}")
    
    def replicate_logs(self):
        """Replicate log entries to followers via callbacks."""
        with self.state_lock:
            if self.state != RaftState.LEADER:
                return
            
            for node_id in self.known_nodes:
                if node_id == self.node_id:
                    continue  # Skip self
                
                try:
                    next_index = self.next_index.get(
                        node_id, self.first_log_index + len(self.log)
                    )
                    
                    if next_index <= self.last_snapshot_index:
                        # Follower is too far behind, send snapshot
                        if callable(self.on_send_snapshot):
                            self.on_send_snapshot(
                                node_id, 
                                self.current_term, 
                                self.node_id, 
                                self.last_snapshot_index, 
                                self.last_snapshot_term, 
                                self.log  # snapshot data
                            )
                        continue
                    
                    array_idx = next_index - self.first_log_index
                    if array_idx < len(self.log):
                        # There are entries to replicate
                        entries_to_send = self.log[array_idx:]
                        
                        prev_log_index = next_index - 1
                        prev_log_term = self.get_term_at_index(prev_log_index)
                        
                        # Send append entries with new log entries
                        if callable(self.on_send_append):
                            self.on_send_append(
                                node_id,
                                self.current_term,
                                self.node_id,
                                prev_log_index,
                                prev_log_term,
                                entries_to_send,
                                self.commit_index
                            )
                    
                except Exception as e:
                    logging.error(f"Node {self.node_id}: Error replicating logs to {node_id}: {e}")
    
    def request_votes_from_all(self):
        """Request votes from all known nodes via callbacks."""
        with self.state_lock:
            if self.state != RaftState.CANDIDATE:
                return
            
            last_log_index, last_log_term = self.get_last_log_info()
            
            for node_id in self.known_nodes:
                if node_id == self.node_id:
                    continue  # Skip self
                
                try:
                    # Send vote request via callback
                    if callable(self.on_send_vote):
                        self.on_send_vote(
                            node_id,
                            self.current_term,
                            self.node_id,
                            last_log_index,
                            last_log_term
                        )
                        
                except Exception as e:
                    logging.error(f"Node {self.node_id}: Error requesting vote from {node_id}: {e}")
    
    def should_replicate_logs(self):
        """Check if there are logs to replicate."""
        with self.state_lock:
            if self.state != RaftState.LEADER:
                return False
            
            # Check if any follower's next_index is behind our log
            for node_id in self.known_nodes:
                if node_id == self.node_id:
                    continue
                
                next_index = self.next_index.get(
                    node_id, self.first_log_index + len(self.log)
                )
                
                if next_index < self.first_log_index + len(self.log):
                    return True
            
            return False