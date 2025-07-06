import logging
import threading
import time
import traceback
from enum import Enum

try:
    from .raft_node import RaftState
    from .discovery_hints import DiscoveryHintSender, RaftState as HintRaftState
except ImportError:
    # Handle case when running tests directly
    from raft_node import RaftState
    from discovery_hints import DiscoveryHintSender, RaftState as HintRaftState


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

        # Service discovery bridge (will be set later)
        self.service_discovery_bridge = None
        
        # Set callback for state changes
        self.raft_node.on_state_change = self.handle_state_change
        
        # Wire up all send callbacks
        self.raft_node.on_send_prevote = self._send_prevote_callback
        self.raft_node.on_send_vote = self._send_vote_callback
        self.raft_node.on_send_append = self._send_append_callback
        self.raft_node.on_send_snapshot = self._send_snapshot_callback
        self.raft_node.on_commit = self._commit_callback
        self.raft_node.on_membership_change = self._membership_change_callback
        
        self.election_timer_thread = None
        self.heartbeat_thread = None
        self.log_replication_thread = None
        
        # Thread stop events
        self.election_timer_stop_event = threading.Event()
        self.heartbeat_stop_event = threading.Event()
        self.log_replication_stop_event = threading.Event()
        
        # Lock to protect thread operations and state changes
        self.thread_lock = threading.RLock()
        
        # Callback for leadership changes
        self.on_leadership_change = None
        
        # Thread-safe replication interval
        self.replication_interval = getattr(args, 'replication_interval', 0.1)  # default 100ms
        
        # Thread state tracking
        self.is_running = False
        
        # Interval between log replication attempts
        self.replication_interval = getattr(args, "replication_interval", self.raft_node.heartbeat_interval)
        
        # Initialize discovery hint sender if discovery service is configured
        # DISABLED: HTTP-based hints don't work with gRPC service discovery
        self.discovery_hint_sender = None
        discovery_host = getattr(args, "discovery_host", None)
        discovery_port = getattr(args, "discovery_port", None)
        
        # Temporarily disable HTTP-based discovery hints since we're using gRPC
        if False and discovery_host and discovery_port:
            self.discovery_hint_sender = DiscoveryHintSender(
                discovery_host=discovery_host,
                discovery_port=discovery_port,
                node_id=raft_node.node_id,
                send_interval=30.0  # Send hints every 30 seconds
            )
            logging.info(f"Node {raft_node.node_id}: Discovery hint sender initialized for {discovery_host}:{discovery_port}")
        else:
            logging.info(f"Node {raft_node.node_id}: Discovery hints disabled (using gRPC service discovery)")
        
        logging.info(f"RAFT Consensus initialized for node {raft_node.node_id}")
    
    def start(self):
        """Start the consensus operations following RAFT paper specification."""
        with self.thread_lock:
            if self.is_running:
                logging.warning(f"Node {self.raft_node.node_id}: Consensus already running")
                return
            
            self.is_running = True
            
            # Start election timer thread only if not in INITIAL state
            # INITIAL state nodes should wait for join approval before participating in elections
            if self.raft_node.state != RaftState.INITIAL:
                self._start_election_timer_thread()
            else:
                logging.info(f"Node {self.raft_node.node_id}: Skipping election timer start - node in INITIAL state")
            
            # Start discovery hint sender if configured
            if self.discovery_hint_sender:
                self.discovery_hint_sender.start()
                logging.debug(f"Node {self.raft_node.node_id}: Started discovery hint sender")
            
            # If we're already a leader, start leader threads
            if self.raft_node.state == RaftState.LEADER:
                self._start_leader_threads()
            
            logging.info(f"Node {self.raft_node.node_id}: RAFT consensus operations started")
    
    def start_election_timer_for_initial_node(self):
        """Start election timer for nodes transitioning out of INITIAL state."""
        with self.thread_lock:
            if self.is_running and self.raft_node.state != RaftState.INITIAL:
                if not self.election_timer_thread or not self.election_timer_thread.is_alive():
                    self._start_election_timer_thread()
                    logging.info(f"Node {self.raft_node.node_id}: Started election timer after leaving INITIAL state")
    
    def stop(self):
        """Stop all consensus operations."""
        logging.info(f"Node {self.raft_node.node_id}: Stopping RAFT consensus operations")
        
        with self.thread_lock:
            if not self.is_running:
                return
            
            self.is_running = False
            
            # Stop all threads
            self._stop_all_threads()
            
            # Stop discovery hint sender if configured
            if self.discovery_hint_sender:
                self.discovery_hint_sender.stop()
                logging.debug(f"Node {self.raft_node.node_id}: Stopped discovery hint sender")
            
            logging.info(f"Node {self.raft_node.node_id}: RAFT consensus operations stopped")
    
    def handle_state_change(self, new_state):
        """
        Handle RAFT node state changes.
        
        Based on RAFT paper: Leaders start heartbeat/replication threads,
        followers/candidates only run election timer.
        
        Args:
            new_state (RaftState): The new state of the node
        """
        logging.info(f"Node {self.raft_node.node_id}: State changed to {new_state}")
        
        with self.thread_lock:
            if new_state == RaftState.LEADER:
                # Start leader-specific threads
                self._start_leader_threads()
                
                # ——————————————————————————————
                # CRITICAL: Immediately propose coordinator entry for this term
                # This ensures all followers know who the coordinator is
                try:
                    current_round = self._get_current_round()
                    idx = self.add_coordinator_update(self.raft_node.node_id, current_round)
                    logging.info(f"Leader {self.raft_node.node_id}: Proposed coordinator entry at index {idx} for round {current_round}")
                except Exception as e:
                    logging.error(f"Leader {self.raft_node.node_id}: Error proposing coordinator update: {e}")
                
                # Update discovery hint sender when becoming leader
                if self.discovery_hint_sender:
                    self.discovery_hint_sender.update_raft_state(
                        new_state=HintRaftState.LEADER,
                        term=self.raft_node.current_term,
                        known_nodes=self.raft_node.known_nodes
                    )
                    logging.debug(f"Node {self.raft_node.node_id}: Updated discovery hint sender for leader state")
                
                # Notify leadership change
                if self.on_leadership_change:
                    try:
                        self.on_leadership_change(self.raft_node.node_id)
                    except Exception as e:
                        logging.error(f"Node {self.raft_node.node_id}: Error in leadership change callback: {e}")
            
            elif new_state in [RaftState.FOLLOWER, RaftState.CANDIDATE]:
                # Stop leader-specific threads
                self._stop_leader_threads()
                
                # Start election timer if not already running and this node was previously in INITIAL state
                if not hasattr(self, 'election_timer_thread') or not self.election_timer_thread.is_alive():
                    logging.info(f"Node {self.raft_node.node_id}: Starting election timer after transitioning from INITIAL state")
                    self._start_election_timer_thread()
                
                # Update discovery hint sender when becoming follower/candidate
                if self.discovery_hint_sender:
                    hint_state = HintRaftState.FOLLOWER if new_state == RaftState.FOLLOWER else HintRaftState.CANDIDATE
                    self.discovery_hint_sender.update_raft_state(
                        new_state=hint_state,
                        term=self.raft_node.current_term,
                        known_nodes=self.raft_node.known_nodes
                    )
                    logging.debug(f"Node {self.raft_node.node_id}: Updated discovery hint sender for {hint_state} state")
                
                # Note: Leader tracking is now handled by RaftNode.get_leader_id()
                
            elif new_state == RaftState.INITIAL:
                # Stop all threads when going back to INITIAL state
                self._stop_leader_threads()
                self._stop_election_timer_thread()
                logging.info(f"Node {self.raft_node.node_id}: Stopped election timer - returning to INITIAL state")
    
    def _start_election_timer_thread(self):
        """Start the election timer thread (always runs for followers/candidates)."""
        if self.election_timer_thread and self.election_timer_thread.is_alive():
            return
        
        self.election_timer_stop_event.clear()
        self.election_timer_thread = threading.Thread(
            target=self._election_timer_thread_func,
            name=f"raft-election-timer-{self.raft_node.node_id}",
            daemon=False
        )
        self.election_timer_thread.start()
        logging.debug(f"Node {self.raft_node.node_id}: Started election timer thread")
    
    def _start_leader_threads(self):
        """Start leader-specific threads (heartbeat and log replication)."""
        if self.raft_node.state != RaftState.LEADER:
            return
        
        # Start heartbeat thread - more robust checking
        if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
            # Ensure old thread is cleaned up
            if self.heartbeat_thread is not None:
                self.heartbeat_stop_event.set()
                self.heartbeat_thread.join(timeout=0.5)
            
            self.heartbeat_stop_event.clear()
            self.heartbeat_thread = threading.Thread(
                target=self._heartbeat_thread_func,
                name=f"raft-heartbeat-{self.raft_node.node_id}",
                daemon=False
            )
            self.heartbeat_thread.start()
            logging.debug(f"Node {self.raft_node.node_id}: Started heartbeat thread")
        
        # Start log replication thread - more robust checking  
        if self.log_replication_thread is None or not self.log_replication_thread.is_alive():
            # Ensure old thread is cleaned up
            if self.log_replication_thread is not None:
                self.log_replication_stop_event.set()
                self.log_replication_thread.join(timeout=0.5)
            
            self.log_replication_stop_event.clear()
            self.log_replication_thread = threading.Thread(
                target=self._log_replication_thread_func,
                name=f"raft-log-replication-{self.raft_node.node_id}",
                daemon=False
            )
            self.log_replication_thread.start()
            logging.debug(f"Node {self.raft_node.node_id}: Started log replication thread")
    
    def _stop_leader_threads(self):
        """Stop leader-specific threads."""
        # Stop heartbeat thread
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_stop_event.set()
            self.heartbeat_thread.join(timeout=2.0)  # Increased timeout
            if self.heartbeat_thread.is_alive():
                logging.error(f"Node {self.raft_node.node_id}: Heartbeat thread did not stop gracefully - forcing termination")
                # Note: Python doesn't have clean thread termination, so we log the issue
            self.heartbeat_thread = None
        
        # Stop log replication thread
        if self.log_replication_thread and self.log_replication_thread.is_alive():
            self.log_replication_stop_event.set()
            self.log_replication_thread.join(timeout=2.0)  # Increased timeout
            if self.log_replication_thread.is_alive():
                logging.error(f"Node {self.raft_node.node_id}: Log replication thread did not stop gracefully - forcing termination")
                # Note: Python doesn't have clean thread termination, so we log the issue
            self.log_replication_thread = None
    
    def _stop_all_threads(self):
        """Stop all RAFT threads."""
        # Stop leader threads first
        self._stop_leader_threads()
        
        # Stop election timer thread
        if self.election_timer_thread and self.election_timer_thread.is_alive():
            self.election_timer_stop_event.set()
            self.election_timer_thread.join(timeout=1.0)
            if self.election_timer_thread.is_alive():
                logging.warning(f"Node {self.raft_node.node_id}: Election timer thread did not stop gracefully")
            self.election_timer_thread = None
    
    def _election_timer_thread_func(self):
        """
        Election timer thread function (RAFT Algorithm requirement).
        
        This thread continuously monitors election timeouts for followers and candidates.
        It's the core of RAFT's leader election mechanism.
        """
        logging.info(f"Node {self.raft_node.node_id}: Election timer thread started")
        
        try:
            while not self.election_timer_stop_event.is_set():
                # Leaders don't need election timeouts
                if self.raft_node.state == RaftState.LEADER:
                    time.sleep(0.1)
                    continue
                
                # Check for election timeout (critical RAFT timing)
                if self.raft_node.is_election_timeout():
                    logging.info(
                        f"Node {self.raft_node.node_id}: Election timeout detected"
                    )

                    try:
                        # Start election - RaftNode will handle vote requests via callbacks
                        self.raft_node.start_election()
                    except Exception as e:
                        logging.error(
                            f"Node {self.raft_node.node_id}: Error starting election: {e}"
                        )
                
                # Short sleep to prevent busy waiting (10ms as per RAFT recommendations)
                time.sleep(0.01)
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Fatal error in election timer thread: {e}")
            logging.error(f"Node {self.raft_node.node_id}: Thread traceback: {traceback.format_exc()}")
        finally:
            logging.info(f"Node {self.raft_node.node_id}: Election timer thread finished")
    
    def _heartbeat_thread_func(self):
        """
        Heartbeat thread function (RAFT Algorithm requirement for leaders).
        
        Leaders must send periodic heartbeats to prevent followers from timing out
        and starting unnecessary elections.
        """
        logging.info(f"Node {self.raft_node.node_id}: Heartbeat thread started")
        
        try:
            while not self.heartbeat_stop_event.is_set():
                # Only leaders send heartbeats
                if self.raft_node.state != RaftState.LEADER:
                    break
                
                try:
                    # Send heartbeats periodically 
                    self.raft_node.send_heartbeats()
                except Exception as e:
                    logging.error(f"Node {self.raft_node.node_id}: Error sending heartbeats: {e}")
                
                # Sleep for heartbeat interval (as specified in RAFT paper)
                # heartbeat_interval is already in seconds
                time.sleep(self.raft_node.heartbeat_interval)
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Fatal error in heartbeat thread: {e}")
            logging.error(f"Node {self.raft_node.node_id}: Thread traceback: {traceback.format_exc()}")
        finally:
            logging.info(f"Node {self.raft_node.node_id}: Heartbeat thread finished")
    
    def _log_replication_thread_func(self):
        """
        Log replication thread function (RAFT Algorithm requirement for leaders).
        
        Leaders must continuously try to replicate log entries to followers
        and update commit indices based on successful replications.
        """
        logging.info(f"Node {self.raft_node.node_id}: Log replication thread started")
        
        try:
            while not self.log_replication_stop_event.is_set():
                # Only leaders replicate logs
                if self.raft_node.state != RaftState.LEADER:
                    break
                
                try:
                    # Check if there are logs to replicate and replicate if needed
                    if self.raft_node.should_replicate_logs():
                        self.raft_node.replicate_logs()
                    
                    # Update commit index based on successful replications
                    self.raft_node.update_commit_index()
                    
                except Exception as e:
                    logging.error(f"Node {self.raft_node.node_id}: Error in log replication: {e}")
                
                # Sleep for configured replication interval
                time.sleep(self.replication_interval)
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Fatal error in log replication thread: {e}")
            logging.error(f"Node {self.raft_node.node_id}: Thread traceback: {traceback.format_exc()}")
        finally:
            logging.info(f"Node {self.raft_node.node_id}: Log replication thread finished")
    
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

    def handle_prevote_request(self, candidate_id, term, last_log_index, last_log_term):
        """Handle a PreVote request from a potential candidate."""
        current_term, prevote_granted = self.raft_node.receive_prevote_request(
            candidate_id, term, last_log_index, last_log_term
        )

        self.worker_manager.send_prevote_response(
            candidate_id, current_term, prevote_granted
        )
    
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
            
            # Start leader threads (handled by state change callback)
            # The handle_state_change method will start the appropriate threads
            
            # Notify leadership change
            if self.on_leadership_change:
                self.on_leadership_change(self.raft_node.node_id)

    def handle_prevote_response(self, voter_id, term, prevote_granted):
        """Handle a PreVote response message."""
        should_start_election = self.raft_node.receive_prevote_response(
            voter_id, term, prevote_granted
        )

        if should_start_election:
            transitioned = self.raft_node.start_election()
            if transitioned and self.raft_node.state == RaftState.CANDIDATE:
                self.request_votes_from_all()
    
    def add_no_op_entry(self):
        """Add a no-op entry to the log to commit previous entries."""
        if self.raft_node.state != RaftState.LEADER:
            return
        
        self.raft_node.add_log_entry({
            'type': 'no-op',
            'timestamp': time.time()
        })
    
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

    def handle_install_snapshot(
        self, leader_id, term, last_incl_idx, last_incl_term, offset, data, done
    ):
        """Process an InstallSnapshot RPC from the leader."""
        try:
            self.raft_node.install_snapshot_chunk(
                last_incl_idx, last_incl_term, offset, data, done
            )
            # Acknowledge snapshot reception using existing snapshot message
            self.worker_manager.send_state_snapshot(
                leader_id,
                self.raft_node.current_term,
                [],
                self.raft_node.commit_index,
            )
        except Exception as e:
            logging.error(
                f"Node {self.raft_node.node_id}: Error handling install snapshot from {leader_id}: {e}"
            )
    
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
    
    def add_membership_change(self, action, node_id, node_info=None, round_num=0, reason="unspecified"):
        """
        Add a membership change to the log (leaders only).
        
        Args:
            action (str): 'add' or 'remove'
            node_id (int): ID of the node to add/remove
            node_info (dict, optional): Connection information for new nodes
            round_num (int, optional): Current training round number
            reason (str, optional): Reason for the membership change
            
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
            # Create a complete membership change entry that includes all current nodes
            # This ensures every follower knows the complete cluster membership
            current_nodes = list(self.raft_node.known_nodes)
            
            # For 'add' actions, prepare complete node_info
            complete_node_info = None
            if action == 'add' and node_info:
                complete_node_info = {
                    'node_id': node_id,
                    'ip_address': node_info.get('ip_address', 'localhost'),
                    'port': node_info.get('port', 9000 + node_id),
                    'capabilities': node_info.get('capabilities', ['grpc', 'fedml']),
                    'timestamp': node_info.get('timestamp', time.time())
                }
            
            # Create the command for the log
            command = {
                'type': 'membership',
                'action': action,
                'node_id': node_id,
                'current_nodes': current_nodes,
                'timestamp': time.time(),
                'round': round_num,
                'reason': reason
            }
            
            # Add node_info for 'add' operations
            if action == 'add' and complete_node_info:
                command['node_info'] = complete_node_info
            
            # Add to the log
            log_index = self.raft_node.add_log_entry(command)
            
            if log_index > 0:
                logging.info(f"Leader {self.raft_node.node_id}: Added membership change ({action} node {node_id}) at index {log_index}")
                # NOTE: Do NOT apply membership change here - it will be applied when committed
                # This prevents duplicate application of membership changes
                return log_index
            else:
                logging.error(f"Leader {self.raft_node.node_id}: Failed to add membership change to log")
                return -1
            
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error adding membership change: {str(e)}")
            return -1
    
    def add_coordinator_update(self, coordinator_id, round_num=0):
        """
        Add a coordinator update to the log (leaders only).
        
        Args:
            coordinator_id (int): ID of the new coordinator
            round_num (int): Current training round number
            
        Returns:
            int: Index of the new log entry, or -1 if not leader
        """
        if self.raft_node.state != RaftState.LEADER:
            logging.warning(f"Node {self.raft_node.node_id}: Cannot add coordinator update - not a leader")
            return -1
        
        command = {
            'type': 'coordinator',
            'coordinator_id': coordinator_id,
            'round': round_num,
            'timestamp': time.time()
        }
        
        log_index = self.raft_node.add_log_entry(command)
        
        if log_index > 0:
            logging.info(f"Leader {self.raft_node.node_id}: Added coordinator update for node {coordinator_id}, round {round_num} at index {log_index}")
        
        return log_index
    
    def get_coordinator_id(self):
        """
        Get the current coordinator ID.
        
        Returns:
            int: ID of the current coordinator (same as leader)
        """
        return self.get_leader_id()
    
    def on_membership_change(self, new_nodes, round_num=0):
        """
        Handle notification of membership changes from the RAFT node.
        
        This method is called when the RAFT node applies a membership change
        log entry. It notifies the worker manager about the change.
        
        Args:
            new_nodes (set): The updated set of known nodes
            round_num (int): The current training round number
        """
        try:
            if self.worker_manager is not None:
                # Notify the worker manager about the membership change
                self.worker_manager.on_membership_change(new_nodes, round_num)
                logging.debug(f"Node {self.raft_node.node_id}: Worker manager notified of membership change")
            else:
                logging.warning(f"Node {self.raft_node.node_id}: No worker manager available to notify about membership change")
            
            # Update discovery hint sender with new membership information
            if self.discovery_hint_sender and self.raft_node.state == RaftState.LEADER:
                self.discovery_hint_sender.update_raft_state(
                    new_state=HintRaftState.LEADER,
                    term=self.raft_node.current_term,
                    known_nodes=new_nodes
                )
                logging.debug(f"Node {self.raft_node.node_id}: Discovery hint sender updated with new membership")
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in on_membership_change: {e}", exc_info=True)
    
    def on_coordinator_change(self, new_coordinator, old_coordinator=None, round_num=0, reason='unspecified'):
        """
        Handle notification of coordinator changes from the RAFT node.
        
        This method is called when the RAFT node applies a coordinator change
        log entry. It notifies the worker manager about the change.
        
        Args:
            new_coordinator (int): The ID of the new coordinator
            old_coordinator (int): The ID of the previous coordinator
            round_num (int): The current training round number
            reason (str): The reason for the coordinator change
        """
        try:
            if self.worker_manager is not None:
                # Notify the worker manager about the coordinator change
                self.worker_manager.on_coordinator_change(
                    new_coordinator=new_coordinator,
                    old_coordinator=old_coordinator,
                    round_num=round_num,
                    reason=reason
                )
                logging.debug(f"Node {self.raft_node.node_id}: Worker manager notified of coordinator change to {new_coordinator}")
            else:
                logging.warning(f"Node {self.raft_node.node_id}: No worker manager available to notify about coordinator change")
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in on_coordinator_change: {e}", exc_info=True)
    
    def on_become_coordinator(self, round_num=0):
        """
        Handle notification that this node has become the coordinator.
        
        This method is called when this node is elected as the new coordinator.
        It triggers the training process in the worker manager.
        
        Args:
            round_num (int): The current training round number
        """
        try:
            if self.worker_manager is not None:
                # Trigger training process in the worker manager
                self.worker_manager.on_become_coordinator(round_num)
                logging.info(f"Node {self.raft_node.node_id}: Triggered training process as new coordinator for round {round_num}")
            else:
                logging.warning(f"Node {self.raft_node.node_id}: No worker manager available to trigger training process")
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in on_become_coordinator: {e}", exc_info=True)
    
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
            
            # Determine the last index available in our log
            last_index = self.raft_node.first_log_index + len(self.raft_node.log) - 1
            start_index = min(self.raft_node.commit_index, last_index)

            # Search through committed log entries from most recent to oldest
            for log_index in range(start_index, self.raft_node.first_log_index - 1, -1):
                array_idx = log_index - self.raft_node.first_log_index
                if array_idx < 0 or array_idx >= len(self.raft_node.log):
                    continue
                entry = self.raft_node.log[array_idx]
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
        return self.raft_node.get_leader_id()
    
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
            last_index = self.raft_node.first_log_index + len(self.raft_node.log) - 1
            end_index = min(self.raft_node.commit_index, last_index)

            # Search through committed log entries
            for log_index in range(self.raft_node.first_log_index, end_index + 1):
                array_idx = log_index - self.raft_node.first_log_index
                if array_idx < 0 or array_idx >= len(self.raft_node.log):
                    continue
                entry = self.raft_node.log[array_idx]
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
            last_index = self.raft_node.first_log_index + len(self.raft_node.log) - 1
            end_index = min(self.raft_node.commit_index, last_index)

            # Search through committed log entries
            for log_index in range(self.raft_node.first_log_index, end_index + 1):
                array_idx = log_index - self.raft_node.first_log_index
                if array_idx < 0 or array_idx >= len(self.raft_node.log):
                    continue
                entry = self.raft_node.log[array_idx]
                if entry.get('type') == 'bandwidth':
                    data = entry.get('data', {})
                    timestamp = data.get('timestamp', 0)
                    if timestamp > latest_timestamp:
                        latest_timestamp = timestamp
                        latest_bandwidth = data
        
        return latest_bandwidth
    
    def get_status(self):
        """
        Get the current status of the RAFT consensus.
        
        Returns:
            dict: Status information including threading state, node state, etc.
        """
        with self.thread_lock:
            return {
                'is_running': self.is_running,
                'node_id': self.raft_node.node_id,
                'node_state': str(self.raft_node.state),
                'current_term': self.raft_node.current_term,
                'current_leader_id': self.get_leader_id(),
                'commit_index': self.raft_node.commit_index,
                'log_length': len(self.raft_node.log),
                'threads': {
                    'election_timer_alive': self.election_timer_thread.is_alive() if self.election_timer_thread else False,
                    'heartbeat_alive': self.heartbeat_thread.is_alive() if self.heartbeat_thread else False,
                    'log_replication_alive': self.log_replication_thread.is_alive() if self.log_replication_thread else False,
                }
            }
    
    # Discovery integration helper methods
    
    def set_discovery_service(self, discovery_host, discovery_port):
        """
        Set or update the discovery service configuration.
        
        This can be called after initialization to configure discovery service.
        
        Args:
            discovery_host (str): Discovery service hostname/IP
            discovery_port (int): Discovery service port
        """
        if self.discovery_hint_sender:
            self.discovery_hint_sender.stop()
        
        self.discovery_hint_sender = DiscoveryHintSender(
            discovery_host=discovery_host,
            discovery_port=discovery_port,
            node_id=self.raft_node.node_id,
            send_interval=30.0
        )
        
        # Start the hint sender if consensus is already running
        if self.is_running:
            self.discovery_hint_sender.start()
        
        logging.info(f"Node {self.raft_node.node_id}: Discovery service configured for {discovery_host}:{discovery_port}")
    
    def get_discovery_stats(self):
        """
        Get statistics from the discovery hint sender.
        
        Returns:
            dict: Discovery hint sender statistics, or None if not configured
        """
        if self.discovery_hint_sender:
            return self.discovery_hint_sender.get_stats()
        return None
    
    def is_discovery_enabled(self):
        """
        Check if discovery service integration is enabled.
        
        Returns:
            bool: True if discovery hint sender is configured and active
        """
        return self.discovery_hint_sender is not None
    
    def get_known_nodes(self):
        """
        Get the set of known nodes in the cluster.
        
        Returns:
            set: Set of known node IDs
        """
        return self.raft_node.known_nodes.copy() if self.raft_node.known_nodes else set()
        
    def get_current_term(self):
        """
        Get the current RAFT term.
        
        Returns:
            int: Current RAFT term
        """
        return self.raft_node.current_term
        
    def __del__(self):
        """
        Destructor to ensure proper cleanup of threads.
        """
        try:
            if hasattr(self, 'is_running') and self.is_running:
                logging.info(f"Node {getattr(self.raft_node, 'node_id', 'unknown')}: Cleaning up RAFT consensus in destructor")
                self.stop()
        except Exception as e:
            # Don't raise exceptions in destructor
            pass

    # =============================================================================
    # Service Discovery Bridge Integration
    # =============================================================================
    
    def register_service_discovery_bridge(self, bridge):
        """
        Register a service discovery bridge for dynamic membership management.
        
        Args:
            bridge (RaftServiceDiscoveryBridge): The bridge instance to register
            
        Returns:
            bool: True if registration was successful, False otherwise
        """
        try:
            if hasattr(self, 'service_discovery_bridge') and self.service_discovery_bridge is not None:
                logging.warning(f"Node {self.raft_node.node_id}: Service discovery bridge already registered")
                return False
            
            self.service_discovery_bridge = bridge
            
            # Set consensus manager reference in bridge
            bridge.set_consensus_manager(self)
            
            logging.info(f"Node {self.raft_node.node_id}: Service discovery bridge registered successfully")
            return True
            
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Node {self.raft_node.node_id}: Error registering service discovery bridge: {e}")
            return False
    
    def unregister_service_discovery_bridge(self):
        """
        Unregister the service discovery bridge.
        
        Returns:
            bool: True if unregistration was successful, False otherwise
        """
        try:
            if hasattr(self, 'service_discovery_bridge'):
                if self.service_discovery_bridge is not None:
                    # Clear consensus manager reference in bridge
                    self.service_discovery_bridge.set_consensus_manager(None)
                    self.service_discovery_bridge = None
                    logging.info(f"Node {self.raft_node.node_id}: Service discovery bridge unregistered")
                return True
            return False
            
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error unregistering service discovery bridge: {e}")
            return False
    
    def handle_service_discovery_hint(self, hint_data):
        """
        Process a service discovery hint from the bridge.
        
        Args:
            hint_data (dict): Service discovery hint containing:
                - event_type: "discovered" or "lost"
                - node_id: ID of the discovered/lost node
                - node_info: Connection information (for discovered nodes)
                - timestamp: Event timestamp
                
        Returns:
            bool: True if hint was processed successfully, False otherwise
        """
        try:
            event_type = hint_data.get('event_type')
            node_id = hint_data.get('node_id')
            node_info = hint_data.get('node_info', {})
            timestamp = hint_data.get('timestamp', time.time())
            
            logging.info(f"Node {self.raft_node.node_id}: Processing service discovery hint - "
                        f"event: {event_type}, node: {node_id}")
            
            if event_type == 'discovered':
                return self._handle_node_discovered(node_id, node_info, timestamp)
            elif event_type == 'lost':
                return self._handle_node_lost(node_id, timestamp)
            else:
                logging.warning(f"Node {self.raft_node.node_id}: Unknown service discovery event type: {event_type}")
                return False
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error handling service discovery hint: {e}")
            return False
    
    def _handle_node_discovered(self, node_id, node_info, timestamp):
        """
        Handle a node discovery event.
        
        Args:
            node_id (int): ID of the discovered node
            node_info (dict): Connection information for the node
            timestamp (float): Discovery timestamp
            
        Returns:
            bool: True if handled successfully, False otherwise
        """
        try:
            # Validate node info
            if not self._validate_node_capabilities(node_id, node_info):
                logging.warning(f"Node {self.raft_node.node_id}: Node {node_id} failed capability validation")
                return False
            
            # Check if node is already known
            if node_id in self.raft_node.known_nodes or (hasattr(self.raft_node, 'pending_node_connection_info') and node_id in self.raft_node.pending_node_connection_info):
                logging.debug(f"Node {self.raft_node.node_id}: Node {node_id} already known, updating connection info")
                # Update connection info if we have it
                if hasattr(self.raft_node, 'node_connection_info') and node_info:
                    self.raft_node.node_connection_info[node_id] = node_info
                return True
            
            # Only leaders can propose membership changes
            if self.raft_node.state == RaftState.LEADER:
                return self.propose_membership_change('add', node_id, node_info)
            else:
                # Forward to leader if we know who it is
                return self._forward_to_leader('membership_proposal', {
                    'action': 'add',
                    'node_id': node_id,
                    'node_info': node_info,
                    'timestamp': timestamp
                })
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error handling node discovered: {e}")
            return False
    
    def _handle_node_lost(self, node_id, timestamp):
        """
        Handle a node loss event.
        
        Args:
            node_id (int): ID of the lost node
            timestamp (float): Loss timestamp
            
        Returns:
            bool: True if handled successfully, False otherwise
        """
        try:
            # Check if node is known
            if node_id not in self.raft_node.known_nodes:
                logging.debug(f"Node {self.raft_node.node_id}: Node {node_id} not in known nodes, ignoring loss event")
                return True
            
            # Only leaders can propose membership changes
            if self.raft_node.state == RaftState.LEADER:
                return self.propose_membership_change('remove', node_id, reason="service_discovery_lost")
            else:
                # Forward to leader if we know who it is
                return self._forward_to_leader('membership_proposal', {
                    'action': 'remove',
                    'node_id': node_id,
                    'reason': 'service_discovery_lost',
                    'timestamp': timestamp
                })
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error handling node lost: {e}")
            return False
    
    def propose_membership_change(self, action, node_id, node_info=None, reason=None):
        """
        Propose a membership change through RAFT consensus.
        
        Args:
            action (str): "add" or "remove"
            node_id (int): ID of the node to add/remove
            node_info (dict, optional): Connection information for new nodes
            reason (str, optional): Reason for the change
            
        Returns:
            bool: True if proposal was successfully added to log, False otherwise
        """
        try:
            if self.raft_node.state != RaftState.LEADER:
                logging.warning(f"Node {self.raft_node.node_id}: Cannot propose membership change - not a leader")
                return False
            
            # Call add_membership_change with node_info preserved
            log_index = self.add_membership_change(
                action=action,
                node_id=node_id,
                node_info=node_info,
                round_num=self._get_current_round(),
                reason=reason or "service_discovery"
            )
            
            success = log_index != -1
            if success:
                logging.info(f"Leader {self.raft_node.node_id}: Proposed membership change - "
                        f"action: {action}, node: {node_id}, log_index: {log_index}")
            else:
                logging.warning(f"Leader {self.raft_node.node_id}: Failed to propose membership change - "
                            f"action: {action}, node: {node_id}")
            
            return success
            
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error proposing membership change: {e}")
            return False
    
    def _validate_node_capabilities(self, node_id, node_info):
        """
        Validate that a discovered node has the required capabilities.
        
        Args:
            node_id (int): ID of the node to validate
            node_info (dict): Node information including capabilities
            
        Returns:
            bool: True if node has required capabilities, False otherwise
        """
        try:
            # Basic validation
            if not isinstance(node_id, int) or node_id < 0:
                logging.warning(f"Node {self.raft_node.node_id}: Invalid node ID: {node_id}")
                return False
            
            # Check for required connection information
            if not node_info.get('ip_address') or not node_info.get('port'):
                logging.warning(f"Node {self.raft_node.node_id}: Node {node_id} missing connection info")
                return False
            
            # Check capabilities if specified
            capabilities = node_info.get('capabilities', [])
            required_capabilities = ['grpc', 'fedml']  # Required for GossipFL
            
            for required_cap in required_capabilities:
                if required_cap not in capabilities:
                    logging.warning(f"Node {self.raft_node.node_id}: Node {node_id} missing capability: {required_cap}")
                    return False
            
            logging.debug(f"Node {self.raft_node.node_id}: Node {node_id} passed capability validation")
            return True
            
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error validating node capabilities: {e}")
            return False
    
    def _forward_to_leader(self, message_type, data):
        """
        Forward a message to the current leader.
        
        Args:
            message_type (str): Type of message to forward
            data (dict): Message data
            
        Returns:
            bool: True if forwarded successfully, False otherwise
        """
        try:
            current_leader_id = self.get_leader_id()
            if not current_leader_id or current_leader_id == self.raft_node.node_id:
                logging.debug(f"Node {self.raft_node.node_id}: No known leader to forward to")
                return False
            
            # Create forwarding message
            forward_message = {
                'type': message_type,
                'data': data,
                'forwarded_by': self.raft_node.node_id,
                'timestamp': time.time()
            }
            
            if hasattr(self.worker_manager, 'send_message_to_node'):
                self.worker_manager.send_message_to_node(current_leader_id, forward_message)
                logging.debug(f"Node {self.raft_node.node_id}: Forwarded {message_type} to leader {current_leader_id}")
                return True
            else:
                logging.warning(f"Node {self.raft_node.node_id}: No method available to forward message to leader")
                return False
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error forwarding to leader: {e}")
            return False
    
    def _get_current_round(self):
        """
        Get the current training round number.
        
        Returns:
            int: Current training round, or 0 if not available
        """
        try:
            if hasattr(self.worker_manager, 'round_idx'):
                return self.worker_manager.round_idx
            elif hasattr(self.args, 'current_round'):
                return self.args.current_round
            else:
                return 0
        except Exception:
            return 0
    
    def get_bridge_status(self):
        """
        Get the current status of the service discovery bridge.
        
        Returns:
            dict: Bridge status information
        """
        try:
            if hasattr(self, 'service_discovery_bridge') and self.service_discovery_bridge:
                return {
                    'registered': True,
                    'active': self.service_discovery_bridge.is_active(),
                    'stats': self.service_discovery_bridge.get_stats() if hasattr(self.service_discovery_bridge, 'get_stats') else {}
                }
            else:
                return {
                    'registered': False,
                    'active': False,
                    'stats': {}
                }
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error getting bridge status: {e}")
            return {'registered': False, 'active': False, 'stats': {}}

    # ============================================================================
    # RaftNode Callback Implementations
    # ============================================================================
    
    def _broadcast(self, rpc_method_name, *args, max_retries=3, retry_delay=0.1):
        """
        Unified method to broadcast RPC to all peers with retry logic.
        
        Args:
            rpc_method_name (str): Name of the worker_manager method to call
            *args: Arguments to pass to the RPC method
            max_retries (int): Maximum number of retry attempts
            retry_delay (float): Delay between retries in seconds
        """
        try:
            for peer in self.raft_node.known_nodes - {self.raft_node.node_id}:
                self._send_to_peer_with_retry(peer, rpc_method_name, *args, 
                                             max_retries=max_retries, retry_delay=retry_delay)
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in broadcast {rpc_method_name}: {e}")
    
    def _send_to_peer(self, node_id, rpc_method_name, *args):
        """
        Send RPC to a specific peer.
        
        Args:
            node_id (int): Target node ID
            rpc_method_name (str): Name of the worker_manager method to call
            *args: Arguments to pass to the RPC method
        """
        try:
            getattr(self.worker_manager, rpc_method_name)(node_id, *args)
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error sending {rpc_method_name} to {node_id}: {e}")
    
    def _send_to_peer_with_retry(self, node_id, rpc_method_name, *args, max_retries=3, retry_delay=0.1):
        """
        Send RPC to a specific peer with retry logic.
        
        Args:
            node_id (int): Target node ID
            rpc_method_name (str): Name of the worker_manager method to call
            *args: Arguments to pass to the RPC method
            max_retries (int): Maximum number of retry attempts
            retry_delay (float): Delay between retries in seconds
        """
        for attempt in range(max_retries):
            try:
                getattr(self.worker_manager, rpc_method_name)(node_id, *args)
                return  # Success, exit retry loop
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"Node {self.raft_node.node_id}: Error sending {rpc_method_name} to {node_id} "
                                   f"(attempt {attempt + 1}/{max_retries}): {e}")
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    logging.error(f"Node {self.raft_node.node_id}: Failed to send {rpc_method_name} to {node_id} "
                                 f"after {max_retries} attempts: {e}")
    
    def _send_prevote_callback(self, candidate_id, term, last_log_index, last_log_term):
        """Callback for sending prevote requests to all peers."""
        self._broadcast('send_prevote_request', term, candidate_id, last_log_index, last_log_term)
    
    def _send_vote_callback(self, candidate_id, term, last_log_index, last_log_term):
        """Callback for sending vote requests to all peers."""
        self._broadcast('send_vote_request', term, candidate_id, last_log_index, last_log_term)
    
    def _send_append_callback(self, node_id, current_term, leader_id, prev_log_index, prev_log_term, 
                             entries, leader_commit_index):
        """Callback for sending append entries to a specific peer with retry logic."""
        self._send_to_peer_with_retry(node_id, 'send_append_entries', 
                                     current_term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index)
    
    def _send_snapshot_callback(self, node_id, current_term, leader_id, last_included_index, 
                               last_included_term, snapshot_data):
        """Callback for sending snapshots to a specific peer with retry logic."""
        self._send_to_peer_with_retry(node_id, 'send_install_snapshot',
                                     current_term, leader_id, last_included_index, last_included_term, 
                                     0, snapshot_data, True)  # offset=0, done=True
    
    def _commit_callback(self, entry):
        """Callback for when entries are committed."""
        try:
            # Handle different types of log entries
            if isinstance(entry, dict):
                command = entry.get('command', {})
                consensus_type = command.get('type')
                
                if consensus_type == 'topology':
                    if self.topology_manager:
                        self.topology_manager.apply_topology_update(command.get('data'))
                        
                elif consensus_type == 'bandwidth':
                    if self.bandwidth_manager:
                        self.bandwidth_manager.apply_bandwidth_update(command.get('data'))
                        
                elif consensus_type == 'membership':
                    self._handle_membership_commit(command)
                    
                elif consensus_type == 'coordinator':
                    self._handle_coordinator_commit(command)
                    
                else:
                    logging.warning(f"Node {self.raft_node.node_id}: Unknown consensus type in commit: {consensus_type}")
                    
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in commit callback: {e}", exc_info=True)
            
    def _membership_change_callback(self, nodes):
        """Callback for membership changes."""
        try:
            # 1) Update service discovery bridge
            if self.service_discovery_bridge:
                self.service_discovery_bridge.handle_membership_change(nodes)
                
            # 2) Notify worker manager with the updated node set
            # Get current round from raft state or fallback to args
            round_num = self._get_current_round()
            
            if self.worker_manager is not None:
                self.worker_manager.on_membership_change(set(nodes), round_num=round_num)
                logging.debug(f"Node {self.raft_node.node_id}: Worker manager notified of membership change to {nodes}")
            else:
                logging.warning(f"Node {self.raft_node.node_id}: No worker manager available to notify about membership change")
                
            # Update discovery hint sender if we're the leader
            if self.discovery_hint_sender and self.raft_node.state == RaftState.LEADER:
                self.discovery_hint_sender.update_raft_state(
                    new_state=HintRaftState.LEADER,
                    term=self.raft_node.current_term,
                    known_nodes=nodes
                )
                logging.debug(f"Node {self.raft_node.node_id}: Discovery hint sender updated with new membership")
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error in membership change callback: {e}", exc_info=True)
    
    def _handle_membership_commit(self, data):
        """Handle committed membership changes."""
        with self.thread_lock:
            try:
                action = data.get('action')
                node_id = data.get('node_id')
                node_info = data.get('node_info')
                current_nodes = set(data.get('current_nodes', []))
                round_num = data.get('round', 0)
                reason = data.get('reason', 'unspecified')
                
                logging.info(f"Node {self.raft_node.node_id}: Committed membership {action} for node {node_id}, reason: {reason}")
            
                # 1) Let the service discovery bridge update its comm_manager
                if self.service_discovery_bridge:
                    self.service_discovery_bridge.handle_membership_change(current_nodes)
                    # If we have node_info for 'add' actions, update the comm_manager directly
                    if action == 'add' and node_info and hasattr(self.service_discovery_bridge, '_notify_comm_manager_membership_change'):
                        self.service_discovery_bridge._notify_comm_manager_membership_change(
                            action='add', node_id=node_id, node_info=node_info
                        )
                
                # 2) Notify the worker_manager so it can update its topology
                if self.worker_manager is not None:
                    self.worker_manager.on_membership_change(current_nodes, round_num=round_num)
                    
                    # If we have node info and the worker_manager has service_discovery_bridge
                    if action == 'add' and node_info and hasattr(self.worker_manager, 'service_discovery_bridge') and \
                    self.worker_manager.service_discovery_bridge is not None and \
                    hasattr(self.worker_manager.service_discovery_bridge, '_notify_comm_manager_membership_change'):
                        self.worker_manager.service_discovery_bridge._notify_comm_manager_membership_change(
                            action='add', node_id=node_id, node_info=node_info
                        )
                
                # 3) Update our internal node set if needed
                # This ensures everyone has the same view of cluster membership
                if action == 'add' and node_id not in self.raft_node.known_nodes:
                    # If we're the leader, we already did this when creating the log entry
                    if self.raft_node.state != RaftState.LEADER:
                        if node_info:
                            self.raft_node.add_node(node_info, round_num)
                        else:
                            self.raft_node.add_node(node_id, round_num)
                elif action == 'remove' and node_id in self.raft_node.known_nodes:
                    # If we're the leader, we already did this when creating the log entry
                    if self.raft_node.state != RaftState.LEADER:
                        self.raft_node.remove_node(node_id, round_num, reason)

            except Exception as e:
                logging.error(f"Node {self.raft_node.node_id}: Error handling membership commit: {e}", exc_info=True)
    
    def _handle_coordinator_commit(self, data):
        """Handle committed coordinator changes."""
        with self.thread_lock:  # Add thread locking for thread safety
            try:
                new_coordinator = data.get('coordinator_id')
                round_num = data.get('round', 0)
                old_coordinator = None
                reason = 'raft-commit'

                logging.info(f"Node {self.raft_node.node_id}: Committed coordinator change to {new_coordinator} for round {round_num}")

                # Get previous coordinator if available through worker_manager
                if hasattr(self.worker_manager, 'coordinator_id'):
                    old_coordinator = self.worker_manager.coordinator_id

                # Notify the worker manager about the coordinator change
                if self.worker_manager is not None:
                    self.worker_manager.on_coordinator_change(
                        new_coordinator=new_coordinator,
                        old_coordinator=old_coordinator,
                        round_num=round_num,
                        reason=reason
                    )
                    # Only trigger on_become_coordinator if this node is the new coordinator and hasn't already been notified
                    if new_coordinator == self.raft_node.node_id:
                        # Use a flag to ensure we only notify once per round
                        if not hasattr(self, '_last_coordinator_round') or self._last_coordinator_round != round_num:
                            self._last_coordinator_round = round_num
                            self.worker_manager.on_become_coordinator(round_num)
                            logging.info(f"Node {self.raft_node.node_id}: Triggered training process as new coordinator for round {round_num}")
                else:
                    logging.warning(f"Node {self.raft_node.node_id}: No worker manager available to notify about coordinator change")

            except Exception as e:
                logging.error(f"Node {self.raft_node.node_id}: Error handling coordinator commit: {e}", exc_info=True)
