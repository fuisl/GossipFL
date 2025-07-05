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

        # Allow the raft_node to access consensus callbacks
        self.raft_node.consensus_manager = self
        
        # Service discovery bridge (will be set later)
        self.service_discovery_bridge = None
        
        # Set callback for state changes
        self.raft_node.on_state_change = self.handle_state_change
        
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
        
        # Current leader ID (None if unknown)
        self.current_leader_id = None
        
        # Current coordinator ID (None if unknown)
        self.current_coordinator_id = None
        
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
            
            # Start election timer thread (always running for followers/candidates)
            self._start_election_timer_thread()
            
            # Start discovery hint sender if configured
            if self.discovery_hint_sender:
                self.discovery_hint_sender.start()
                logging.debug(f"Node {self.raft_node.node_id}: Started discovery hint sender")
            
            # If we're already a leader, start leader threads
            if self.raft_node.state == RaftState.LEADER:
                self._start_leader_threads()
            
            logging.info(f"Node {self.raft_node.node_id}: RAFT consensus operations started")
    
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
                self.current_leader_id = self.raft_node.node_id
                
                # Start leader-specific threads
                self._start_leader_threads()
                
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
                
                # Update discovery hint sender when becoming follower/candidate
                if self.discovery_hint_sender:
                    hint_state = HintRaftState.FOLLOWER if new_state == RaftState.FOLLOWER else HintRaftState.CANDIDATE
                    self.discovery_hint_sender.update_raft_state(
                        new_state=hint_state,
                        term=self.raft_node.current_term,
                        known_nodes=self.raft_node.known_nodes
                    )
                    logging.debug(f"Node {self.raft_node.node_id}: Updated discovery hint sender for {hint_state} state")
                
                # If we were the leader and now we're not, clear our leader status
                if self.current_leader_id == self.raft_node.node_id:
                    self.current_leader_id = None
    
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
                        # Start election (which may initiate a PreVote phase)
                        transitioned = self.raft_node.start_election()

                        # If we actually became a candidate, request votes
                        if transitioned and self.raft_node.state == RaftState.CANDIDATE:
                            self.request_votes_from_all()
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
                    # Send heartbeats to all followers (RAFT Algorithm Step)
                    self.send_heartbeats()
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
                    # Replicate logs to followers (RAFT Algorithm Step)
                    self.replicate_logs()
                    
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
    
    def request_votes_from_all(self):
        """Send vote requests to all other nodes."""
        if self.raft_node.state != RaftState.CANDIDATE:
            return
        
        last_log_index, last_log_term = self.raft_node.get_last_log_info()
        
        for node_id in self.raft_node.known_nodes:
            if node_id == self.raft_node.node_id:
                continue  # Skip self
            
            # Send vote request to node
            try:
                self.worker_manager.send_vote_request(
                    node_id,
                    self.raft_node.current_term,
                    last_log_index,
                    last_log_term
                )
            except Exception as e:
                logging.error(f"Node {self.raft_node.node_id}: Error sending vote request to {node_id}: {e}")

    def broadcast_prevote_request(self, candidate_id, term, last_log_index, last_log_term):
        """Send PreVote requests to all other nodes."""
        for node_id in self.raft_node.known_nodes:
            if node_id == self.raft_node.node_id:
                continue

            try:
                self.worker_manager.send_prevote_request(
                    node_id,
                    term,
                    last_log_index,
                    last_log_term,
                )
            except Exception as e:
                logging.error(f"Node {self.raft_node.node_id}: Error sending prevote request to {node_id}: {e}")

    def send_heartbeats(self):
        """Send heartbeat messages to all followers."""
        if self.raft_node.state != RaftState.LEADER:
            return
        
        for node_id in self.raft_node.known_nodes:
            if node_id == self.raft_node.node_id:
                continue  # Skip self

            try:
                next_index = self.raft_node.next_index.get(
                    node_id, self.raft_node.first_log_index + len(self.raft_node.log)
                )

                if next_index <= self.raft_node.last_snapshot_index:
                    self._send_snapshot(node_id)
                    continue

                prev_log_index = next_index - 1
                prev_log_term = self.raft_node.get_term_at_index(prev_log_index)
                
                self.worker_manager.send_append_entries(
                    node_id,
                    self.raft_node.current_term,
                    prev_log_index,
                    prev_log_term,
                    [],  # Empty entries for heartbeat
                    self.raft_node.commit_index
                )
            except Exception as e:
                logging.error(f"Node {self.raft_node.node_id}: Error sending heartbeat to {node_id}: {e}")
    
    def replicate_logs(self):
        """Replicate log entries to followers."""
        if self.raft_node.state != RaftState.LEADER:
            return
        
        for node_id in self.raft_node.known_nodes:
            if node_id == self.raft_node.node_id:
                continue  # Skip self
            
            try:
                next_index = self.raft_node.next_index.get(
                    node_id, self.raft_node.first_log_index + len(self.raft_node.log)
                )

                if next_index <= self.raft_node.last_snapshot_index:
                    # follower is too far behind, send snapshot
                    self._send_snapshot(node_id)
                    continue

                array_idx = next_index - self.raft_node.first_log_index
                entries_to_send = self.raft_node.log[array_idx:]

                prev_log_index = next_index - 1
                prev_log_term = self.raft_node.get_term_at_index(prev_log_index)

                self.worker_manager.send_append_entries(
                    node_id,
                    self.raft_node.current_term,
                    prev_log_index,
                    prev_log_term,
                    entries_to_send,
                    self.raft_node.commit_index,
                )
            except Exception as e:
                logging.error(f"Node {self.raft_node.node_id}: Error replicating logs to {node_id}: {e}")

    def _send_snapshot(self, node_id):
        """Send a snapshot of the current state to the given follower."""
        try:
            self.worker_manager.send_install_snapshot(
                node_id,
                self.raft_node.current_term,
                self.raft_node.last_snapshot_index,
                self.raft_node.last_snapshot_term,
                0,
                self.raft_node.log,
                True,
            )
            logging.debug(
                f"Node {self.raft_node.node_id}: Sent snapshot to follower {node_id}"
            )
        except Exception as e:
            logging.error(
                f"Node {self.raft_node.node_id}: Error sending snapshot to {node_id}: {e}"
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
            
            # Set current leader
            self.current_leader_id = self.raft_node.node_id
            
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
    
    def add_membership_change(self, action, node_id, round_num=0, reason="unspecified"):
        """
        Add a membership change to the log (leaders only).
        
        Args:
            action (str): 'add' or 'remove'
            node_id (int): ID of the node to add/remove
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
            if action == 'add':
                idx = self.raft_node.add_node(node_id, round_num)
                return idx
            elif action == 'remove':
                idx = self.raft_node.remove_node(node_id, round_num, reason)
                return idx
            
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
                # Update our internal tracking of current coordinator
                self.current_coordinator_id = new_coordinator
                
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
        
        self.send_state_to_new_node(node_id)
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
                'current_leader_id': self.current_leader_id,
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
        
    def get_leader_id(self):
        """
        Get the current leader ID.
        
        Returns:
            int: ID of the current leader, or None if no leader is known
        """
        return self.current_leader_id
    
    def is_leader(self):
        """
        Check if this node is the current leader.
        
        Returns:
            bool: True if this node is the leader
        """
        return self.raft_node.state == RaftState.LEADER
    
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
            if node_id in self.raft_node.known_nodes:
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
            
            # Use enhanced add_node method if available and adding a node
            if action == 'add' and hasattr(self.raft_node, 'enhance_add_node_with_connection_info') and node_info:
                log_index = self.raft_node.enhance_add_node_with_connection_info(
                    node_id, node_info, round_num=self._get_current_round()
                )
            else:
                # Use existing membership change method
                log_index = self.add_membership_change(action, node_id, round_num=self._get_current_round(), reason=reason or "service_discovery")
            
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
    
    def coordinate_new_node_sync(self, node_id):
        """
        Coordinate state synchronization for a new node joining the cluster.
        
        Args:
            node_id (int): ID of the new node
            
        Returns:
            bool: True if sync coordination was successful, False otherwise
        """
        try:
            if self.raft_node.state != RaftState.LEADER:
                logging.warning(f"Node {self.raft_node.node_id}: Cannot coordinate sync - not a leader")
                return False
            
            # Create state sync response for the new node
            sync_response = self._create_state_snapshot(node_id)
            
            # Send state sync response to the new node
            return self._send_state_sync_response(node_id, sync_response)
            
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error coordinating new node sync: {e}")
            return False
    
    def _create_state_snapshot(self, target_node_id):
        """
        Create a complete state snapshot for a new node.
        
        Args:
            target_node_id (int): ID of the node requesting the snapshot
            
        Returns:
            dict: State snapshot containing cluster state
        """
        try:
            # Get current cluster state
            cluster_state = self.raft_node.get_cluster_state()
            
            # Create comprehensive state snapshot
            snapshot = {
                'type': 'state_sync_response',
                'leader_id': self.raft_node.node_id,
                'term': self.raft_node.current_term,
                'known_nodes': cluster_state['known_nodes'],
                'commit_index': self.raft_node.commit_index,
                'log_entries': self._get_committed_log_entries(),
                'timestamp': time.time()
            }
            
            # Add connection information if available
            if hasattr(self.raft_node, 'node_connection_info'):
                snapshot['node_connection_info'] = dict(self.raft_node.node_connection_info)
            
            # Add current model state if available
            if hasattr(self.raft_node, 'model_state') and self.raft_node.model_state:
                snapshot['model_state'] = self.raft_node.model_state
                snapshot['model_version'] = getattr(self.raft_node, 'current_model_version', 0)
            
            logging.debug(f"Leader {self.raft_node.node_id}: Created state snapshot for node {target_node_id}")
            return snapshot
            
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error creating state snapshot: {e}")
            return {}
    
    def _get_committed_log_entries(self):
        """
        Get all committed log entries for state synchronization.
        
        Returns:
            list: List of committed log entries
        """
        try:
            committed_entries = []
            
            # Get entries up to commit index
            for i in range(self.raft_node.commit_index):
                log_array_index = i + 1 - self.raft_node.first_log_index
                if 0 <= log_array_index < len(self.raft_node.log):
                    committed_entries.append(self.raft_node.log[log_array_index])
            
            return committed_entries
            
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error getting committed log entries: {e}")
            return []
    
    def _send_state_sync_response(self, target_node_id, sync_response):
        """
        Send state sync response to a specific node.
        
        Args:
            target_node_id (int): ID of the target node
            sync_response (dict): State sync response data
            
        Returns:
            bool: True if sent successfully, False otherwise
        """
        try:
            if hasattr(self.worker_manager, 'send_message_to_node'):
                # Send via worker manager
                self.worker_manager.send_message_to_node(target_node_id, sync_response)
                logging.info(f"Leader {self.raft_node.node_id}: Sent state sync response to node {target_node_id}")
                return True
            else:
                logging.warning(f"Node {self.raft_node.node_id}: No method available to send state sync response")
                return False
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error sending state sync response: {e}")
            return False
    
    def broadcast_state_sync_request(self, request_data):
        """
        Broadcast a state sync request to find the current leader.
        
        Args:
            request_data (dict): State sync request data
            
        Returns:
            bool: True if broadcast was successful, False otherwise
        """
        try:
            if hasattr(self.worker_manager, 'broadcast_message'):
                self.worker_manager.broadcast_message(request_data)
                logging.info(f"Node {self.raft_node.node_id}: Broadcasted state sync request")
                return True
            else:
                logging.warning(f"Node {self.raft_node.node_id}: No method available to broadcast state sync request")
                return False
                
        except Exception as e:
            logging.error(f"Node {self.raft_node.node_id}: Error broadcasting state sync request: {e}")
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
            if not self.current_leader_id or self.current_leader_id == self.raft_node.node_id:
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
                self.worker_manager.send_message_to_node(self.current_leader_id, forward_message)
                logging.debug(f"Node {self.raft_node.node_id}: Forwarded {message_type} to leader {self.current_leader_id}")
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
