import logging
import random
import time
import threading
from enum import Enum

class RaftState(Enum):
    """
    Represents the possible states of a RAFT node.
    """
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


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
        self.state = RaftState.FOLLOWER
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
        
        logging.info(f"RAFT Node {self.node_id} initialized in FOLLOWER state")
    
    def update_known_nodes(self, total_nodes):
        """
        Update the set of known nodes.
        
        Args:
            total_nodes (int): Total number of nodes in the network
        """
        self.known_nodes = set(range(total_nodes))
        self.total_nodes = len(self.known_nodes)
        self.majority = self.total_nodes // 2 + 1
        logging.info(f"Node {self.node_id}: Known nodes updated, total={self.total_nodes}, majority={self.majority}")
    
    def reset_election_timeout(self):
        """Reset the election timeout with a random value."""
        self.election_timeout = random.uniform(self.min_election_timeout, self.max_election_timeout)
        logging.debug(f"Node {self.node_id}: Reset election timeout to {self.election_timeout*1000:.2f}ms")
    
    def update_heartbeat(self):
        """Update the last heartbeat time."""
        self.last_heartbeat_time = time.time()
    
    def is_election_timeout(self):
        """Check if election timeout has occurred."""
        return time.time() - self.last_heartbeat_time > self.election_timeout
    
    def start_election(self):
        """
        Start a new election (transition to CANDIDATE state).
        
        Returns:
            bool: True if successfully started election, False otherwise
        """
        with self.state_lock:
            if self.state == RaftState.LEADER:
                return False  # Leaders don't start elections
                
            # Increment term, vote for self
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
                
            logging.info(f"Node {self.node_id}: Starting election for term {self.current_term}")
            return True
    
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
            # If term > currentTerm, convert to follower
            if term > self.current_term:
                self.become_follower(term)
            
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
                
                logging.info(f"Node {self.node_id}: Granted vote to {candidate_id} for term {term}")
            
            return self.current_term, vote_granted
    
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
            # If term > currentTerm, convert to follower
            if term > self.current_term:
                self.become_follower(term)
                return False
            
            # Ignore if not a candidate or from a previous term
            if self.state != RaftState.CANDIDATE or term < self.current_term:
                return False
            
            # Count the vote
            if vote_granted:
                self.votes_received.add(voter_id)
                logging.info(f"Node {self.node_id}: Received vote from {voter_id}, " +
                            f"total votes: {len(self.votes_received)}/{self.majority}")
                
                # Check if we have majority
                if len(self.votes_received) >= self.majority:
                    self.become_leader()
                    return True
            
            return False
    
    def become_follower(self, term):
        """
        Transition to FOLLOWER state.
        
        Args:
            term (int): The new current term
        """
        with self.state_lock:
            old_state = self.state
            self.state = RaftState.FOLLOWER
            self.current_term = term
            self.voted_for = None
            
            # Reset election timeout
            self.reset_election_timeout()
            self.update_heartbeat()
            
            # Only notify if state actually changed
            if old_state != RaftState.FOLLOWER and self.on_state_change:
                self.on_state_change(RaftState.FOLLOWER)
                
            logging.info(f"Node {self.node_id}: Became FOLLOWER for term {term}")
    
    def become_leader(self):
        """Transition to LEADER state."""
        with self.state_lock:
            if self.state != RaftState.CANDIDATE:
                return False
                
            self.state = RaftState.LEADER
            
            # Initialize leader state
            self.next_index = {node_id: len(self.log) + 1 for node_id in self.known_nodes if node_id != self.node_id}
            self.match_index = {node_id: 0 for node_id in self.known_nodes if node_id != self.node_id}
            
            # Notify state change
            if self.on_state_change:
                self.on_state_change(RaftState.LEADER)
                
            logging.info(f"Node {self.node_id}: Became LEADER for term {self.current_term}")
            return True
    
    def is_log_up_to_date(self, last_log_index, last_log_term):
        """
        Check if candidate's log is at least as up-to-date as this node's log.
        
        Args:
            last_log_index (int): Index of candidate's last log entry
            last_log_term (int): Term of candidate's last log entry
            
        Returns:
            bool: True if candidate's log is at least as up-to-date
        """
        # Get this node's last log entry
        my_last_log_index = len(self.log)
        my_last_log_term = self.log[my_last_log_index - 1]['term'] if my_last_log_index > 0 else 0
        
        # Compare logs
        if last_log_term > my_last_log_term:
            return True
        elif last_log_term == my_last_log_term and last_log_index >= my_last_log_index:
            return True
        else:
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
            # Apply topology update
            logging.info(f"Node {self.node_id}: Applying topology update from log entry {entry['index']}")
            # Handle topology update logic here
        elif command_type == 'membership':
            # Apply membership change
            logging.info(f"Node {self.node_id}: Applying membership change from log entry {entry['index']}")
            # Handle membership change logic here
        else:
            logging.warning(f"Node {self.node_id}: Unknown command type in log entry: {command_type}")
    
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
