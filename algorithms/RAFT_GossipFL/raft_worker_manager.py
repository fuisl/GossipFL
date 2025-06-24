import logging
import threading
import time
import traceback

from mpi4py import MPI
from fedml_core.distributed.communication.message import Message

from algorithms.SAPS_FL.decentralized_worker_manager import DecentralizedWorkerManager
from utils.context import raise_error_without_process, get_lock
from utils.tracker import get_metric_info
from .raft_node import RaftState
from .raft_messages import RaftMessage


class RaftWorkerManager(DecentralizedWorkerManager):
    """
    Extends the DecentralizedWorkerManager to integrate with RAFT consensus.
    
    This manager uses RAFT for leader election, topology consensus, and
    coordination of the federated learning process.
    """
    
    def __init__(self, args, comm, rank, size, worker, topology_manager, 
                 model_trainer, timer, metrics, raft_consensus, bandwidth_manager=None):
        """
        Initialize the RAFT worker manager.
        
        Args:
            args: Configuration parameters
            comm: MPI communicator
            rank: Rank of this process
            size: Total number of processes
            worker: The worker for training
            topology_manager: The topology manager
            model_trainer: The model trainer
            timer: Timer for performance measurement
            metrics: Metrics for evaluation
            raft_consensus: The RAFT consensus manager
            bandwidth_manager: The bandwidth manager (optional)
        """
        super().__init__(args, comm, rank, size, worker, topology_manager, model_trainer, timer, metrics)
        
        self.raft_consensus = raft_consensus
        self.bandwidth_manager = bandwidth_manager
        
        # Register for leadership changes
        self.raft_consensus.on_leadership_change = self.handle_leadership_change
        
        # State variables
        self.is_coordinator = False
        self.coordinator_id = None
        
        # Thread synchronization
        self.consensus_established_event = threading.Event()
        
        logging.info(f"RaftWorkerManager initialized for node {rank}")
    
    def register_message_receive_handlers(self):
        """Register message handlers for RAFT and GossipFL messages."""
        # First register handlers from the parent class
        super().register_message_receive_handlers()
        
        # Register RAFT message handlers
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_REQUEST_VOTE,
            self.handle_request_vote)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_VOTE_RESPONSE,
            self.handle_vote_response)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_APPEND_ENTRIES,
            self.handle_append_entries)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_APPEND_RESPONSE,
            self.handle_append_response)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_STATE_SNAPSHOT,
            self.handle_state_snapshot)

        # Initialization parameter exchange
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_STATE_REQUEST,
            self.handle_state_request)
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_PARAM_REQUEST,
            self.handle_param_request)
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_PARAM_RESPONSE,
            self.handle_param_response)
    
    def run(self):
        """Run the worker manager with RAFT integration."""
        # Start the RAFT consensus
        self.raft_consensus.start()
        
        # Start the training thread
        self.training_thread.start()
        
        # Wait for initial consensus to be established
        logging.info(f"Node {self.worker_index} waiting for initial consensus")
        # In a real implementation, we might want to add a timeout here
        # self.consensus_established_event.wait(timeout=30.0)
        
        logging.debug("Wait for the barrier!")
        comm = MPI.COMM_WORLD
        comm.Barrier()
        time.sleep(1)
        logging.debug("MPI exit barrier!")
        
        # Run the worker
        super().run()
    
    def handle_leadership_change(self, leader_id):
        """
        Handle a change in RAFT leadership.
        
        Args:
            leader_id (int): ID of the new leader
        """
        old_coordinator = self.coordinator_id
        self.coordinator_id = leader_id
        
        # If this node became the leader
        if leader_id == self.worker_index and not self.is_coordinator:
            logging.info(f"Node {self.worker_index} is now the COORDINATOR (RAFT leader)")
            self.is_coordinator = True
            
            # Start coordinator duties
            self.coodinator_thread = threading.Thread(name="coordinator", target=self.run_coordinator)
            self.coodinator_thread.start()
            self.notify_clients()
            
            # Signal that consensus is established
            self.consensus_established_event.set()
        
        # If this node is no longer the leader
        elif leader_id != self.worker_index and self.is_coordinator:
            logging.info(f"Node {self.worker_index} is no longer the COORDINATOR")
            self.is_coordinator = False
            
            # Stop coordinator duties if needed
            # This will happen naturally in the coordinator thread
        
        # If the leader changed but this node was not involved
        elif old_coordinator != leader_id:
            logging.info(f"COORDINATOR changed from {old_coordinator} to {leader_id}")
            
            # Signal that consensus is established
            self.consensus_established_event.set()
    
    def run_sync(self):
        """Run the training process synchronously with RAFT consensus."""
        try:
            # For the first iteration, wait for RAFT to establish leadership and initial topology
            self.wait_for_initial_consensus()
            
            # Get topology through RAFT
            self.topology_manager.generate_topology(t=self.global_round_idx)
            self.worker.refresh_gossip_info()
            self.refresh_gossip_info()
            
            # Reset the neighbor_hat_params for storing new values
            self.worker.init_neighbor_hat_params()
            
            for epoch in range(self.epochs):
                self.epoch = epoch
                
                # Update worker's dataset and data loader
                with raise_error_without_process():
                    self.worker.train_local.sampler.set_epoch(epoch)
                
                self.epoch_init()
                
                for iteration in range(self.worker.num_iterations):
                    self.iteration = iteration
                    logging.debug("wait start_epoch_event")
                    
                    self.start_epoch_event.wait()
                    logging.debug("Begin iteration")
                    
                    # Rest of the training loop from DecentralizedWorkerManager
                    # This is the same as in the original SAPS_FL implementation
                    # ...
                    
                    # But topology is now obtained through RAFT
                    self.topology_manager.generate_topology(t=self.global_round_idx)
                    self.worker.refresh_gossip_info()
                    self.refresh_gossip_info()
                    
                    # Report to coordinator using the RAFT leader
                    self.test_and_send_to_coordinator(iteration, epoch)
                    
                    self.start_epoch_event.clear()
                    self.sync_receive_all_event.clear()
                
        except Exception as e:
            logging.error(f"Error in run_sync: {e}")
            logging.error(traceback.format_exc())
            raise e
    
    def wait_for_initial_consensus(self):
        """Wait for initial RAFT consensus to be established."""
        # Wait until a leader is elected
        while self.raft_consensus.get_leader_id() is None:
            logging.debug(f"Node {self.worker_index} waiting for leader election")
            time.sleep(0.1)
        
        self.coordinator_id = self.raft_consensus.get_leader_id()
        logging.info(f"Initial consensus established, leader is {self.coordinator_id}")

        # If this node has not been initialized, request state and parameters
        if self.raft_consensus.raft_node.state == RaftState.INITIAL and self.worker_index != self.coordinator_id:
            self.send_state_request(self.coordinator_id)
            self.send_param_request(self.coordinator_id)

            # Wait until state is updated by the leader
            for _ in range(50):
                if self.raft_consensus.raft_node.state != RaftState.INITIAL:
                    break
                time.sleep(0.1)

        # In a real implementation, we might also wait for initial topology/bandwidth
    
    # RAFT message handlers
    
    def handle_request_vote(self, msg_params):
        """
        Handle a RAFT RequestVote message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        candidate_id = msg_params.get(RaftMessage.MSG_ARG_CANDIDATE_ID)
        last_log_index = msg_params.get(RaftMessage.MSG_ARG_LAST_LOG_INDEX)
        last_log_term = msg_params.get(RaftMessage.MSG_ARG_LAST_LOG_TERM)
        
        logging.debug(f"Received RequestVote from {sender_id}, term={term}, candidate={candidate_id}")
        
        # Process the vote request
        self.raft_consensus.handle_vote_request(candidate_id, term, last_log_index, last_log_term)
    
    def handle_vote_response(self, msg_params):
        """
        Handle a RAFT VoteResponse message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        vote_granted = msg_params.get(RaftMessage.MSG_ARG_VOTE_GRANTED)
        
        logging.debug(f"Received VoteResponse from {sender_id}, term={term}, granted={vote_granted}")
        
        # Process the vote response
        self.raft_consensus.handle_vote_response(sender_id, term, vote_granted)
    
    def handle_append_entries(self, msg_params):
        """
        Handle a RAFT AppendEntries message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        prev_log_index = msg_params.get(RaftMessage.MSG_ARG_PREV_LOG_INDEX)
        prev_log_term = msg_params.get(RaftMessage.MSG_ARG_PREV_LOG_TERM)
        entries = msg_params.get(RaftMessage.MSG_ARG_ENTRIES)
        leader_commit = msg_params.get(RaftMessage.MSG_ARG_LEADER_COMMIT)
        
        logging.debug(f"Received AppendEntries from {sender_id}, term={term}, entries={len(entries) if entries else 0}")
        
        # Process the append entries
        self.raft_consensus.handle_append_entries(sender_id, term, prev_log_index, prev_log_term, entries, leader_commit)
    
    def handle_append_response(self, msg_params):
        """
        Handle a RAFT AppendResponse message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        success = msg_params.get(RaftMessage.MSG_ARG_SUCCESS)
        match_index = msg_params.get(RaftMessage.MSG_ARG_MATCH_INDEX, 0)
        
        logging.debug(f"Received AppendResponse from {sender_id}, term={term}, success={success}")
        
        # Process the append response
        self.raft_consensus.handle_append_response(sender_id, term, success, match_index)
    
    def handle_state_snapshot(self, msg_params):
        """
        Handle a RAFT StateSnapshot message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        log = msg_params.get(RaftMessage.MSG_ARG_LOG)
        commit_index = msg_params.get(RaftMessage.MSG_ARG_COMMIT_INDEX)
        
        logging.debug(f"Received StateSnapshot from {sender_id}, term={term}, log_size={len(log) if log else 0}")
        
        # Process the state snapshot
        self.raft_consensus.handle_state_snapshot(term, log, commit_index)

    def handle_state_request(self, msg_params):
        """Handle a snapshot request from a follower."""
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        if self.raft_consensus.is_leader():
            self.send_state_snapshot(
                sender_id,
                self.raft_consensus.raft_node.current_term,
                self.raft_consensus.raft_node.log,
                self.raft_consensus.raft_node.commit_index,
            )

    def handle_param_request(self, msg_params):
        """Handle a model parameter request from a follower."""
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        if self.raft_consensus.is_leader() and hasattr(self.worker, "model_trainer"):
            params = self.worker.model_trainer.get_model_params()
            self.send_model_params(sender_id, params)

    def handle_param_response(self, msg_params):
        """Receive model parameters from the leader."""
        params = msg_params.get(RaftMessage.MSG_ARG_MODEL_PARAMS)
        if params is not None and hasattr(self.worker, "model_trainer"):
            self.worker.model_trainer.set_model_params(params)
    
    # RAFT message sending methods
    
    def send_vote_request(self, receiver_id, term, last_log_index, last_log_term):
        """
        Send a RAFT RequestVote message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            last_log_index (int): Index of last log entry
            last_log_term (int): Term of last log entry
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_REQUEST_VOTE, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_CANDIDATE_ID, self.worker_index)
        message.add_params(RaftMessage.MSG_ARG_LAST_LOG_INDEX, last_log_index)
        message.add_params(RaftMessage.MSG_ARG_LAST_LOG_TERM, last_log_term)
        
        self.send_message(message)
    
    def send_vote_response(self, receiver_id, term, vote_granted):
        """
        Send a RAFT VoteResponse message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            vote_granted (bool): Whether the vote was granted
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_VOTE_RESPONSE, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_VOTE_GRANTED, vote_granted)
        
        self.send_message(message)
    
    def send_append_entries(self, receiver_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        """
        Send a RAFT AppendEntries message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            prev_log_index (int): Index of log entry immediately preceding new ones
            prev_log_term (int): Term of prev_log_index entry
            entries (list): List of log entries to append
            leader_commit (int): Leader's commit index
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_APPEND_ENTRIES, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_PREV_LOG_INDEX, prev_log_index)
        message.add_params(RaftMessage.MSG_ARG_PREV_LOG_TERM, prev_log_term)
        message.add_params(RaftMessage.MSG_ARG_ENTRIES, entries)
        message.add_params(RaftMessage.MSG_ARG_LEADER_COMMIT, leader_commit)
        
        self.send_message(message)
    
    def send_append_response(self, receiver_id, term, success, match_index):
        """
        Send a RAFT AppendResponse message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            success (bool): Whether the append was successful
            match_index (int): Index of highest log entry known to be replicated
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_APPEND_RESPONSE, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_SUCCESS, success)
        message.add_params(RaftMessage.MSG_ARG_MATCH_INDEX, match_index)
        
        self.send_message(message)
    
    def send_state_snapshot(self, receiver_id, term, log, commit_index):
        """
        Send a RAFT StateSnapshot message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            log (list): Complete log from leader
            commit_index (int): Leader's commit index
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_STATE_SNAPSHOT, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_LOG, log)
        message.add_params(RaftMessage.MSG_ARG_COMMIT_INDEX, commit_index)

        self.send_message(message)

    def send_state_request(self, receiver_id):
        """Request the latest state snapshot from the leader."""
        message = Message(
            RaftMessage.MSG_TYPE_RAFT_STATE_REQUEST, self.get_sender_id(), receiver_id
        )
        self.send_message(message)

    def send_param_request(self, receiver_id):
        """Request model parameters from the leader."""
        message = Message(
            RaftMessage.MSG_TYPE_RAFT_PARAM_REQUEST, self.get_sender_id(), receiver_id
        )
        self.send_message(message)

    def send_model_params(self, receiver_id, params):
        """Send model parameters to a follower."""
        message = Message(
            RaftMessage.MSG_TYPE_RAFT_PARAM_RESPONSE, self.get_sender_id(), receiver_id
        )
        message.add_params(RaftMessage.MSG_ARG_MODEL_PARAMS, params)
        self.send_message(message)
    
    # Override key methods from DecentralizedWorkerManager
    
    def test_and_send_to_coordinator(self, iteration, epoch):
        """
        Test the model and send results to the coordinator.
        
        Override to use the RAFT leader as coordinator.
        
        Args:
            iteration (int): Current iteration
            epoch (int): Current epoch
        """
        # Similar to original implementation, but use RAFT leader as coordinator
        coordinator_id = self.raft_consensus.get_leader_id()
        if coordinator_id is None:
            logging.warning("No leader elected yet, using default coordinator (0)")
            coordinator_id = 0
        
        if (iteration == self.worker.num_iterations - 1) and \
           (self.epoch % self.args.frequency_of_the_test == 0 or self.epoch == self.args.epochs - 1):
            # If at the end of an epoch and testing is needed
            self.worker.test(self.epoch, self.test_tracker, self.metrics)
            
            train_metric_info, test_metric_info = get_metric_info(
                self.train_tracker, self.test_tracker, time_stamp=self.epoch, if_reset=True,
                metrics=self.metrics)
            
            if self.worker_index == coordinator_id:
                with get_lock(self.total_metric_lock):
                    self.flag_client_finish_dict[self.worker_index] = True
                    self.total_test_tracker.update_metrics(test_metric_info, test_metric_info['n_samples'])
                    self.total_train_tracker.update_metrics(train_metric_info, train_metric_info['n_samples'])
                    self.check_worker_finish_and_notify()
            else:
                self.send_notify_to_coordinator(coordinator_id, train_metric_info, test_metric_info)
        else:
            # If not testing, just reset trackers and notify
            self.reset_train_test_tracker(self.train_tracker, self.test_tracker)
            if self.worker_index == coordinator_id:
                with get_lock(self.total_metric_lock):
                    self.flag_client_finish_dict[self.worker_index] = True
                    self.check_worker_finish_and_notify()
            else:
                self.send_notify_to_coordinator(coordinator_id)
