import logging
import threading
import time
import traceback
import numpy as np

# Note: Migrated from MPI to gRPC-based communication
from fedml_core.distributed.communication.message import Message

from algorithms.SAPS_FL.decentralized_worker_manager import DecentralizedWorkerManager
from utils.context import raise_error_without_process, get_lock, raise_MPI_error
from utils.tracker import get_metric_info
from .raft_node import RaftState
from .raft_messages import RaftMessage
from .service_discovery_bridge import RaftServiceDiscoveryBridge


class RaftWorkerManager(DecentralizedWorkerManager):
    """
    Main entry point for RAFT-based federated learning nodes.
    
    This manager integrates RAFT consensus with federated learning, using the
    service discovery bridge for all dynamic membership management. All node
    joining, leaving, and cluster coordination flows through RAFT consensus.
    """
    
    def __init__(self, args, comm, node_id, size, worker, topology_manager, 
                 model_trainer, timer, metrics, raft_consensus, bandwidth_manager=None):
        """
        Initialize the RAFT worker manager.
        
        Args:
            args: Configuration parameters
            comm: Communication manager (gRPC-based)
            node_id: Unique identifier for this node in the cluster
            size: Initial cluster size (may change dynamically)
            worker: The worker for training
            topology_manager: The topology manager
            model_trainer: The model trainer
            timer: Timer for performance measurement
            metrics: Metrics for evaluation
            raft_consensus: The RAFT consensus manager
            bandwidth_manager: The bandwidth manager (optional)
        """
        # Call parent with node_id as rank for compatibility
        super().__init__(args, comm, node_id, size, worker, topology_manager, model_trainer, timer, metrics)
        
        # Ensure com_manager is available for compatibility with parent classes
        # In gRPC mode, this may be different from the traditional MPI com_manager
        if not hasattr(self, 'com_manager'):
            self.com_manager = comm  # Use the provided comm manager
        
        # Store args for later use
        self.args = args
        self.raft_consensus = raft_consensus
        self.bandwidth_manager = bandwidth_manager
        
        # Store node identification for gRPC-based communication
        self.node_id = node_id
        self.cluster_size = size  # Initial size, can change dynamically
        
        # Initialize service discovery bridge for dynamic membership management
        self.service_discovery_bridge = RaftServiceDiscoveryBridge(node_id, raft_consensus, self)
        
        # Register the bridge with this worker manager as the communication manager
        self.service_discovery_bridge.register_with_comm_manager(self)
        
        # Register the bridge with consensus for membership change notifications
        if self.raft_consensus is not None:
            self.raft_consensus.register_service_discovery_bridge(self.service_discovery_bridge)
            self.raft_consensus.on_leadership_change = self.handle_leadership_change
            self.raft_consensus.on_state_commit = self.handle_raft_state_commit
        
        # State variables for RAFT-FL integration
        self.is_coordinator = False
        self.coordinator_id = None
        self.current_topology = None
        self.current_bandwidth = None
        self.raft_round_state = None  # Track RAFT-managed round state
        
        # Thread synchronization
        self.consensus_established_event = threading.Event()
        self.topology_ready_event = threading.Event()
        self.round_start_authorized = threading.Event()
        self.round_start_event = threading.Event()  # Used to trigger next training round
        
        # Override SAPS_FL coordinator detection
        self._override_saps_coordinator_logic()
        
        logging.info(f"RaftWorkerManager initialized for node {node_id} with service discovery bridge")
    
    def get_sender_id(self):
        """Override to use node_id instead of MPI rank for gRPC communication."""
        return self.node_id
    
    def get_comm_manager(self):
        """Get the communication manager for the service discovery bridge."""
        return getattr(self, '_comm_manager', None)
    
    def set_raft_consensus(self, raft_consensus):
        """
        Set the raft_consensus reference and register callbacks after initialization.
        
        This is needed because of circular dependencies in initialization order.
        
        Args:
            raft_consensus: The RAFT consensus manager
        """
        self.raft_consensus = raft_consensus
        
        # Now register for leadership changes and state updates
        if self.raft_consensus is not None:
            self.raft_consensus.on_leadership_change = self.handle_leadership_change
            self.raft_consensus.on_state_commit = self.handle_raft_state_commit
            logging.info(f"RAFT consensus callbacks registered for node {self.node_id}")
    
    def register_message_receive_handlers(self):
        """Register message handlers for RAFT and GossipFL messages."""
        # First register handlers from the parent class
        super().register_message_receive_handlers()
        
        # Register RAFT message handlers
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_REQUEST_VOTE,
            self.handle_request_vote)

        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_PREVOTE_REQUEST,
            self.handle_prevote_request)

        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_VOTE_RESPONSE,
            self.handle_vote_response)

        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_PREVOTE_RESPONSE,
            self.handle_prevote_response)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_APPEND_ENTRIES,
            self.handle_append_entries)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_APPEND_RESPONSE,
            self.handle_append_response)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_STATE_SNAPSHOT,
            self.handle_state_snapshot)
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_INSTALL_SNAPSHOT,
            self.handle_install_snapshot)

        # Enhanced message handlers for improved node joining
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_LEADER_REDIRECT,
            self.handle_leader_redirect)

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
        
        # Join protocol handlers
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_JOIN_REQUEST,
            self.handle_join_request)
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_RAFT_JOIN_RESPONSE,
            self.handle_join_response)
    
    def run(self):
        """Run the worker manager with RAFT integration and service discovery bridge."""

        # Start RAFT consensus services
        self.raft_consensus.start()

        # If joining an existing cluster, use the service discovery bridge
        if getattr(self.args, "join_existing_cluster", False):
            logging.info("Joining existing cluster through service discovery bridge")
            self.service_discovery_bridge.handle_node_discovered(self.node_id)
            
            # Wait for synchronization to complete (best effort)
            max_wait_time = 30
            wait_start = time.time()
            while not self.raft_consensus.is_state_synchronized() and (
                time.time() - wait_start
            ) < max_wait_time:
                time.sleep(1)

            if not self.raft_consensus.is_state_synchronized():
                logging.warning(
                    "Failed to synchronize state within timeout - proceeding anyway"
                )
            else:
                logging.info("Successfully synchronized with existing cluster")

        # Start the training thread
        self.training_thread.start()

        # Wait for all peers to be ready using RAFT consensus
        logging.debug("Wait for consensus establishment!")
        # In gRPC mode, we rely on RAFT consensus rather than MPI barriers
        self.consensus_established_event.wait(timeout=30)
        time.sleep(1)
        logging.debug("Consensus established, proceeding!")

        # Hand off to the parent run loop
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
        if leader_id == self.node_id and not self.is_coordinator:
            logging.info(f"Node {self.node_id} is now the COORDINATOR (RAFT leader)")
            self.is_coordinator = True
            
            # Start coordinator duties
            self.coodinator_thread = threading.Thread(name="coordinator", target=self.run_coordinator)
            self.coodinator_thread.start()
            self.notify_clients()
            
            # Signal that consensus is established
            self.consensus_established_event.set()
        
        # If this node is no longer the leader
        elif leader_id != self.node_id and self.is_coordinator:
            logging.info(f"Node {self.node_id} is no longer the COORDINATOR")
            self.is_coordinator = False
            
            # Stop coordinator duties if needed
            # This will happen naturally in the coordinator thread
        
        # If the leader changed but this node was not involved
        elif old_coordinator != leader_id:
            logging.info(f"COORDINATOR changed from {old_coordinator} to {leader_id}")
            
            # Signal that consensus is established
            self.consensus_established_event.set()
    
    def on_membership_change(self, new_nodes, round_num=0):
        """
        Handle notification of membership changes from the consensus manager.
        
        This method is called by the service discovery bridge when membership changes
        are committed through RAFT consensus.
        
        Args:
            new_nodes (set): The updated set of known nodes
            round_num (int): The current training round number
        """
        with raise_MPI_error():
            try:
                old_nodes = self.topology_manager.get_neighbor_list() if hasattr(self, 'topology_manager') else set()
                
                # Log significant changes
                added = [n for n in new_nodes if n not in old_nodes]
                removed = [n for n in old_nodes if n not in new_nodes]
                
                if added or removed:
                    logging.info(f"Node {self.node_id}: Membership change at round {round_num}:" +
                                f" Added={added}, Removed={removed}")
                    
                    # Update topology if needed
                    if hasattr(self, 'topology_manager'):
                        self.topology_manager.update_nodes(new_nodes)
                        
                        # If this node is leader/coordinator, propagate updated topology
                        if self.is_coordinator:
                            self.update_topology_consensus()
                    
                    # Notify the service discovery bridge about the membership change
                    if self.service_discovery_bridge:
                        self.service_discovery_bridge.on_membership_change(new_nodes)
                        
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error handling membership change: {e}", exc_info=True)
    
    def on_coordinator_change(self, new_coordinator, old_coordinator=None, round_num=0, reason='unspecified'):
        """
        Handle notification of coordinator changes from the consensus manager.
        
        Args:
            new_coordinator (int): The ID of the new coordinator
            old_coordinator (int): The ID of the previous coordinator (may be None)
            round_num (int): The current training round number
            reason (str): The reason for the coordinator change
        """
        with raise_MPI_error():
            try:
                # This method is called when a coordinator change is applied from a log entry
                # It should NOT start the coordinator process directly, as that's handled by on_become_coordinator
                
                # Update our tracking
                self.coordinator_id = new_coordinator
                logging.info(f"Node {self.node_id}: Coordinator changed from {old_coordinator} to {new_coordinator} " +
                           f"at round {round_num} (reason: {reason})")
                
                # Update any dependent components
                if hasattr(self, 'worker') and self.worker is not None:
                    self.worker.set_coordinator(new_coordinator)
                    
                # Signal that consensus is established
                self.consensus_established_event.set()
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error handling coordinator change: {e}", exc_info=True)
    
    def on_become_coordinator(self, round_num=0):
        """
        Handle notification that this node has become the coordinator.
        
        This method is called when this node is designated as coordinator and should
        trigger the start of a training round.
        
        Args:
            round_num (int): The current training round number
        """
        with raise_MPI_error():
            try:
                if not self.is_coordinator:
                    # Update state
                    old_state = self.is_coordinator
                    self.is_coordinator = True
                    self.coordinator_id = self.node_id
                    
                    logging.info(f"Node {self.node_id}: Becoming coordinator for round {round_num}")
                    
                    # If not already running the coordinator thread, start it
                    if old_state != self.is_coordinator:
                        self.coodinator_thread = threading.Thread(name="coordinator", target=self.run_coordinator)
                        self.coodinator_thread.start()
                    
                    # Notify clients about the new coordinator
                    self.notify_clients()
                    
                    # Trigger the start of a training round if we're already in training mode
                    if hasattr(self, 'training_thread') and self.training_thread is not None and self.training_thread.is_alive():
                        # Signal to the training thread that it should proceed with the next round
                        logging.info(f"Node {self.node_id}: Triggering next training round as coordinator")
                        if hasattr(self, 'round_start_event'):
                            self.round_start_event.set()
            except Exception as e:
                logging.error(f"Node {self.node_id}: Error becoming coordinator: {e}", exc_info=True)
    
    def run_sync(self):
        """Run the training process synchronously with RAFT consensus."""
        with raise_MPI_error():
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
                    # Fix: Check if sampler has set_epoch method before calling
                    if hasattr(self.worker.train_local.sampler, 'set_epoch'):
                        self.worker.train_local.sampler.set_epoch(epoch)
                
                self.epoch_init()
                
                for iteration in range(self.worker.num_iterations):
                    self.iteration = iteration
                    logging.debug("wait start_epoch_event")
                    
                    self.start_epoch_event.wait()
                    logging.debug("Begin iteration")
                    
                    # Get model params
                    from utils.data_utils import get_data
                    from utils.tensor_buffer import TensorBuffer
                    params, _ = get_data(
                        self.worker.param_groups, self.worker.param_names, is_get_grad=False
                    )
                    flatten_params = TensorBuffer(params)

                    # compress
                    sync_buffer = {
                        "original_shapes": self.worker.shapes,
                        "flatten_params": flatten_params,
                    }
                    self.compressor.compress(sync_buffer)
                    self.selected_shapes = sync_buffer["selected_shapes"]

                    # begin to send model
                    logging.debug("Begin send and receive")
                    logging.debug(self.topology_manager.topology)
                    for neighbor_idx in self.topology_manager.get_out_neighbor_idx_list(self.node_id):
                        if self.compression in ["randomk", "topk"]:
                            self.send_sparse_params_to_neighbors(neighbor_idx, 
                                sync_buffer["flatten_selected_values"].buffer.cpu(),
                                sync_buffer["flatten_selected_indices"].buffer.cpu(),
                                self.worker.get_dataset_len())
                        elif self.compression == "quantize":
                            self.send_quant_params_to_neighbors(neighbor_idx,
                                sync_buffer["flatten_quantized_values"].buffer.cpu(),
                                self.worker.get_dataset_len())
                        elif self.compression == "sign":
                            self.send_sign_params_to_neighbors(neighbor_idx,
                                sync_buffer["flatten_sign_values"].buffer.cpu(),
                                self.worker.get_dataset_len())
                        else:
                            # For no compression, send the full parameters
                            self.send_result_to_neighbors(neighbor_idx,
                                sync_buffer["flatten_params"].buffer.cpu(),
                                self.worker.get_dataset_len())
                    
                    # wait for receiving all
                    self.sync_receive_all_event.wait()
                    self.worker.aggregate(self.compressor, self.selected_shapes, self.gossip_info)

                    # Get weighted hat params and apply the local gradient.
                    self.neighbor_transfer_lock.acquire()

                    # add the sparsed part
                    self.compressor.uncompress_direct(
                        sync_buffer, self.worker.neighbor_hat_params["memory"],
                        self.selected_shapes, self.worker.shapes)
                    sync_buffer["flatten_params"].unpack(params)

                    if self.neighbor_transfer_lock.locked():
                        self.neighbor_transfer_lock.release()

                    # Handle communication failures
                    import numpy as np
                    if self.args.Failure_chance is not None and np.random.rand(1) < self.args.Failure_chance:
                        logging.info("Communication Failure happens on worker: {}, Failure_chance: {}".format(
                            self.node_id, self.args.Failure_chance))
                    else:
                        self.lr_schedule(self.epoch, self.iteration, self.global_round_idx,
                                        self.worker.num_iterations, self.args.warmup_epochs)
                        # update x_half to x_{t+1} by SGD
                        loss, output, target \
                            = self.worker.train_one_step(self.epoch, self.iteration,
                                                            self.train_tracker, self.metrics)

                    self.start_epoch_event.clear()
                    self.sync_receive_all_event.clear()

                    """
                        Before send msg to coordinator,
                        generate topology firstly through RAFT consensus.
                    """
                    self.topology_manager.generate_topology(t=self.global_round_idx)
                    self.worker.refresh_gossip_info()
                    self.refresh_gossip_info()
                    # reset the neighbor_hat_params for storing new values
                    self.worker.init_neighbor_hat_params()

                    # Report to coordinator using the RAFT leader
                    self.test_and_send_to_coordinator(iteration, epoch)
    
    def wait_for_initial_consensus(self):
        """
        Wait for initial RAFT consensus to be established.
        
        Uses the service discovery bridge for proper cluster joining.
        """
        # Wait until a leader is elected
        while self.raft_consensus.get_leader_id() is None:
            logging.debug(f"Node {self.node_id} waiting for leader election")
            time.sleep(0.1)
        
        self.coordinator_id = self.raft_consensus.get_leader_id()
        logging.info(f"Initial consensus established, leader is {self.coordinator_id}")

        # If this node needs to join the cluster, use the service discovery bridge
        if (self.raft_consensus.raft_node.state == RaftState.INITIAL and 
            self.node_id != self.coordinator_id):
            
            logging.info(f"Node {self.node_id} joining cluster through service discovery bridge")
            
            # Use the bridge to handle joining
            node_info = {
                'node_id': self.node_id,
                'ip_address': getattr(self.args, 'ip_address', 'localhost'),
                'port': getattr(self.args, 'port', 8080),
                'capabilities': ['grpc', 'fedml'],
                'timestamp': time.time()
            }
            
            if self.service_discovery_bridge:
                self.service_discovery_bridge.handle_node_discovered(self.node_id, node_info)
            
            # Wait for state synchronization to complete
            max_wait_rounds = 100  # Allow more time for full synchronization
            for wait_round in range(max_wait_rounds):
                # Check if we've received and processed the state snapshot
                if (self.raft_consensus.raft_node.state != RaftState.INITIAL and
                    self.raft_consensus.raft_node.commit_index > 0):
                    logging.info(f"Node {self.node_id} successfully synchronized "
                               f"(state: {self.raft_consensus.raft_node.state}, "
                               f"commit_index: {self.raft_consensus.raft_node.commit_index})")
                    break
                time.sleep(0.1)
            else:
                logging.warning(f"Node {self.node_id} state synchronization may be incomplete")

        # Ensure topology and bandwidth are also synchronized
        # These should be updated through the state snapshot, but verify
        if self.topology_manager.get_topology() is None:
            logging.info("Topology not yet available, waiting...")
            for _ in range(50):
                if self.topology_manager.get_topology() is not None:
                    break
                time.sleep(0.1)
                
        if self.bandwidth_manager and self.bandwidth_manager.get_bandwidth() is None:
            logging.info("Bandwidth not yet available, waiting...")
            for _ in range(50):
                if self.bandwidth_manager.get_bandwidth() is not None:
                    break
                time.sleep(0.1)

        logging.info(f"Node {self.node_id} is ready to start training")
    
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

    def handle_prevote_request(self, msg_params):
        """Handle a RAFT PreVote request message."""
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        candidate_id = msg_params.get(RaftMessage.MSG_ARG_CANDIDATE_ID)
        last_log_index = msg_params.get(RaftMessage.MSG_ARG_LAST_LOG_INDEX)
        last_log_term = msg_params.get(RaftMessage.MSG_ARG_LAST_LOG_TERM)

        logging.debug(
            f"Received PreVoteRequest from {sender_id}, term={term}, candidate={candidate_id}"
        )

        self.raft_consensus.handle_prevote_request(
            candidate_id, term, last_log_index, last_log_term
        )
    
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

    def handle_prevote_response(self, msg_params):
        """Handle a RAFT PreVote response message."""
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        prevote_granted = msg_params.get(RaftMessage.MSG_ARG_VOTE_GRANTED)

        logging.debug(
            f"Received PreVoteResponse from {sender_id}, term={term}, granted={prevote_granted}"
        )

        self.raft_consensus.handle_prevote_response(sender_id, term, prevote_granted)
    
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
        match_index = msg_params.get(RaftMessage.MSG_ARG_MATCH_INDEX)
        
        logging.debug(f"Received AppendResponse from {sender_id}, term={term}, success={success}")
        
        # Process the append response
        self.raft_consensus.handle_append_response(sender_id, term, success, match_index)
    
    def handle_state_snapshot(self, msg_params):
        """
        Handle a RAFT StateSnapshot message.
        
        Enhanced to process comprehensive state packages for proper node joining.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        log = msg_params.get(RaftMessage.MSG_ARG_LOG)
        commit_index = msg_params.get(RaftMessage.MSG_ARG_COMMIT_INDEX)
        state_package = msg_params.get(RaftMessage.MSG_ARG_STATE_PACKAGE)
        
        logging.debug(f"Received StateSnapshot from {sender_id}, term={term}, "
                     f"log_size={len(log) if log else 0}, commit_index={commit_index}")
        
        # First, process the basic RAFT state snapshot
        self.raft_consensus.handle_state_snapshot(term, log, commit_index)
        
        # If there's a comprehensive state package, initialize from it
        if state_package is not None:
            logging.info(f"Processing comprehensive state package from {sender_id}")
            success = self.initialize_from_state_snapshot(state_package)
            if success:
                logging.info(f"Successfully joined cluster via state snapshot from {sender_id}")
            else:
                logging.error(f"Failed to process state package from {sender_id}")

    def handle_install_snapshot(self, msg_params):
        """Handle an InstallSnapshot message from the leader."""
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.MSG_ARG_TERM)
        last_idx = msg_params.get(RaftMessage.MSG_ARG_LAST_INCLUDED_INDEX)
        last_term = msg_params.get(RaftMessage.MSG_ARG_LAST_INCLUDED_TERM)
        offset = msg_params.get(RaftMessage.MSG_ARG_OFFSET)
        data = msg_params.get(RaftMessage.MSG_ARG_DATA)
        done = msg_params.get(RaftMessage.MSG_ARG_DONE)

        logging.debug(
            f"Received InstallSnapshot from {sender_id}, term={term}, index={last_idx}, offset={offset}"
        )

        self.raft_consensus.handle_install_snapshot(
            sender_id, term, last_idx, last_term, offset, data, done
        )

    def handle_leader_redirect(self, msg_params):
        """
        Handle a leader redirect message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        leader_id = msg_params.get(RaftMessage.MSG_ARG_LEADER_ID)
        
        logging.info(f"Received leader redirect from {sender_id}, leader is {leader_id}")
        
        # Update our knowledge of the current leader and request state from them
        if leader_id is not None and leader_id != self.node_id:
            self.coordinator_id = leader_id
            self.send_state_request(leader_id)

    def handle_state_request(self, msg_params):
        """
        Handle a state request from a follower.
        
        This method is enhanced to ensure proper RAFT-based state synchronization
        for new nodes joining the cluster.
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        
        if not self.raft_consensus.is_leader():
            # If we're not the leader, redirect to the current leader
            leader_id = self.raft_consensus.get_leader_id()
            if leader_id is not None:
                logging.info(f"Redirecting state request from {sender_id} to leader {leader_id}")
                self.send_leader_redirect(sender_id, leader_id)
            else:
                logging.warning(f"No leader available for state request from {sender_id}")
            return
        
        # As the leader, send comprehensive state snapshot
        logging.info(f"Sending RAFT-synchronized state snapshot to node {sender_id}")
        self.send_comprehensive_state_snapshot(
            sender_id,
            self.raft_consensus.raft_node.current_term,
            self.raft_consensus.raft_node.log,
            self.raft_consensus.raft_node.commit_index,
        )

    def handle_param_request(self, msg_params):
        """
        Handle a model parameter request from a follower.
        
        Enhanced to ensure parameter consistency with RAFT state.
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        
        if not self.raft_consensus.is_leader():
            # Redirect to leader for consistent parameter state
            leader_id = self.raft_consensus.get_leader_id()
            if leader_id is not None:
                logging.info(f"Redirecting param request from {sender_id} to leader {leader_id}")
                self.send_leader_redirect(sender_id, leader_id)
            return
        
        # Send parameters with RAFT state metadata for consistency checking
        if hasattr(self.worker, "model_trainer"):
            params = self.worker.model_trainer.get_model_params()
            # Include RAFT state metadata for consistency checking
            raft_metadata = {
                'term': self.raft_consensus.raft_node.current_term,
                'commit_index': self.raft_consensus.raft_node.commit_index,
                'leader_id': self.node_id
            }
            self.send_model_params_with_metadata(sender_id, params, raft_metadata)

    def handle_param_response(self, msg_params):
        """
        Receive model parameters from the leader.
        
        Enhanced to validate RAFT consistency before applying parameters.
        """
        params = msg_params.get(RaftMessage.MSG_ARG_MODEL_PARAMS)
        raft_metadata = msg_params.get(RaftMessage.MSG_ARG_RAFT_METADATA)
        
        if params is not None and hasattr(self.worker, "model_trainer"):
            # Validate RAFT consistency before applying parameters
            sender_term = raft_metadata.get('term', 0)
            sender_commit_index = raft_metadata.get('commit_index', 0)
            
            if self.validate_raft_consistency(sender_term, sender_commit_index):
                self.worker.model_trainer.set_model_params(params)
                logging.info(f"Applied model parameters from leader "
                           f"(term: {sender_term}, commit_index: {sender_commit_index})")
            else:
                logging.warning(f"Rejected model parameters due to RAFT inconsistency "
                              f"(term: {sender_term}, commit_index: {sender_commit_index})")
                # Request fresh state from current leader
                leader_id = self.raft_consensus.get_leader_id()
                if leader_id is not None:
                    self.send_state_request(leader_id)
    
    # Join protocol handlers
    
    def handle_join_request(self, msg_params):
        """
        Handle join request messages from new nodes.
        
        Args:
            msg_params: Message parameters containing join request data
        """
        try:
            # Extract sender ID from message parameters using standardized method
            sender_id = msg_params.get_sender_id()
            node_info = msg_params.get(RaftMessage.MSG_ARG_NODE_INFO)
            
            logging.info(f"Received join request from node {sender_id}")
            
            # Forward to the service discovery bridge
            if self.service_discovery_bridge:
                success = self.service_discovery_bridge.handle_join_request(sender_id, node_info)
                
                # Send join response back to the requesting node
                if sender_id is not None:
                    leader_id = self.node_id if self.is_coordinator else self.coordinator_id
                    self.send_join_response(sender_id, success, leader_id)
        
        except Exception as e:
            traceback.print_exc()
            logging.error(f"Error handling join request: {e}")
    
    def handle_join_response(self, msg_params):
        """
        Handle a join response from the cluster.
        
        This method processes join responses and initiates proper state synchronization.
        """
        sender_id = msg_params.get_sender_id()
        success = msg_params.get(RaftMessage.MSG_ARG_SUCCESS)
        leader_id = msg_params.get(RaftMessage.MSG_ARG_LEADER_ID)
        error_msg = msg_params.get(RaftMessage.MSG_ARG_ERROR_MSG)
        
        if success and leader_id is not None:
            logging.info(f"Join request accepted, leader is {leader_id}")
            self.coordinator_id = leader_id
            
            # Transition from INITIAL to FOLLOWER state
            if self.raft_consensus.raft_node.state == RaftState.INITIAL:
                self.raft_consensus.raft_node.state = RaftState.FOLLOWER
                self.raft_consensus.raft_node.current_term = 0
                self.raft_consensus.raft_node.voted_for = None
                self.raft_consensus.raft_node.reset_election_timeout()
                logging.info(f"Node {self.node_id} transitioned from INITIAL to FOLLOWER after successful join")
            
            # Request state synchronization from leader
            if leader_id != self.node_id:
                self.send_state_request(leader_id)
        else:
            logging.error(f"Join request rejected by {sender_id}: {error_msg}")
            # Could implement retry logic here

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
        message.add_params(RaftMessage.MSG_ARG_CANDIDATE_ID, self.node_id)
        message.add_params(RaftMessage.MSG_ARG_LAST_LOG_INDEX, last_log_index)
        message.add_params(RaftMessage.MSG_ARG_LAST_LOG_TERM, last_log_term)
        
        self.send_message(message)

    def send_prevote_request(self, receiver_id, term, last_log_index, last_log_term):
        """Send a RAFT PreVote request message."""
        message = Message(RaftMessage.MSG_TYPE_RAFT_PREVOTE_REQUEST, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_CANDIDATE_ID, self.node_id)
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

    def send_prevote_response(self, receiver_id, term, prevote_granted):
        """Send a RAFT PreVote response message."""
        message = Message(
            RaftMessage.MSG_TYPE_RAFT_PREVOTE_RESPONSE, self.get_sender_id(), receiver_id
        )
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_VOTE_GRANTED, prevote_granted)

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

    def send_install_snapshot(
        self,
        receiver_id,
        term,
        last_included_index,
        last_included_term,
        offset,
        data,
        done,
    ):
        """Send a RAFT InstallSnapshot message."""
        message = Message(
            RaftMessage.MSG_TYPE_RAFT_INSTALL_SNAPSHOT,
            self.get_sender_id(),
            receiver_id,
        )
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(
            RaftMessage.MSG_ARG_LAST_INCLUDED_INDEX, last_included_index
        )
        message.add_params(
            RaftMessage.MSG_ARG_LAST_INCLUDED_TERM, last_included_term
        )
        message.add_params(RaftMessage.MSG_ARG_OFFSET, offset)
        message.add_params(RaftMessage.MSG_ARG_DATA, data)
        message.add_params(RaftMessage.MSG_ARG_DONE, done)

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
    
    def send_join_request(self, receiver_id, node_info=None):
        """
        Send a join request to a node in the cluster.
        
        Args:
            receiver_id (int): ID of the receiver
            node_info (dict): Optional node information
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_JOIN_REQUEST, self.get_sender_id(), receiver_id)
        if node_info:
            message.add_params(RaftMessage.MSG_ARG_NODE_INFO, node_info)
        self.send_message(message)
    
    def send_join_response(self, receiver_id, success, leader_id=None, error_msg=None):
        """
        Send a join response to a requesting node.
        
        Args:
            receiver_id (int): ID of the receiver
            success (bool): Whether the join was successful
            leader_id (int): ID of the current leader (if known)
            error_msg (str): Error message if join failed
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_JOIN_RESPONSE, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_SUCCESS, success)
        if leader_id is not None:
            message.add_params(RaftMessage.MSG_ARG_LEADER_ID, leader_id)
        if error_msg:
            message.add_params(RaftMessage.MSG_ARG_ERROR_MSG, error_msg)
        self.send_message(message)
    
    # Enhanced RAFT state synchronization methods
    
    def send_comprehensive_state_snapshot(self, receiver_id, term, log, commit_index):
        """
        Send a comprehensive state snapshot including all necessary data for a new node.
        
        This ensures that joining nodes get the complete, consistent state including:
        - RAFT log and term information
        - Model parameters
        - Topology configuration  
        - Bandwidth configuration
        - Current training round information
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current RAFT term
            log (list): Complete RAFT log
            commit_index (int): Leader's commit index
        """
        # Get current model parameters if available
        model_params = None
        if hasattr(self.worker, "model_trainer"):
            model_params = self.worker.model_trainer.get_model_params()
        
        # Get latest topology and bandwidth from their respective managers
        topology = self.topology_manager.get_topology()
        bandwidth = self.bandwidth_manager.get_bandwidth()
        
        # Package comprehensive state
        state_package = {
            'raft_term': term,
            'raft_log': log,
            'raft_commit_index': commit_index,
            'model_params': model_params,
            'topology': topology.tolist() if topology is not None else None,
            'bandwidth': bandwidth.tolist() if bandwidth is not None else None,
            'current_round': getattr(self, 'epoch', 0),
            'leader_id': self.node_id,
            'timestamp': time.time()
        }
        
        message = Message(RaftMessage.MSG_TYPE_RAFT_STATE_SNAPSHOT, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_TERM, term)
        message.add_params(RaftMessage.MSG_ARG_LOG, log)
        message.add_params(RaftMessage.MSG_ARG_COMMIT_INDEX, commit_index)
        message.add_params(RaftMessage.MSG_ARG_STATE_PACKAGE, state_package)

        self.send_message(message)
        logging.info(f"Sent comprehensive state snapshot to node {receiver_id} "
                    f"(term: {term}, commit_index: {commit_index})")

    def send_leader_redirect(self, receiver_id, leader_id):
        """
        Send a redirect message to inform a node about the current leader.
        
        Args:
            receiver_id (int): ID of the node to redirect
            leader_id (int): ID of the current leader
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_LEADER_REDIRECT, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_LEADER_ID, leader_id)
        self.send_message(message)

    def send_model_params_with_metadata(self, receiver_id, params, raft_metadata):
        """
        Send model parameters with RAFT consistency metadata.
        
        Args:
            receiver_id (int): ID of the receiver
            params: Model parameters
            raft_metadata (dict): RAFT state metadata for consistency validation
        """
        message = Message(RaftMessage.MSG_TYPE_RAFT_PARAM_RESPONSE, self.get_sender_id(), receiver_id)
        message.add_params(RaftMessage.MSG_ARG_MODEL_PARAMS, params)
        message.add_params(RaftMessage.MSG_ARG_RAFT_METADATA, raft_metadata)
        self.send_message(message)

    def validate_raft_consistency(self, sender_term, sender_commit_index):
        """
        Validate RAFT consistency before applying received state.
        
        Args:
            sender_term (int): Term from the sender
            sender_commit_index (int): Commit index from the sender
            
        Returns:
            bool: True if the state is consistent and should be applied
        """
        current_term = self.raft_consensus.raft_node.current_term
        current_commit_index = self.raft_consensus.raft_node.commit_index
        
        # Accept if sender has higher or equal term and commit index
        if sender_term >= current_term and sender_commit_index >= current_commit_index:
            return True
        
        # If terms are equal, accept if commit index is not too far behind
        if sender_term == current_term and abs(sender_commit_index - current_commit_index) <= 5:
            return True
            
        return False

    def initialize_from_state_snapshot(self, state_package):
        """
        Initialize node state from a comprehensive state snapshot.
        
        This method is called when a node receives a complete state package
        and needs to synchronize its state with the cluster.
        
        Args:
            state_package (dict): Complete state information from leader
        """
        try:
            # Update RAFT state first
            raft_term = state_package.get('raft_term', 0)
            raft_log = state_package.get('raft_log', [])
            raft_commit_index = state_package.get('raft_commit_index', 0)
            
            # Apply RAFT state through consensus manager
            self.raft_consensus.handle_state_snapshot(raft_term, raft_log, raft_commit_index)
            
            # Update model parameters if available
            model_params = state_package.get('model_params')
            if model_params is not None and hasattr(self.worker, "model_trainer"):
                self.worker.model_trainer.set_model_params(model_params)
                logging.info("Applied model parameters from state snapshot")
            
            # Update topology if available
            topology_data = state_package.get('topology')
            if topology_data is not None:
                topology = np.array(topology_data)
                round_num = state_package.get('current_round', 0)
                # Update topology manager directly using the provided matrix
                self.topology_manager.update_topology_matrix(topology, round_num)
                logging.info("Applied topology from state snapshot")
            
            # Update bandwidth if available
            bandwidth_data = state_package.get('bandwidth')
            if bandwidth_data is not None:
                bandwidth = np.array(bandwidth_data)
                ts = state_package.get('timestamp', 0)
                self.bandwidth_manager.apply_bandwidth_update({
                    'timestamp': ts,
                    'matrix': bandwidth
                })
                logging.info("Applied bandwidth from state snapshot")
            
            # Update training round information
            current_round = state_package.get('current_round', 0)
            if hasattr(self, 'epoch'):
                self.epoch = current_round
                logging.info(f"Synchronized to training round {current_round}")
            
            logging.info(f"Successfully initialized from state snapshot "
                        f"(term: {raft_term}, commit_index: {raft_commit_index})")
            
            return True
            
        except Exception as e:
            logging.error(f"Failed to initialize from state snapshot: {str(e)}")
            return False

    def request_cluster_join(self, leader_id=None):
        """
        Request to join the cluster using the service discovery bridge.
        
        This method uses the service discovery bridge to properly join the cluster
        through RAFT consensus rather than direct state requests.
        
        Args:
            leader_id (int, optional): ID of the known leader. If None, will discover.
        """
        if self.service_discovery_bridge:
            # Use the bridge to handle joining
            node_info = {
                'node_id': self.node_id,
                'ip_address': getattr(self.args, 'ip_address', 'localhost'),
                'port': getattr(self.args, 'port', 8080),
                'capabilities': ['grpc', 'fedml'],
                'timestamp': time.time()
            }
            
            # The bridge will handle the join request and state synchronization
            self.service_discovery_bridge.handle_node_discovered(self.node_id, node_info)
            
            logging.info(f"Requested cluster join through service discovery bridge")
            return True
        else:
            # Fallback to direct join request
            if leader_id is None:
                leader_id = self.raft_consensus.get_leader_id()
            
            if leader_id is None:
                # Broadcast join request to all known nodes
                logging.info("Broadcasting join request to all peers")
                for peer_id in range(self.worker_num):
                    if peer_id != self.node_id:
                        self.send_join_request(peer_id)
            else:
                # Send join request to the leader
                logging.info(f"Sending join request to leader {leader_id}")
                self.send_join_request(leader_id)
            
            return True

    def _override_saps_coordinator_logic(self):
        """
        Override SAPS_FL's hardcoded coordinator logic to use RAFT leader election.
        """
        # Patch the parent class's run method to use RAFT leadership
        original_run = super().run
        
        def raft_enabled_run():
            # Start RAFT consensus first
            self.raft_consensus.start()
            
            # Wait for leadership establishment instead of hardcoded coordinator
            self.wait_for_initial_consensus()
            
            # Start training thread
            self.training_thread.start()
            
            # gRPC-based synchronization using RAFT consensus
            logging.debug("Wait for consensus establishment!")
            # In gRPC mode, we rely on RAFT consensus rather than MPI barriers  
            self.consensus_established_event.wait(timeout=30)
            time.sleep(1)
            logging.debug("Consensus established, proceeding!")
            
            # If this node is the RAFT leader, start coordinator duties
            if self.is_coordinator:
                logging.debug("RAFT LEADER notify clients to start!")
                self.coodinator_thread.start()
                self.notify_clients()
            
            # Continue with normal message loop
            super(DecentralizedWorkerManager, self).run()
        
        # Replace the run method
        self.run = raft_enabled_run
    
    def handle_raft_state_commit(self, log_entry):
        """
        Handle committed log entries from RAFT.
        
        Args:
            log_entry: The committed log entry containing state updates
        """
        if log_entry.get('type') == 'topology_update':
            topology_data = log_entry.get('topology')
            bandwidth_data = log_entry.get('bandwidth')
            round_num = log_entry.get('round')
            
            logging.info(f"Applying RAFT topology update for round {round_num}")
            
            # Update topology manager with RAFT-committed topology
            self.topology_manager.topology = topology_data
            self.topology_manager.bandwidth = bandwidth_data
            self.current_topology = topology_data
            self.current_bandwidth = bandwidth_data
            
            # Signal that topology is ready
            self.topology_ready_event.set()
            
        elif log_entry.get('type') == 'round_start':
            round_num = log_entry.get('round')
            
            logging.info(f"RAFT authorized round {round_num} start")
            self.raft_round_state = log_entry
            self.round_start_authorized.set()
            
        elif log_entry.get('type') == 'member_change':
            # Handle dynamic membership changes
            new_members = log_entry.get('members')
            logging.info(f"RAFT membership change: {new_members}")
            self.handle_membership_change(new_members)
    
    def handle_membership_change(self, new_members):
        """
        Handle dynamic changes in cluster membership.
        
        Args:
            new_members: List of active member IDs
        """
        # Update worker number and related state
        old_worker_number = self.size
        new_worker_number = len(new_members)
        
        if old_worker_number != new_worker_number:
            logging.info(f"Cluster size changed from {old_worker_number} to {new_worker_number}")
            
            # Update size
            self.size = new_worker_number
            self.worker_number = new_worker_number
            
            # Notify topology manager of membership change
            if hasattr(self.topology_manager, 'update_membership'):
                self.topology_manager.update_membership(new_members)
            
            # Regenerate topology if we're the leader
            if self.is_coordinator:
                self.propose_topology_update(force_regenerate=True)

    def refresh_gossip_info(self):
        """Refresh local gossip information from the topology manager."""
        self.neighbors_info = self.topology_manager.topology
        self.gossip_info = self.topology_manager.topology[self.node_id]

    def lr_schedule(self, epoch, iteration, round_idx, num_iterations, warmup_epochs):
        """Delegate learning rate scheduling to the parent implementation."""
        super().lr_schedule(epoch, iteration, round_idx, num_iterations, warmup_epochs)

    def send_notify_to_coordinator(self, receive_id=None, train_metric_info=None, test_metric_info=None):
        """Send a notification to the current RAFT leader."""
        if receive_id is None:
            receive_id = self.coordinator_id if self.coordinator_id is not None else 0
        super().send_notify_to_coordinator(receive_id, train_metric_info, test_metric_info)

    def cleanup(self):
        """Clean up resources when shutting down."""
        try:
            # Stop the service discovery bridge
            if self.service_discovery_bridge:
                self.service_discovery_bridge.stop()
                logging.info("Service discovery bridge stopped")
            
            # Unregister the bridge from consensus
            if self.raft_consensus:
                self.raft_consensus.unregister_service_discovery_bridge()
                logging.info("Service discovery bridge unregistered")
                
            # Stop RAFT consensus
            if self.raft_consensus:
                self.raft_consensus.stop()
                logging.info("RAFT consensus stopped")
                
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")
    
    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.cleanup()
        except Exception:
            pass  # Don't raise exceptions in destructor
    
    def send_message_to_node(self, node_id, message):
        """
        Send a message to a specific node.
        
        This method is used by the service discovery bridge to communicate with nodes.
        
        Args:
            node_id (int): ID of the target node
            message (dict): Message to send
        """
        try:
            # Convert dict message to fedml Message format
            msg_type = message.get('type', 'unknown')
            fedml_msg = Message(msg_type, self.get_sender_id(), node_id)
            
            # Add all message parameters
            for key, value in message.items():
                if key != 'type':
                    fedml_msg.add_params(key, value)
            
            self.send_message(fedml_msg)
            logging.debug(f"Sent message {msg_type} to node {node_id}")
            
        except Exception as e:
            logging.error(f"Error sending message to node {node_id}: {e}")
    
    def broadcast_message(self, message):
        """
        Broadcast a message to all known nodes.
        
        This method is used by the service discovery bridge.
        
        Args:
            message (dict): Message to broadcast
        """
        try:
            known_nodes = self.raft_consensus.get_known_nodes()
            for node_id in known_nodes:
                if node_id != self.node_id:
                    self.send_message_to_node(node_id, message)
            
        except Exception as e:
            logging.error(f"Error broadcasting message: {e}")
