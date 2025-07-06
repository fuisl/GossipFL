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
        
        # Also store it as _comm_manager for consistent access through get_comm_manager()
        self._comm_manager = comm
        
        # Store args for later use
        self.args = args
        self.raft_consensus = raft_consensus
        self.bandwidth_manager = bandwidth_manager
        
        # Store node identification for gRPC-based communication
        self.node_id = node_id
        # Note: cluster_size is initial size, actual size tracked in known_nodes
        # and will be updated dynamically through RAFT membership changes
        
        # Initialize service discovery bridge for dynamic membership management
        self.service_discovery_bridge = RaftServiceDiscoveryBridge(raft_consensus, comm)
        
        # Register the bridge with this worker manager as the communication manager
        self.service_discovery_bridge.register_with_comm_manager(self)
        
        # Register the bridge with consensus for membership change notifications
        if self.raft_consensus is not None:
            self.raft_consensus.register_service_discovery_bridge(self.service_discovery_bridge)
            
            # Register all callbacks for RAFT consensus
            self._register_raft_callbacks()
        
        # State variables for RAFT-FL integration
        self.is_coordinator = False
        self.coordinator_id = None
        self.current_topology = None
        self.current_bandwidth = None
        self.raft_round_state = None  # Track RAFT-managed round state
        
        # Thread synchronization
        self.consensus_established_event = threading.Event()
        self.topology_ready_event = threading.Event()
        # Signals when a round is authorized to start by RAFT consensus
        self.round_start_authorized = threading.Event()
        # Used to trigger next training round - should be integrated with round_start_authorized
        self.round_start_event = threading.Event()  # Used to trigger next training round
        
        # # Override SAPS_FL coordinator detection
        # self._override_saps_coordinator_logic()
        
        logging.info(f"RaftWorkerManager initialized for node {node_id} with service discovery bridge")
    
    def get_sender_id(self):
        """Override to use node_id instead of MPI rank for gRPC communication."""
        return self.node_id
    
    def get_comm_manager(self):
        """Get the communication manager for the service discovery bridge."""
        return getattr(self, '_comm_manager', None)
    
    def set_raft_consensus(self, raft_consensus):
        """Enhanced version with better circular dependency handling."""
        self.raft_consensus = raft_consensus
        
        # Register callbacks with consensus
        self._register_raft_callbacks()
        
        # If bridge exists, register it with consensus
        if hasattr(self, 'service_discovery_bridge') and self.service_discovery_bridge:
            if not self.service_discovery_bridge.is_registered_with_consensus():
                self.raft_consensus.register_service_discovery_bridge(self.service_discovery_bridge)
    
    def register_message_receive_handlers(self):
        """Register message handlers for RAFT and GossipFL messages."""
        # First register handlers from the parent class
        super().register_message_receive_handlers()
        
        # Register RAFT message handlers
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_REQUEST_VOTE,
            self.handle_request_vote)

        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_PREVOTE_REQUEST,
            self.handle_prevote_request)

        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_VOTE_RESPONSE,
            self.handle_vote_response)

        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_PREVOTE_RESPONSE,
            self.handle_prevote_response)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_APPEND_ENTRIES,
            self.handle_append_entries)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_APPEND_RESPONSE,
            self.handle_append_response)
        
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_STATE_SNAPSHOT,
            self.handle_state_snapshot)
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT,
            self.handle_install_snapshot)

        # Enhanced message handlers for improved node joining
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_LEADER_REDIRECT,
            self.handle_leader_redirect)

        # Initialization parameter exchange
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_STATE_REQUEST,
            self.handle_state_request)
        
        # Join protocol handlers
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_JOIN_REQUEST,
            self.handle_join_request)
        self.register_message_receive_handler(
            RaftMessage.MSG_TYPE_JOIN_RESPONSE,
            self.handle_join_response)
    
    def run(self):
        """
        Unified run method for RAFT-based worker manager with gRPC communication.
        
        This method implements a fully event-driven, RAFT-based execution flow:
        1. Start RAFT consensus service
        2. Handle initial cluster joining
        3. Start training thread
        4. Enter the message processing loop
        
        All synchronization happens through RAFT consensus events rather than
        traditional barriers or coordinator-based synchronization.
        """
        logging.info(f"Node {self.node_id}: Starting RAFT-based worker manager")
        
        # Step 1: Start RAFT consensus service
        self.raft_consensus.start()
        
        # Step 2: Handle initial joining and synchronization
        self._initial_join()
        
        # Step 3: Start the training thread
        self.training_thread.start()
        
        # Step 4: If this node is the RAFT leader, start coordinator duties
        if self.is_coordinator:
            logging.info(f"Node {self.node_id} is the initial coordinator (RAFT leader)")
            self.coordinator_thread = threading.Thread(
                name="coordinator", 
                target=self.run_coordinator,
                daemon=True
            )
            self.coordinator_thread.start()
            self.notify_clients()
        else:
            logging.info(f"Node {self.node_id} is a follower, coordinator is node {self.coordinator_id}")
        
        # Step 5: Signal that we're fully initialized
        self.consensus_established_event.set()
        logging.info(f"Node {self.node_id}: Consensus established, entering message loop")
        
        # Step 6: Enter message processing loop (parent's parent run)
        # We skip DecentralizedWorkerManager.run() as it has SAPS-specific logic
        super(DecentralizedWorkerManager, self).run()
    
    def handle_leadership_change(self, leader_id):
        """
        Handle a change in RAFT leadership.
        
        This method ensures proper coordination of training when leadership changes.
        
        Args:
            leader_id (int): ID of the new leader
        """
        old_coordinator = self.coordinator_id
        self.coordinator_id = leader_id
        
        # If this node became the leader
        if leader_id == self.node_id and not self.is_coordinator:
            logging.info(f"Node {self.node_id} is now the COORDINATOR (RAFT leader)")
            self.is_coordinator = True
            
            # Start coordinator duties if not already running
            if not hasattr(self, 'coordinator_thread') or not self.coordinator_thread.is_alive():
                self.coordinator_thread = threading.Thread(
                    name="coordinator", 
                    target=self.run_coordinator,
                    daemon=True  # Make thread daemon so it exits when main thread exits
                )
                self.coordinator_thread.start()
                logging.info(f"Node {self.node_id}: Started coordinator thread")
                
            # Notify clients about the new coordinator
            self.notify_clients()
            
            # Signal that consensus is established
            self.consensus_established_event.set()
            
            # Signal that a new round can start under this coordinator
            self.round_start_authorized.set()
        
        # If this node is no longer the leader
        elif leader_id != self.node_id and self.is_coordinator:
            logging.info(f"Node {self.node_id} is no longer the COORDINATOR")
            self.is_coordinator = False
            
            # No need to explicitly stop the coordinator thread
            # It will check self.is_coordinator in its loop and exit naturally
            
            # Clear round start authorization since we're no longer coordinator
            self.round_start_authorized.clear()
        
        # If the leader changed but this node was not involved
        elif old_coordinator != leader_id:
            logging.info(f"COORDINATOR changed from {old_coordinator} to {leader_id}")
            
            # Clear any existing round authorization and wait for new coordinator
            self.round_start_authorized.clear()
            
            # Signal that consensus is established with the new leader
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
                    if self.service_discovery_bridge and hasattr(self.service_discovery_bridge, 'on_membership_change_callback'):
                        if self.service_discovery_bridge.on_membership_change_callback:
                            self.service_discovery_bridge.on_membership_change_callback('update', list(new_nodes))
                        
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
                        self.coordinator_thread = threading.Thread(name="coordinator", target=self.run_coordinator, daemon=True)
                        self.coordinator_thread.start()
                    
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
                    logging.debug("Waiting for RAFT authorization to start round")
                    
                    # Wait for RAFT consensus to authorize the round start
                    self.round_start_authorized.wait()
                    logging.debug("RAFT authorized round start, beginning iteration")
                    
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

                    # Clear events for next iteration
                    self.round_start_authorized.clear()
                    self.sync_receive_all_event.clear()

                    """
                        Before send msg to coordinator,
                        wait for latest RAFT-committed topology.
                    """
                    # Wait for the latest topology to be ready through RAFT consensus
                    logging.debug(f"Node {self.node_id}: Waiting for latest RAFT-committed topology")
                    self.topology_ready_event.wait()
                    self.topology_ready_event.clear()  # Reset for next iteration
                    
                    # Now update topology through RAFT-committed data
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
        
        DEPRECATED: This method is kept for backward compatibility.
        New code should use _initial_join() instead, which provides a more
        comprehensive joining and synchronization process.
        """
        logging.warning("wait_for_initial_consensus is deprecated, use _initial_join instead")
        self._initial_join()
    # RAFT message handlers
    
    def handle_request_vote(self, msg_params):
        """
        Handle a RAFT RequestVote message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.ARG_TERM)
        candidate_id = msg_params.get(RaftMessage.ARG_CANDIDATE_ID)
        last_log_index = msg_params.get(RaftMessage.ARG_LAST_LOG_INDEX)
        last_log_term = msg_params.get(RaftMessage.ARG_LAST_LOG_TERM)
        
        logging.debug(f"Received RequestVote from {sender_id}, term={term}, candidate={candidate_id}")
        
        # Process the vote request
        self.raft_consensus.handle_vote_request(candidate_id, term, last_log_index, last_log_term)

    def handle_prevote_request(self, msg_params):
        """Handle a RAFT PreVote request message."""
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.ARG_TERM)
        candidate_id = msg_params.get(RaftMessage.ARG_CANDIDATE_ID)
        last_log_index = msg_params.get(RaftMessage.ARG_LAST_LOG_INDEX)
        last_log_term = msg_params.get(RaftMessage.ARG_LAST_LOG_TERM)

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
        term = msg_params.get(RaftMessage.ARG_TERM)
        vote_granted = msg_params.get(RaftMessage.ARG_VOTE_GRANTED)
        
        logging.debug(f"Received VoteResponse from {sender_id}, term={term}, granted={vote_granted}")

        # Process the vote response
        self.raft_consensus.handle_vote_response(sender_id, term, vote_granted)

    def handle_prevote_response(self, msg_params):
        """Handle a RAFT PreVote response message."""
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.ARG_TERM)
        prevote_granted = msg_params.get(RaftMessage.ARG_VOTE_GRANTED)

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
        term = msg_params.get(RaftMessage.ARG_TERM)
        prev_log_index = msg_params.get(RaftMessage.ARG_PREV_LOG_INDEX)
        prev_log_term = msg_params.get(RaftMessage.ARG_PREV_LOG_TERM)
        entries = msg_params.get(RaftMessage.ARG_ENTRIES)
        leader_commit = msg_params.get(RaftMessage.ARG_LEADER_COMMIT)

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
        term = msg_params.get(RaftMessage.ARG_TERM)
        success = msg_params.get(RaftMessage.ARG_SUCCESS)
        match_index = msg_params.get(RaftMessage.ARG_MATCH_INDEX)
        
        logging.debug(f"Received AppendResponse from {sender_id}, term={term}, success={success}")
        
        # Process the append response
        self.raft_consensus.handle_append_response(sender_id, term, success, match_index)
    
    def handle_state_snapshot(self, msg_params):
        """
        Handle a RAFT StateSnapshot message.

        Enhanced to drive the RaftNode’s high‐level state sync callback
        and then process any comprehensive state_package for FL.
        """
        sender_id     = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term          = msg_params.get(RaftMessage.ARG_TERM)
        log_entries   = msg_params.get(RaftMessage.ARG_ENTRIES, [])
        commit_index  = msg_params.get(RaftMessage.ARG_COMMIT_INDEX, 0)
        state_package = msg_params.get(RaftMessage.ARG_STATE_PACKAGE)

        logging.debug(
            f"Received StateSnapshot from {sender_id}, "
            f"term={term}, log_size={len(log_entries)}, commit_index={commit_index}"
        )

        self.raft_consensus.handle_state_snapshot(term, log_entries, commit_index)

        sync_payload = {
            'leader_id':    sender_id,
            'term':         term,
            'known_nodes':  list(self.raft_consensus.get_known_nodes()),
            'commit_index': commit_index,
            'log_entries':  list(log_entries),
            'timestamp':    time.time(),
        }

        self.raft_consensus.raft_node.handle_state_sync_response(sync_payload)

        if state_package is not None:
            logging.info(f"Processing comprehensive state package from {sender_id}")
            success = self.initialize_from_state_snapshot(state_package)
            if success:
                logging.info(f"Successfully joined cluster via state snapshot from {sender_id}")
            else:
                logging.error(f"Failed to process state package from {sender_id}")

    def handle_install_snapshot(self, msg_params):
        """
        Handle an InstallSnapshot message from the leader.
        
        This method accumulates snapshot chunks and applies the complete snapshot
        once all chunks are received.
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        term = msg_params.get(RaftMessage.ARG_TERM)
        last_idx = msg_params.get(RaftMessage.ARG_LAST_INCLUDED_INDEX)
        last_term = msg_params.get(RaftMessage.ARG_LAST_INCLUDED_TERM)
        offset = msg_params.get(RaftMessage.ARG_OFFSET)
        data = msg_params.get(RaftMessage.ARG_DATA)
        done = msg_params.get(RaftMessage.ARG_DONE)

        logging.debug(
            f"Received InstallSnapshot from {sender_id}, term={term}, index={last_idx}, offset={offset}, done={done}"
        )

        # First, pass the snapshot chunk to the RAFT consensus manager
        self.raft_consensus.handle_install_snapshot(
            sender_id, term, last_idx, last_term, offset, data, done
        )
        
        # If this is the final chunk, apply the complete state package
        if done and data and isinstance(data, dict) and RaftMessage.ARG_STATE_PACKAGE in data:
            state_package = data.get(RaftMessage.ARG_STATE_PACKAGE)
            if state_package:
                logging.info(f"Applying complete state package from snapshot")
                success = self.initialize_from_state_snapshot(state_package)
                if success:
                    logging.info(f"Successfully applied snapshot from {sender_id}")
                else:
                    logging.error(f"Failed to apply snapshot from {sender_id}")
            else:
                logging.warning(f"Snapshot marked as done but no state package found")

    def handle_leader_redirect(self, msg_params):
        """
        Handle a leader redirect message.
        
        Args:
            msg_params (dict): Message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        leader_id = msg_params.get(RaftMessage.ARG_LEADER_ID)
        
        logging.info(f"Received leader redirect from {sender_id}, leader is {leader_id}")
        
        # Update our knowledge of the current leader and request state from them
        if leader_id is not None and leader_id != self.node_id:
            self.coordinator_id = leader_id
            self.send_state_request(leader_id)
    
    def send_vote_request(self, receiver_id, term, last_log_index, last_log_term):
        """
        Send a RAFT RequestVote message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            last_log_index (int): Index of last log entry
            last_log_term (int): Term of last log entry
        """
        content = {
            RaftMessage.ARG_TERM: term,
            RaftMessage.ARG_CANDIDATE_ID: self.node_id,
            RaftMessage.ARG_LAST_LOG_INDEX: last_log_index,
            RaftMessage.ARG_LAST_LOG_TERM: last_log_term
        }
        
        self.send_raft_message(RaftMessage.MSG_TYPE_REQUEST_VOTE, receiver_id, content)

    def send_prevote_request(self, receiver_id, term, last_log_index, last_log_term):
        """Send a RAFT PreVote request message."""
        content = {
            RaftMessage.ARG_TERM: term,
            RaftMessage.ARG_CANDIDATE_ID: self.node_id,
            RaftMessage.ARG_LAST_LOG_INDEX: last_log_index,
            RaftMessage.ARG_LAST_LOG_TERM: last_log_term
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_PREVOTE_REQUEST, receiver_id, content)
    
    def send_vote_response(self, receiver_id, term, vote_granted):
        """
        Send a RAFT VoteResponse message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            vote_granted (bool): Whether the vote was granted
        """
        content = {
            RaftMessage.ARG_TERM: term,
            RaftMessage.ARG_VOTE_GRANTED: vote_granted
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_VOTE_RESPONSE, receiver_id, content)

    def send_prevote_response(self, receiver_id, term, prevote_granted):
        """Send a RAFT PreVote response message."""
        content = {
            RaftMessage.ARG_TERM: term,
            RaftMessage.ARG_VOTE_GRANTED: prevote_granted
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_PREVOTE_RESPONSE, receiver_id, content)
    
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
        content = {
            RaftMessage.ARG_TERM: term,
            RaftMessage.ARG_PREV_LOG_INDEX: prev_log_index,
            RaftMessage.ARG_PREV_LOG_TERM: prev_log_term,
            RaftMessage.ARG_ENTRIES: entries,
            RaftMessage.ARG_LEADER_COMMIT: leader_commit
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_APPEND_ENTRIES, receiver_id, content)
    
    def send_append_response(self, receiver_id, term, success, match_index):
        """
        Send a RAFT AppendResponse message.
        
        Args:
            receiver_id (int): ID of the receiver
            term (int): Current term
            success (bool): Whether the append was successful
            match_index (int): Index of highest log entry known to be replicated
        """
        content = {
            RaftMessage.ARG_TERM: term,
            RaftMessage.ARG_SUCCESS: success,
            RaftMessage.ARG_MATCH_INDEX: match_index
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_APPEND_RESPONSE, receiver_id, content)

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
        content = {
            RaftMessage.ARG_TERM: term,
            RaftMessage.ARG_LAST_INCLUDED_INDEX: last_included_index,
            RaftMessage.ARG_LAST_INCLUDED_TERM: last_included_term,
            RaftMessage.ARG_OFFSET: offset,
            RaftMessage.ARG_DATA: data,
            RaftMessage.ARG_DONE: done
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT, receiver_id, content)

    def send_leader_redirect(self, receiver_id, leader_id):
        """
        Send a redirect message to inform a node about the current leader.
        
        Args:
            receiver_id (int): ID of the node to redirect
            leader_id (int): ID of the current leader
        """
        content = {
            RaftMessage.ARG_LEADER_ID: leader_id
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_LEADER_REDIRECT, receiver_id, content)

    def send_state_request(self, receiver_id):
        """
        Send a state request message to another node, typically the leader.
        
        This is used to request the current state of the system, including
        cluster membership, model parameters, and other metadata.
        
        Args:
            receiver_id (int): ID of the node to request state from
        """
        content = {
            # Add any relevant parameters for state request
            RaftMessage.ARG_NODE_ID: self.node_id,
        }
        self.send_raft_message(RaftMessage.MSG_TYPE_STATE_REQUEST, receiver_id, content)

    def initialize_from_state_snapshot(self, state_package):
        """
        Initialize node state from a comprehensive state snapshot.
        
        This method is called when a node receives a complete state package
        and needs to synchronize its state with the cluster.
        
        Args:
            state_package (dict): Complete state information from leader
        """
        try:
            # Extract current nodes information from the state package
            current_nodes = state_package.get(RaftMessage.ARG_CURRENT_NODES)
            if current_nodes is not None:
                logging.info(f"Received cluster members: {current_nodes}")
                # Update our local node list with the received list
                self.raft_consensus.update_known_nodes(current_nodes)
            
            # Update leader and term information if present
            leader_id = state_package.get(RaftMessage.ARG_LEADER_ID)
            if leader_id is not None and leader_id != self.node_id:
                logging.info(f"Setting leader to {leader_id}")
                self.coordinator_id = leader_id
                if self.raft_consensus and self.raft_consensus.raft_node:
                    self.raft_consensus.raft_node.leader_id = leader_id
            
            # Update RAFT term if present
            term = state_package.get(RaftMessage.ARG_TERM)
            if term is not None and self.raft_consensus and self.raft_consensus.raft_node:
                if term > self.raft_consensus.raft_node.current_term:
                    logging.info(f"Updating term to {term}")
                    self.raft_consensus.raft_node.current_term = term
                    self.raft_consensus.raft_node.voted_for = None
                    self.raft_consensus.raft_node.state = RaftState.FOLLOWER
            
            # Update model parameters if available
            model_params = state_package.get(RaftMessage.ARG_MODEL_PARAMS)
            if model_params is not None and hasattr(self.worker, "model_trainer"):
                self.worker.model_trainer.set_model_params(model_params)
                logging.info("Applied model parameters from state snapshot")
            
            # Update coordinator and round information
            coordinator_id = state_package.get(RaftMessage.MSG_ARG_COORDINATOR)
            if coordinator_id is not None:
                self.coordinator_id = coordinator_id
                logging.info(f"Set coordinator to {coordinator_id}")
            
            round_num = state_package.get(RaftMessage.ARG_ROUND)
            if round_num is not None and hasattr(self, 'round'):
                self.round = round_num
                logging.info(f"Set training round to {round_num}")
            
            # Signal that consensus has been established, which can unblock waiting threads
            self.consensus_established_event.set()
            
            logging.info(f"Successfully initialized from state snapshot")
            return True
            
        except Exception as e:
            logging.error(f"Failed to initialize from state snapshot: {str(e)}")
            traceback.print_exc()
            return False
    
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
    
    def handle_committed_entry(self, entry):
        """
        Handle committed log entries from RAFT consensus.
        
        This method contains all the application-specific logic that was previously 
        in the RaftNode's _apply_* methods. It processes different types of committed 
        entries and applies them to the federated learning system.
        
        Args:
            entry (dict): The committed log entry containing command information
        """
        try:
            command = entry.get('command', {})
            command_type = command.get('type')
            
            logging.info(f"Node {self.node_id}: Processing committed entry of type {command_type} from log index {entry.get('index')}")
            
            if command_type == 'topology':
                self._handle_topology_update(command)
            elif command_type == 'topology_delta':
                self._handle_topology_delta(command)
            elif command_type == 'bandwidth':
                self._handle_bandwidth_update(command)
            elif command_type == 'bandwidth_delta':
                self._handle_bandwidth_delta(command)
            elif command_type == 'membership':
                self._handle_membership_change(command)
            elif command_type == 'coordinator':
                self._handle_coordinator_change(command)
            elif command_type == 'batched_updates':
                self._handle_batched_updates(command, entry)
            elif command_type == 'no-op':
                logging.debug(f"Node {self.node_id}: Processed no-op entry {entry.get('index')}")
            else:
                logging.warning(f"Node {self.node_id}: Unknown command type in log entry: {command_type}")
                
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error handling committed entry: {e}", exc_info=True)
    
    def _handle_topology_update(self, command):
        """Handle full topology update command."""
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
    
    def _handle_topology_delta(self, command):
        """Handle incremental topology update command."""
        base_version = command.get('base_version')
        changes = command.get('changes', [])
        
        # Check if the base version is in a compacted part of the log
        if (hasattr(self.raft_consensus, 'raft_node') and 
            base_version < self.raft_consensus.raft_node.last_snapshot_index):
            logging.warning(f"Node {self.node_id}: Cannot apply topology delta with base_version {base_version} - " +
                          f"version predates last snapshot")
            
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
                # If error occurs, request full update
                if hasattr(self, 'request_topology_update') and callable(self.request_topology_update):
                    logging.info(f"Node {self.node_id}: Requesting full topology update due to error")
                    self.request_topology_update()
        else:
            # Store changes for later application
            if not hasattr(self, 'pending_topology_changes'):                
                self.pending_topology_changes = []
            self.pending_topology_changes.append((base_version, changes))
            logging.debug(f"Node {self.node_id}: Stored {len(changes)} topology changes for later application")

    def _handle_bandwidth_update(self, command):
        """Handle full bandwidth matrix update command."""
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
    
    def _handle_bandwidth_delta(self, command):
        """Handle incremental bandwidth update command."""
        base_version = command.get('base_version')
        changes = command.get('changes', {})
        
        # Check if the base version is in a compacted part of the log
        if (hasattr(self.raft_consensus, 'raft_node') and 
            base_version < self.raft_consensus.raft_node.last_snapshot_index):
            logging.warning(f"Node {self.node_id}: Cannot apply bandwidth delta with base_version {base_version} - " +
                          f"version predates last snapshot")
            
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
                # If error occurs, request full update
                if hasattr(self, 'request_bandwidth_update') and callable(self.request_bandwidth_update):
                    logging.info(f"Node {self.node_id}: Requesting full bandwidth update due to error")
                    self.request_bandwidth_update()
        else:
            # Store changes for later application
            if not hasattr(self, 'pending_bandwidth_changes'):
                self.pending_bandwidth_changes = []
            self.pending_bandwidth_changes.append((base_version, changes))
            logging.debug(f"Node {self.node_id}: Stored {len(changes)} bandwidth changes for later application")
    
    def _handle_membership_change(self, command):
        """Handle membership change command with connection information."""
        try:
            action = command.get('action')
            node_id = command.get('node_id')
            node_info = command.get('node_info', {})
            current_nodes = command.get('current_nodes')
            current_nodes_info = command.get('current_nodes_info', {})
            round_num = command.get('round', 0)
            
            # Get the RAFT node for state updates
            raft_node = getattr(self.raft_consensus, 'raft_node', None)
            if raft_node is None:
                logging.error(f"Node {self.node_id}: No RAFT node available for membership change")
                return
            
            old_known_nodes = set(raft_node.known_nodes)  # Copy for change detection
            
            if action == 'add':
                # Add the node to known nodes if not already present
                if node_id not in raft_node.known_nodes:
                    raft_node.known_nodes.add(node_id)
                    logging.info(f"Node {self.node_id}: Added node {node_id} to known nodes at round {round_num}")
                    
                    # Store connection information
                    if node_info and ('ip_address' in node_info or 'port' in node_info):
                        if not hasattr(raft_node, 'node_connection_info'):
                            raft_node.node_connection_info = {}
                        raft_node.node_connection_info[node_id] = node_info
                        logging.debug(f"Node {self.node_id}: Stored connection info for node {node_id}")
                        
            elif action == 'remove':
                # Remove the node from known nodes if present
                if node_id in raft_node.known_nodes:
                    raft_node.known_nodes.remove(node_id)
                    logging.info(f"Node {self.node_id}: Removed node {node_id} from known nodes at round {round_num}")
                    
                    # Clean up connection information
                    if hasattr(raft_node, 'node_connection_info') and node_id in raft_node.node_connection_info:
                        del raft_node.node_connection_info[node_id]
                        logging.debug(f"Node {self.node_id}: Removed connection info for node {node_id}")
            
            # If current_nodes is provided, use it to update known nodes
            if current_nodes is not None:
                raft_node.known_nodes = set(current_nodes)
                logging.info(f"Node {self.node_id}: Updated known nodes to {current_nodes} at round {round_num}")
            
            # Update connection info for all nodes if provided
            if current_nodes_info:
                if not hasattr(raft_node, 'node_connection_info'):
                    raft_node.node_connection_info = {}
                raft_node.node_connection_info.update(current_nodes_info)
                logging.debug(f"Node {self.node_id}: Updated connection info for {len(current_nodes_info)} nodes")
            
            # Only call update if there was actually a change
            if old_known_nodes != raft_node.known_nodes:
                # Update known nodes count and notify any monitoring components
                raft_node.update_known_nodes(node_ids=list(raft_node.known_nodes))
                
                # Notify service discovery bridge to update communication manager
                if hasattr(self, 'service_discovery_bridge') and self.service_discovery_bridge:
                    if action == 'add' and node_info:
                        self.service_discovery_bridge._notify_comm_manager_membership_change(
                            action='add',
                            node_id=node_id,
                            node_info=node_info
                        )
                    elif action == 'remove':
                        self.service_discovery_bridge._notify_comm_manager_membership_change(
                            action='remove',
                            node_id=node_id
                        )
                        
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error handling membership change: {e}", exc_info=True)
    
    def _handle_coordinator_change(self, command):
        """Handle coordinator change command."""
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
            old_coordinator = self.coordinator_id
            
            # Update the coordinator information
            if self.coordinator_id != coordinator_id:
                self.coordinator_id = coordinator_id
                coordinator_changed = True
                
                # Update is_coordinator flag
                self.is_coordinator = (coordinator_id == self.node_id)
            
            if coordinator_changed:
                logging.info(f"Node {self.node_id}: Updated coordinator from {old_coordinator} to {coordinator_id} at round {round_num} (reason: {reason})")
                
                # Handle coordinator change notifications
                self.on_coordinator_change(
                    new_coordinator=coordinator_id,
                    old_coordinator=old_coordinator,
                    round_num=round_num,
                    reason=reason
                )
                
                # If this node is the new coordinator, initiate training
                if self.is_coordinator:
                    logging.info(f"Node {self.node_id}: I am the new coordinator for round {round_num}")
                    self.on_become_coordinator(round_num)
                    
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error handling coordinator change: {e}", exc_info=True)
    
    def _handle_batched_updates(self, command, entry):
        """Handle batched updates command."""
        logging.info(f"Node {self.node_id}: Applying batched updates from log entry {entry.get('index')}")
        for update in command.get('updates', []):
            # Create a new command entry for each update in the batch
            self.handle_committed_entry({'term': entry['term'], 'index': entry['index'], 'command': update})

    def broadcast_prevote_request(self, candidate_id, term, last_log_index, last_log_term):
        """
        Broadcast prevote requests to all known nodes.
        
        Args:
            candidate_id (int): ID of the candidate requesting prevotes
            term (int): Term for which prevotes are requested
            last_log_index (int): Index of candidate's last log entry
            last_log_term (int): Term of candidate's last log entry
        """
        if not hasattr(self.raft_consensus, 'raft_node') or self.raft_consensus.raft_node is None:
            return
            
        known_nodes = self.raft_consensus.raft_node.known_nodes
        for node_id in known_nodes:
            if node_id != self.node_id:
                self.send_prevote_request(node_id, term, last_log_index, last_log_term)

    def broadcast_vote_request(self, candidate_id, term, last_log_index, last_log_term):
        """
        Broadcast vote requests to all known nodes.
        
        Args:
            candidate_id (int): ID of the candidate requesting votes
            term (int): Term for which votes are requested
            last_log_index (int): Index of candidate's last log entry
            last_log_term (int): Term of candidate's last log entry
        """
        if not hasattr(self.raft_consensus, 'raft_node') or self.raft_consensus.raft_node is None:
            return
            
        known_nodes = self.raft_consensus.raft_node.known_nodes
        for node_id in known_nodes:
            if node_id != self.node_id:
                self.send_vote_request(node_id, term, last_log_index, last_log_term)

    def broadcast_append_entries(self, entries=None, is_heartbeat=False):
        """
        Broadcast append entries to all followers (for leaders).
        
        Args:
            entries (list): List of entries to append (None for heartbeat)
            is_heartbeat (bool): Whether this is a heartbeat message
        """
        if not hasattr(self.raft_consensus, 'raft_node') or self.raft_consensus.raft_node is None:
            return
            
        raft_node = self.raft_consensus.raft_node
        if raft_node.state != RaftState.LEADER:
            return
            
        # Send append entries to all followers
        for node_id in raft_node.known_nodes:
            if node_id != self.node_id:
                # Get the appropriate log entries for this follower
                next_index = raft_node.next_index.get(node_id, 1)
                prev_log_index = next_index - 1
                prev_log_term = raft_node.get_term_at_index(prev_log_index) if prev_log_index > 0 else 0
                
                # Use provided entries or empty list for heartbeat
                send_entries = entries if entries is not None else []
                
                self.send_append_entries(
                    node_id, 
                    raft_node.current_term, 
                    prev_log_index, 
                    prev_log_term, 
                    send_entries, 
                    raft_node.commit_index
                )
    
    def _register_raft_callbacks(self):
        """
        Register all callbacks for RAFT consensus and node.
        
        This centralizes the callback registration in one place to avoid duplication.
        """
        if self.raft_consensus is None:
            logging.warning(f"Node {self.node_id}: Cannot register callbacks - no RAFT consensus instance")
            return
            
        # Register for leadership changes and state updates
        self.raft_consensus.on_leadership_change = self.handle_leadership_change
        self.raft_consensus.on_state_commit = self.handle_raft_state_commit
        
        # Register all callbacks for pure state machine operation
        if hasattr(self.raft_consensus, 'raft_node') and self.raft_consensus.raft_node is not None:
            raft_node = self.raft_consensus.raft_node
            
            # Application-specific callbacks
            raft_node.on_commit = self.handle_committed_entry
            raft_node.on_membership_change = self.on_membership_change
            
            # Communication callbacks
            raft_node.on_send_prevote = self.broadcast_prevote_request
            raft_node.on_send_vote = self.broadcast_vote_request
            raft_node.on_send_append = self.broadcast_append_entries
            
            logging.info(f"All RAFT node callbacks registered for node {self.node_id}")
            
        logging.info(f"RAFT consensus callbacks registered for node {self.node_id}")
        
    def _initial_join(self):
        """
        Handle the initial joining process for a node.
        
        This method unifies all the joining logic in one place:
        1. Wait for a leader to be elected
        2. Join the cluster via the service discovery bridge
        3. Wait for state synchronization
        4. Ensure topology and bandwidth are synchronized
        """
        # Step 1: Wait until a leader is elected
        while self.raft_consensus.get_leader_id() is None:
            logging.debug(f"Node {self.node_id} waiting for leader election")
            time.sleep(0.1)
        
        # Update our coordinator information
        self.coordinator_id = self.raft_consensus.get_leader_id()
        logging.info(f"Node {self.node_id}: Initial consensus established, leader is node {self.coordinator_id}")
        
        # Set coordinator flag if this node is the leader
        if self.coordinator_id == self.node_id:
            self.is_coordinator = True
            logging.info(f"Node {self.node_id} is the RAFT leader and will act as coordinator")
        
        # Step 2: If this node is not the coordinator or explicitly joining, handle join process
        should_join = (
            self.node_id != self.coordinator_id or 
            getattr(self.args, "join_existing_cluster", False) or
            self.raft_consensus.raft_node.state == RaftState.INITIAL
        )
        
        if should_join:
            logging.info(f"Node {self.node_id}: Joining cluster through service discovery bridge")
            
            # Prepare node information for joining
            node_info = {
                'node_id': self.node_id,
                'ip_address': getattr(self.args, 'ip_address', 'localhost'),
                'port': getattr(self.args, 'port', 8080),
                'capabilities': ['grpc', 'fedml'],
                'timestamp': time.time()
            }
            
            # Let the bridge handle the joining process
            if self.service_discovery_bridge:
                self.service_discovery_bridge.handle_node_discovered(self.node_id, node_info)
        
        # Step 3: Wait for state synchronization to complete
        max_wait_time = 30
        wait_start = time.time()
        
        # For new nodes, we need to wait for them to transition out of INITIAL state
        if self.raft_consensus.raft_node.state == RaftState.INITIAL:
            logging.info(f"Node {self.node_id}: Waiting for initial state synchronization")
            
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
        
        # For all nodes, ensure we have state synchronized
        while not self.raft_consensus.is_state_synchronized() and (time.time() - wait_start) < max_wait_time:
            time.sleep(0.1)
                
        if not self.raft_consensus.is_state_synchronized():
            logging.warning(f"Node {self.node_id}: Failed to synchronize state within timeout - proceeding anyway")
        else:
            logging.info(f"Node {self.node_id}: Successfully synchronized with existing cluster")

        # Step 4: Ensure topology and bandwidth are also synchronized
        if self.topology_manager.get_topology() is None:
            logging.info(f"Node {self.node_id}: Topology not yet available, waiting...")
            for _ in range(50):
                if self.topology_manager.get_topology() is not None:
                    break
                time.sleep(0.1)
                
        if self.bandwidth_manager and self.bandwidth_manager.get_bandwidth() is None:
            logging.info(f"Node {self.node_id}: Bandwidth not yet available, waiting...")
            for _ in range(50):
                if self.bandwidth_manager.get_bandwidth() is not None:
                    break
                time.sleep(0.1)

        logging.info(f"Node {self.node_id}: Ready to start training")
    
    def send_raft_message(self, msg_type, receiver_id, content=None):
        """
        Centralized method for sending RAFT messages through the communication manager.
        
        All RAFT message sending should go through this method to ensure consistent
        message handling and proper routing.
        
        Args:
            msg_type: The RAFT message type from RaftMessage
            receiver_id: The ID of the receiving node
            content: The content/payload of the message (optional)
        """
        # Create a new message with the given type, sender, and receiver
        message = Message(msg_type, self.get_sender_id(), receiver_id)
        
        # Add any content if provided
        if content is not None:
            for key, value in content.items():
                message.add(key, value)
        
        # Get the communication manager and send the message
        comm_manager = self.get_comm_manager()
        if comm_manager:
            comm_manager.send_message(message)
        else:
            # Fallback to the parent class's send_message method
            self.send_message(message)
        
        logging.debug(f"Node {self.node_id} sent {msg_type} to node {receiver_id}")
    
    def handle_state_request(self, msg_params):
        """
        Handle a state request from another node.
        
        This method packages up the current state of the system and sends it
        back to the requesting node, including cluster membership and state metadata.
        
        Args:
            msg_params: The message parameters
        """
        sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
        node_id = msg_params.get(RaftMessage.ARG_NODE_ID)
        
        logging.info(f"Received state request from node {sender_id}")
        
        # Only respond if we are the leader and have a valid state
        if (self.raft_consensus and 
            self.raft_consensus.raft_node and 
            self.raft_consensus.raft_node.is_leader()):
            
            # Create a state package with current system state
            state_package = {
                # Include all necessary state information
                RaftMessage.ARG_CURRENT_NODES: self.raft_consensus.get_cluster_members(),
                RaftMessage.ARG_LEADER_ID: self.node_id,
                RaftMessage.ARG_TERM: self.raft_consensus.raft_node.current_term,
                # Include other metadata as needed
            }
            
            # Include model parameters and training state if available
            if hasattr(self, 'latest_model_params') and self.latest_model_params is not None:
                state_package[RaftMessage.ARG_MODEL_PARAMS] = self.latest_model_params
            
            # Include coordinator and round information if available
            if self.coordinator_id is not None:
                state_package[RaftMessage.MSG_ARG_COORDINATOR] = self.coordinator_id
            if hasattr(self, 'round') and self.round is not None:
                state_package[RaftMessage.ARG_ROUND] = self.round
            
            # Send state snapshot back to requesting node
            content = {
                RaftMessage.ARG_STATE_PACKAGE: state_package
            }
            self.send_raft_message(RaftMessage.MSG_TYPE_STATE_SNAPSHOT, sender_id, content)
            logging.info(f"Sent state snapshot to node {sender_id}")
        else:
            # If not leader, redirect to the leader
            if self.raft_consensus and self.raft_consensus.raft_node:
                leader_id = self.raft_consensus.raft_node.leader_id
                if leader_id is not None and leader_id != self.node_id and leader_id != sender_id:
                    logging.info(f"Redirecting node {sender_id} to leader {leader_id}")
                    self.send_leader_redirect(sender_id, leader_id)
    
    def finish(self):
        """
        Perform a clean shutdown of the RAFT worker manager.
        
        This method ensures that all threads are properly terminated
        and resources are released.
        """
        logging.info(f"Node {self.node_id}: Beginning clean shutdown")
        
        # Set flags to signal threads to exit
        if hasattr(self, 'running') and self.running:
            self.running = False
            
        # Signal any waiting threads to proceed and then exit
        if hasattr(self, 'round_start_authorized'):
            self.round_start_authorized.set()
        if hasattr(self, 'topology_ready_event'):
            self.topology_ready_event.set()
        if hasattr(self, 'consensus_established_event'):
            self.consensus_established_event.set()
        if hasattr(self, 'sync_receive_all_event'):
            self.sync_receive_all_event.set()
            
        # Give threads a moment to notice the exit flags
        time.sleep(0.5)
        
        # Shut down RAFT consensus
        if hasattr(self, 'raft_consensus') and self.raft_consensus is not None:
            logging.info(f"Node {self.node_id}: Shutting down RAFT consensus")
            if hasattr(self.raft_consensus, 'stop'):
                self.raft_consensus.stop()
                
        # Call the parent class's finish method
        super().finish()
        
        logging.info(f"Node {self.node_id}: Shutdown complete")
    
    def handle_join_request(self, msg_params):
        """
        Handle join request from another node.
        Delegates to the service discovery bridge for processing.
        
        Args:
            msg_params: Message parameters containing sender and node info
        """
        try:
            sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
            node_info = msg_params.get('node_info', {})
            
            logging.info(f"Node {self.node_id}: Received join request from node {sender_id}")
            
            # Delegate to the service discovery bridge if available
            if self.service_discovery_bridge:
                self.service_discovery_bridge.handle_join_request(sender_id, node_info)
            else:
                logging.warning(f"Node {self.node_id}: No service discovery bridge available to handle join request")
                
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error handling join request: {e}", exc_info=True)
    
    def handle_join_response(self, msg_params):
        """
        Handle join response from another node.
        Delegates to the service discovery bridge for processing.
        
        Args:
            msg_params: Message parameters containing response details
        """
        try:
            sender_id = msg_params.get(RaftMessage.MSG_ARG_KEY_SENDER)
            status = msg_params.get('status', 'unknown')
            
            logging.info(f"Node {self.node_id}: Received join response from node {sender_id}, status: {status}")
            
            # Delegate to the service discovery bridge if available
            if self.service_discovery_bridge:
                self.service_discovery_bridge.handle_join_response(sender_id, msg_params)
            else:
                logging.warning(f"Node {self.node_id}: No service discovery bridge available to handle join response")
                
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error handling join response: {e}", exc_info=True)