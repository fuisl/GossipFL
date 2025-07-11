import logging
import threading
import time
from typing import Set, Dict, Any, Optional, Callable, List
from .raft_messages import RaftMessage
from .raft_consensus import RaftConsensus
from .raft_node import RaftState

class RaftServiceDiscoveryBridge:
    """
    Bridge between DynamicGRPCCommManager and RaftConsensus.
    
    Responsibilities:
    1. Wire RAFT consensus to communication layer (outgoing messages)
    2. Wire communication layer to RAFT consensus (incoming messages)
    3. Handle service discovery events and propose membership changes
    4. Process consensus membership changes and update registries
    
    All callback flows are explicitly registered during initialization.
    """
    def __init__(self, consensus: RaftConsensus, comm_manager):
        """
        Initialize the bridge with consensus and communication components.
        
        Args:
            consensus: The RAFT consensus manager
            comm_manager: The communication manager
        """
        self.consensus = consensus
        self.comm_manager = comm_manager
        self.lock = threading.RLock()
        
        # Initialize pending join responses for event-driven responses
        self.pending_join_responses = {}
        
        # Store original commit callback for chaining
        self._original_commit_callback = None
        
        # Initialize recursion guard
        self._in_commit_handler = False
        
        # Ensure comm_manager has raft_handlers dictionary
        if not hasattr(self.comm_manager, '_raft_handlers'):
            self.comm_manager._raft_handlers = {}
            
        # Complete initialization in a deterministic order
        self._init_bridge()
        
        logging.info(f"RaftServiceDiscoveryBridge initialized for node {self.consensus.raft_node.node_id}")
    
    def _init_bridge(self):
        """Initialize the bridge in a specific order to avoid circular dependencies."""
        # 1. Register bridge with comm manager
        self._register_with_comm_manager()
        
        # 2. Wire consensus → comm (outgoing messages)
        self._wire_consensus_to_comm()
        
        # 3. Wire comm → consensus (incoming messages)
        self._wire_comm_to_consensus()
        
        # 4. Hook service discovery callbacks
        self._register_discovery_callbacks()
        
        # 5. Hook consensus commit callbacks
        self._register_consensus_callbacks()
    
    def _register_with_comm_manager(self):
        """Register this bridge with the communication manager."""
        try:
            if hasattr(self.comm_manager, 'register_service_discovery_bridge'):
                self.comm_manager.register_service_discovery_bridge(self)
                logging.info(f"Bridge registered with comm manager for node {self.consensus.raft_node.node_id}")
            else:
                logging.warning("Communication manager does not support register_service_discovery_bridge")
        except Exception as e:
            logging.error(f"Error registering bridge with comm manager: {e}", exc_info=True)
    
    def _wire_consensus_to_comm(self):
        """
        Wire consensus to comm manager by setting callback handlers on the RAFT node.
        These callbacks are called when the node needs to send RAFT messages.
        """
        try:
            # Get the send_raft_message function from comm manager
            if not hasattr(self.comm_manager, 'send_raft_message'):
                logging.error("Communication manager does not have send_raft_message method")
                return
                
            cm = self.comm_manager.send_raft_message
            rn = self.consensus.raft_node
            
            # Set outbound message callbacks on the RAFT node
            rn.on_send_prevote = lambda cid, term, lli, llt: cm(
                _msg(RaftMessage.MSG_TYPE_PREVOTE_REQUEST,
                    term=term,
                    candidate_id=cid,
                    last_log_index=lli,
                    last_log_term=llt))
                    
            rn.on_send_vote = lambda cid, term, lli, llt: cm(
                _msg(RaftMessage.MSG_TYPE_REQUEST_VOTE,
                    term=term,
                    candidate_id=cid,
                    last_log_index=lli,
                    last_log_term=llt))
                    
            rn.on_send_append = lambda peer, term, lid, pli, plt, entries, lcommit: cm(
                _msg(RaftMessage.MSG_TYPE_APPEND_ENTRIES,
                    leader_id=lid,
                    term=term,
                    prev_log_index=pli,
                    prev_log_term=plt,
                    entries=entries,
                    leader_commit=lcommit),
                receiver=peer)
                
            rn.on_send_snapshot = lambda peer, term, lid, idx, term0, data: cm(
                _msg(RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT,
                    leader_id=lid,
                    term=term,
                    last_included_index=idx,
                    last_included_term=term0,
                    data=data),
                receiver=peer)
    
            # Set response callbacks
            rn.on_send_prevote_response = lambda peer, term, granted: cm(
                _msg(RaftMessage.MSG_TYPE_PREVOTE_RESPONSE,
                    term=term,
                    vote_granted=granted),
                receiver=peer)
                
            rn.on_send_vote_response = lambda peer, term, granted: cm(
                _msg(RaftMessage.MSG_TYPE_VOTE_RESPONSE,
                    term=term,
                    vote_granted=granted),
                receiver=peer)
                
            rn.on_send_append_response = lambda peer, term, success, match_idx: cm(
                _msg(RaftMessage.MSG_TYPE_APPEND_RESPONSE,
                    term=term,
                    success=success,
                    match_index=match_idx),
                receiver=peer)
                
            logging.info(f"RAFT node outbound message callbacks wired to comm manager")
        except Exception as e:
            logging.error(f"Error wiring consensus to comm: {e}", exc_info=True)
    
    def _wire_comm_to_consensus(self):
        """
        Wire comm manager to consensus by registering message handlers.
        These handlers are called when RAFT messages are received.
        """
        try:
            # Check if comm manager has add_raft_handler method
            if not hasattr(self.comm_manager, 'add_raft_handler'):
                logging.error("Communication manager does not have add_raft_handler method")
                return
                
            cm = self.comm_manager
            
            # Explicit parameter mapping for each message type
            # This is much more robust than trying to filter generically
            def handle_prevote_request(params):
                return self.consensus.handle_prevote_request(
                    candidate_id=params.get(RaftMessage.ARG_CANDIDATE_ID),
                    term=params.get(RaftMessage.ARG_TERM),
                    last_log_index=params.get(RaftMessage.ARG_LAST_LOG_INDEX),
                    last_log_term=params.get(RaftMessage.ARG_LAST_LOG_TERM)
                )
            
            def handle_prevote_response(params):
                return self.consensus.handle_prevote_response(
                    voter_id=params.get(RaftMessage.MSG_ARG_KEY_SENDER),
                    term=params.get(RaftMessage.ARG_TERM),
                    prevote_granted=params.get(RaftMessage.ARG_VOTE_GRANTED)
                )
            
            def handle_vote_request(params):
                return self.consensus.handle_vote_request(
                    candidate_id=params.get(RaftMessage.ARG_CANDIDATE_ID),
                    term=params.get(RaftMessage.ARG_TERM),
                    last_log_index=params.get(RaftMessage.ARG_LAST_LOG_INDEX),
                    last_log_term=params.get(RaftMessage.ARG_LAST_LOG_TERM)
                )
            
            def handle_vote_response(params):
                return self.consensus.handle_vote_response(
                    voter_id=params.get(RaftMessage.MSG_ARG_KEY_SENDER),
                    term=params.get(RaftMessage.ARG_TERM),
                    vote_granted=params.get(RaftMessage.ARG_VOTE_GRANTED)
                )
            
            def handle_append_entries(params):
                return self.consensus.handle_append_entries(
                    leader_id=params.get(RaftMessage.MSG_ARG_KEY_SENDER),
                    term=params.get(RaftMessage.ARG_TERM),
                    prev_log_index=params.get(RaftMessage.ARG_PREV_LOG_INDEX),
                    prev_log_term=params.get(RaftMessage.ARG_PREV_LOG_TERM),
                    entries=params.get(RaftMessage.ARG_ENTRIES, []),
                    leader_commit=params.get(RaftMessage.ARG_LEADER_COMMIT)
                )
            
            def handle_append_response(params):
                return self.consensus.handle_append_response(
                    follower_id=params.get(RaftMessage.MSG_ARG_KEY_SENDER),
                    term=params.get(RaftMessage.ARG_TERM),
                    success=params.get(RaftMessage.ARG_SUCCESS),
                    match_index=params.get(RaftMessage.ARG_MATCH_INDEX)
                )
            
            def handle_install_snapshot(params):
                return self.consensus.handle_install_snapshot(
                    leader_id=params.get(RaftMessage.MSG_ARG_KEY_SENDER),
                    term=params.get(RaftMessage.ARG_TERM),
                    last_incl_idx=params.get(RaftMessage.ARG_LAST_INCLUDED_INDEX),
                    last_incl_term=params.get(RaftMessage.ARG_LAST_INCLUDED_TERM),
                    offset=params.get(RaftMessage.ARG_OFFSET, 0),  # Add missing offset parameter
                    data=params.get(RaftMessage.ARG_DATA),
                    done=params.get(RaftMessage.ARG_DONE, True)    # Add missing done parameter
                )
            
            def handle_join_request(params):
                sender_id = params.get(RaftMessage.MSG_ARG_KEY_SENDER)
                node_info = params.get(RaftMessage.ARG_NODE_INFO, {})
                return self.handle_join_request(sender_id, node_info)
            
            def handle_join_response(params):
                sender_id = params.get(RaftMessage.MSG_ARG_KEY_SENDER)
                return self.handle_join_response(sender_id, params)
            
            # Register handlers for each RAFT message type
            handler_map = {
                RaftMessage.MSG_TYPE_PREVOTE_REQUEST: handle_prevote_request,
                RaftMessage.MSG_TYPE_PREVOTE_RESPONSE: handle_prevote_response,
                RaftMessage.MSG_TYPE_REQUEST_VOTE: handle_vote_request,
                RaftMessage.MSG_TYPE_VOTE_RESPONSE: handle_vote_response,
                RaftMessage.MSG_TYPE_APPEND_ENTRIES: handle_append_entries,
                RaftMessage.MSG_TYPE_APPEND_RESPONSE: handle_append_response,
                RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT: handle_install_snapshot,
                RaftMessage.MSG_TYPE_JOIN_REQUEST: handle_join_request,
                RaftMessage.MSG_TYPE_JOIN_RESPONSE: handle_join_response
            }
            
            # Register all handlers with error handling
            for msg_type, handler in handler_map.items():
                def safe_handler(params, h=handler):
                    try:
                        return h(params)
                    except Exception as e:
                        logging.error(f"Error handling RAFT message type {msg_type}: {e}", exc_info=True)
                        raise
                
                cm.add_raft_handler(msg_type, safe_handler)
                
            logging.info(f"RAFT message handlers registered with comm manager")
        except Exception as e:
            logging.error(f"Error wiring comm to consensus: {e}", exc_info=True)   
    def _register_discovery_callbacks(self):
        """
        Register callbacks for service discovery events.
        These are called when nodes are discovered or lost.
        """
        try:
            # Set node discovery callbacks on comm manager
            if hasattr(self.comm_manager, 'set_node_discovered_callback'):
                self.comm_manager.set_node_discovered_callback(self._on_node_discovered)
            else:
                # Fallback to direct assignment if method doesn't exist
                self.comm_manager.on_node_discovered = self._on_node_discovered
                
            if hasattr(self.comm_manager, 'set_node_lost_callback'):
                self.comm_manager.set_node_lost_callback(self._on_node_lost)
            else:
                # Fallback to direct assignment if method doesn't exist
                self.comm_manager.on_node_lost = self._on_node_lost
                
            logging.info(f"Service discovery callbacks registered")
        except Exception as e:
            logging.error(f"Error registering discovery callbacks: {e}", exc_info=True)
    
    def _register_consensus_callbacks(self):
        """
        Register callbacks for consensus events.
        These are called when log entries are committed.
        """
        try:
            # Set commit callbacks on RAFT node
            self.consensus.raft_node.on_membership_change = self._on_commit_membership
            
            # Chain our commit handler with existing consensus handler
            # Store original callback if it exists
            self._original_commit_callback = getattr(self.consensus.raft_node, 'on_commit', None)
            self.consensus.raft_node.on_commit = self._on_commit_chained
            
            logging.info(f"Consensus commit callbacks registered")
        except Exception as e:
            logging.error(f"Error registering consensus callbacks: {e}", exc_info=True)
    
    def _on_node_discovered(self, node_id, node_info):
        """
        Handle node discovery event.
        Proposes a membership change to add the discovered node.
        
        Args:
            node_id: ID of the discovered node
            node_info: Information about the discovered node
        """
        try:
            # Skip self
            if node_id == self.consensus.raft_node.node_id:
                return
                
            logging.info(f"Bridge: node discovered {node_id}")
            
            # Propose membership change to add the node
            self.consensus.propose_membership_change(
                action='add',
                node_id=node_id,
                node_info=node_info,
                reason='service_discovery'
            )
        except Exception as e:
            logging.error(f"Error handling node discovery: {e}", exc_info=True)
    
    def _on_node_lost(self, node_id):
        """
        Handle node lost event.
        Proposes a membership change to remove the lost node.
        
        Args:
            node_id: ID of the lost node
        """
        try:
            # Skip self
            if node_id == self.consensus.raft_node.node_id:
                return
                
            logging.info(f"Bridge: node lost {node_id}")
            
            # Propose membership change to remove the node
            self.consensus.propose_membership_change(
                action='remove',
                node_id=node_id,
                reason='service_discovery_lost'
            )
        except Exception as e:
            logging.error(f"Error handling node lost: {e}", exc_info=True)
    
    def _on_commit_membership(self, new_nodes: Set[int], round_num=0):
        """
        Handle membership change commit.
        Updates the comm manager registry and notifies the worker manager.
        
        Args:
            new_nodes: Set of node IDs in the new membership
            round_num: Current round number
        """
        try:
            # Build node list with information
            node_list = []
            for nid in new_nodes:
                try:
                    info = self.comm_manager.get_node_info(nid)
                    if info:
                        node_list.append({
                            'node_id': nid,
                            'ip_address': info.ip_address,
                            'port': info.port,
                            'capabilities': info.capabilities,
                            'metadata': info.metadata
                        })
                except Exception as e:
                    logging.warning(f"Error getting info for node {nid}: {e}")
            
            # Update comm manager registry
            if hasattr(self.comm_manager, '_on_bridge_node_registry_update'):
                self.comm_manager._on_bridge_node_registry_update(node_list)
            else:
                logging.warning("Communication manager does not support _on_bridge_node_registry_update")
            
            # Chain to worker manager if available
            worker_manager = getattr(self.comm_manager, 'worker_manager', None)
            if worker_manager and hasattr(worker_manager, 'on_membership_change'):
                try:
                    worker_manager.on_membership_change(new_nodes, round_num)
                except Exception as e:
                    logging.error(f"Error calling worker_manager.on_membership_change: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Error handling membership change commit: {e}", exc_info=True)
    
    def _on_commit_chained(self, entry):
        """
        Chained commit handler that calls both bridge handlers and original consensus handler.
        
        Args:
            entry: The committed log entry
        """
        # Guard against infinite recursion
        if hasattr(self, '_in_commit_handler') and self._in_commit_handler:
            return
        
        try:
            self._in_commit_handler = True
            
            # First call the original consensus commit handler if it exists and it's not ourselves
            if (self._original_commit_callback and 
                self._original_commit_callback != self._on_commit_chained):
                self._original_commit_callback(entry)
            
            # Then call our bridge-specific commit handler for join responses
            self.on_commit(entry, entry.get('index', 0))
            
        except Exception as e:
            # Use print instead of logging to avoid potential recursion in logging
            print(f"Error in chained commit handler: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._in_commit_handler = False
    
    def _on_commit_any(self, entry):
        """
        Handle any log entry commit.
        Currently only used for special entries like coordinator commands.
        
        Args:
            entry: The committed log entry
        """
        try:
            # Currently no general handling, but this could be extended
            pass
        except Exception as e:
            logging.error(f"Error handling log entry commit: {e}", exc_info=True)

    def handle_join_request(self, sender_id: int, node_info: Dict):
        """
        Handle join request from another node.
        
        This method is called when a node wants to join the cluster.
        If this node is the leader, it will propose membership change through RAFT.
        If not the leader, it will redirect the request to the current leader.
        
        Args:
            sender_id: ID of the node requesting to join
            node_info: Connection information for the joining node
        """
        try:
            logging.info(f"Bridge: Received join request from node {sender_id}")
            
            # Skip if this is our own node
            if sender_id == self.consensus.raft_node.node_id:
                logging.debug(f"Ignoring join request from self")
                return
            
            # Check for duplicate join request
            pending_join_key = f"join_{sender_id}"
            if pending_join_key in self.pending_join_responses:
                logging.info(f"Bridge: Duplicate join request from {sender_id}, ignoring")
                return
            
            # Check if node is already a cluster member
            if sender_id in self.consensus.raft_node.known_nodes:
                logging.info(f"Bridge: Node {sender_id} already in cluster, sending approval")
                self._send_join_response(sender_id, True, "Already a cluster member")
                return
            
            # Check if we're the leader
            if self.consensus.raft_node.state == RaftState.LEADER:
                logging.info(f"Bridge: Processing join request as leader for node {sender_id}")
                
                # Store pending join request - response will be sent after commit
                pending_join_key = f"join_{sender_id}"
                self.pending_join_responses[pending_join_key] = {
                    'node_id': sender_id,
                    'node_info': node_info,
                    'timestamp': time.time()
                }
                
                # Propose membership change to add the node through RAFT consensus
                # This should be the ONLY place membership change is proposed for join requests
                success = self.consensus.propose_membership_change(
                    action='add',
                    node_id=sender_id,
                    node_info=node_info,
                    reason='join_request'
                )
                
                if not success:
                    # Clean up pending response and send immediate rejection
                    del self.pending_join_responses[pending_join_key]
                    self._send_join_response(sender_id, False, "Failed to add to RAFT log")
                    logging.error(f"Bridge: Failed to propose membership change for node {sender_id}")
                else:
                    logging.info(f"Bridge: Proposed membership change for node {sender_id}, waiting for commit")
                
            else:
                # We're not the leader - redirect to current leader
                current_leader = self.consensus.raft_node.current_leader_id
                if current_leader and current_leader != sender_id:
                    logging.info(f"Bridge: Redirecting join request from {sender_id} to leader {current_leader}")
                    self._send_join_response(sender_id, False, f"Not leader, redirect to {current_leader}", current_leader)
                else:
                    logging.warning(f"Bridge: No known leader to redirect join request from {sender_id}")
                    self._send_join_response(sender_id, False, "No current leader known")
                    
        except Exception as e:
            logging.error(f"Error handling join request from {sender_id}: {e}", exc_info=True)
            self._send_join_response(sender_id, False, f"Error processing join request: {e}")

    def handle_join_response(self, sender_id: int, response_params):
        """
        Handle join response from another node.
        
        This method is called when we receive a response to our join request.
        
        Args:
            sender_id: ID of the node sending the response
            response_params: Response parameters including status and message
        """
        try:
            # Handle both dict and protobuf message objects
            if hasattr(response_params, 'get') and callable(response_params.get):
                # Try dict-style access first
                try:
                    join_approved = response_params.get(RaftMessage.ARG_JOIN_APPROVED, False)
                    message = response_params.get('message', 'No message')
                    redirect_leader = response_params.get(RaftMessage.ARG_LEADER_ID)
                except TypeError:
                    # If that fails, try attribute access (protobuf style)
                    join_approved = getattr(response_params, RaftMessage.ARG_JOIN_APPROVED, False)
                    message = getattr(response_params, 'message', 'No message')
                    redirect_leader = getattr(response_params, RaftMessage.ARG_LEADER_ID, None)
            else:
                # Assume it's a dict-like object with direct attribute access
                join_approved = getattr(response_params, RaftMessage.ARG_JOIN_APPROVED, False)
                message = getattr(response_params, 'message', 'No message')
                redirect_leader = getattr(response_params, RaftMessage.ARG_LEADER_ID, None)
            
            logging.info(f"Bridge: Received join response from {sender_id}: approved={join_approved}, message='{message}'")
            
            if join_approved:
                logging.info(f"Join request approved by node {sender_id}")
                # Clear the join progress flag since we were accepted
                if hasattr(self, '_join_in_progress'):
                    self._join_in_progress = False
                    
                # Transition from INITIAL to FOLLOWER state now that we're approved
                if self.consensus.raft_node.state == RaftState.INITIAL:
                    with self.consensus.raft_node.state_lock:
                        self.consensus.raft_node.become_follower(self.consensus.raft_node.current_term)
                        logging.info(f"Node {self.consensus.raft_node.node_id}: Transitioned from INITIAL to FOLLOWER after join approval")
                        
                        # Start election timer now that we're a proper cluster member
                        if self.consensus:
                            self.consensus.start_election_timer_for_initial_node()
                
                # The membership change will be propagated through RAFT consensus
                # and handled by the commit callback when committed
                
            elif redirect_leader:
                logging.info(f"Join request redirected to leader {redirect_leader}")
                # Clear progress flag and retry with the actual leader
                if hasattr(self, '_join_in_progress'):
                    self._join_in_progress = False
                # Send join request to the actual leader
                self._send_join_request_to_leader(redirect_leader)
                
            else:
                logging.warning(f"Join request rejected by {sender_id}: {message}")
                # Clear progress flag on rejection
                if hasattr(self, '_join_in_progress'):
                    self._join_in_progress = False
                
        except Exception as e:
            logging.error(f"Error handling join response from {sender_id}: {e}", exc_info=True)
            # Clear progress flag on error
            if hasattr(self, '_join_in_progress'):
                self._join_in_progress = False

    def _send_join_request_to_leader(self, leader_id: int):
        """
        Send join request to the cluster leader.
        
        Args:
            leader_id: ID of the current cluster leader
        """
        try:
            # Get our node information
            node_info = {
                'ip_address': getattr(self.comm_manager, 'ip_address', 'localhost'),
                'port': getattr(self.comm_manager, 'port', 9000 + self.consensus.raft_node.node_id),
                'capabilities': ['grpc', 'fedml', 'raft'],
                'timestamp': time.time()
            }
            
            # Prepare join request content
            content = {
                RaftMessage.ARG_NODE_INFO: node_info,
                RaftMessage.ARG_TIMESTAMP: time.time()
            }
            
            logging.info(f"Bridge: Sending join request to leader {leader_id}")
            
            # Send via worker manager's RAFT message system
            if (hasattr(self, 'worker_manager') and self.worker_manager and 
                hasattr(self.worker_manager, 'send_raft_message') and
                hasattr(self.worker_manager, 'raft_consensus')):  # Verify it's actually a worker manager
                
                logging.debug(f"Bridge: Using worker manager to send join request: {type(self.worker_manager).__name__}")
                self.worker_manager.send_raft_message(
                    RaftMessage.MSG_TYPE_JOIN_REQUEST, 
                    leader_id, 
                    content
                )
            else:
                logging.error("Worker manager not available or does not support RAFT message sending")
                logging.debug(f"Bridge: worker_manager = {getattr(self, 'worker_manager', None)}")
                if hasattr(self, 'worker_manager') and self.worker_manager:
                    logging.debug(f"Bridge: worker_manager type = {type(self.worker_manager).__name__}")
                    logging.debug(f"Bridge: worker_manager has send_raft_message = {hasattr(self.worker_manager, 'send_raft_message')}")
                    logging.debug(f"Bridge: worker_manager has raft_consensus = {hasattr(self.worker_manager, 'raft_consensus')}")
                
        except Exception as e:
            logging.error(f"Error sending join request to leader {leader_id}: {e}", exc_info=True)

    def _send_join_response(self, receiver_id: int, approved: bool, message: str, redirect_leader: int = None):
        """
        Send join response to a requesting node.
        
        Args:
            receiver_id: ID of the node to send response to
            approved: Whether the join request was approved
            message: Response message
            redirect_leader: ID of leader to redirect to (if not approved)
        """
        try:
            # Prepare join response content
            content = {
                RaftMessage.ARG_JOIN_APPROVED: approved,
                'message': message,
                RaftMessage.ARG_TIMESTAMP: time.time()
            }
            
            if redirect_leader:
                content[RaftMessage.ARG_LEADER_ID] = redirect_leader
            
            logging.info(f"Bridge: Sending join response to {receiver_id}: approved={approved}")
            
            # Send via worker manager's RAFT message system
            if (hasattr(self, 'worker_manager') and self.worker_manager and 
                hasattr(self.worker_manager, 'send_raft_message') and
                hasattr(self.worker_manager, 'raft_consensus')):  # Verify it's actually a worker manager
                
                logging.debug(f"Bridge: Using worker manager to send join response: {type(self.worker_manager).__name__}")
                self.worker_manager.send_raft_message(
                    RaftMessage.MSG_TYPE_JOIN_RESPONSE, 
                    receiver_id, 
                    content
                )
            else:
                logging.error("Worker manager not available or does not support RAFT message sending")
                logging.debug(f"Bridge: worker_manager = {getattr(self, 'worker_manager', None)}")
                if hasattr(self, 'worker_manager') and self.worker_manager:
                    logging.debug(f"Bridge: worker_manager type = {type(self.worker_manager).__name__}")
                    logging.debug(f"Bridge: worker_manager has send_raft_message = {hasattr(self.worker_manager, 'send_raft_message')}")
                    logging.debug(f"Bridge: worker_manager has raft_consensus = {hasattr(self.worker_manager, 'raft_consensus')}")
                
        except Exception as e:
            logging.error(f"Error sending join response to {receiver_id}: {e}", exc_info=True)

    def send_join_request_to_cluster(self, discovered_nodes: List[int], leader_hint: int = None):
        """
        Send join requests to discovered nodes to join an existing cluster.
        
        This method implements the event-driven join protocol where a new node
        actively requests to join the cluster rather than relying on periodic discovery.
        
        Args:
            discovered_nodes: List of node IDs discovered from service discovery
            leader_hint: Hint about who the current leader might be (from service discovery)
        """
        try:
            # Filter out ourselves
            other_nodes = [nid for nid in discovered_nodes if nid != self.consensus.raft_node.node_id]
            
            if not other_nodes:
                logging.info("Bridge: No other nodes to send join request to")
                return
            
            # Check if we're already in the process of joining
            if hasattr(self, '_join_in_progress') and self._join_in_progress:
                logging.debug("Bridge: Join already in progress, skipping duplicate request")
                return
                
            # Mark join as in progress to prevent duplicates
            self._join_in_progress = True
            
            # Try to determine who the leader is from multiple sources
            current_leader = self.consensus.raft_node.current_leader_id
            
            # Prioritize sources: our RAFT knowledge > service discovery hint > first discovered node
            target_node = None
            if current_leader and current_leader in other_nodes:
                # We know who the leader is from RAFT state, send directly
                target_node = current_leader
                logging.info(f"Bridge: Sending join request directly to known RAFT leader {target_node}")
            elif leader_hint and leader_hint in other_nodes:
                # Use service discovery hint
                target_node = leader_hint
                logging.info(f"Bridge: Sending join request to service discovery leader hint {target_node}")
            else:
                # Don't know the leader, send to first discovered node (it will redirect if needed)
                target_node = other_nodes[0]
                logging.info(f"Bridge: Sending join request to {target_node} (will redirect if not leader)")
            
            self._send_join_request_to_leader(target_node)
                
        except Exception as e:
            logging.error(f"Error sending join request to cluster: {e}", exc_info=True)
            # Reset join progress flag on error
            if hasattr(self, '_join_in_progress'):
                self._join_in_progress = False

    def set_comm_manager_callbacks(self, on_membership_change=None, on_node_registry_update=None):
        """
        Set callbacks that will be called when membership changes are committed.
        
        Args:
            on_membership_change: Callback for individual membership changes
            on_node_registry_update: Callback for full registry updates
        """
        self.on_membership_change_callback = on_membership_change
        self.on_node_registry_update_callback = on_node_registry_update
        logging.info(f"Communication manager callbacks registered with bridge")

    def register_with_comm_manager(self, comm_manager):
        """
        Register this bridge with the communication manager.
        
        This method is called by both the communication manager during its
        register_service_discovery_bridge() method and by the RaftWorkerManager
        during initialization.
        
        Args:
            comm_manager: The communication manager to register with
        """
        # Store a reference to the communication manager if not already set
        if not hasattr(self, 'comm_manager') or self.comm_manager is None:
            self.comm_manager = comm_manager
            
        # If this is a worker manager (has send_raft_message), store it separately
        # Check for worker manager by looking for RaftWorkerManager-specific methods
        if (hasattr(comm_manager, 'send_raft_message') and 
            hasattr(comm_manager, 'raft_consensus') and
            hasattr(comm_manager, 'topology_manager')):
            self.worker_manager = comm_manager
            logging.info(f"Bridge: Stored worker manager reference for RAFT message sending")
        elif hasattr(comm_manager, 'send_raft_message'):
            # This is likely the communication manager, don't overwrite worker manager
            logging.debug(f"Bridge: Communication manager also has send_raft_message, keeping existing worker manager")
        
        # Register callbacks for service discovery events
        if hasattr(comm_manager, 'on_node_discovered'):
            self._original_on_node_discovered = comm_manager.on_node_discovered
            comm_manager.on_node_discovered = self._on_node_discovered
        
        if hasattr(comm_manager, 'on_node_lost'):
            self._original_on_node_lost = comm_manager.on_node_lost
            comm_manager.on_node_lost = self._on_node_lost
        
        logging.info(f"Bridge registered with communication manager")
        return True
    
    def set_consensus_manager(self, consensus_manager):
        """
        Set the consensus manager reference.
        
        This is called by RaftConsensus.register_service_discovery_bridge() to 
        establish bidirectional references between consensus and bridge.
        
        Args:
            consensus_manager: The consensus manager instance
        """
        self.consensus = consensus_manager
        
        # Re-register callbacks that depend on consensus
        self._register_consensus_callbacks()
        
        logging.info(f"Consensus manager registered with bridge")
        return True
    
    def cleanup(self):
        """
        Clean up resources and restore original callbacks.
        
        This method is called during test teardown to properly clean up resources.
        """
        try:
            # Restore original callbacks if they exist
            if hasattr(self, '_original_on_node_discovered') and hasattr(self.comm_manager, 'on_node_discovered'):
                self.comm_manager.on_node_discovered = self._original_on_node_discovered
                
            if hasattr(self, '_original_on_node_lost') and hasattr(self.comm_manager, 'on_node_lost'):
                self.comm_manager.on_node_lost = self._original_on_node_lost
            
            # Clear RAFT node callbacks to prevent memory leaks
            if self.consensus and self.consensus.raft_node:
                node = self.consensus.raft_node
                node.on_membership_change = None
                node.on_commit = None
                node.on_send_prevote = None
                node.on_send_vote = None
                node.on_send_append = None
                node.on_send_snapshot = None
                node.on_send_prevote_response = None
                node.on_send_vote_response = None
                node.on_send_append_response = None
            
            # Clear references to break circular dependencies
            self.consensus = None
            self.comm_manager = None
            
            logging.info("Bridge cleanup completed")
        except Exception as e:
            logging.error(f"Error during bridge cleanup: {e}", exc_info=True)

    def on_commit(self, log_entry: Dict[str, Any], log_index: int):
        """
        Handle committed log entries - this is where join responses should be sent.
        
        This method is called by the consensus manager when log entries are committed.
        It handles event-driven responses for join requests and registry updates.
        
        Args:
            log_entry: The committed log entry
            log_index: The index of the committed entry
        """
        try:
            # Handle the log entry based on its structure
            # Check if this is a standard RAFT log entry with a command
            command = log_entry.get('command', {})
            if not command:
                # If no command, this might be a direct entry format
                command = log_entry
            
            if command.get('type') == 'membership':
                action = command.get('action')
                node_id = command.get('node_id')
                
                if action == 'add' and node_id:
                    # Check if this was a pending join request
                    pending_join_key = f"join_{node_id}"
                    if pending_join_key in self.pending_join_responses:
                        # Now send the join response after consensus is reached
                        self._send_join_response(node_id, True, "Join request accepted by leader")
                        del self.pending_join_responses[pending_join_key]
                        logging.info(f"Bridge: Sent join response to {node_id} after commit")
                    
                    # CRITICAL: Send snapshot to synchronize ANY newly added node
                    # This handles both explicit join requests AND service discovery additions
                    self._send_snapshot_to_new_node(node_id)
                    logging.info(f"Bridge: Sending snapshot to newly added node {node_id}")
                    
                    # Update registry with new node - this is critical for communication
                    node_info = command.get('node_info', {})
                    if node_info:
                        self._update_registry_for_node(node_id, node_info)
                        logging.info(f"Bridge: Updated registry for node {node_id} after commit")
                    else:
                        logging.warning(f"Bridge: No node_info available for node {node_id} - registry not updated")
                        
                elif action == 'remove' and node_id:
                    # Notify comm manager of node removal
                    self._notify_comm_manager_membership_change('remove', node_id)
                    logging.info(f"Bridge: Processed node {node_id} removal after commit")
                        
        except Exception as e:
            logging.error(f"Bridge: Error in commit callback: {e}", exc_info=True)

    def _update_registry_for_node(self, node_id: int, node_info: Dict[str, Any]):
        """
        Update communication manager registry with new node info.
        
        Args:
            node_id: ID of the node to register
            node_info: Connection information for the node
        """
        try:
            if hasattr(self.comm_manager, 'add_node_to_registry'):
                self.comm_manager.add_node_to_registry(node_info)
                logging.info(f"Bridge: Refreshed node registry for node {node_id}")
                return
                
            logging.warning(f"Bridge: No method available to update registry for node {node_id}")
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            logging.error(f"Bridge: Failed to register node {node_id}: {e}")

    def _notify_comm_manager_membership_change(self, action: str, node_id: int, node_info: Dict[str, Any] = None):
        """
        Notify communication manager of membership changes.
        
        Args:
            action: 'add' or 'remove'
            node_id: ID of the node
            node_info: Connection info (for add operations)
        """
        logging.debug(f"Bridge: Notifying comm manager of membership change: action={action}, node_id={node_id}")
        try:
            if action == 'add' and node_info:
                self._update_registry_for_node(node_id, node_info)
            elif action == 'remove':
                #TODO
                # Remove from registry if possible
                if hasattr(self.comm_manager, 'client_registry') and node_id in self.comm_manager.client_registry:
                    del self.comm_manager.client_registry[node_id]
                    logging.info(f"Bridge: Removed node {node_id} from client registry")
        except Exception as e:
            logging.error(f"Bridge: Error notifying comm manager of membership change: {e}")
    
    def _send_snapshot_to_new_node(self, node_id: int):
        """
        Send current state snapshot to a newly joined node.
        
        Args:
            node_id: ID of the newly joined node
        """
        try:
            logging.info(f"Bridge: Attempting to send snapshot to node {node_id}")
            
            if (not hasattr(self, 'worker_manager') or not self.worker_manager or
                not hasattr(self.worker_manager, 'raft_consensus') or 
                not self.worker_manager.raft_consensus):
                logging.warning(f"Bridge: Cannot send snapshot to {node_id} - no worker manager or consensus")
                logging.debug(f"Bridge: worker_manager = {getattr(self, 'worker_manager', 'None')}")
                return
                
            raft_node = self.worker_manager.raft_consensus.raft_node
            if not raft_node or raft_node.state != RaftState.LEADER:
                logging.warning(f"Bridge: Cannot send snapshot to {node_id} - not leader (state: {raft_node.state if raft_node else 'None'})")
                return
            
            # Get current state snapshot
            last_log_index, last_log_term = raft_node.get_last_log_info()
            
            # Create snapshot data with current cluster state
            snapshot_data = {
                'cluster_state': {
                    'nodes': list(raft_node.known_nodes),
                    'current_term': raft_node.current_term,
                    'commit_index': raft_node.commit_index,
                    'last_applied': raft_node.last_applied
                },
                'log_entries': raft_node.log.copy() if hasattr(raft_node, 'log') else [],
                'metadata': {
                    'snapshot_term': raft_node.current_term,
                    'snapshot_index': last_log_index
                }
            }
            
            logging.info(f"Bridge: Sending snapshot to new node {node_id} (index: {last_log_index}, term: {last_log_term})")
            
            # Send install snapshot message
            if hasattr(self.worker_manager, 'send_install_snapshot'):
                logging.debug(f"Bridge: Using worker_manager.send_install_snapshot for node {node_id}")
                self.worker_manager.send_install_snapshot(
                    receiver_id=node_id,
                    term=raft_node.current_term,
                    last_included_index=last_log_index,
                    last_included_term=last_log_term,
                    offset=0,  # Start from beginning of snapshot
                    data=snapshot_data,
                    done=True  # Single snapshot message
                )
                
                # Reset next_index for this node to start from the snapshot
                if hasattr(raft_node, 'next_index'):
                    raft_node.next_index[node_id] = last_log_index + 1
                    raft_node.match_index[node_id] = last_log_index  # Set to last_log_index, not 0
                    logging.info(f"Bridge: Reset next_index for {node_id} to {last_log_index + 1}, match_index to {last_log_index}")
            else:
                logging.error(f"Bridge: Worker manager does not have send_install_snapshot method")
                logging.debug(f"Bridge: Available worker_manager methods: {[m for m in dir(self.worker_manager) if not m.startswith('_')]}")
                
        except Exception as e:
            logging.error(f"Bridge: Error sending snapshot to new node {node_id}: {e}", exc_info=True)
def _msg(msg_type: int, receiver=None, **kwargs):
    """
    Create a message envelope for RAFT messages.
    
    Args:
        msg_type: Type of RAFT message
        receiver: Target node ID (for directed messages)
        **kwargs: Additional message parameters
        
    Returns:
        Message envelope dictionary
    """
    envelope = {
        RaftMessage.ARG_TYPE: msg_type,
        'message_source': 'raft'  # Mark as RAFT message for filtering
    }
    envelope.update(kwargs)
    
    # Add receiver if specified
    if receiver is not None:
        envelope[RaftMessage.ARG_RECEIVER] = receiver
        
    return envelope