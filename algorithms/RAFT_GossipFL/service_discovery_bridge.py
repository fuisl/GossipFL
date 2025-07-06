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
            
            # Helper function to filter out envelope parameters before calling consensus methods
            def _filter_params(params):
                """Filter out message envelope parameters that consensus methods don't expect."""
                filtered = {}
                envelope_keys = {
                    RaftMessage.ARG_TYPE, 
                    RaftMessage.ARG_SENDER, 
                    RaftMessage.ARG_RECEIVER,
                    'message_source'
                }
                for key, value in params.items():
                    if key not in envelope_keys:
                        filtered[key] = value
                return filtered
            
            # Register handlers for each RAFT message type
            handler_map = {
                RaftMessage.MSG_TYPE_PREVOTE_REQUEST: 
                    lambda p: self.consensus.handle_prevote_request(**_filter_params(p)),
                RaftMessage.MSG_TYPE_PREVOTE_RESPONSE: 
                    lambda p: self.consensus.handle_prevote_response(**_filter_params(p)),
                RaftMessage.MSG_TYPE_REQUEST_VOTE: 
                    lambda p: self.consensus.handle_vote_request(**_filter_params(p)),
                RaftMessage.MSG_TYPE_VOTE_RESPONSE: 
                    lambda p: self.consensus.handle_vote_response(**_filter_params(p)),
                RaftMessage.MSG_TYPE_APPEND_ENTRIES: 
                    lambda p: self.consensus.handle_append_entries(**_filter_params(p)),
                RaftMessage.MSG_TYPE_APPEND_RESPONSE: 
                    lambda p: self.consensus.handle_append_response(**_filter_params(p)),
                RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT: 
                    lambda p: self.consensus.handle_install_snapshot(**_filter_params(p))
            }
            
            # Register all handlers
            for msg_type, handler in handler_map.items():
                # Use default parameter to capture handler by value, not reference
                cm.add_raft_handler(msg_type, (lambda params, h=handler: h(params)))
                
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
            self.consensus.raft_node.on_commit = self._on_commit_any
            
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
            
            # Check if we're the leader
            if self.consensus.raft_node.state == RaftState.LEADER:
                logging.info(f"Bridge: Processing join request as leader for node {sender_id}")
                
                # Propose membership change to add the node through RAFT consensus
                self.consensus.propose_membership_change(
                    action='add',
                    node_id=sender_id,
                    node_info=node_info,
                    reason='join_request'
                )
                
                # Send positive response
                self._send_join_response(sender_id, True, "Join request accepted by leader")
                
            else:
                # We're not the leader - redirect to current leader
                current_leader = self.consensus.raft_node.leader_id
                if current_leader and current_leader != sender_id:
                    logging.info(f"Bridge: Redirecting join request from {sender_id} to leader {current_leader}")
                    self._send_join_response(sender_id, False, f"Not leader, redirect to {current_leader}", current_leader)
                else:
                    logging.warning(f"Bridge: No known leader to redirect join request from {sender_id}")
                    self._send_join_response(sender_id, False, "No current leader known")
                    
        except Exception as e:
            logging.error(f"Error handling join request from {sender_id}: {e}", exc_info=True)
            self._send_join_response(sender_id, False, f"Error processing join request: {e}")

    def handle_join_response(self, sender_id: int, response_params: Dict):
        """
        Handle join response from another node.
        
        This method is called when we receive a response to our join request.
        
        Args:
            sender_id: ID of the node sending the response
            response_params: Response parameters including status and message
        """
        try:
            join_approved = response_params.get(RaftMessage.ARG_JOIN_APPROVED, False)
            message = response_params.get('message', 'No message')
            redirect_leader = response_params.get(RaftMessage.ARG_LEADER_ID)
            
            logging.info(f"Bridge: Received join response from {sender_id}: approved={join_approved}, message='{message}'")
            
            if join_approved:
                logging.info(f"Join request approved by node {sender_id}")
                # The membership change will be propagated through RAFT consensus
                # and handled by _on_commit_membership when committed
                
            elif redirect_leader:
                logging.info(f"Join request redirected to leader {redirect_leader}")
                # Send join request to the actual leader
                self._send_join_request_to_leader(redirect_leader)
                
            else:
                logging.warning(f"Join request rejected by {sender_id}: {message}")
                
        except Exception as e:
            logging.error(f"Error handling join response from {sender_id}: {e}", exc_info=True)

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
            
            # Prepare join request message
            message_params = {
                RaftMessage.ARG_SENDER: self.consensus.raft_node.node_id,
                RaftMessage.ARG_RECEIVER: leader_id,
                RaftMessage.ARG_TYPE: RaftMessage.MSG_TYPE_JOIN_REQUEST,
                RaftMessage.ARG_NODE_INFO: node_info,
                RaftMessage.ARG_TIMESTAMP: time.time()
            }
            
            logging.info(f"Bridge: Sending join request to leader {leader_id}")
            
            # Send via communication manager
            if hasattr(self.comm_manager, 'send_message'):
                self.comm_manager.send_message(leader_id, message_params)
            else:
                logging.error("Communication manager does not support send_message")
                
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
            # Prepare join response message
            message_params = {
                RaftMessage.ARG_SENDER: self.consensus.raft_node.node_id,
                RaftMessage.ARG_RECEIVER: receiver_id,
                RaftMessage.ARG_TYPE: RaftMessage.MSG_TYPE_JOIN_RESPONSE,
                RaftMessage.ARG_JOIN_APPROVED: approved,
                'message': message,
                RaftMessage.ARG_TIMESTAMP: time.time()
            }
            
            if redirect_leader:
                message_params[RaftMessage.ARG_LEADER_ID] = redirect_leader
            
            logging.info(f"Bridge: Sending join response to {receiver_id}: approved={approved}")
            
            # Send via communication manager
            if hasattr(self.comm_manager, 'send_message'):
                self.comm_manager.send_message(receiver_id, message_params)
            else:
                logging.error("Communication manager does not support send_message")
                
        except Exception as e:
            logging.error(f"Error sending join response to {receiver_id}: {e}", exc_info=True)

    def send_join_request_to_cluster(self, discovered_nodes: List[int]):
        """
        Send join requests to discovered nodes to join an existing cluster.
        
        This method implements the event-driven join protocol where a new node
        actively requests to join the cluster rather than relying on periodic discovery.
        
        Args:
            discovered_nodes: List of node IDs discovered from service discovery
        """
        try:
            # Filter out ourselves
            other_nodes = [nid for nid in discovered_nodes if nid != self.consensus.raft_node.node_id]
            
            if not other_nodes:
                logging.info("Bridge: No other nodes to send join request to")
                return
                
            # Try to determine who the leader is from our knowledge
            current_leader = self.consensus.raft_node.leader_id
            
            if current_leader and current_leader in other_nodes:
                # We know who the leader is, send directly
                logging.info(f"Bridge: Sending join request directly to known leader {current_leader}")
                self._send_join_request_to_leader(current_leader)
            else:
                # Don't know the leader, send to first discovered node (it will redirect if needed)
                target_node = other_nodes[0]
                logging.info(f"Bridge: Sending join request to {target_node} (will redirect if not leader)")
                self._send_join_request_to_leader(target_node)
                
        except Exception as e:
            logging.error(f"Error sending join request to cluster: {e}", exc_info=True)

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