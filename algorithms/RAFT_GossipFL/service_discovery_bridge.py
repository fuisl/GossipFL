"""
RAFT Service Discovery Bridge

Simple bridge between service discovery events and RAFT consensus operations.
When nodes are discovered/lost, this bridge converts them to RAFT membership proposals.
"""

import logging
import threading
import time
from typing import Optional, Dict, Any


class RaftServiceDiscoveryBridge:
    """
    Bridge between service discovery and RAFT consensus.
    
    Receives service discovery events and translates them into RAFT operations.
    """
    
    def __init__(self, node_id: int, raft_consensus=None, worker_manager=None):
        """
        Initialize the service discovery bridge.
        
        Args:
            node_id: Local node identifier
            raft_consensus: RAFT consensus manager (can be set later)
            worker_manager: Worker manager for sending messages (can be set later)
        """
        self.node_id = node_id
        self.raft_consensus = raft_consensus
        self.worker_manager = worker_manager
        self.comm_manager = None
        self.lock = threading.RLock()
        
        logging.info(f"Service discovery bridge initialized for node {node_id}")
    
    def set_raft_consensus(self, raft_consensus):
        """Set the RAFT consensus manager reference."""
        with self.lock:
            self.raft_consensus = raft_consensus
            logging.info(f"Bridge: RAFT consensus manager set for node {self.node_id}")
    
    def set_consensus_manager(self, consensus_manager):
        """Set the consensus manager reference (alias for set_raft_consensus)."""
        self.set_raft_consensus(consensus_manager)
    
    def register_with_comm_manager(self, comm_manager):
        """
        Register the bridge with the communication manager.
        
        This replaces the original callbacks with bridge-aware versions.
        
        Args:
            comm_manager: The DynamicGRPCCommManager instance
        """
        with self.lock:
            self.comm_manager = comm_manager
            
            # Store original callbacks if they exist
            self._original_on_node_discovered = getattr(comm_manager, 'on_node_discovered', None)
            self._original_on_node_lost = getattr(comm_manager, 'on_node_lost', None)
            
            # Replace callbacks with bridge versions
            comm_manager.on_node_discovered = self._on_node_discovered_bridge
            comm_manager.on_node_lost = self._on_node_lost_bridge
            
            logging.info(f"Bridge: Registered with communication manager for node {self.node_id}")
    
    def _on_node_discovered_bridge(self, discovered_node_id: int, node_info):
        """Bridge callback for node discovered events."""
        try:
            # Skip self-events
            if discovered_node_id == self.node_id:
                return
                
            logging.info(f"Bridge: Node {discovered_node_id} discovered")
            
            # Process through RAFT if we have consensus manager
            if self.raft_consensus:
                self._propose_add_node(discovered_node_id, node_info)
            
            # Call original callback if it exists
            if self._original_on_node_discovered:
                self._original_on_node_discovered(discovered_node_id, node_info)
                
        except Exception as e:
            logging.error(f"Bridge: Error processing node discovered event for {discovered_node_id}: {e}")
    
    def _on_node_lost_bridge(self, lost_node_id: int):
        """Bridge callback for node lost events."""
        try:
            # Skip self-events
            if lost_node_id == self.node_id:
                return
                
            logging.info(f"Bridge: Node {lost_node_id} lost")
            
            # Process through RAFT if we have consensus manager
            if self.raft_consensus:
                self._propose_remove_node(lost_node_id)
            
            # Call original callback if it exists
            if self._original_on_node_lost:
                self._original_on_node_lost(lost_node_id)
                
        except Exception as e:
            logging.error(f"Bridge: Error processing node lost event for {lost_node_id}: {e}")
    
    def _propose_add_node(self, node_id: int, node_info):
        """Propose adding a node through RAFT consensus."""
        try:
            # Create membership entry data
            entry_data = {
                'node_id': node_id,
                'ip_address': node_info.ip_address,
                'port': node_info.port,
                'action': 'add'
            }
            
            # Propose through RAFT consensus
            self.raft_consensus.propose_membership_change('add', entry_data)
            logging.info(f"Bridge: Proposed adding node {node_id} through RAFT")
            
        except Exception as e:
            logging.error(f"Bridge: Failed to propose adding node {node_id}: {e}")
    
    def _propose_remove_node(self, node_id: int):
        """Propose removing a node through RAFT consensus."""
        try:
            # Create membership entry data
            entry_data = {
                'node_id': node_id,
                'action': 'remove'
            }
            
            # Propose through RAFT consensus
            self.raft_consensus.propose_membership_change('remove', entry_data)
            logging.info(f"Bridge: Proposed removing node {node_id} through RAFT")
            
        except Exception as e:
            logging.error(f"Bridge: Failed to propose removing node {node_id}: {e}")
    
    def handle_join_request(self, joining_node_id: int, node_info: Dict[str, Any]) -> bool:
        """
        Handle a join request from a new node.
        
        Args:
            joining_node_id: ID of the node requesting to join
            node_info: Information about the joining node
            
        Returns:
            True if join request was processed, False otherwise
        """
        try:
            logging.info(f"Bridge: Handling join request from node {joining_node_id}")
            
            # Check if we have RAFT consensus
            if not self.raft_consensus:
                logging.warning(f"Bridge: No RAFT consensus manager for join request")
                return False
            
            # Only leaders can process join requests
            if not self.raft_consensus.is_leader():
                logging.info(f"Bridge: Not leader, forwarding join request to leader")
                return self._forward_join_request_to_leader(joining_node_id, node_info)
            
            # Propose adding the node
            self._propose_add_node(joining_node_id, type('NodeInfo', (), node_info)())
            return True
            
        except Exception as e:
            logging.error(f"Bridge: Error handling join request from {joining_node_id}: {e}")
            return False
    
    def handle_node_discovered(self, node_id: int, node_info: Dict[str, Any] = None):
        """Handle a node discovery event for joining the cluster."""
        try:
            # If this is self-discovery, send join request to discovered nodes
            if node_id == self.node_id:
                logging.info(f"Bridge: Self-discovery for node {node_id}, sending join request")
                
                # Get discovered nodes from communication manager
                if not self.comm_manager:
                    logging.error(f"Bridge: No communication manager available")
                    return False
                
                cluster_nodes = self.comm_manager.get_cluster_nodes_info()
                other_nodes = [n for n in cluster_nodes.keys() if n != self.node_id]
                
                if not other_nodes:
                    logging.warning(f"Bridge: No other nodes discovered, cannot join cluster")
                    return False
                
                # Create default node info if not provided
                if node_info is None:
                    node_info = {
                        'node_id': node_id,
                        'ip_address': 'localhost',
                        'port': 9000 + node_id,
                        'capabilities': ['grpc', 'fedml'],
                        'timestamp': time.time()
                    }
                
                # Send join request to the first discovered node (assume it's leader or will forward)
                target_node = other_nodes[0]
                
                if hasattr(self, 'worker_manager') and self.worker_manager:
                    self.worker_manager.send_join_request(target_node, node_info)
                    logging.info(f"Bridge: Sent join request to node {target_node}")
                    return True
                else:
                    logging.error(f"Bridge: No worker manager available to send join request")
                    return False
                    
        except Exception as e:
            logging.error(f"Bridge: Error handling node discovered event for {node_id}: {e}")
            return False

    def on_membership_change(self, new_nodes: set, round_num: int = 0):
        """
        Handle membership change notifications from RAFT consensus.
        
        This is called when RAFT consensus has applied a membership change.
        
        Args:
            new_nodes: Set of all current cluster nodes
            round_num: Training round number (if applicable)
        """
        try:
            logging.info(f"Bridge: Membership changed to {new_nodes}")
            
            # Update communication manager's node registry
            if self.comm_manager:
                self._update_comm_manager_registry(new_nodes)
                
        except Exception as e:
            logging.error(f"Bridge: Error handling membership change: {e}")
    
    def _update_comm_manager_registry(self, new_nodes: set):
        """Update the communication manager's node registry with new membership."""
        try:
            # Convert node set to cluster info format
            cluster_info = {'nodes': []}
            
            for node_id in new_nodes:
                try:
                    # Get node info from comm manager if available
                    node_info = self.comm_manager.get_node_info(node_id)
                    cluster_info['nodes'].append({
                        'node_id': node_id,
                        'ip_address': node_info.ip_address,
                        'port': node_info.port
                    })
                except Exception as e:
                    logging.warning(f"Bridge: Could not get info for node {node_id}: {e}")
            
            # Update the registry
            if hasattr(self.comm_manager, '_update_node_registry_from_discovery'):
                self.comm_manager._update_node_registry_from_discovery(cluster_info)
                logging.info(f"Bridge: Updated comm manager registry with {len(cluster_info['nodes'])} nodes")
            
        except Exception as e:
            logging.error(f"Bridge: Failed to update comm manager registry: {e}")
    
    def stop(self):
        """
        Stop the service discovery bridge and clean up resources.
        """
        try:
            with self.lock:
                logging.info(f"Bridge: Stopping service discovery bridge for node {self.node_id}")
                
                # Restore original callbacks if we have a comm manager
                if self.comm_manager:
                    if hasattr(self, '_original_on_node_discovered'):
                        self.comm_manager.on_node_discovered = self._original_on_node_discovered
                    if hasattr(self, '_original_on_node_lost'):
                        self.comm_manager.on_node_lost = self._original_on_node_lost
                
                # Clear references
                self.raft_consensus = None
                self.comm_manager = None
                
                logging.info(f"Bridge: Service discovery bridge stopped for node {self.node_id}")
                
        except Exception as e:
            logging.error(f"Bridge: Error stopping service discovery bridge: {e}")

    def _forward_join_request_to_leader(self, joining_node_id: int, node_info: Dict[str, Any]) -> bool:
        """Forward join request to the current leader."""
        try:
            # Get the current leader from RAFT consensus
            leader_id = self.raft_consensus.get_current_leader()
            
            if leader_id is None:
                logging.warning(f"Bridge: No known leader to forward join request")
                return False
            
            # Send join request message to leader
            if hasattr(self, 'worker_manager') and self.worker_manager:
                self.worker_manager.send_join_request(leader_id, node_info)
                logging.info(f"Bridge: Forwarded join request from {joining_node_id} to leader {leader_id}")
                return True
            else:
                logging.error(f"Bridge: No worker manager available to forward join request")
                return False
                
        except Exception as e:
            logging.error(f"Bridge: Error forwarding join request to leader: {e}")
            return False
        
    def set_worker_manager(self, worker_manager):
        """Set the worker manager reference."""
        with self.lock:
            self.worker_manager = worker_manager
            logging.info(f"Bridge: Worker manager set for node {self.node_id}")