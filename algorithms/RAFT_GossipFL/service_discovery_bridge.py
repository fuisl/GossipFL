#!/usr/bin/env python3

"""
RAFT Service Discovery Bridge

This component bridges service discovery events with RAFT consensus operations.
It processes incoming service discovery hints and converts them to RAFT membership
proposals, enabling dynamic node joining through consensus.

Key Features:
- Processes service discovery hints (node discovered/lost)
- Converts hints to RAFT membership proposals
- Coordinates state synchronization for new nodes
- Validates node capabilities and metadata
- Handles bridge failures gracefully
"""

import logging
import threading
import time
from typing import Optional, Dict, Any, Set, Callable
from dataclasses import dataclass
from enum import Enum


class BridgeState(Enum):
    """Bridge operational states."""
    INACTIVE = "inactive"
    INITIALIZING = "initializing"
    ACTIVE = "active"
    ERROR = "error"


@dataclass
class NodeDiscoveryEvent:
    """Represents a node discovery event from service discovery."""
    event_type: str  # "discovered" or "lost"
    node_id: int
    node_info: Optional[Dict[str, Any]] = None
    timestamp: float = 0.0


class RaftServiceDiscoveryBridge:
    """
    Bridge between service discovery and RAFT consensus.
    
    This component receives service discovery events and translates them
    into RAFT consensus operations for dynamic membership management.
    """
    
    def __init__(self, node_id: int, raft_consensus=None, 
                 validation_timeout: float = 10.0, 
                 sync_timeout: float = 30.0):
        """
        Initialize the service discovery bridge.
        
        Args:
            node_id: Local node identifier
            raft_consensus: RAFT consensus manager (can be set later)
            validation_timeout: Timeout for node validation (seconds)
            sync_timeout: Timeout for state synchronization (seconds)
        """
        self.node_id = node_id
        self.raft_consensus = raft_consensus
        self.validation_timeout = validation_timeout
        self.sync_timeout = sync_timeout
        
        # Bridge state
        self.state = BridgeState.INACTIVE
        self.comm_manager = None
        self.lock = threading.RLock()
        
        # Event tracking
        self.pending_events: Set[int] = set()  # Node IDs with pending events
        self.processed_events: Dict[int, float] = {}  # Node ID -> timestamp
        self.failed_events: Dict[int, str] = {}  # Node ID -> error reason
        
        # Statistics
        self.events_processed = 0
        self.events_failed = 0
        self.nodes_added = 0
        self.nodes_removed = 0
        
        logging.info(f"Service discovery bridge initialized for node {node_id}")
    
    def set_raft_consensus(self, raft_consensus):
        """
        Set the RAFT consensus manager reference.
        
        Args:
            raft_consensus: RAFT consensus manager instance
        """
        with self.lock:
            self.raft_consensus = raft_consensus
            if raft_consensus and self.state == BridgeState.INACTIVE:
                self.state = BridgeState.INITIALIZING
                logging.info(f"Bridge: RAFT consensus manager set for node {self.node_id}")
    
    def set_consensus_manager(self, consensus_manager):
        """
        Set the consensus manager reference (alias for set_raft_consensus).
        
        Args:
            consensus_manager: RAFT consensus manager instance
        """
        self.set_raft_consensus(consensus_manager)
    
    def register_with_comm_manager(self, comm_manager):
        """
        Register the bridge with the communication manager.
        
        This replaces the original callbacks with bridge-aware versions
        that process service discovery events through RAFT consensus.
        
        Args:
            comm_manager: The DynamicGRPCCommManager instance
        """
        with self.lock:
            self.comm_manager = comm_manager
            
            # Store original callbacks if they exist
            self._original_on_node_discovered = comm_manager.on_node_discovered
            self._original_on_node_lost = comm_manager.on_node_lost
            
            # Replace callbacks with bridge versions
            comm_manager.on_node_discovered = self._on_node_discovered_bridge
            comm_manager.on_node_lost = self._on_node_lost_bridge
            
            self.state = BridgeState.ACTIVE
            
            logging.info(f"Bridge: Registered with communication manager for node {self.node_id}")
    
    def _on_node_discovered_bridge(self, discovered_node_id: int, node_info):
        """
        Bridge callback for node discovered events.
        
        Args:
            discovered_node_id: ID of the discovered node
            node_info: NodeInfo object with node details
        """
        try:
            # Create discovery event
            event = NodeDiscoveryEvent(
                event_type="discovered",
                node_id=discovered_node_id,
                node_info={
                    'ip_address': node_info.ip_address,
                    'port': node_info.port,
                    'capabilities': node_info.capabilities,
                    'metadata': node_info.metadata
                },
                timestamp=time.time()
            )
            
            # Process the event
            self._process_discovery_event(event)
            
            # Call original callback if it exists
            if self._original_on_node_discovered:
                self._original_on_node_discovered(discovered_node_id, node_info)
                
        except Exception as e:
            logging.error(f"Bridge: Error processing node discovered event for {discovered_node_id}: {e}")
            self._record_event_failure(discovered_node_id, str(e))
    
    def _on_node_lost_bridge(self, lost_node_id: int):
        """
        Bridge callback for node lost events.
        
        Args:
            lost_node_id: ID of the lost node
        """
        try:
            # Create loss event
            event = NodeDiscoveryEvent(
                event_type="lost",
                node_id=lost_node_id,
                timestamp=time.time()
            )
            
            # Process the event
            self._process_discovery_event(event)
            
            # Call original callback if it exists
            if self._original_on_node_lost:
                self._original_on_node_lost(lost_node_id)
                
        except Exception as e:
            logging.error(f"Bridge: Error processing node lost event for {lost_node_id}: {e}")
            self._record_event_failure(lost_node_id, str(e))
    
    def _process_discovery_event(self, event: NodeDiscoveryEvent):
        """
        Process a service discovery event through RAFT consensus.
        
        Args:
            event: The discovery event to process
        """
        with self.lock:
            try:
                # Check bridge state
                if self.state != BridgeState.ACTIVE:
                    logging.warning(f"Bridge: Cannot process event in {self.state} state")
                    return
                
                # Check if we have RAFT consensus
                if not self.raft_consensus:
                    logging.warning(f"Bridge: No RAFT consensus manager available")
                    self._record_event_failure(event.node_id, "No RAFT consensus manager")
                    return
                
                # Skip self-events
                if event.node_id == self.node_id:
                    logging.debug(f"Bridge: Skipping self-event for node {event.node_id}")
                    return
                
                # Check for duplicate events
                if event.node_id in self.pending_events:
                    logging.debug(f"Bridge: Event for node {event.node_id} already pending")
                    return
                
                # Mark as pending
                self.pending_events.add(event.node_id)
                
                logging.info(f"Bridge: Processing {event.event_type} event for node {event.node_id}")
                
                # Process based on event type
                if event.event_type == "discovered":
                    success = self._process_node_discovered(event)
                elif event.event_type == "lost":
                    success = self._process_node_lost(event)
                else:
                    logging.error(f"Bridge: Unknown event type: {event.event_type}")
                    success = False
                
                # Update statistics
                if success:
                    self.events_processed += 1
                    self.processed_events[event.node_id] = event.timestamp
                    if event.event_type == "discovered":
                        self.nodes_added += 1
                    elif event.event_type == "lost":
                        self.nodes_removed += 1
                else:
                    self.events_failed += 1
                
                # Remove from pending
                self.pending_events.discard(event.node_id)
                
            except Exception as e:
                logging.error(f"Bridge: Error processing discovery event: {e}")
                self._record_event_failure(event.node_id, str(e))
                self.pending_events.discard(event.node_id)
    
    def _process_node_discovered(self, event: NodeDiscoveryEvent) -> bool:
        """
        Process a node discovered event.
        
        Args:
            event: The node discovered event
            
        Returns:
            bool: True if successfully processed, False otherwise
        """
        try:
            node_id = event.node_id
            node_info = event.node_info
            
            # Validate node capabilities
            if not self._validate_node_capabilities(node_info):
                logging.warning(f"Bridge: Node {node_id} failed capability validation")
                return False
            
            # Check if we're the RAFT leader (only leader processes membership changes)
            if not self._is_raft_leader():
                logging.debug(f"Bridge: Not RAFT leader, forwarding membership proposal to leader")
                return self._forward_to_leader(event)
            
            # Propose membership change through RAFT consensus
            success = self._propose_membership_addition(node_id, node_info)
            
            if success:
                logging.info(f"Bridge: Successfully proposed addition of node {node_id}")
                # State synchronization will be handled when the membership change is committed
            else:
                logging.warning(f"Bridge: Failed to propose addition of node {node_id}")
            
            return success
            
        except Exception as e:
            logging.error(f"Bridge: Error processing node discovered for {event.node_id}: {e}")
            return False
    
    def _process_node_lost(self, event: NodeDiscoveryEvent) -> bool:
        """
        Process a node lost event.
        
        Args:
            event: The node lost event
            
        Returns:
            bool: True if successfully processed, False otherwise
        """
        try:
            node_id = event.node_id
            
            # Check if we're the RAFT leader
            if not self._is_raft_leader():
                logging.debug(f"Bridge: Not RAFT leader, forwarding membership removal to leader")
                return self._forward_to_leader(event)
            
            # Propose membership removal through RAFT consensus
            success = self._propose_membership_removal(node_id)
            
            if success:
                logging.info(f"Bridge: Successfully proposed removal of node {node_id}")
            else:
                logging.warning(f"Bridge: Failed to propose removal of node {node_id}")
            
            return success
            
        except Exception as e:
            logging.error(f"Bridge: Error processing node lost for {event.node_id}: {e}")
            return False
    
    def _validate_node_capabilities(self, node_info: Dict[str, Any]) -> bool:
        """
        Validate that a discovered node has required capabilities.
        
        Args:
            node_info: Node information dictionary
            
        Returns:
            bool: True if node is valid, False otherwise
        """
        try:
            capabilities = node_info.get('capabilities', [])
            
            # Check for required capabilities
            required_capabilities = ['grpc', 'fedml']
            for cap in required_capabilities:
                if cap not in capabilities:
                    logging.warning(f"Bridge: Node missing required capability: {cap}")
                    return False
            
            # Validate IP address and port
            ip_address = node_info.get('ip_address')
            port = node_info.get('port')
            
            if not ip_address or not port:
                logging.warning(f"Bridge: Node missing IP address or port")
                return False
            
            # Additional validation can be added here
            return True
            
        except Exception as e:
            logging.error(f"Bridge: Error validating node capabilities: {e}")
            return False
    
    def _is_raft_leader(self) -> bool:
        """
        Check if the local node is the current RAFT leader.
        
        Returns:
            bool: True if local node is leader, False otherwise
        """
        try:
            if not self.raft_consensus:
                return False
            
            return self.raft_consensus.is_leader()
            
        except Exception as e:
            logging.error(f"Bridge: Error checking RAFT leadership: {e}")
            return False
    
    def _propose_membership_addition(self, node_id: int, node_info: Dict[str, Any]) -> bool:
        """
        Propose adding a node to the cluster through RAFT consensus.
        
        Args:
            node_id: ID of the node to add
            node_info: Node information
            
        Returns:
            bool: True if proposal was successful, False otherwise
        """
        try:
            if not self.raft_consensus:
                logging.error("Bridge: No RAFT consensus manager available")
                return False
            
            # Call RAFT consensus to propose membership addition
            success = self.raft_consensus.propose_membership_change(
                action="add",
                node_id=node_id,
                node_info=node_info
            )
            
            return success
            
        except Exception as e:
            logging.error(f"Bridge: Error proposing membership addition for {node_id}: {e}")
            return False
    
    def _propose_membership_removal(self, node_id: int) -> bool:
        """
        Propose removing a node from the cluster through RAFT consensus.
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            bool: True if proposal was successful, False otherwise
        """
        try:
            if not self.raft_consensus:
                logging.error("Bridge: No RAFT consensus manager available")
                return False
            
            # Call RAFT consensus to propose membership removal
            success = self.raft_consensus.propose_membership_change(
                action="remove",
                node_id=node_id
            )
            
            return success
            
        except Exception as e:
            logging.error(f"Bridge: Error proposing membership removal for {node_id}: {e}")
            return False
    
    def _forward_to_leader(self, event: NodeDiscoveryEvent) -> bool:
        """
        Forward a membership event to the RAFT leader.
        
        Args:
            event: The discovery event to forward
            
        Returns:
            bool: True if successfully forwarded, False otherwise
        """
        try:
            # Forward to consensus manager if available
            if self.raft_consensus:
                hint_data = {
                    'event_type': event.event_type,
                    'node_id': event.node_id,
                    'node_info': event.node_info,
                    'timestamp': event.timestamp
                }
                return self.raft_consensus._forward_to_leader('membership_proposal', {
                    'action': 'add' if event.event_type == 'discovered' else 'remove',
                    'node_id': event.node_id,
                    'node_info': event.node_info if event.event_type == 'discovered' else None,
                    'timestamp': event.timestamp
                })
            else:
                logging.warning(f"Bridge: No RAFT consensus manager available for forwarding")
                return False
            
        except Exception as e:
            logging.error(f"Bridge: Error forwarding event to leader: {e}")
            return False
    
    def _record_event_failure(self, node_id: int, error_message: str):
        """
        Record a failed event for debugging.
        
        Args:
            node_id: Node ID that failed
            error_message: Error description
        """
        with self.lock:
            self.failed_events[node_id] = error_message
            self.events_failed += 1
    
    def coordinate_state_sync(self, node_id: int, node_info: Dict[str, Any]) -> bool:
        """
        Coordinate state synchronization for a newly joined node.
        
        This is called when a membership change is committed and we need
        to synchronize state with the new node.
        
        Args:
            node_id: ID of the new node
            node_info: Node information
            
        Returns:
            bool: True if synchronization was successful, False otherwise
        """
        try:
            logging.info(f"Bridge: Coordinating state synchronization for node {node_id}")
            
            if not self.raft_consensus:
                logging.error("Bridge: No RAFT consensus manager for state sync")
                return False
            
            # Delegate to RAFT consensus for state synchronization
            success = self.raft_consensus.coordinate_new_node_sync(node_id)
            
            if success:
                logging.info(f"Bridge: State synchronization completed for node {node_id}")
            else:
                logging.warning(f"Bridge: State synchronization failed for node {node_id}")
            
            return success
            
        except Exception as e:
            logging.error(f"Bridge: Error coordinating state sync for node {node_id}: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get bridge statistics.
        
        Returns:
            dict: Statistics about bridge operations
        """
        with self.lock:
            return {
                'state': self.state.value,
                'events_processed': self.events_processed,
                'events_failed': self.events_failed,
                'nodes_added': self.nodes_added,
                'nodes_removed': self.nodes_removed,
                'pending_events': len(self.pending_events),
                'failed_events': len(self.failed_events)
            }
    
    def is_active(self) -> bool:
        """
        Check if the bridge is active.
        
        Returns:
            bool: True if bridge is active (ACTIVE or INITIALIZING), False otherwise
        """
        with self.lock:
            return self.state in [BridgeState.ACTIVE, BridgeState.INITIALIZING]
    
    def cleanup(self):
        """Clean up bridge resources."""
        with self.lock:
            self.state = BridgeState.INACTIVE
            
            # Restore original callbacks if possible
            if self.comm_manager:
                self.comm_manager.on_node_discovered = getattr(self, '_original_on_node_discovered', None)
                self.comm_manager.on_node_lost = getattr(self, '_original_on_node_lost', None)
            
            logging.info(f"Bridge: Cleanup completed for node {self.node_id}")
