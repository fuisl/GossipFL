"""
gRPC-based Gateway Server for RAFT+GossipFL Dynamic Node Discovery

This module implements a gRPC-based gateway server that integrates with the
existing gRPC communication infrastructure while providing service discovery
capabilities for dynamic node joining.
"""

import logging
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Callable
from dataclasses import dataclass
from enum import Enum
import grpc
from concurrent import futures
import queue

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2
from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc


class GatewayStateSync:
    """
    Handles synchronization between RAFT consensus events and gateway state.
    
    This component makes the gateway eventually consistent with RAFT state
    by listening to RAFT events and updating gateway registry accordingly.
    """
    
    def __init__(self, gateway_state, node_id: int):
        self.gateway_state = gateway_state
        self.node_id = node_id
        self.event_queue = queue.Queue()
        self.event_subscribers = []
        self.sync_thread = None
        self.sync_running = False
        self.sync_lock = threading.Lock()
        
        logging.info(f"Gateway state sync initialized for node {node_id}")
    
    def start(self):
        """Start the gateway state synchronization thread."""
        with self.sync_lock:
            if not self.sync_running:
                self.sync_running = True
                self.sync_thread = threading.Thread(
                    target=self._sync_worker,
                    name=f"gateway-state-sync-{self.node_id}",
                    daemon=True
                )
                self.sync_thread.start()
                logging.info(f"Gateway state sync started for node {self.node_id}")
    
    def stop(self):
        """Stop the gateway state synchronization thread."""
        with self.sync_lock:
            if self.sync_running:
                self.sync_running = False
                # Signal the thread to stop
                self.event_queue.put(None)
                if self.sync_thread:
                    self.sync_thread.join(timeout=5.0)
                logging.info(f"Gateway state sync stopped for node {self.node_id}")
    
    def handle_raft_event(self, event):
        """
        Handle RAFT events and update gateway state accordingly.
        
        Args:
            event: GatewayEvent from RAFT consensus
        """
        try:
            # Queue the event for processing
            self.event_queue.put(event)
            
            # Also notify subscribers immediately for real-time updates
            self._notify_subscribers(event)
            
        except Exception as e:
            logging.error(f"Gateway state sync: Error handling RAFT event: {e}", exc_info=True)
    
    def subscribe_to_events(self, callback: Callable):
        """Subscribe to gateway events."""
        with self.sync_lock:
            self.event_subscribers.append(callback)
    
    def unsubscribe_from_events(self, callback: Callable):
        """Unsubscribe from gateway events."""
        with self.sync_lock:
            if callback in self.event_subscribers:
                self.event_subscribers.remove(callback)
    
    def _sync_worker(self):
        """Worker thread for processing RAFT events."""
        while self.sync_running:
            try:
                # Get event from queue (blocking)
                event = self.event_queue.get(timeout=1.0)
                
                # Check for stop signal
                if event is None:
                    break
                
                # Process the event
                self._process_event(event)
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.error(f"Gateway state sync worker error: {e}", exc_info=True)
    
    def _process_event(self, event):
        """Process a single RAFT event."""
        try:
            if event.event_type == "membership_change":
                self._process_membership_change(event)
            elif event.event_type == "leader_change":
                self._process_leader_change(event)
            elif event.event_type == "bootstrap_complete":
                self._process_bootstrap_complete(event)
            elif event.event_type == "state_reconciliation_request":
                self._handle_reconciliation_request(event)
            elif event.event_type == "state_inconsistency":
                self._handle_inconsistency_report(event)
            else:
                logging.warning(f"Gateway state sync: Unknown event type: {event.event_type}")
                
        except Exception as e:
            logging.error(f"Gateway state sync: Error processing event {event.event_type}: {e}", exc_info=True)
    
    def _process_membership_change(self, event):
        """Process membership change event."""
        logging.info(f"Gateway state sync: Processing membership change - "
                   f"Added: {event.added_nodes}, Removed: {event.removed_nodes}")
        
        with self.gateway_state.lock:
            # Remove nodes that left
            for node_id in event.removed_nodes:
                if node_id in self.gateway_state.nodes:
                    self.gateway_state.nodes[node_id].status = NodeStatus.LEAVING
                    logging.info(f"Gateway state sync: Marked node {node_id} as leaving")
            
            # Update registry version
            self.gateway_state.registry_version += 1
    
    def _process_leader_change(self, event):
        """Process leader change event."""
        logging.info(f"Gateway state sync: Processing leader change - "
                   f"From: {event.old_leader}, To: {event.new_leader}")
        
        success = self.gateway_state.update_leader(event.new_leader)
        if success:
            logging.info(f"Gateway state sync: Updated leader to {event.new_leader}")
        else:
            logging.warning(f"Gateway state sync: Failed to update leader to {event.new_leader}")
    
    def _process_bootstrap_complete(self, event):
        """Process bootstrap completion event."""
        logging.info(f"Gateway state sync: Processing bootstrap complete - "
                   f"Leader: {event.initial_leader}, Nodes: {event.initial_nodes}")
        
        # Mark bootstrap as complete
        with self.gateway_state.lock:
            self.gateway_state.bootstrap_initiated = True
            self.gateway_state.leader_id = event.initial_leader
    
    def _notify_subscribers(self, event):
        """Notify all event subscribers."""
        with self.sync_lock:
            for callback in self.event_subscribers:
                try:
                    callback(event)
                except Exception as e:
                    logging.error(f"Gateway state sync: Error notifying subscriber: {e}", exc_info=True)


class GatewayEventBroadcaster:
    """
    Handles broadcasting of gateway events to subscribed clients.
    
    This component provides real-time event streaming to clients
    instead of requiring them to poll for updates.
    """
    
    def __init__(self):
        self.subscribers = {}  # {subscriber_id: subscriber_info}
        self.event_history = []  # Keep recent events for new subscribers
        self.max_history = 100  # Maximum events to keep in history
        self.broadcaster_lock = threading.Lock()
        
        logging.info("Gateway event broadcaster initialized")
    
    def subscribe(self, subscriber_id: str, event_callback: Callable):
        """
        Subscribe to gateway events.
        
        Args:
            subscriber_id: Unique identifier for the subscriber
            event_callback: Function to call when events occur
        """
        with self.broadcaster_lock:
            self.subscribers[subscriber_id] = {
                'callback': event_callback,
                'subscribed_at': time.time(),
                'event_count': 0
            }
            logging.info(f"Gateway: Client {subscriber_id} subscribed to events")
    
    def unsubscribe(self, subscriber_id: str):
        """Unsubscribe from gateway events."""
        with self.broadcaster_lock:
            if subscriber_id in self.subscribers:
                del self.subscribers[subscriber_id]
                logging.info(f"Gateway: Client {subscriber_id} unsubscribed from events")
    
    def broadcast_event(self, event_data):
        """
        Broadcast an event to all subscribers.
        
        Args:
            event_data: Dictionary containing event information
        """
        with self.broadcaster_lock:
            # Add to event history
            self.event_history.append({
                'timestamp': time.time(),
                'event': event_data
            })
            
            # Trim history if needed
            if len(self.event_history) > self.max_history:
                self.event_history.pop(0)
            
            # Notify all subscribers
            failed_subscribers = []
            for subscriber_id, subscriber_info in self.subscribers.items():
                try:
                    subscriber_info['callback'](event_data)
                    subscriber_info['event_count'] += 1
                except Exception as e:
                    logging.error(f"Gateway: Error notifying subscriber {subscriber_id}: {e}")
                    failed_subscribers.append(subscriber_id)
            
            # Remove failed subscribers
            for subscriber_id in failed_subscribers:
                self.unsubscribe(subscriber_id)
    
    def get_recent_events(self, since_timestamp: float = None) -> List[Dict]:
        """
        Get recent events since a specific timestamp.
        
        Args:
            since_timestamp: Only return events after this timestamp
            
        Returns:
            List of event dictionaries
        """
        with self.broadcaster_lock:
            if since_timestamp is None:
                return [item['event'] for item in self.event_history]
            else:
                return [item['event'] for item in self.event_history 
                       if item['timestamp'] > since_timestamp]
    
    def get_stats(self) -> Dict:
        """Get broadcaster statistics."""
        with self.broadcaster_lock:
            return {
                'total_subscribers': len(self.subscribers),
                'total_events_in_history': len(self.event_history),
                'subscribers': {
                    subscriber_id: {
                        'subscribed_at': info['subscribed_at'],
                        'event_count': info['event_count']
                    }
                    for subscriber_id, info in self.subscribers.items()
                }
            }


class NodeStatus(Enum):
    """Status of a node in the gateway registry."""
    JOINING = "joining"
    ACTIVE = "active"
    SUSPECTED = "suspected"
    FAILED = "failed"
    LEAVING = "leaving"


@dataclass
class GatewayNodeInfo:
    """Information about a registered node."""
    node_id: int
    ip_address: str
    port: int
    capabilities: List[str]
    status: NodeStatus
    last_seen: float
    join_timestamp: float
    metadata: Dict[str, str]
    
    def to_protobuf(self) -> grpc_comm_manager_pb2.NodeInfo:
        """Convert to protobuf NodeInfo message."""
        return grpc_comm_manager_pb2.NodeInfo(
            node_id=self.node_id,
            ip_address=self.ip_address,
            port=self.port,
            capabilities=self.capabilities,
            status=self.status.value,
            last_seen=self.last_seen,
            join_timestamp=self.join_timestamp,
            metadata=self.metadata
        )
    
    @classmethod
    def from_protobuf(cls, pb_node: grpc_comm_manager_pb2.NodeInfo) -> 'GatewayNodeInfo':
        """Create from protobuf NodeInfo message."""
        return cls(
            node_id=pb_node.node_id,
            ip_address=pb_node.ip_address,
            port=pb_node.port,
            capabilities=list(pb_node.capabilities),
            status=NodeStatus(pb_node.status),
            last_seen=pb_node.last_seen,
            join_timestamp=pb_node.join_timestamp,
            metadata=dict(pb_node.metadata)
        )


class GRPCGatewayState:
    """Manages the gateway's state and registry using thread-safe operations."""
    
    def __init__(self, node_id: int = 0, enable_raft_sync: bool = True):
        self.nodes: Dict[int, GatewayNodeInfo] = {}
        self.bootstrap_initiated = False
        self.leader_id: Optional[int] = None
        self.registry_version = 0
        self.lock = threading.RLock()
        self.start_time = time.time()
        
        # Configuration
        self.node_timeout = 300.0  # 5 minutes timeout
        self.heartbeat_interval = 30.0  # 30 seconds heartbeat
        
        # State consistency management
        self.processed_events: Dict[str, float] = {}  # {event_id: timestamp}
        self.last_leader_change_term = 0  # Track RAFT term for leader changes
        self.last_membership_change_time = 0  # Track membership change ordering
        self.consistency_lock = threading.RLock()
        self.max_event_history = 1000  # Keep last 1000 events for deduplication
        
        # RAFT state synchronization
        self.enable_raft_sync = enable_raft_sync
        self.state_sync = None
        if enable_raft_sync:
            self.state_sync = GatewayStateSync(self, node_id)
            
        logging.info(f"GRPCGatewayState initialized with RAFT sync: {enable_raft_sync}")
        
    def start_raft_sync(self):
        """Start RAFT state synchronization."""
        if self.state_sync:
            self.state_sync.start()
            
    def stop_raft_sync(self):
        """Stop RAFT state synchronization."""
        if self.state_sync:
            self.state_sync.stop()
            
    def get_raft_event_handler(self):
        """Get the RAFT event handler for this gateway state."""
        if self.state_sync:
            return self.state_sync.handle_raft_event
        return None
        
    def register_node(self, node_id: int, ip_address: str, port: int, 
                     capabilities: List[str], metadata: Dict[str, str] = None) -> Dict:
        """Register a new node or update existing node."""
        with self.lock:
            current_time = time.time()
            
            if node_id in self.nodes:
                # Update existing node
                node = self.nodes[node_id]
                node.ip_address = ip_address
                node.port = port
                node.capabilities = capabilities
                node.last_seen = current_time
                node.status = NodeStatus.ACTIVE
                if metadata:
                    node.metadata.update(metadata)
                
                logging.info(f"Updated existing node {node_id}")
                return {
                    "status": "updated",
                    "node_id": node_id,
                    "is_bootstrap": False,
                    "registry_version": self.registry_version
                }
            else:
                # Register new node
                node = GatewayNodeInfo(
                    node_id=node_id,
                    ip_address=ip_address,
                    port=port,
                    capabilities=capabilities,
                    status=NodeStatus.JOINING,
                    last_seen=current_time,
                    join_timestamp=current_time,
                    metadata=metadata or {}
                )
                
                self.nodes[node_id] = node
                self.registry_version += 1
                
                # Check if this is bootstrap scenario
                is_bootstrap = not self.bootstrap_initiated and len(self.nodes) == 1
                if is_bootstrap:
                    self.bootstrap_initiated = True
                    self.leader_id = node_id
                    logging.info(f"Bootstrap initiated with node {node_id} as leader")
                
                # Update node status to active
                node.status = NodeStatus.ACTIVE
                
                logging.info(f"Registered new node {node_id}, bootstrap: {is_bootstrap}")
                
                return {
                    "status": "registered",
                    "node_id": node_id,
                    "is_bootstrap": is_bootstrap,
                    "registry_version": self.registry_version
                }
    
    def get_node_list(self, requesting_node_id: Optional[int] = None) -> List[GatewayNodeInfo]:
        """Get list of active nodes."""
        with self.lock:
            active_nodes = []
            for node_id, node in self.nodes.items():
                if node.status == NodeStatus.ACTIVE and node_id != requesting_node_id:
                    active_nodes.append(node)
            return active_nodes
    
    def get_leader_info(self) -> Optional[GatewayNodeInfo]:
        """Get current leader information."""
        with self.lock:
            if self.leader_id and self.leader_id in self.nodes:
                leader = self.nodes[self.leader_id]
                if leader.status == NodeStatus.ACTIVE:
                    return leader
            return None
    
    def update_leader(self, new_leader_id: int) -> bool:
        """Update leader information."""
        with self.lock:
            if new_leader_id in self.nodes:
                self.leader_id = new_leader_id
                logging.info(f"Leader updated to node {new_leader_id}")
                return True
            return False
    
    def heartbeat(self, node_id: int) -> Dict:
        """Process heartbeat from a node."""
        with self.lock:
            if node_id in self.nodes:
                self.nodes[node_id].last_seen = time.time()
                return {
                    "status": "acknowledged",
                    "registry_version": self.registry_version
                }
            return {"status": "not_found"}
    
    def remove_node(self, node_id: int) -> bool:
        """Remove a node from the registry."""
        with self.lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                self.registry_version += 1
                
                # Handle leader failure
                if self.leader_id == node_id:
                    self.leader_id = None
                    logging.warning(f"Leader node {node_id} removed")
                
                logging.info(f"Removed node {node_id}")
                return True
            return False
    
    def cleanup_inactive_nodes(self) -> List[int]:
        """Remove nodes that haven't sent heartbeats recently."""
        with self.lock:
            current_time = time.time()
            inactive_nodes = []
            
            for node_id, node in list(self.nodes.items()):
                if current_time - node.last_seen > self.node_timeout:
                    inactive_nodes.append(node_id)
                    self.remove_node(node_id)
            
            return inactive_nodes
    
    def get_stats(self) -> Dict:
        """Get gateway statistics."""
        with self.lock:
            stats = {
                "total_nodes": len(self.nodes),
                "active_nodes": len([n for n in self.nodes.values() if n.status == NodeStatus.ACTIVE]),
                "leader_id": self.leader_id or 0,
                "registry_version": self.registry_version,
                "bootstrap_initiated": self.bootstrap_initiated,
                "uptime": time.time() - self.start_time
            }
            return stats
    
    def is_duplicate_event(self, event_id: str) -> bool:
        """
        Check if an event has already been processed.
        
        Args:
            event_id: Unique identifier for the event
            
        Returns:
            True if the event has already been processed
        """
        with self.consistency_lock:
            if event_id in self.processed_events:
                logging.debug(f"Gateway: Duplicate event detected: {event_id}")
                return True
            
            # Clean old events to prevent memory growth
            current_time = time.time()
            cutoff_time = current_time - 600  # Keep events for 10 minutes
            
            old_events = [eid for eid, timestamp in self.processed_events.items()
                         if timestamp < cutoff_time]
            for eid in old_events:
                del self.processed_events[eid]
            
            # Keep only the most recent events
            if len(self.processed_events) > self.max_event_history:
                # Remove oldest events
                sorted_events = sorted(self.processed_events.items(), key=lambda x: x[1])
                events_to_remove = len(self.processed_events) - self.max_event_history + 100
                for eid, _ in sorted_events[:events_to_remove]:
                    del self.processed_events[eid]
            
            # Mark this event as processed
            self.processed_events[event_id] = current_time
            return False
    
    def is_stale_leader_change(self, term: int) -> bool:
        """
        Check if a leader change notification is stale.
        
        Args:
            term: RAFT term for the leader change
            
        Returns:
            True if the leader change is stale (from an older term)
        """
        with self.consistency_lock:
            if term < self.last_leader_change_term:
                logging.debug(f"Gateway: Stale leader change detected - "
                            f"term {term} < current term {self.last_leader_change_term}")
                return True
            
            if term > self.last_leader_change_term:
                self.last_leader_change_term = term
                logging.debug(f"Gateway: Updated leader change term to {term}")
            
            return False
    
    def is_stale_membership_change(self, timestamp: float) -> bool:
        """
        Check if a membership change notification is stale.
        
        Args:
            timestamp: Timestamp of the membership change
            
        Returns:
            True if the membership change is stale (older than last processed)
        """
        with self.consistency_lock:
            if timestamp < self.last_membership_change_time:
                logging.debug(f"Gateway: Stale membership change detected - "
                            f"timestamp {timestamp} < current {self.last_membership_change_time}")
                return True
            
            if timestamp > self.last_membership_change_time:
                self.last_membership_change_time = timestamp
                logging.debug(f"Gateway: Updated membership change time to {timestamp}")
            
            return False
    
    def validate_notification_consistency(self, event_data: Dict) -> bool:
        """
        Validate that a notification is consistent with current state.
        
        Args:
            event_data: Event data dictionary
            
        Returns:
            True if the notification is consistent and should be processed
        """
        event_type = event_data.get('type')
        event_id = event_data.get('event_id')
        
        if not event_id:
            logging.warning(f"Gateway: Event missing event_id: {event_data}")
            return False
        
        # Check for duplicate events
        if self.is_duplicate_event(event_id):
            return False
        
        # Type-specific validation
        if event_type == 'leader_change':
            term = event_data.get('term', 0)
            if self.is_stale_leader_change(term):
                return False
        
        elif event_type == 'membership_change':
            timestamp = event_data.get('timestamp', 0)
            if self.is_stale_membership_change(timestamp):
                return False
        
        return True
    
    def get_consistency_stats(self) -> Dict:
        """Get statistics about state consistency."""
        with self.consistency_lock:
            return {
                'processed_events_count': len(self.processed_events),
                'last_leader_change_term': self.last_leader_change_term,
                'last_membership_change_time': self.last_membership_change_time,
                'registry_version': self.registry_version
            }
    
    # ...existing methods...


class GRPCGatewayServicer(grpc_comm_manager_pb2_grpc.GatewayServiceServicer):
    """gRPC servicer implementation for the gateway service."""
    
    def __init__(self, gateway_state: GRPCGatewayState, event_broadcaster: GatewayEventBroadcaster = None):
        self.gateway_state = gateway_state
        self.event_broadcaster = event_broadcaster
        
    def RegisterNode(self, request, context):
        """Handle node registration requests."""
        try:
            result = self.gateway_state.register_node(
                node_id=request.node_id,
                ip_address=request.ip_address,
                port=request.port,
                capabilities=list(request.capabilities),
                metadata=dict(request.metadata)
            )
            
            # Get additional information for response
            nodes = self.gateway_state.get_node_list(request.node_id)
            leader = self.gateway_state.get_leader_info()
            
            response = grpc_comm_manager_pb2.RegisterNodeResponse(
                status=result["status"],
                node_id=result["node_id"],
                is_bootstrap=result["is_bootstrap"],
                registry_version=result["registry_version"],
                nodes=[node.to_protobuf() for node in nodes],
                leader=leader.to_protobuf() if leader else None,
                message="Registration successful"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Registration error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.RegisterNodeResponse()
    
    def GetNodes(self, request, context):
        """Handle get nodes requests."""
        try:
            nodes = self.gateway_state.get_node_list(
                request.requesting_node_id if request.requesting_node_id != 0 else None
            )
            
            response = grpc_comm_manager_pb2.GetNodesResponse(
                nodes=[node.to_protobuf() for node in nodes],
                registry_version=self.gateway_state.registry_version
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Get nodes error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetNodesResponse()
    
    def GetLeader(self, request, context):
        """Handle get leader requests."""
        try:
            leader = self.gateway_state.get_leader_info()
            
            response = grpc_comm_manager_pb2.GetLeaderResponse(
                leader=leader.to_protobuf() if leader else None,
                registry_version=self.gateway_state.registry_version,
                message="Leader found" if leader else "No leader registered"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Get leader error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetLeaderResponse()
    
    def UpdateLeader(self, request, context):
        """Handle leader update requests."""
        try:
            success = self.gateway_state.update_leader(request.leader_id)
            
            response = grpc_comm_manager_pb2.UpdateLeaderResponse(
                status="updated" if success else "failed",
                leader_id=request.leader_id,
                message="Leader updated successfully" if success else "Leader not found in registry"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Update leader error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.UpdateLeaderResponse()
    
    def Heartbeat(self, request, context):
        """Handle heartbeat requests."""
        try:
            result = self.gateway_state.heartbeat(request.node_id)
            
            response = grpc_comm_manager_pb2.HeartbeatResponse(
                status=result["status"],
                registry_version=result.get("registry_version", 0)
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Heartbeat error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.HeartbeatResponse()
    
    def HealthCheck(self, request, context):
        """Handle health check requests."""
        try:
            response = grpc_comm_manager_pb2.HealthCheckResponse(
                status="healthy",
                timestamp=time.time(),
                version="1.0.0"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Health check error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.HealthCheckResponse()
    
    def RemoveNode(self, request, context):
        """Handle node removal requests."""
        try:
            success = self.gateway_state.remove_node(request.node_id)
            
            response = grpc_comm_manager_pb2.RemoveNodeResponse(
                status="removed" if success else "not_found",
                node_id=request.node_id,
                message="Node removed successfully" if success else "Node not found"
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Remove node error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.RemoveNodeResponse()
    
    def GetStats(self, request, context):
        """Handle stats requests."""
        try:
            stats = self.gateway_state.get_stats()
            
            response = grpc_comm_manager_pb2.GetStatsResponse(
                total_nodes=stats["total_nodes"],
                active_nodes=stats["active_nodes"],
                leader_id=stats["leader_id"],
                registry_version=stats["registry_version"],
                bootstrap_initiated=stats["bootstrap_initiated"],
                uptime=stats["uptime"]
            )
            
            return response
            
        except Exception as e:
            logging.error(f"Get stats error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return grpc_comm_manager_pb2.GetStatsResponse()


class GRPCGatewayServer:
    """
    Standalone gRPC gateway server for service discovery.
    
    This is NOT a RAFT node - it's a separate service that provides
    discovery capabilities for RAFT nodes joining the network.
    The gateway receives notifications from RAFT nodes and maintains
    an eventually consistent view of the cluster state.
    """
    
    def __init__(self, host='0.0.0.0', port=8090, max_workers=10):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        
        # Gateway state - eventually consistent with RAFT cluster
        self.gateway_state = GRPCGatewayState(node_id=0, enable_raft_sync=False)
        
        # Event broadcasting for real-time updates to clients
        self.event_broadcaster = GatewayEventBroadcaster()
        
        self.server = None
        self.cleanup_thread = None
        self.is_running = False
        
        logging.info(f"Standalone Gateway server initialized on {host}:{port}")
    
    def receive_cluster_notification(self, event_data):
        """
        Receive cluster state notifications from RAFT nodes.
        
        This is how RAFT nodes notify the gateway of state changes.
        The gateway doesn't participate in RAFT consensus, it just
        receives notifications and updates its registry accordingly.
        
        Enhanced with state consistency validation to handle:
        - Duplicate notifications
        - Stale/out-of-order notifications
        - Conflicting state updates
        
        Args:
            event_data: Dictionary containing event information
        """
        try:
            # Validate notification consistency
            if not self.gateway_state.validate_notification_consistency(event_data):
                logging.debug(f"Gateway: Skipping inconsistent notification: {event_data.get('event_id')}")
                return
            
            event_type = event_data.get('type')
            source_node = event_data.get('source_node_id')
            
            logging.info(f"Gateway: Processing {event_type} from node {source_node}")
            
            if event_type == 'membership_change':
                self._handle_membership_notification(event_data)
            elif event_type == 'leader_change':
                self._handle_leader_notification(event_data)
            elif event_type == 'bootstrap_complete':
                self._handle_bootstrap_notification(event_data)
            elif event_type == 'state_reconciliation_request':
                self._handle_reconciliation_request(event_data)
            elif event_type == 'state_inconsistency':
                self._handle_inconsistency_report(event_data)
            else:
                logging.warning(f"Gateway: Unknown event type: {event_type}")
                return
                
            # Broadcast the event to subscribed clients (only if processed)
            self.event_broadcaster.broadcast_event(event_data)
            
            logging.debug(f"Gateway: Successfully processed {event_type} from node {source_node}")
            
        except Exception as e:
            logging.error(f"Gateway: Error processing cluster notification: {e}", exc_info=True)
    
    def _handle_membership_notification(self, event_data):
        """Handle membership change notifications from RAFT nodes."""
        added_nodes = event_data.get('added_nodes', [])
        removed_nodes = event_data.get('removed_nodes', [])
        current_nodes = event_data.get('current_nodes', [])
        source_node = event_data.get('source_node_id')
        
        logging.info(f"Gateway: Processing membership change from node {source_node} - "
                   f"Added: {added_nodes}, Removed: {removed_nodes}")
        
        with self.gateway_state.lock:
            # Validate that we're not processing conflicting updates
            # If we have current_nodes list, use it for validation
            if current_nodes:
                # Check if this is consistent with our current state
                known_active_nodes = {nid for nid, node in self.gateway_state.nodes.items() 
                                    if node.status == NodeStatus.ACTIVE}
                
                # Log any inconsistencies for debugging
                expected_after_change = (known_active_nodes | set(added_nodes)) - set(removed_nodes)
                if set(current_nodes) != expected_after_change:
                    logging.warning(f"Gateway: Membership change inconsistency detected - "
                                  f"Expected nodes after change: {expected_after_change}, "
                                  f"Notification claims: {set(current_nodes)}")
                    # Still process the change but log the inconsistency
            
            # Process removed nodes
            for node_id in removed_nodes:
                if node_id in self.gateway_state.nodes:
                    self.gateway_state.nodes[node_id].status = NodeStatus.LEAVING
                    logging.info(f"Gateway: Marked node {node_id} as leaving")
                else:
                    logging.debug(f"Gateway: Node {node_id} not in registry (already removed?)")
            
            # Process added nodes (they should register themselves, but we track the add)
            for node_id in added_nodes:
                if node_id not in self.gateway_state.nodes:
                    logging.debug(f"Gateway: Node {node_id} added but not yet registered")
                else:
                    # Ensure node is marked as active
                    self.gateway_state.nodes[node_id].status = NodeStatus.ACTIVE
                    logging.info(f"Gateway: Confirmed node {node_id} as active")
            
            # Update registry version
            self.gateway_state.registry_version += 1
            
            logging.info(f"Gateway: Membership change processed, registry version: {self.gateway_state.registry_version}")
    
    def _handle_leader_notification(self, event_data):
        """Handle leader change notifications from RAFT nodes."""
        new_leader = event_data.get('new_leader')
        old_leader = event_data.get('old_leader')
        term = event_data.get('term', 0)
        source_node = event_data.get('source_node_id')
        
        logging.info(f"Gateway: Processing leader change from node {source_node} - "
                   f"From: {old_leader}, To: {new_leader}, Term: {term}")
        
        with self.gateway_state.lock:
            # Validate the leader change
            if new_leader is not None:
                # Check if the new leader is a known node
                if new_leader not in self.gateway_state.nodes:
                    logging.warning(f"Gateway: New leader {new_leader} not in registry")
                    # Don't reject the change, but log it
                
                # Update leader
                old_leader_id = self.gateway_state.leader_id
                success = self.gateway_state.update_leader(new_leader)
                
                if success:
                    logging.info(f"Gateway: Leader updated from {old_leader_id} to {new_leader}")
                else:
                    logging.error(f"Gateway: Failed to update leader to {new_leader}")
            else:
                logging.warning(f"Gateway: Leader change notification missing new_leader")
    
    def _handle_bootstrap_notification(self, event_data):
        """Handle bootstrap completion notifications from RAFT nodes."""
        initial_leader = event_data.get('initial_leader')
        initial_nodes = event_data.get('initial_nodes', [])
        source_node = event_data.get('source_node_id')
        
        logging.info(f"Gateway: Processing bootstrap complete from node {source_node} - "
                   f"Leader: {initial_leader}, Nodes: {initial_nodes}")
        
        with self.gateway_state.lock:
            # Only process if bootstrap hasn't been initiated yet
            if not self.gateway_state.bootstrap_initiated:
                self.gateway_state.bootstrap_initiated = True
                
                if initial_leader is not None:
                    self.gateway_state.leader_id = initial_leader
                    logging.info(f"Gateway: Bootstrap complete - Leader: {initial_leader}")
                else:
                    logging.warning(f"Gateway: Bootstrap complete but no initial leader specified")
            else:
                logging.debug(f"Gateway: Bootstrap already initiated, ignoring duplicate notification")
        
    def _handle_reconciliation_request(self, event_data):
        """Handle state reconciliation requests from RAFT nodes."""
        requesting_node = event_data.get('source_node_id')
        target_state = event_data.get('target_state', {})
        
        logging.info(f"Gateway: Processing state reconciliation request from node {requesting_node}")
        
        with self.gateway_state.lock:
            # Update the gateway state to match the target state
            for node_id, node_info in target_state.get('nodes', {}).items():
                if node_id in self.gateway_state.nodes:
                    # Update existing node information
                    node = self.gateway_state.nodes[node_id]
                    node.ip_address = node_info.get('ip_address', node.ip_address)
                    node.port = node_info.get('port', node.port)
                    node.capabilities = node_info.get('capabilities', node.capabilities)
                    node.status = NodeStatus(node_info.get('status', node.status.value))
                    node.last_seen = node_info.get('last_seen', node.last_seen)
                    node.join_timestamp = node_info.get('join_timestamp', node.join_timestamp)
                    node.metadata = node_info.get('metadata', node.metadata)
                    
                    logging.info(f"Gateway: Updated node {node_id} from reconciliation request")
                else:
                    # Register new node found in reconciliation
                    new_node = GatewayNodeInfo(
                        node_id=node_id,
                        ip_address=node_info.get('ip_address', ''),
                        port=node_info.get('port', 0),
                        capabilities=node_info.get('capabilities', []),
                        status=NodeStatus(node_info.get('status', 'joining')),
                        last_seen=node_info.get('last_seen', time.time()),
                        join_timestamp=node_info.get('join_timestamp', time.time()),
                        metadata=node_info.get('metadata', {})
                    )
                    self.gateway_state.nodes[node_id] = new_node
                    logging.info(f"Gateway: Registered new node {node_id} from reconciliation request")
            
            # Update leader if specified
            new_leader_id = target_state.get('leader_id')
            if new_leader_id and new_leader_id != self.gateway_state.leader_id:
                self.gateway_state.update_leader(new_leader_id)
                logging.info(f"Gateway: Leader updated to {new_leader_id} from reconciliation request")
            
            # Update registry version
            self.gateway_state.registry_version += 1
            
            logging.info(f"Gateway: State reconciliation processed, registry version: {self.gateway_state.registry_version}")
    
    def _handle_inconsistency_report(self, event_data):
        """Handle state inconsistency reports from RAFT nodes."""
        reported_state = event_data.get('reported_state', {})
        source_node = event_data.get('source_node_id')
        
        logging.info(f"Gateway: Processing state inconsistency report from node {source_node}")
        
        with self.gateway_state.lock:
            # Update the gateway state based on the reported inconsistent state
            for node_id, node_info in reported_state.get('nodes', {}).items():
                if node_id in self.gateway_state.nodes:
                    # Update existing node information with caution
                    node = self.gateway_state.nodes[node_id]
                    original_status = node.status
                    node.status = NodeStatus(node_info.get('status', node.status.value))
                    node.last_seen = node_info.get('last_seen', node.last_seen)
                    
                    # Log any significant changes
                    if node.status != original_status:
                        logging.info(f"Gateway: Node {node_id} status updated to {node.status} from inconsistency report")
                else:
                    logging.debug(f"Gateway: Node {node_id} not in registry, ignoring inconsistency report")
            
            # Leader information may also be updated in the report
            reported_leader_id = reported_state.get('leader_id')
            if reported_leader_id and reported_leader_id != self.gateway_state.leader_id:
                self.gateway_state.update_leader(reported_leader_id)
                logging.info(f"Gateway: Leader updated to {reported_leader_id} from inconsistency report")
            
            # Update registry version
            self.gateway_state.registry_version += 1
            
            logging.info(f"Gateway: State inconsistency report processed, registry version: {self.gateway_state.registry_version}")
    
    def start(self):
        """Start the standalone gRPC gateway server."""
        if self.is_running:
            logging.warning("Gateway server is already running")
            return
        
        try:
            # Create gRPC server
            self.server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=self.max_workers),
                options=[
                    ('grpc.max_send_message_length', 1000 * 1024 * 1024),
                    ('grpc.max_receive_message_length', 1000 * 1024 * 1024),
                ]
            )
            
            # Add gateway servicer with event broadcasting support
            gateway_servicer = GRPCGatewayServicer(self.gateway_state, self.event_broadcaster)
            grpc_comm_manager_pb2_grpc.add_GatewayServiceServicer_to_server(
                gateway_servicer, self.server
            )
            
            # Add insecure port
            listen_addr = f'{self.host}:{self.port}'
            self.server.add_insecure_port(listen_addr)
            
            # Start server
            self.server.start()
            
            # Start cleanup thread
            self.cleanup_thread = threading.Thread(target=self._cleanup_loop)
            self.cleanup_thread.daemon = True
            self.cleanup_thread.start()
            
            self.is_running = True
            logging.info(f"Standalone gRPC Gateway server started on {listen_addr}")
            
        except Exception as e:
            logging.error(f"Failed to start gRPC gateway server: {e}")
            raise
    
    def stop(self):
        """Stop the gRPC gateway server."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.server:
            self.server.stop(grace=5)
        
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5)
        
        logging.info("Standalone gRPC Gateway server stopped")
    
    def wait_for_termination(self):
        """Wait for the server to terminate."""
        if self.server:
            self.server.wait_for_termination()
    
    def _cleanup_loop(self):
        """Periodic cleanup of inactive nodes."""
        while self.is_running:
            try:
                inactive_nodes = self.gateway_state.cleanup_inactive_nodes()
                if inactive_nodes:
                    logging.info(f"Cleaned up inactive nodes: {inactive_nodes}")
                
                time.sleep(self.gateway_state.heartbeat_interval)
                
            except Exception as e:
                logging.error(f"Error in cleanup loop: {e}")
                time.sleep(10)  # Sleep longer on error
    
    def get_state(self) -> GRPCGatewayState:
        """Get the current gateway state."""
        return self.gateway_state


def main():
    """Main function for running the standalone gRPC gateway server."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Standalone gRPC Gateway Server for RAFT+GossipFL')
    parser.add_argument('--host', default='0.0.0.0', help='Gateway host address')
    parser.add_argument('--port', type=int, default=8090, help='Gateway port')
    parser.add_argument('--max-workers', type=int, default=10, help='Max worker threads')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create and start standalone gateway server
    gateway = GRPCGatewayServer(host=args.host, port=args.port, max_workers=args.max_workers)
    
    try:
        gateway.start()
        logging.info("Standalone gRPC Gateway server is running. Press Ctrl+C to stop.")
        gateway.wait_for_termination()
        
    except KeyboardInterrupt:
        logging.info("Shutting down gRPC gateway server...")
        gateway.stop()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        gateway.stop()
