"""
Gateway Notifier for RAFT Nodes

This module provides a client for RAFT nodes to notify the standalone
gateway server about cluster state changes. This maintains the correct
architecture where the gateway is not a RAFT participant but receives
notifications about cluster state changes.
"""

import logging
import time
import threading
from typing import Dict, Set, Optional
import grpc
from concurrent import futures
import requests
import json

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2
from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc


class GatewayNotifier:
    """
    Client for RAFT nodes to notify the standalone gateway server.
    
    This class provides methods for RAFT nodes to notify the gateway
    about cluster state changes. The gateway is not a RAFT participant
    but needs to maintain an eventually consistent view of the cluster.
    
    Key features for state consistency:
    - Leader-only notifications (primary mechanism)
    - Follower fallback when leader is unreachable
    - Event deduplication and ordering
    - Smart retry logic
    """
    
    def __init__(self, gateway_host: str = "localhost", gateway_port: int = 8090, 
                 node_id: int = 0, max_retries: int = 3, raft_consensus=None):
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.node_id = node_id
        self.max_retries = max_retries
        self.raft_consensus = raft_consensus  # Reference to RAFT consensus for state checking
        
        # HTTP endpoint for notifications (fallback if gRPC not available)
        self.gateway_url = f"http://{gateway_host}:{gateway_port}"
        
        # gRPC channel for notifications
        self.channel = None
        self.stub = None
        
        # Notification queue for async delivery
        self.notification_queue = []
        self.notification_lock = threading.Lock()
        self.notification_thread = None
        self.notification_running = False
        
        # State consistency management
        self.last_leader_heartbeat = time.time()
        self.leader_timeout = 10.0  # seconds
        self.last_sent_events = {}  # {event_type: {event_id: timestamp}}
        self.event_sequence = 0
        self.consistency_lock = threading.Lock()
        
        logging.info(f"Gateway notifier initialized for node {node_id} -> {gateway_host}:{gateway_port}")
    
    def set_raft_consensus(self, raft_consensus):
        """Set RAFT consensus reference (for delayed initialization)."""
        self.raft_consensus = raft_consensus
    
    def update_leader_heartbeat(self):
        """Update the last leader heartbeat timestamp."""
        self.last_leader_heartbeat = time.time()
    
    def should_notify_gateway(self, event_type: str) -> bool:
        """
        Determine if this node should notify the gateway.
        
        Rules:
        1. Bootstrap events: Always notify (any node can bootstrap)
        2. Leader: Always notify (primary mechanism)
        3. Follower: Only notify if leader seems down (fallback)
        4. Candidate: Never notify (during elections)
        
        Args:
            event_type: Type of event to notify about
            
        Returns:
            True if this node should notify gateway
        """
        with self.consistency_lock:
            # Always notify for bootstrap events
            if event_type == 'bootstrap_complete':
                return True
            
            # If no RAFT consensus reference, assume we should notify
            if not self.raft_consensus:
                return True
            
            try:
                # Get current RAFT state
                current_state = self.raft_consensus.raft_node.state
                current_leader = self.raft_consensus.current_leader_id
                
                # Leader should always notify
                if current_state.name == 'LEADER':
                    return True
                
                # Followers notify only if leader seems down
                if current_state.name == 'FOLLOWER':
                    time_since_leader = time.time() - self.last_leader_heartbeat
                    leader_seems_down = time_since_leader > self.leader_timeout
                    
                    if leader_seems_down:
                        logging.warning(f"Node {self.node_id}: Leader seems down "
                                      f"({time_since_leader:.1f}s since heartbeat), "
                                      f"taking over gateway notifications")
                        return True
                    else:
                        logging.debug(f"Node {self.node_id}: Follower skipping notification "
                                    f"(leader active: {current_leader})")
                        return False
                
                # Candidates don't notify during elections
                if current_state.name == 'CANDIDATE':
                    logging.debug(f"Node {self.node_id}: Candidate skipping notification "
                                f"(election in progress)")
                    return False
                
                # Default: allow notification
                return True
                
            except Exception as e:
                logging.warning(f"Node {self.node_id}: Error checking RAFT state: {e}")
                # On error, default to allowing notification
                return True
    
    def _generate_event_id(self, notification: Dict) -> str:
        """
        Generate unique event ID for deduplication.
        
        Args:
            notification: Notification dictionary
            
        Returns:
            Unique event ID string
        """
        with self.consistency_lock:
            self.event_sequence += 1
            
            # Include sequence number to ensure uniqueness
            event_id = f"{notification['type']}_{self.node_id}_{self.event_sequence}_{notification['timestamp']}"
            
            # For certain events, include state-specific info
            if notification['type'] == 'leader_change':
                event_id += f"_term{notification.get('term', 0)}"
            elif notification['type'] == 'membership_change':
                # Sort nodes for consistent ID regardless of order
                current_nodes = sorted(notification.get('current_nodes', []))
                event_id += f"_nodes{hash(tuple(current_nodes))}"
            
            return event_id
    
    def _is_duplicate_event(self, event_id: str, event_type: str) -> bool:
        """
        Check if this event has already been sent recently.
        
        Args:
            event_id: Unique event identifier
            event_type: Type of event
            
        Returns:
            True if this is a duplicate event
        """
        with self.consistency_lock:
            # Clean old events (older than 5 minutes)
            current_time = time.time()
            cutoff_time = current_time - 300  # 5 minutes
            
            if event_type not in self.last_sent_events:
                self.last_sent_events[event_type] = {}
            
            # Remove old events
            old_events = [eid for eid, timestamp in self.last_sent_events[event_type].items()
                         if timestamp < cutoff_time]
            for eid in old_events:
                del self.last_sent_events[event_type][eid]
            
            # Check if this event ID exists
            if event_id in self.last_sent_events[event_type]:
                return True
            
            # Record this event
            self.last_sent_events[event_type][event_id] = current_time
            return False
    
    def start(self):
        """Start the notification delivery thread."""
        with self.notification_lock:
            if not self.notification_running:
                self.notification_running = True
                self.notification_thread = threading.Thread(
                    target=self._notification_worker,
                    name=f"gateway-notifier-{self.node_id}",
                    daemon=True
                )
                self.notification_thread.start()
                logging.info(f"Gateway notifier started for node {self.node_id}")
    
    def stop(self):
        """Stop the notification delivery thread."""
        with self.notification_lock:
            if self.notification_running:
                self.notification_running = False
                if self.notification_thread:
                    self.notification_thread.join(timeout=5.0)
                
                # Close gRPC channel
                if self.channel:
                    self.channel.close()
                    
                logging.info(f"Gateway notifier stopped for node {self.node_id}")
    
    def notify_membership_change(self, added_nodes: Set[int], removed_nodes: Set[int], 
                                current_nodes: Set[int], round_num: int = 0):
        """
        Notify gateway about membership changes.
        
        Args:
            added_nodes: Set of node IDs that were added
            removed_nodes: Set of node IDs that were removed
            current_nodes: Set of all current node IDs
            round_num: Current round number
        """
        # Check if this node should notify
        if not self.should_notify_gateway('membership_change'):
            logging.debug(f"Node {self.node_id}: Skipping membership change notification "
                        f"(not leader or leader is active)")
            return
        
        notification = {
            'type': 'membership_change',
            'source_node_id': self.node_id,
            'timestamp': time.time(),
            'added_nodes': list(added_nodes),
            'removed_nodes': list(removed_nodes),
            'current_nodes': list(current_nodes),
            'round_num': round_num
        }
        
        # Generate event ID and check for duplicates
        event_id = self._generate_event_id(notification)
        if self._is_duplicate_event(event_id, 'membership_change'):
            logging.debug(f"Node {self.node_id}: Skipping duplicate membership change event {event_id}")
            return
        
        notification['event_id'] = event_id
        self._queue_notification(notification)
        logging.info(f"Node {self.node_id}: Queued membership change notification - "
                   f"Added: {added_nodes}, Removed: {removed_nodes}, Event ID: {event_id}")
    
    def notify_leader_change(self, old_leader: Optional[int], new_leader: int, term: int):
        """
        Notify gateway about leader changes.
        
        Args:
            old_leader: Previous leader node ID (None if unknown)
            new_leader: New leader node ID
            term: Current RAFT term
        """
        # Check if this node should notify
        if not self.should_notify_gateway('leader_change'):
            logging.debug(f"Node {self.node_id}: Skipping leader change notification "
                        f"(not leader or leader is active)")
            return
        
        notification = {
            'type': 'leader_change',
            'source_node_id': self.node_id,
            'timestamp': time.time(),
            'old_leader': old_leader,
            'new_leader': new_leader,
            'term': term
        }
        
        # Generate event ID and check for duplicates
        event_id = self._generate_event_id(notification)
        if self._is_duplicate_event(event_id, 'leader_change'):
            logging.debug(f"Node {self.node_id}: Skipping duplicate leader change event {event_id}")
            return
        
        notification['event_id'] = event_id
        self._queue_notification(notification)
        logging.info(f"Node {self.node_id}: Queued leader change notification - "
                   f"From: {old_leader}, To: {new_leader}, Term: {term}, Event ID: {event_id}")
    
    def notify_bootstrap_complete(self, initial_leader: int, initial_nodes: Set[int]):
        """
        Notify gateway about bootstrap completion.
        
        Args:
            initial_leader: Initial leader node ID
            initial_nodes: Set of initial node IDs
        """
        # Bootstrap events are always sent (any node can bootstrap)
        notification = {
            'type': 'bootstrap_complete',
            'source_node_id': self.node_id,
            'timestamp': time.time(),
            'initial_leader': initial_leader,
            'initial_nodes': list(initial_nodes)
        }
        
        # Generate event ID and check for duplicates
        event_id = self._generate_event_id(notification)
        if self._is_duplicate_event(event_id, 'bootstrap_complete'):
            logging.debug(f"Node {self.node_id}: Skipping duplicate bootstrap complete event {event_id}")
            return
        
        notification['event_id'] = event_id
        self._queue_notification(notification)
        logging.info(f"Node {self.node_id}: Queued bootstrap complete notification - "
                   f"Leader: {initial_leader}, Nodes: {initial_nodes}, Event ID: {event_id}")
    
    def _queue_notification(self, notification):
        """Queue a notification for delivery."""
        with self.notification_lock:
            self.notification_queue.append(notification)
    
    def _notification_worker(self):
        """Worker thread for delivering notifications with smart retry handling."""
        while self.notification_running:
            try:
                # Get notifications to deliver
                notifications_to_deliver = []
                current_time = time.time()
                
                with self.notification_lock:
                    # Separate immediate vs delayed notifications
                    immediate_notifications = []
                    delayed_notifications = []
                    
                    for notification in self.notification_queue:
                        retry_timestamp = notification.get('retry_timestamp', 0)
                        if retry_timestamp <= current_time:
                            immediate_notifications.append(notification)
                        else:
                            delayed_notifications.append(notification)
                    
                    # Update queue with only delayed notifications
                    self.notification_queue = delayed_notifications
                    notifications_to_deliver = immediate_notifications
                
                # Deliver ready notifications
                for notification in notifications_to_deliver:
                    self._deliver_notification(notification)
                
                # Sleep before next check (shorter if we have delayed notifications)
                if delayed_notifications:
                    time.sleep(1.0)  # Check more frequently when retries are pending
                else:
                    time.sleep(2.0)  # Normal polling interval
                
            except Exception as e:
                logging.error(f"Gateway notifier worker error: {e}")
                time.sleep(5.0)  # Sleep longer on error
    
    def _deliver_notification(self, notification):
        """
        Deliver a single notification to the gateway.
        
        Enhanced with state validation and smart retry logic.
        """
        # Validate notification before sending
        if not self._validate_notification(notification):
            logging.warning(f"Node {self.node_id}: Invalid notification, discarding: {notification}")
            return
        
        # Check if we should still send this notification
        if not self._should_still_send(notification):
            logging.debug(f"Node {self.node_id}: Notification no longer relevant, discarding: {notification}")
            return
        
        success = False
        
        # Try HTTP delivery first (simpler, more reliable)
        try:
            success = self._deliver_via_http(notification)
        except Exception as e:
            logging.warning(f"HTTP delivery failed: {e}")
        
        # Try gRPC delivery if HTTP failed
        if not success:
            try:
                success = self._deliver_via_grpc(notification)
            except Exception as e:
                logging.warning(f"gRPC delivery failed: {e}")
        
        if not success:
            # Handle failed delivery
            self._handle_delivery_failure(notification)
        else:
            logging.debug(f"Successfully delivered notification: {notification['type']} "
                        f"(ID: {notification.get('event_id', 'unknown')})")
    
    def _validate_notification(self, notification) -> bool:
        """
        Validate notification data before sending.
        
        Enhanced validation includes:
        - Required field validation
        - Type-specific validation
        - State consistency checks
        - Temporal validation
        
        Args:
            notification: Notification dictionary to validate
            
        Returns:
            True if notification is valid
        """
        required_fields = ['type', 'source_node_id', 'timestamp', 'event_id']
        
        # Check required fields
        for field in required_fields:
            if field not in notification:
                logging.error(f"Node {self.node_id}: Missing required field '{field}' in notification")
                return False
        
        # Validate event ID format
        event_id = notification.get('event_id', '')
        if not event_id or not isinstance(event_id, str):
            logging.error(f"Node {self.node_id}: Invalid event_id in notification")
            return False
        
        # Validate timestamp
        timestamp = notification.get('timestamp', 0)
        if not isinstance(timestamp, (int, float)) or timestamp <= 0:
            logging.error(f"Node {self.node_id}: Invalid timestamp in notification")
            return False
        
        # Check timestamp isn't too far in the past or future
        current_time = time.time()
        max_age = 600  # 10 minutes
        max_future = 60  # 1 minute
        
        if current_time - timestamp > max_age:
            logging.warning(f"Node {self.node_id}: Notification too old ({current_time - timestamp:.1f}s)")
            return False
        
        if timestamp - current_time > max_future:
            logging.warning(f"Node {self.node_id}: Notification timestamp too far in future")
            return False
        
        # Validate specific notification types
        if notification['type'] == 'membership_change':
            required_membership_fields = ['added_nodes', 'removed_nodes', 'current_nodes']
            for field in required_membership_fields:
                if field not in notification:
                    logging.error(f"Node {self.node_id}: Missing field '{field}' in membership change")
                    return False
                if not isinstance(notification[field], list):
                    logging.error(f"Node {self.node_id}: Field '{field}' must be a list")
                    return False
        
        elif notification['type'] == 'leader_change':
            required_leader_fields = ['new_leader', 'term']
            for field in required_leader_fields:
                if field not in notification:
                    logging.error(f"Node {self.node_id}: Missing field '{field}' in leader change")
                    return False
            
            # Validate term is a positive integer
            term = notification.get('term', 0)
            if not isinstance(term, int) or term < 0:
                logging.error(f"Node {self.node_id}: Invalid term in leader change: {term}")
                return False
        
        elif notification['type'] == 'bootstrap_complete':
            required_bootstrap_fields = ['initial_leader', 'initial_nodes']
            for field in required_bootstrap_fields:
                if field not in notification:
                    logging.error(f"Node {self.node_id}: Missing field '{field}' in bootstrap complete")
                    return False
        
        return True
    
    def _should_still_send(self, notification) -> bool:
        """
        Check if this notification is still relevant to send.
        
        Args:
            notification: Notification to check
            
        Returns:
            True if notification should still be sent
        """
        # Check notification age (don't send very old notifications)
        max_age = 300  # 5 minutes
        age = time.time() - notification['timestamp']
        if age > max_age:
            logging.warning(f"Node {self.node_id}: Notification too old ({age:.1f}s), discarding")
            return False
        
        # For leader change notifications, check if we have newer information
        if notification['type'] == 'leader_change' and self.raft_consensus:
            try:
                current_term = self.raft_consensus.raft_node.current_term
                notification_term = notification.get('term', 0)
                
                if notification_term < current_term:
                    logging.debug(f"Node {self.node_id}: Leader change notification outdated "
                                f"(term {notification_term} < current {current_term})")
                    return False
            except Exception as e:
                logging.warning(f"Node {self.node_id}: Error checking term: {e}")
        
        return True
    
    def _handle_delivery_failure(self, notification):
        """
        Handle failed notification delivery with smart retry logic.
        
        Args:
            notification: Failed notification
        """
        retry_count = notification.get('retry_count', 0)
        
        if retry_count < self.max_retries:
            # Calculate exponential backoff delay
            base_delay = 2.0  # seconds
            delay = base_delay * (2 ** retry_count)
            max_delay = 60.0  # maximum 1 minute delay
            delay = min(delay, max_delay)
            
            # Update retry count and timestamp
            notification['retry_count'] = retry_count + 1
            notification['retry_timestamp'] = time.time() + delay
            
            # Re-queue with delay
            with self.notification_lock:
                self.notification_queue.append(notification)
            
            logging.info(f"Node {self.node_id}: Retrying notification in {delay:.1f}s "
                       f"(attempt {retry_count + 1}/{self.max_retries})")
        else:
            logging.error(f"Node {self.node_id}: Failed to deliver notification after "
                        f"{self.max_retries} attempts: {notification}")
            
            # Could implement persistent storage or dead letter queue here
            self._handle_permanent_failure(notification)
    
    def _handle_permanent_failure(self, notification):
        """
        Handle permanently failed notifications.
        
        Args:
            notification: Permanently failed notification
        """
        # For now, just log the failure
        # In production, might want to:
        # - Store in persistent queue for later retry
        # - Send to dead letter queue
        # - Alert operators
        
        logging.error(f"Node {self.node_id}: Permanently failed notification: "
                    f"Type: {notification['type']}, "
                    f"ID: {notification.get('event_id', 'unknown')}, "
                    f"Timestamp: {notification['timestamp']}")
        
        # Could implement metrics here
        # self.failed_notifications_counter.inc(labels={'type': notification['type']})
    
    def _deliver_via_http(self, notification):
        """Deliver notification via HTTP POST."""
        try:
            url = f"{self.gateway_url}/api/cluster-notification"
            response = requests.post(url, json=notification, timeout=10.0)
            
            if response.status_code == 200:
                return True
            else:
                logging.warning(f"HTTP delivery failed with status {response.status_code}")
                return False
                
        except Exception as e:
            logging.warning(f"HTTP delivery exception: {e}")
            return False
    
    def _deliver_via_grpc(self, notification):
        """Deliver notification via gRPC."""
        try:
            # Create gRPC channel if needed
            if not self.channel:
                self.channel = grpc.insecure_channel(f"{self.gateway_host}:{self.gateway_port}")
                self.stub = grpc_comm_manager_pb2_grpc.GatewayServiceStub(self.channel)
            
            # Convert notification to gRPC request
            request = self._notification_to_grpc_request(notification)
            
            # Send the request
            response = self.stub.ClusterNotification(request, timeout=10.0)
            
            if response.status == "success":
                return True
            else:
                logging.warning(f"gRPC delivery failed: {response.message}")
                return False
                
        except Exception as e:
            logging.warning(f"gRPC delivery exception: {e}")
            
            # Reset connection on error
            if self.channel:
                self.channel.close()
                self.channel = None
                self.stub = None
            
            return False
    
    def _notification_to_grpc_request(self, notification):
        """Convert notification dictionary to gRPC request."""
        # This would need the proper protobuf message definition
        # For now, we'll use a generic approach
        request = grpc_comm_manager_pb2.ClusterNotificationRequest()
        request.notification_type = notification['type']
        request.source_node_id = notification['source_node_id']
        request.timestamp = notification['timestamp']
        request.data = json.dumps(notification)
        
        return request
    
    def test_connection(self) -> bool:
        """Test connection to gateway."""
        try:
            # Try HTTP health check
            response = requests.get(f"{self.gateway_url}/health", timeout=5.0)
            if response.status_code == 200:
                return True
        except:
            pass
        
        try:
            # Try gRPC health check
            if not self.channel:
                self.channel = grpc.insecure_channel(f"{self.gateway_host}:{self.gateway_port}")
                self.stub = grpc_comm_manager_pb2_grpc.GatewayServiceStub(self.channel)
            
            request = grpc_comm_manager_pb2.HealthCheckRequest()
            response = self.stub.HealthCheck(request, timeout=5.0)
            
            if response.status == "healthy":
                return True
                
        except Exception as e:
            logging.debug(f"Gateway connection test failed: {e}")
            
            # Reset connection on error
            if self.channel:
                self.channel.close()
                self.channel = None
                self.stub = None
        
        return False
    
    def get_stats(self) -> Dict:
        """Get notifier statistics with state consistency information."""
        with self.notification_lock:
            with self.consistency_lock:
                # Count delayed notifications
                current_time = time.time()
                delayed_count = sum(1 for n in self.notification_queue 
                                  if n.get('retry_timestamp', 0) > current_time)
                
                # Get RAFT state info if available
                raft_state = "unknown"
                current_leader = "unknown"
                is_leader = False
                
                if self.raft_consensus:
                    try:
                        raft_state = self.raft_consensus.raft_node.state.name
                        current_leader = self.raft_consensus.current_leader_id
                        is_leader = (raft_state == 'LEADER')
                    except:
                        pass
                
                return {
                    'node_id': self.node_id,
                    'gateway_host': self.gateway_host,
                    'gateway_port': self.gateway_port,
                    'notification_running': self.notification_running,
                    'pending_notifications': len(self.notification_queue),
                    'delayed_notifications': delayed_count,
                    'event_sequence': self.event_sequence,
                    'last_leader_heartbeat_age': current_time - self.last_leader_heartbeat,
                    'leader_timeout': self.leader_timeout,
                    'state_consistency': {
                        'raft_state': raft_state,
                        'current_leader': current_leader,
                        'is_leader': is_leader,
                        'should_notify': self.should_notify_gateway('test'),
                        'total_event_types': len(self.last_sent_events),
                        'recent_events': {
                            event_type: len(events) 
                            for event_type, events in self.last_sent_events.items()
                        }
                    }
                }
    
    def request_state_reconciliation(self):
        """
        Request state reconciliation from the gateway.
        
        This method can be called when we detect state inconsistencies
        or when a node rejoins the cluster after a network partition.
        """
        if not self.should_notify_gateway('reconciliation'):
            logging.debug(f"Node {self.node_id}: Skipping state reconciliation request "
                        f"(not leader or leader is active)")
            return
        
        reconciliation_request = {
            'type': 'state_reconciliation_request',
            'source_node_id': self.node_id,
            'timestamp': time.time(),
            'current_state': self._get_current_node_state()
        }
        
        # Generate event ID for the reconciliation request
        event_id = self._generate_event_id(reconciliation_request)
        reconciliation_request['event_id'] = event_id
        
        # Send immediately (high priority)
        self._deliver_notification(reconciliation_request)
        
        logging.info(f"Node {self.node_id}: Sent state reconciliation request")
    
    def _get_current_node_state(self) -> Dict:
        """Get current node's view of the cluster state."""
        if not self.raft_consensus:
            return {}
        
        try:
            return {
                'node_id': self.node_id,
                'raft_state': self.raft_consensus.raft_node.state.name,
                'current_term': self.raft_consensus.raft_node.current_term,
                'current_leader': self.raft_consensus.current_leader_id,
                'cluster_size': len(self.raft_consensus.raft_node.peers) + 1,
                'timestamp': time.time()
            }
        except Exception as e:
            logging.warning(f"Node {self.node_id}: Error getting current state: {e}")
            return {}
    
    def notify_state_inconsistency(self, inconsistency_type: str, details: Dict):
        """
        Notify about detected state inconsistencies.
        
        Args:
            inconsistency_type: Type of inconsistency detected
            details: Additional details about the inconsistency
        """
        if not self.should_notify_gateway('state_inconsistency'):
            logging.debug(f"Node {self.node_id}: Skipping state inconsistency notification")
            return
        
        notification = {
            'type': 'state_inconsistency',
            'source_node_id': self.node_id,
            'timestamp': time.time(),
            'inconsistency_type': inconsistency_type,
            'details': details,
            'current_state': self._get_current_node_state()
        }
        
        # Generate event ID and check for duplicates
        event_id = self._generate_event_id(notification)
        if self._is_duplicate_event(event_id, 'state_inconsistency'):
            logging.debug(f"Node {self.node_id}: Skipping duplicate state inconsistency event")
            return
        
        notification['event_id'] = event_id
        self._queue_notification(notification)
        
        logging.warning(f"Node {self.node_id}: Queued state inconsistency notification - "
                      f"Type: {inconsistency_type}, Details: {details}")
    
    # ...existing methods...
