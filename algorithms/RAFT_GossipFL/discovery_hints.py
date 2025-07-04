#!/usr/bin/env python3

"""
RAFT Leader Notification Component for Pure Service Discovery

This component implements the correct pattern from gateway.md:
- Only the RAFT leader provides hints to the discovery service
- Discovery service is not part of RAFT consensus
- Leader notifications are for discovery optimization only
- Notifications are sent via simple HTTP/gRPC calls
- No complex state synchronization
"""

import logging
import requests
import time
import threading
import json
from typing import Optional, Dict, Any, Set
from enum import Enum
from dataclasses import dataclass


class RaftState(Enum):
    """RAFT node states."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LeaderNotification:
    """Simple leader notification for discovery hint."""
    node_id: int
    term: int
    timestamp: float
    node_list: Set[int]  # Current active nodes


class DiscoveryHintSender:
    """
    Sends discovery hints from RAFT leader to discovery service.
    
    This component follows the gateway.md pattern:
    - Only leaders send hints
    - Hints are for discovery optimization only
    - No complex state synchronization
    - Simple HTTP/gRPC calls
    - Graceful failure handling
    """
    
    def __init__(self, discovery_host: str = 'localhost', discovery_port: int = 8090,
                 node_id: int = 0, send_interval: float = 30.0):
        self.discovery_host = discovery_host
        self.discovery_port = discovery_port
        self.node_id = node_id
        self.send_interval = send_interval
        
        # State tracking
        self.current_raft_state = RaftState.FOLLOWER
        self.current_term = 0
        self.last_known_nodes: Set[int] = set()
        self.last_hint_sent = 0
        
        # Threading
        self.sender_thread: Optional[threading.Thread] = None
        self.is_running = False
        self.lock = threading.Lock()
        
        logging.info(f"Discovery hint sender initialized for node {node_id}")
    
    def update_raft_state(self, new_state: RaftState, term: int, known_nodes: Set[int]):
        """
        Update RAFT state information.
        
        This is called by the RAFT consensus when state changes occur.
        """
        with self.lock:
            state_changed = (
                self.current_raft_state != new_state or
                self.current_term != term or
                self.last_known_nodes != known_nodes
            )
            
            self.current_raft_state = new_state
            self.current_term = term
            self.last_known_nodes = known_nodes.copy()
            
            if state_changed:
                logging.info(f"RAFT state updated: {new_state}, term: {term}, nodes: {known_nodes}")
                
                # Send immediate hint if we became leader
                if new_state == RaftState.LEADER:
                    self._send_leader_hint_async()
    
    def start(self):
        """Start the discovery hint sender."""
        if self.is_running:
            return
        
        self.is_running = True
        self.sender_thread = threading.Thread(target=self._sender_loop)
        self.sender_thread.daemon = True
        self.sender_thread.start()
        
        logging.info("Discovery hint sender started")
    
    def stop(self):
        """Stop the discovery hint sender."""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.sender_thread:
            self.sender_thread.join(timeout=5)
        
        logging.info("Discovery hint sender stopped")
    
    def _sender_loop(self):
        """Main sender loop - only sends hints when leader."""
        while self.is_running:
            try:
                with self.lock:
                    should_send = (
                        self.current_raft_state == RaftState.LEADER and
                        time.time() - self.last_hint_sent > self.send_interval
                    )
                
                if should_send:
                    self._send_leader_hint()
                
                time.sleep(min(self.send_interval / 4, 10))  # Check frequently but don't spam
                
            except Exception as e:
                logging.error(f"Discovery hint sender error: {e}")
                time.sleep(10)  # Sleep longer on error
    
    def _send_leader_hint_async(self):
        """Send leader hint asynchronously."""
        def send_hint():
            try:
                self._send_leader_hint()
            except Exception as e:
                logging.error(f"Async leader hint send error: {e}")
        
        threading.Thread(target=send_hint, daemon=True).start()
    
    def _send_leader_hint(self):
        """
        Send leader hint to discovery service.
        
        This is a simple HTTP call to update the discovery service's
        leader hint for optimization purposes.
        """
        try:
            with self.lock:
                if self.current_raft_state != RaftState.LEADER:
                    return  # Only leaders send hints
                
                notification = LeaderNotification(
                    node_id=self.node_id,
                    term=self.current_term,
                    timestamp=time.time(),
                    node_list=self.last_known_nodes
                )
            
            # Send hint via HTTP (simple and reliable)
            hint_data = {
                "leader_id": notification.node_id,
                "term": notification.term,
                "timestamp": notification.timestamp,
                "active_nodes": list(notification.node_list)
            }
            
            url = f"http://{self.discovery_host}:{self.discovery_port}/leader_hint"
            
            response = requests.post(
                url,
                json=hint_data,
                timeout=5.0,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                with self.lock:
                    self.last_hint_sent = time.time()
                logging.debug(f"Leader hint sent successfully to discovery service")
            else:
                logging.warning(f"Discovery service returned status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            logging.debug(f"Discovery service unreachable (this is normal): {e}")
        except Exception as e:
            logging.error(f"Error sending leader hint: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about hint sending."""
        with self.lock:
            return {
                "node_id": self.node_id,
                "current_state": self.current_raft_state.value,
                "current_term": self.current_term,
                "known_nodes": list(self.last_known_nodes),
                "last_hint_sent": self.last_hint_sent,
                "discovery_endpoint": f"{self.discovery_host}:{self.discovery_port}"
            }


class DiscoveryHintReceiver:
    """
    Receives discovery hints for the pure service discovery.
    
    This is a simple HTTP endpoint that receives leader hints
    from RAFT leaders to optimize discovery.
    """
    
    def __init__(self, discovery_service):
        self.discovery_service = discovery_service
        self.hint_stats = {
            "hints_received": 0,
            "hints_accepted": 0,
            "hints_rejected": 0,
            "last_hint_time": 0
        }
        
        logging.info("Discovery hint receiver initialized")
    
    def receive_leader_hint(self, hint_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Receive and process a leader hint.
        
        This updates the discovery service's leader hint for optimization.
        """
        try:
            self.hint_stats["hints_received"] += 1
            self.hint_stats["last_hint_time"] = time.time()
            
            leader_id = hint_data.get("leader_id")
            term = hint_data.get("term", 0)
            timestamp = hint_data.get("timestamp", 0)
            active_nodes = hint_data.get("active_nodes", [])
            
            # Basic validation
            if not leader_id or not isinstance(leader_id, int):
                self.hint_stats["hints_rejected"] += 1
                return {"status": "error", "message": "Invalid leader_id"}
            
            # Update discovery service leader hint
            success = self.discovery_service.leader_hint(leader_id)
            
            if success:
                self.hint_stats["hints_accepted"] += 1
                logging.info(f"Discovery: Accepted leader hint for node {leader_id}")
                return {
                    "status": "accepted",
                    "leader_id": leader_id,
                    "registry_version": self.discovery_service.registry_version
                }
            else:
                self.hint_stats["hints_rejected"] += 1
                logging.warning(f"Discovery: Rejected leader hint for node {leader_id}")
                return {
                    "status": "rejected",
                    "message": "Leader not in registry or inactive"
                }
                
        except Exception as e:
            self.hint_stats["hints_rejected"] += 1
            logging.error(f"Error processing leader hint: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_stats(self) -> Dict[str, Any]:
        """Get hint receiver statistics."""
        return self.hint_stats.copy()


# Integration helpers for existing RAFT code

def integrate_discovery_hints_with_raft(raft_consensus, discovery_host='localhost', discovery_port=8090):
    """
    Integrate discovery hints with existing RAFT consensus.
    
    This function shows how to add discovery hints to existing RAFT code
    without changing the core consensus logic.
    """
    # Create hint sender
    hint_sender = DiscoveryHintSender(
        discovery_host=discovery_host,
        discovery_port=discovery_port,
        node_id=getattr(raft_consensus, 'node_id', 0)
    )
    
    # Store reference in RAFT consensus
    raft_consensus.discovery_hint_sender = hint_sender
    
    # Start hint sender
    hint_sender.start()
    
    # Add hook to update RAFT state
    original_handle_state_change = getattr(raft_consensus, 'handle_state_change', None)
    
    def enhanced_handle_state_change(old_state, new_state):
        # Call original handler
        if original_handle_state_change:
            original_handle_state_change(old_state, new_state)
        
        # Update hint sender
        known_nodes = getattr(raft_consensus, 'known_nodes', set())
        current_term = getattr(raft_consensus, 'current_term', 0)
        
        hint_sender.update_raft_state(
            new_state=new_state,
            term=current_term,
            known_nodes=known_nodes
        )
    
    # Replace handler
    raft_consensus.handle_state_change = enhanced_handle_state_change
    
    logging.info("Discovery hints integrated with RAFT consensus")
    return hint_sender


def add_discovery_hints_to_membership_changes(raft_consensus):
    """
    Add discovery hints to membership change handling.
    
    This ensures the discovery service gets updated when membership changes.
    """
    hint_sender = getattr(raft_consensus, 'discovery_hint_sender', None)
    if not hint_sender:
        logging.warning("No discovery hint sender found in RAFT consensus")
        return
    
    # Hook into membership changes
    original_on_membership_change = getattr(raft_consensus, 'on_membership_change', None)
    
    def enhanced_on_membership_change(added_nodes, removed_nodes, current_nodes):
        # Call original handler
        if original_on_membership_change:
            original_on_membership_change(added_nodes, removed_nodes, current_nodes)
        
        # Update hint sender with new membership
        current_term = getattr(raft_consensus, 'current_term', 0)
        current_state = getattr(raft_consensus, 'state', RaftState.FOLLOWER)
        
        hint_sender.update_raft_state(
            new_state=current_state,
            term=current_term,
            known_nodes=set(current_nodes)
        )
    
    # Replace handler
    raft_consensus.on_membership_change = enhanced_on_membership_change
    
    logging.info("Discovery hints added to membership change handling")
