#!/usr/bin/env python3

"""
gRPC-based hint sender for pure service discovery.

This replaces the HTTP-based hint sender with gRPC calls.
"""

import grpc
import logging
import time
from typing import Optional
from discovery_hints import RaftState, LeaderNotification, DiscoveryHintSender
from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2_grpc
from fedml_core.distributed.communication.grpc import grpc_comm_manager_pb2


class GRPCHintSender(DiscoveryHintSender):
    """gRPC-based hint sender for service discovery."""
    
    def __init__(self, discovery_host: str = 'localhost', discovery_port: int = 8090,
                 node_id: int = 0, send_interval: float = 30.0):
        super().__init__(discovery_host, discovery_port, node_id, send_interval)
        self.channel: Optional[grpc.Channel] = None
        self.stub: Optional[grpc_comm_manager_pb2_grpc.GatewayServiceStub] = None
    
    def _send_leader_hint(self):
        """Send leader hint via gRPC."""
        if not self._ensure_grpc_connection():
            return
        
        try:
            # Create notification from current state
            with self.lock:
                notification = LeaderNotification(
                    node_id=self.node_id,
                    term=self.current_term,
                    timestamp=time.time(),
                    node_list=self.last_known_nodes
                )
            
            # Create gRPC request
            request = grpc_comm_manager_pb2.UpdateLeaderRequest(
                leader_id=notification.node_id
            )
            
            # Send hint
            response = self.stub.UpdateLeader(request, timeout=5.0)
            
            if response.status == "updated":
                with self.lock:
                    self.last_hint_sent = time.time()
                logging.debug(f"Leader hint sent successfully via gRPC")
            else:
                logging.warning(f"Discovery service ignored hint: {response.message}")
                
        except grpc.RpcError as e:
            logging.debug(f"gRPC discovery service unreachable: {e}")
        except Exception as e:
            logging.error(f"Error sending gRPC leader hint: {e}")
    
    def _ensure_grpc_connection(self) -> bool:
        """Ensure gRPC connection is established."""
        if self.channel is None:
            try:
                self.channel = grpc.insecure_channel(
                    f'{self.discovery_host}:{self.discovery_port}',
                    options=[
                        ('grpc.max_receive_message_length', 1000 * 1024 * 1024),
                        ('grpc.max_send_message_length', 1000 * 1024 * 1024),
                    ]
                )
                self.stub = grpc_comm_manager_pb2_grpc.GatewayServiceStub(self.channel)
                return True
            except Exception as e:
                logging.debug(f"Could not create gRPC channel: {e}")
                return False
        return True
    
    def stop(self):
        """Stop the hint sender and close gRPC connection."""
        super().stop()
        
        if self.channel:
            self.channel.close()
            self.channel = None
            self.stub = None
