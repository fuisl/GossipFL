#!/usr/bin/env python3
"""
Standalone RAFT Node Runner for Manual Testing

This script creates a standalone RAFT node for manual testing of the 
RAFT consensus implementation with service discovery. It mocks the 
federated learning components and focuses on demonstrating:

1. Node bootstrapping and cluster formation
2. Leader election and state transitions
3. Dynamic node joining and leaving
4. Service discovery integration
5. Communication refresh and topology updates

Usage:
    python standalone_raft_node.py --node-id 0 --discovery-host localhost --discovery-port 8080
    python standalone_raft_node.py --node-id 1 --discovery-host localhost --discovery-port 8080
    python standalone_raft_node.py --node-id 2 --discovery-host localhost --discovery-port 8080

Test Scenarios:
    - Bootstrap: Start node 0 first, it should become leader
    - Join: Start additional nodes, they should discover and join cluster
    - Election: Kill leader node, new leader should be elected
    - Rejoin: Restart killed node, it should rejoin as follower
"""

import sys
import os
import argparse
import logging
import time
import threading
import json
import signal
from typing import Dict, List, Optional, Any
from unittest.mock import Mock, MagicMock
from dataclasses import dataclass
import grpc
import traceback

# Add the project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from algorithms.RAFT_GossipFL.raft_worker_manager import RaftWorkerManager
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
from algorithms.RAFT_GossipFL.raft_node import RaftNode
from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager


@dataclass
class MockArgs:
    """Mock configuration arguments."""
    join_existing_cluster: bool = False
    ip_address: str = 'localhost'
    port: int = 8080
    discovery_host: str = 'localhost'
    discovery_port: int = 8080
    node_id: int = 0
    bootstrap: bool = False
    min_election_timeout: int = 150
    max_election_timeout: int = 300
    heartbeat_interval: int = 50
    raft_log_compaction_threshold: int = 100
    client_num_in_total: Optional[int] = None
    comm_round: int = 100
    epochs: int = 1
    batch_size: int = 32
    lr: float = 0.01
    model: str = 'resnet18'
    dataset: str = 'cifar10'
    data_dir: str = './data'
    log_level: str = 'INFO'
    backend: str = 'gRPC'
    compression: str = "topk"
    compress_ratio: float = 0.1
    quantize_level: int = 8
    is_biased: int = 0
    # Additional fields that might be needed
    warmup_epochs: int = 0
    Failure_chance: Optional[float] = None


class MockTrainer:
    """Mock trainer for federated learning."""
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.model_params = {'weights': f'mock_weights_node_{node_id}', 'version': 1}
        self.training_round = 0
        self.local_epochs = 0
        self.is_training = False
        
    def train(self, round_idx: int) -> Dict[str, Any]:
        """Mock training process."""
        self.training_round = round_idx
        self.is_training = True
        
        # Simulate training time
        time.sleep(0.1)
        
        # Update model parameters
        self.model_params['version'] += 1
        self.model_params['weights'] = f'mock_weights_node_{self.node_id}_round_{round_idx}'
        
        # Generate mock metrics
        metrics = {
            'accuracy': 0.8 + (round_idx * 0.01),
            'loss': 1.0 - (round_idx * 0.01),
            'samples': 1000,
            'round': round_idx
        }
        
        self.is_training = False
        logging.info(f"Node {self.node_id}: Completed training round {round_idx}")
        return metrics
    
    def get_model_params(self) -> Dict[str, Any]:
        """Get current model parameters."""
        return self.model_params
    
    def set_model_params(self, params: Dict[str, Any]):
        """Set model parameters."""
        self.model_params = params
        logging.info(f"Node {self.node_id}: Updated model parameters")


class MockTopologyManager:
    """Mock topology manager."""
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.neighbors = set()
        # Initialize topology with the current node to avoid KeyError
        self.topology = {node_id: []}
        
    def get_topology(self) -> Dict[int, List[int]]:
        """Get current topology."""
        return self.topology
    
    def get_neighbor_list(self) -> set:
        """Get neighbor list."""
        return self.neighbors
    
    def update_topology(self, new_topology: Dict[int, List[int]]):
        """Update topology."""
        self.topology = new_topology
        self.neighbors = set(new_topology.get(self.node_id, []))
        logging.info(f"Node {self.node_id}: Updated topology with {len(self.neighbors)} neighbors")
    
    def generate_topology(self, t: int = 0):
        """Generate topology for a given round."""
        # For testing, just maintain the existing topology
        pass
    
    def get_out_neighbor_idx_list(self, node_id: int) -> List[int]:
        """Get list of outgoing neighbors for a node."""
        return self.topology.get(node_id, [])


class MockBandwidthManager:
    """Mock bandwidth manager."""
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.bandwidth_data = {}
        
    def get_bandwidth(self) -> Optional[Dict[int, float]]:
        """Get bandwidth measurements."""
        return self.bandwidth_data
    
    def update_bandwidth(self, measurements: Dict[int, float]):
        """Update bandwidth measurements."""
        self.bandwidth_data = measurements
        logging.info(f"Node {self.node_id}: Updated bandwidth measurements")
    
    def apply_bandwidth_update(self, update_data: Dict[str, Any]):
        """Apply bandwidth update from state snapshot."""
        if 'matrix' in update_data:
            # Convert matrix to dict format
            matrix = update_data['matrix']
            self.bandwidth_data = {i: float(val) for i, val in enumerate(matrix) if val > 0}


class MockTimer:
    """Mock timer for performance measurement."""
    
    def __init__(self):
        self.start_time = time.time()
        self.timers = {}
        
    def start_timer(self, name: str):
        """Start a timer."""
        self.timers[name] = time.time()
        
    def end_timer(self, name: str) -> float:
        """End a timer and return duration."""
        if name in self.timers:
            duration = time.time() - self.timers[name]
            del self.timers[name]
            return duration
        return 0.0


class MockMetrics:
    """Mock metrics collector."""
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.metrics = {}
        # Add metric_names attribute expected by RuntimeTracker
        self.metric_names = ['accuracy', 'loss', 'samples', 'round']
        
    def record_metric(self, name: str, value: Any):
        """Record a metric."""
        self.metrics[name] = value
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics."""
        return self.metrics


class StandaloneRaftNode:
    """Standalone RAFT node for manual testing."""
    
    def __init__(self, args: MockArgs):
        self.args = args
        self.node_id = args.node_id
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, args.log_level),
            format=f'%(asctime)s - Node{self.node_id} - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f'Node{self.node_id}')
        
        # Create gRPC communication manager
        self.comm_manager = DynamicGRPCCommManager(
            host=args.ip_address,
            port=args.port,
            node_id=self.node_id,
            client_num=1,  # Just one client for testing
            topic="raft_test",
            service_discovery_host=args.discovery_host,
            service_discovery_port=args.discovery_port,
            capabilities=["raft", "fedml"],
            metadata={"test_mode": "true"},
            use_service_discovery=True
        )
        self.trainer = MockTrainer(self.node_id)
        self.topology_manager = MockTopologyManager(self.node_id)
        self.bandwidth_manager = MockBandwidthManager(self.node_id)
        self.timer = MockTimer()
        self.metrics = MockMetrics(self.node_id)
        
        # Create mock worker
        self.worker = Mock()
        self.worker.node_id = self.node_id
        self.worker.num_iterations = 10  # Mock number of iterations per epoch
        self.worker.param_groups = []  # Mock parameter groups
        self.worker.param_names = []   # Mock parameter names
        self.worker.shapes = {}        # Mock parameter shapes
        self.worker.neighbor_hat_params = {"memory": {}}  # Mock neighbor parameters
        
        # Mock methods that might be called
        self.worker.refresh_gossip_info = Mock()
        self.worker.init_neighbor_hat_params = Mock()
        self.worker.get_dataset_len = Mock(return_value=1000)
        self.worker.aggregate = Mock()
        self.worker.train_one_step = Mock(return_value=(0.5, None, None))  # (loss, output, target)
        self.worker.set_coordinator = Mock()
        
        # Create RAFT components
        self.raft_node = RaftNode(self.node_id, args)
        
        # First create RaftConsensus with temporary worker manager
        self.raft_consensus = RaftConsensus(
            raft_node=self.raft_node,
            worker_manager=None,  # Will be set after RaftWorkerManager is created
            args=args,
            bandwidth_manager=self.bandwidth_manager,
            topology_manager=self.topology_manager
        )
        
        # Now create the actual RAFT worker manager
        self.worker_manager = RaftWorkerManager(
            args=args,
            comm=self.comm_manager,
            node_id=self.node_id,
            size=1,  # Initial size, will be updated dynamically
            worker=self.worker,
            topology_manager=self.topology_manager,
            model_trainer=self.trainer,
            timer=self.timer,
            metrics=self.metrics,
            raft_consensus=self.raft_consensus,
            bandwidth_manager=self.bandwidth_manager
        )

        self.worker_manager.register_message_receive_handlers()
        
        # Now set the worker manager in the consensus
        self.raft_consensus.worker_manager = self.worker_manager
        
        # Register the service discovery bridge with the communication manager
        if hasattr(self.worker_manager, 'service_discovery_bridge'):
            self.worker_manager.service_discovery_bridge.register_with_comm_manager(self.comm_manager)
            self.logger.info("Service discovery bridge registered with communication manager")
        
        # Setup monitoring
        self.status_monitor = StatusMonitor(self)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.logger.info(f"Standalone RAFT node {self.node_id} initialized")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
        # Force exit if graceful shutdown takes too long
        import threading
        def force_exit():
            time.sleep(10)  # Give 10 seconds for graceful shutdown
            self.logger.warning("Forcing exit due to shutdown timeout")
            os._exit(1)
    
        threading.Thread(target=force_exit, daemon=True).start()
    
    def start(self):
        """Start the RAFT node."""
        try:
            self.running = True
            self.logger.info(f"Starting RAFT node {self.node_id}")
            
            # Start gRPC communication manager
            self.comm_thread = threading.Thread(
                target=self.worker_manager.com_manager.handle_receive_message,
                daemon=True,
                name=f"CommHandler-{self.node_id}"
            )
            self.comm_thread.start()
            
            # Start status monitoring
            self.status_monitor.start()

            # Initialize RAFT state based on discovered nodes
            self._initialize_raft_state()
            
            # Start RAFT consensus
            self.raft_consensus.start()
            
            self.logger.info(f"RAFT node {self.node_id} started successfully")
            
            # Main loop - just keep the node running
            while self.running and not self.shutdown_event.is_set():
                time.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Error starting node: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def shutdown(self):
        """Shutdown the RAFT node."""
        self.logger.info(f"Shutting down RAFT node {self.node_id}")
        self.running = False
        self.shutdown_event.set()
        
        try:
            # Stop RAFT consensus FIRST (this is critical for leaders)
            if hasattr(self, 'raft_consensus'):
                self.logger.info("Stopping RAFT consensus...")
                self.raft_consensus.stop()
                
            # Stop status monitoring
            if hasattr(self, 'status_monitor'):
                self.logger.info("Stopping status monitor...")
                self.status_monitor.stop()
                
            # Stop gRPC communication manager
            if hasattr(self, 'comm_manager'):
                self.logger.info("Stopping communication manager...")
                if hasattr(self.comm_manager, 'stop_receive_message'):
                    self.comm_manager.stop_receive_message()
                # Force close gRPC connections
                if hasattr(self.comm_manager, 'close_all_connections'):
                    self.comm_manager.close_all_connections()
            
            # Wait for threads to finish with timeout
            if hasattr(self, 'comm_thread') and self.comm_thread.is_alive():
                self.logger.info("Waiting for communication thread to finish...")
                self.comm_thread.join(timeout=2)
                if self.comm_thread.is_alive():
                    self.logger.warning("Communication thread did not finish gracefully")
                    
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        
        self.logger.info(f"RAFT node {self.node_id} shutdown complete")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current node status."""
        try:
            raft_state = self.raft_node.state.name if hasattr(self.raft_node, 'state') else 'UNKNOWN'
            
            # Get connected nodes from the gRPC comm manager
            connected_nodes = []
            if hasattr(self.comm_manager, 'node_registry'):
                connected_nodes = list(self.comm_manager.node_registry.keys())
            
            return {
                'node_id': self.node_id,
                'raft_state': raft_state,
                'current_term': getattr(self.raft_node, 'current_term', 0),
                'commit_index': getattr(self.raft_node, 'commit_index', 0),
                'known_nodes': list(getattr(self.raft_node, 'known_nodes', set())),
                'is_leader': raft_state == 'LEADER',
                'training_round': self.trainer.training_round,
                'is_training': self.trainer.is_training,
                'connected_nodes': connected_nodes,
                'running': self.running
            }
        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {'error': str(e)}
    
    def _initialize_raft_state(self):
        """Initialize RAFT state based on discovered nodes."""
        self.logger.info("Initializing RAFT state based on discovered nodes...")
        
        # Get cluster nodes from communication manager
        cluster_nodes = self.comm_manager.get_cluster_nodes_info()
        discovered_nodes = list(cluster_nodes.keys())
        
        if not discovered_nodes:
            self.logger.warning("No nodes discovered from service discovery")
            # Wait a bit more and try again
            time.sleep(3)
            cluster_nodes = self.comm_manager.get_cluster_nodes_info()
            discovered_nodes = list(cluster_nodes.keys())
        
        if not discovered_nodes:
            self.logger.warning("Still no nodes discovered - assuming bootstrap node")
            discovered_nodes = [self.node_id]
        
        # Update known nodes in RAFT
        self.raft_node.update_known_nodes(node_ids=discovered_nodes)
        self.logger.info(f"Discovered nodes: {discovered_nodes}")
        
        # Simple logic: if only this node exists, it's the bootstrap node
        other_nodes = [n for n in discovered_nodes if n != self.node_id]
        
        if len(other_nodes) == 0:
            # Bootstrap node: become leader immediately
            self.logger.info("Bootstrap node detected - becoming leader")
            result = self.raft_node.bootstrap_detection(set(discovered_nodes))
            if result:
                self.logger.info("Bootstrap successful - node is now leader")
            else:
                self.logger.error("Bootstrap failed")
        else:
            # Joining node: actively initiate join protocol
            self.logger.info(f"Joining existing cluster with nodes: {other_nodes}")
            self.logger.info("Initiating cluster join via request_cluster_join...")
            
            try:
                # Use the worker manager's request_cluster_join to initiate handshake
                join_result = self.worker_manager.request_cluster_join()
                if join_result:
                    self.logger.info("Cluster join request sent successfully")
                    self.logger.info("Waiting for leader to process join and sync state...")
                else:
                    self.logger.error("Failed to send cluster join request")
                    self.logger.info("Will stay in INITIAL state and wait for leader contact")
            except Exception as e:
                self.logger.error(f"Error during cluster join: {e}")
                self.logger.info("Will stay in INITIAL state and wait for leader contact")


class StatusMonitor:
    """Monitor and display node status."""
    
    def __init__(self, node: StandaloneRaftNode):
        self.node = node
        self.running = False
        self.thread = None
        
    def start(self):
        """Start status monitoring."""
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        
    def stop(self):
        """Stop status monitoring."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1)
            
    def _monitor_loop(self):
        """Monitor loop."""
        while self.running:
            try:
                status = self.node.get_status()
                self._print_status(status)
                time.sleep(5)  # Update every 5 seconds
            except Exception as e:
                self.node.logger.error(f"Status monitor error: {e}")
                time.sleep(1)
    
    def _print_status(self, status: Dict[str, Any]):
        """Print status information."""
        print(f"\n{'='*60}")
        print(f"Node {status['node_id']} Status - {time.strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        print(f"RAFT State:       {status['raft_state']}")
        print(f"Current Term:     {status['current_term']}")
        print(f"Commit Index:     {status['commit_index']}")
        print(f"Known Nodes:      {status['known_nodes']}")
        print(f"Connected Nodes:  {status['connected_nodes']}")
        print(f"Training Round:   {status['training_round']}")
        print(f"Is Training:      {status['is_training']}")
        print(f"Is Leader:        {status['is_leader']}")
        print(f"Running:          {status['running']}")
        print(f"{'='*60}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Standalone RAFT Node for Manual Testing")
    parser.add_argument('--node-id', type=int, required=True, help='Node ID (unique integer)')
    parser.add_argument('--discovery-host', default='localhost', help='Service discovery host')
    parser.add_argument('--discovery-port', type=int, default=8080, help='Service discovery port')
    parser.add_argument('--ip-address', default='localhost', help='Node IP address')
    parser.add_argument('--port', type=int, help='Node port (auto-assigned if not specified)')
    parser.add_argument('--bootstrap', action='store_true', help='Bootstrap mode (first node)')
    parser.add_argument('--min-election-timeout', type=int, default=150, help='Min election timeout (ms)')
    parser.add_argument('--max-election-timeout', type=int, default=300, help='Max election timeout (ms)')
    parser.add_argument('--heartbeat-interval', type=int, default=50, help='Heartbeat interval (ms)')
    parser.add_argument('--log-level', default='DEBUG', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    parser.add_argument('--comm-round', type=int, default=100, help='Communication rounds')

    args = parser.parse_args()
    
    # Auto-assign port if not specified
    if args.port is None:
        args.port = 9000 + args.node_id
    
    # Create mock args
    mock_args = MockArgs(
        node_id=args.node_id,
        discovery_host=args.discovery_host,
        discovery_port=args.discovery_port,
        ip_address=args.ip_address,
        port=args.port,
        bootstrap=args.bootstrap,
        min_election_timeout=args.min_election_timeout,
        max_election_timeout=args.max_election_timeout,
        heartbeat_interval=args.heartbeat_interval,
        log_level=args.log_level,
        comm_round=args.comm_round
    )
    
    # Print startup information
    print("=" * 80)
    print(f"🚀 Starting RAFT Node {args.node_id}")
    print("=" * 80)
    print(f"Node ID:          {args.node_id}")
    print(f"Address:          {args.ip_address}:{args.port}")
    print(f"Discovery:        {args.discovery_host}:{args.discovery_port}")
    print(f"Bootstrap Mode:   {args.bootstrap}")
    print(f"Election Timeout: {args.min_election_timeout}-{args.max_election_timeout}ms")
    print(f"Heartbeat:        {args.heartbeat_interval}ms")
    print(f"Log Level:        {args.log_level}")
    print()
    print("Expected Behavior:")
    if args.bootstrap or args.node_id == 0:
        print("  - This node should become LEADER (bootstrap node)")
        print("  - Wait for other nodes to join and become followers")
    else:
        print("  - This node should discover existing cluster")
        print("  - Join as FOLLOWER and sync with leader")
    print()
    print("Manual Testing:")
    print("  - Watch the status updates every 5 seconds")
    print("  - Check service discovery monitor for cluster state")
    print("  - Test leader election by stopping the leader node")
    print("  - Test dynamic joining by starting nodes in different orders")
    print()
    print("Press Ctrl+C to stop this node...")
    print("=" * 80)
    
    # Create and start node
    node = None
    try:
        node = StandaloneRaftNode(mock_args)
        node.start()
    except KeyboardInterrupt:
        print("\nReceived interrupt signal...")
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        return 1
    finally:
        if node:
            node.shutdown()

    return 0


if __name__ == '__main__':
    sys.exit(main())
