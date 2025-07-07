#!/usr/bin/env python3
"""
Enhanced Standalone RAFT Node with Federated Learning Bootstrap

This script creates a standalone RAFT federated learning node that uses the real
gRPC communication manager and service discovery infrastructure.
"""

import sys
import os
import argparse
import logging
import time
import threading
import signal
import torch
import numpy as np
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Add the project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Try to import required modules with fallbacks
try:
    from algorithms.RAFT_GossipFL.raft_worker_manager import RaftWorkerManager
    from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
    from algorithms.RAFT_GossipFL.raft_node import RaftNode, RaftState
    from algorithms.SAPS_FL.decentralized_worker import DecentralizedWorker
    from algorithms.SAPS_FL.SAPS_topology_manager import SAPSTopologyManager
    from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager
    RAFT_AVAILABLE = True
except ImportError as e:
    logging.warning(f"RAFT modules not available: {e}")
    RAFT_AVAILABLE = False

try:
    from utils.timer_with_cuda import Timer
    from utils.metrics import Metrics
    UTILS_AVAILABLE = True
except ImportError:
    logging.warning("Utils modules not available, using mocks")
    UTILS_AVAILABLE = False

# Import MyModelTrainer with fallback
try:
    from algorithms.SAPS_FL.MyModelTrainer import MyModelTrainer
    MODEL_TRAINER_AVAILABLE = True
except ImportError as e:
    logging.warning(f"MyModelTrainer not available: {e}")
    MODEL_TRAINER_AVAILABLE = False


class MockDataLoader:
    """Mock data loader for testing."""
    
    def __init__(self, node_id: int, batch_size: int = 32, num_batches: int = 10):
        self.node_id = node_id
        self.batch_size = batch_size
        self.num_batches = num_batches
        self.current_batch = 0
        
    def __iter__(self):
        self.current_batch = 0
        return self
    
    def __next__(self):
        if self.current_batch >= self.num_batches:
            raise StopIteration
        
        # Generate mock data
        x = torch.randn(self.batch_size, 3, 32, 32)  # CIFAR-10 like data
        y = torch.randint(0, 10, (self.batch_size,))  # 10 classes
        
        self.current_batch += 1
        return x, y
    
    def __len__(self):
        return self.num_batches


class MockTimer:
    """Mock timer for testing."""
    
    def __init__(self, verbosity_level=0, log_fn=None):
        self.verbosity_level = verbosity_level
        self.log_fn = log_fn
        
    def start_timer(self, name: str):
        pass
        
    def end_timer(self, name: str):
        return 0.0


class MockMetrics:
    """Mock metrics for testing."""
    
    def __init__(self, classes, task='classification'):
        self.classes = classes
        self.task = task
        
    def evaluate(self, loss, output, target):
        """Mock evaluation that returns dummy metrics."""
        # Calculate accuracy
        if hasattr(output, 'argmax'):
            pred = output.argmax(dim=1)
            correct = (pred == target).float().mean()
            acc = correct.item()
        else:
            acc = 0.5  # dummy accuracy
            
        return {
            'Loss': loss.item() if hasattr(loss, 'item') else float(loss),
            'Acc1': acc,
            'Top1_Acc': acc
        }


class SimpleModelTrainer:
    """Simple model trainer that doesn't rely on complex scheduler infrastructure."""
    
    def __init__(self, model, device, args):
        self.model = model
        self.device = device
        self.args = args
        self.id = None
        
        # Setup criterion
        self.criterion = torch.nn.CrossEntropyLoss().to(device)
        
        # Setup optimizer without complex param groups
        self.optimizer = torch.optim.SGD(
            self.model.parameters(),
            lr=args.lr,
            weight_decay=args.wd,
            momentum=args.momentum,
            nesterov=args.nesterov
        )
        
        # Simple learning rate scheduler
        self.lr_scheduler = torch.optim.lr_scheduler.StepLR(
            self.optimizer, 
            step_size=10, 
            gamma=0.9
        )
        
        # Create param groups for compatibility with SAPS
        self.param_groups = self.optimizer.param_groups
        self.param_names = list(enumerate([f"param_group_{i}" for i in range(len(self.param_groups))]))
        
    def epoch_init(self):
        """Initialize epoch-specific state."""
        pass

    def epoch_end(self):
        """Clean up epoch-specific state."""
        pass
    
    def set_id(self, trainer_id):
        """Set the trainer ID."""
        self.id = trainer_id
        
    def get_model_params(self):
        """Get model parameters."""
        return self.model.cpu().state_dict()

    def set_model_params(self, model_parameters):
        """Set model parameters."""
        self.model.load_state_dict(model_parameters)

    def lr_schedule(self, epoch):
        """Update learning rate."""
        self.lr_scheduler.step()

    def train_one_step(self, train_batch_data, device, args, tracker=None, metrics=None):
        """Train one step."""
        self.model.to(device)
        self.model.train()
        
        x, labels = train_batch_data
        x, labels = x.to(device), labels.to(device)
        
        self.optimizer.zero_grad()
        output = self.model(x)
        loss = self.criterion(output, labels)
        loss.backward()
        self.optimizer.step()
        
        if tracker is not None and metrics is not None:
            metric_stat = metrics.evaluate(loss, output, labels)
            tracker.update_metrics(metric_stat, n_samples=labels.size(0))

        return loss, output, labels

    def test(self, test_data, device, args, tracker=None, metrics=None):
        """Test the model."""
        self.model.eval()
        self.model.to(device)
        
        with torch.no_grad():
            for batch_idx, (x, labels) in enumerate(test_data):
                x = x.to(device)
                labels = labels.to(device)
                output = self.model(x)
                loss = self.criterion(output, labels)
                
                if metrics is not None and tracker is not None:
                    metric_stat = metrics.evaluate(loss, output, labels)
                    tracker.update_metrics(metric_stat, n_samples=labels.size(0))
                    logging.info(f'(Trainer_ID {self.id}. Testing Iter: {batch_idx} \tLoss: {loss.item():.6f} ACC1:{metric_stat["Acc1"]:.4f})')
                else:
                    logging.info(f'(Trainer_ID {self.id}. Testing Iter: {batch_idx} \tLoss: {loss.item():.6f}')

    def train(self, train_data, device, args):
        """Train method for compatibility."""
        pass

    def test_on_the_server(self, train_data_local_dict, test_data_local_dict, device, args=None):
        """Server test method for compatibility."""
        pass


class EnhancedModelTrainer:
    """Enhanced model trainer that tries to use MyModelTrainer but falls back to SimpleModelTrainer."""
    
    def __new__(cls, model, device, args):
        if MODEL_TRAINER_AVAILABLE:
            try:
                return MyModelTrainer(model, device, args)
            except Exception as e:
                logging.warning(f"Failed to create MyModelTrainer: {e}")
                logging.warning("Falling back to SimpleModelTrainer")
        
        return SimpleModelTrainer(model, device, args)


def create_simple_model(model_name: str, num_classes: int = 10):
    """Create a simple model for testing."""
    if model_name == 'simple_cnn':
        import torch.nn as nn
        return nn.Sequential(
            nn.Conv2d(3, 32, 3, padding=1),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d((1, 1)),
            nn.Flatten(),
            nn.Linear(32, num_classes)
        )
    elif model_name == 'resnet18':
        try:
            from torchvision.models import resnet18
            return resnet18(num_classes=num_classes)
        except ImportError:
            logging.warning("torchvision not available, using simple CNN")
            return create_simple_model('simple_cnn', num_classes)
    else:
        # Default simple linear model
        return torch.nn.Sequential(
            torch.nn.Flatten(),
            torch.nn.Linear(3*32*32, 128),
            torch.nn.ReLU(),
            torch.nn.Linear(128, num_classes)
        )


class RaftFederatedLearningNode:
    """RAFT node with federated learning capabilities using real communication manager."""
    
    def __init__(self, args):
        self.args = args
        self.node_id = args.node_id
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Setup logging
        logging.basicConfig(
            level=getattr(logging, args.log_level),
            format=f'%(asctime)s - Node{self.node_id} - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f'FLNode{self.node_id}')
        
        # Check if RAFT is available
        if not RAFT_AVAILABLE:
            self.logger.error("RAFT modules not available. Please check your installation.")
            raise ImportError("RAFT modules required but not available")
        
        # Initialize components
        self._setup_fl_components()
        self._setup_communication_and_raft()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.logger.info(f"RAFT Federated Learning Node {self.node_id} initialized")
    
    def _setup_fl_components(self):
        """Setup federated learning components."""
        # Create model
        self.model = create_simple_model(self.args.model, 10)
        
        # Setup device
        self.device = torch.device('cuda' if torch.cuda.is_available() and self.args.use_cuda else 'cpu')
        self.model.to(self.device)
        
        # Create model trainer
        self.model_trainer = EnhancedModelTrainer(self.model, self.device, self.args)
        
        # Set trainer ID
        if hasattr(self.model_trainer, 'set_id'):
            self.model_trainer.set_id(self.node_id)
        else:
            self.model_trainer.id = self.node_id
        
        # Create mock data loaders
        self.train_data_local = MockDataLoader(self.node_id, self.args.batch_size, 50)
        self.test_data_local = MockDataLoader(self.node_id, self.args.batch_size, 10)
        
        # Mock data dictionaries
        self.train_data_local_dict = {self.node_id: self.train_data_local}
        self.test_data_local_dict = {self.node_id: self.test_data_local}
        self.train_data_local_num_dict = {self.node_id: 1000}
        
        # Create managers
        self.topology_manager = SAPSTopologyManager(self.args)
        
        # Generate initial topology before creating worker
        self.topology_manager.generate_topology(0)  # Initialize with round 0
        
        # Create timer and metrics (with fallbacks)
        if UTILS_AVAILABLE:
            self.timer = Timer(verbosity_level=1)
            self.metrics = Metrics([1], task='classification')
        else:
            self.timer = MockTimer(verbosity_level=1)
            self.metrics = MockMetrics([1], task='classification')
        
        # Create worker
        self.worker = DecentralizedWorker(
            worker_index=self.node_id,
            topology_manager=self.topology_manager,
            train_data_global=None,
            test_data_global=None,
            train_data_num=1000,
            train_data_local_dict=self.train_data_local_dict,
            test_data_local_dict=self.test_data_local_dict,
            train_data_local_num_dict=self.train_data_local_num_dict,
            worker_number=self.args.total_nodes,
            device=self.device,
            model=self.model,
            args=self.args,
            model_trainer=self.model_trainer,
            timer=self.timer,
            metrics=self.metrics
        )
        
        self.logger.info("Federated learning components initialized")
        
    
    def _setup_communication_and_raft(self):
        """Setup communication manager and RAFT components following standalone_raft_node.py pattern."""
        try:
            # Create REAL gRPC communication manager (like standalone_raft_node.py)
            self.logger.info("Creating gRPC communication manager...")
            self.comm_manager = DynamicGRPCCommManager(
                host=self.args.ip_address,
                port=self.args.port,
                node_id=self.node_id,
                client_num=self.args.total_nodes,
                topic="raft_fl",
                service_discovery_host=self.args.discovery_host,
                service_discovery_port=self.args.discovery_port,
                capabilities=["raft", "fedml", "gossip"],
                metadata={
                    "model": self.args.model,
                    "dataset": self.args.dataset,
                    "test_mode": "federated_learning"
                },
                use_service_discovery=True
            )
            
            # Create RAFT components
            self.logger.info("Creating RAFT components...")
            self.raft_node = RaftNode(self.node_id, self.args)
            
            # Create RAFT consensus
            self.raft_consensus = RaftConsensus(
                raft_node=self.raft_node,
                worker_manager=None,  # Will be set after RaftWorkerManager is created
                args=self.args,
                bandwidth_manager=None,  # Optional for now
                topology_manager=self.topology_manager
            )
            
            # Create RAFT worker manager with REAL communication manager
            self.logger.info("Creating RAFT worker manager...")
            self.worker_manager = RaftWorkerManager(
                args=self.args,
                comm=self.comm_manager,  # Use REAL communication manager
                node_id=self.node_id,
                size=self.args.total_nodes,
                worker=self.worker,
                topology_manager=self.topology_manager,
                model_trainer=self.model_trainer,
                timer=self.timer,
                metrics=self.metrics,
                raft_consensus=self.raft_consensus,
                bandwidth_manager=None
            )
            
            # Complete circular references
            self.raft_consensus.worker_manager = self.worker_manager
            
            # Register message handlers for RAFT messages
            self.logger.info("Registering message handlers...")
            self.worker_manager.register_message_receive_handlers()
            
            # Register the service discovery bridge with the communication manager
            if hasattr(self.worker_manager, 'service_discovery_bridge'):
                self.raft_consensus.register_service_discovery_bridge(self.worker_manager.service_discovery_bridge)
                self.worker_manager.service_discovery_bridge.register_with_comm_manager(self.comm_manager)
                self.logger.info("Service discovery bridge registered with consensus and communication manager")
            
            self.logger.info("Communication and RAFT components initialized")
            
        except Exception as e:
            self.logger.error(f"Error setting up communication and RAFT components: {e}")
            raise
    
    def start(self):
        """Start the federated learning node following standalone_raft_node.py pattern."""
        try:
            self.running = True
            self.logger.info(f"Starting RAFT Federated Learning Node {self.node_id}")
            
            # Step 1: Start gRPC communication manager
            self.logger.info("Starting communication manager...")
            self.comm_manager.start_message_handling()
            
            # Step 2: Initialize RAFT state based on discovered nodes (like standalone_raft_node.py)
            self._initialize_raft_state()
            
            # Step 3: Start RAFT consensus
            self.logger.info("Starting RAFT consensus...")
            self.raft_consensus.start()
            
            # Step 4: Start the worker manager which will handle federated learning
            self.logger.info("Starting worker manager...")
            self.worker_manager.run()
            
        except Exception as e:
            self.logger.error(f"Error starting node: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _initialize_raft_state(self):
        """Initialize RAFT state based on discovered nodes (from standalone_raft_node.py)."""
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
        self.raft_node.update_known_nodes(discovered_nodes)
        self.logger.info(f"Discovered nodes: {discovered_nodes}")
        
        # Simple logic: if only this node exists or bootstrap flag is set, become leader
        other_nodes = [n for n in discovered_nodes if n != self.node_id]
        
        if len(other_nodes) == 0 or self.args.bootstrap:
            # Bootstrap node: transition to leader state
            self.logger.info("Bootstrap node detected - transitioning to leader state")
            with self.raft_node.state_lock:
                # Increment term and become leader via proper state transitions
                self.raft_node.current_term += 1
                self.raft_node.voted_for = self.node_id
                
                # Transition INITIAL -> FOLLOWER -> CANDIDATE -> LEADER for bootstrap
                if self.raft_node.state == RaftState.INITIAL:
                    self.raft_node.become_follower(self.raft_node.current_term)
                    self.logger.debug(f"Bootstrap node transitioned to FOLLOWER in term {self.raft_node.current_term}")
                
                # Now transition FOLLOWER -> CANDIDATE
                self.raft_node.state = RaftState.CANDIDATE
                self.logger.debug(f"Bootstrap node transitioned to CANDIDATE in term {self.raft_node.current_term}")
                
                # Finally CANDIDATE -> LEADER
                self.raft_node.become_leader()
                self.logger.info(f"Node is now leader in term {self.raft_node.current_term}")
        else:
            # Joining node: stay in INITIAL state and send join request
            self.logger.info(f"Joining existing cluster with nodes: {other_nodes}")
            # DO NOT transition to FOLLOWER yet - stay in INITIAL state until join is approved
            self.logger.info("Node will stay in INITIAL state until join request is approved")
            
            # Send join request to the cluster through the service discovery bridge
            if hasattr(self.worker_manager, 'service_discovery_bridge'):
                self.logger.info("Sending join request to existing cluster...")
                # Get leader hint from communication manager
                leader_hint = self.comm_manager.get_leader_hint()
                if leader_hint:
                    self.logger.info(f"Using leader hint from service discovery: {leader_hint}")
                self.worker_manager.service_discovery_bridge.send_join_request_to_cluster(discovered_nodes, leader_hint)
            else:
                self.logger.warning("No service discovery bridge available to send join request")
            
            self.logger.info("Node will wait for join response and leader contact to sync state")
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.shutdown()
    
    def shutdown(self):
        """Shutdown the node."""
        self.logger.info(f"Shutting down RAFT Federated Learning Node {self.node_id}")
        self.running = False
        self.shutdown_event.set()
        
        try:
            # Stop RAFT worker manager first
            if hasattr(self, 'worker_manager'):
                self.logger.info("Stopping worker manager...")
                self.worker_manager.finish()
            
            # Stop RAFT consensus
            if hasattr(self, 'raft_consensus'):
                self.logger.info("Stopping RAFT consensus...")
                self.raft_consensus.stop()
            
            # Stop communication manager
            if hasattr(self, 'comm_manager'):
                self.logger.info("Stopping communication manager...")
                self.comm_manager.cleanup()
                
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        
        self.logger.info(f"Node {self.node_id} shutdown complete")
    
    def get_status(self):
        """Get node status for monitoring."""
        try:
            raft_state = self.raft_node.state.name if hasattr(self.raft_node, 'state') else 'UNKNOWN'
            
            # Get connected nodes from the gRPC comm manager
            connected_nodes = []
            if hasattr(self.comm_manager, 'get_cluster_nodes_info'):
                cluster_info = self.comm_manager.get_cluster_nodes_info()
                connected_nodes = list(cluster_info.keys())
            
            return {
                'node_id': self.node_id,
                'raft_state': raft_state,
                'current_term': getattr(self.raft_node, 'current_term', 0),
                'commit_index': getattr(self.raft_node, 'commit_index', 0),
                'known_nodes': list(getattr(self.raft_node, 'known_nodes', set())),
                'is_leader': raft_state == 'LEADER',
                'connected_nodes': connected_nodes,
                'running': self.running
            }
        except Exception as e:
            self.logger.error(f"Error getting status: {e}")
            return {'error': str(e)}


def create_args_from_command_line():
    """Create enhanced args with FL-specific parameters."""
    parser = argparse.ArgumentParser(description="RAFT Federated Learning Node")
    
    # Node configuration
    parser.add_argument('--node-id', type=int, required=True, help='Node ID')
    parser.add_argument('--total-nodes', type=int, default=3, help='Expected total nodes')
    parser.add_argument('--discovery-host', default='localhost', help='Service discovery host')
    parser.add_argument('--discovery-port', type=int, default=8080, help='Service discovery port')
    parser.add_argument('--ip-address', default='localhost', help='Node IP address')
    parser.add_argument('--port', type=int, help='Node port (auto-assigned if not specified)')
    parser.add_argument('--bootstrap', action='store_true', help='Bootstrap mode')
    
    # RAFT configuration
    parser.add_argument('--min-election-timeout', type=int, default=150, help='Min election timeout (ms)')
    parser.add_argument('--max-election-timeout', type=int, default=300, help='Max election timeout (ms)')
    parser.add_argument('--heartbeat-interval', type=int, default=50, help='Heartbeat interval (ms)')
    
    # Federated Learning configuration
    parser.add_argument('--comm-round', type=int, default=10, help='Communication rounds')
    parser.add_argument('--epochs', type=int, default=1, help='Local epochs per round')
    parser.add_argument('--batch-size', type=int, default=32, help='Batch size')
    parser.add_argument('--lr', type=float, default=0.01, help='Learning rate')
    parser.add_argument('--model', default='simple_cnn', help='Model architecture')
    parser.add_argument('--dataset', default='mock_cifar10', help='Dataset')
    parser.add_argument('--use-cuda', action='store_true', help='Use CUDA if available')
    
    # SAPS FL configuration
    parser.add_argument('--compression', default='topk', help='Compression method')
    parser.add_argument('--compress-ratio', type=float, default=0.1, help='Compression ratio')
    parser.add_argument('--quantize-level', type=int, default=8, help='Quantization level')
    parser.add_argument('--is-biased', type=int, default=0, help='Biased compression')
    parser.add_argument('--bandwidth-type', default='random', help='Bandwidth type for SAPS')
    parser.add_argument('--B-thres', type=float, default=1.0, help='SAPS bandwidth threshold')
    parser.add_argument('--T-thres', type=float, default=1.0, help='SAPS time threshold')
    
    # Optimizer configuration
    parser.add_argument('--client-optimizer', default='sgd', help='Client optimizer')
    parser.add_argument('--wd', type=float, default=1e-4, help='Weight decay')
    parser.add_argument('--momentum', type=float, default=0.9, help='Momentum')
    parser.add_argument('--nesterov', action='store_true', help='Nesterov momentum')
    
    # Scheduler configuration (missing attributes that might be needed)
    parser.add_argument('--client-index', type=int, default=0, help='Client index')
    parser.add_argument('--lr-scheduler', default='cosine', help='Learning rate scheduler')
    parser.add_argument('--lr-decay-rate', type=float, default=0.998, help='LR decay rate')
    parser.add_argument('--lr-decay-epoch', type=int, default=1, help='LR decay epoch')
    parser.add_argument('--lr-patience', type=int, default=10, help='LR patience')
    parser.add_argument('--lr-threshold', type=float, default=0.0001, help='LR threshold')
    
    # Additional configuration
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    parser.add_argument('--warmup-epochs', type=int, default=0, help='Warmup epochs')
    parser.add_argument('--task', default='classification', help='Task type')
    
    args = parser.parse_args()
    
    # Auto-assign port if not specified
    if args.port is None:
        args.port = 9000 + args.node_id
    
    # Set client_index to node_id for compatibility
    args.client_index = args.node_id
    
    # Add additional required attributes
    args.join_existing_cluster = not args.bootstrap  # Join if not bootstrap
    args.raft_log_compaction_threshold = 100
    args.client_num_in_total = args.total_nodes
    args.data_dir = './data'
    args.backend = 'gRPC'
    args.Failure_chance = None
    
    return args


def main():
    """Main function for federated learning with RAFT."""
    args = create_args_from_command_line()
    
    # Print startup information
    print("=" * 80)
    print(f"🚀 Starting RAFT Federated Learning Node {args.node_id}")
    print("=" * 80)
    print(f"Node Configuration:")
    print(f"  Node ID:          {args.node_id}")
    print(f"  Address:          {args.ip_address}:{args.port}")
    print(f"  Discovery:        {args.discovery_host}:{args.discovery_port}")
    print(f"  Bootstrap Mode:   {args.bootstrap}")
    print()
    print(f"Federated Learning Configuration:")
    print(f"  Model:            {args.model}")
    print(f"  Dataset:          {args.dataset}")
    print(f"  Communication Rounds: {args.comm_round}")
    print(f"  Local Epochs:     {args.epochs}")
    print(f"  Batch Size:       {args.batch_size}")
    print(f"  Learning Rate:    {args.lr}")
    print(f"  Compression:      {args.compression} ({args.compress_ratio})")
    print()
    print(f"RAFT Configuration:")
    print(f"  Election Timeout: {args.min_election_timeout}-{args.max_election_timeout}ms")
    print(f"  Heartbeat:        {args.heartbeat_interval}ms")
    print()
    print("Expected Behavior:")
    if args.bootstrap or args.node_id == 0:
        print("  - This node should become RAFT leader (coordinator)")
        print("  - It will coordinate federated learning rounds")
        print("  - Other nodes will join and become followers")
    else:
        print("  - This node should discover existing cluster")
        print("  - Join as RAFT follower")
        print("  - Participate in coordinated federated learning")
    print()
    print("Usage:")
    print("  1. Start first node with --bootstrap or --node-id 0")
    print("  2. Start additional nodes without --bootstrap")
    print("  3. Watch logs for RAFT leader election and FL training")
    print("  4. Nodes will communicate via gossip protocol")
    print()
    print("Press Ctrl+C to stop...")
    print("=" * 80)
    
    # Create and start node
    node = None
    try:
        node = RaftFederatedLearningNode(args)
        node.start()
    except KeyboardInterrupt:
        print("\nReceived interrupt signal...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if node:
            node.shutdown()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())