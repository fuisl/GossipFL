"""
Enhanced Standalone RAFT Node with Federated Learning Bootstrap

This extends the basic RAFT node to include actual federated learning capabilities.
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

# Add the project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from algorithms.RAFT_GossipFL.raft_worker_manager import RaftWorkerManager
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
from algorithms.RAFT_GossipFL.raft_node import RaftNode, RaftState
from algorithms.SAPS_FL.MyModelTrainer import MyModelTrainer
from algorithms.SAPS_FL.decentralized_worker import DecentralizedWorker
from algorithms.SAPS_FL.SAPS_topology_manager import SAPSTopologyManager
from fedml_core.distributed.communication.grpc.grpc_comm_manager import DynamicGRPCCommManager
from utils.timer_with_cuda import Timer
from utils.metrics import Metrics


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
        
        # Generate mock data (adjust dimensions for your model)
        x = torch.randn(self.batch_size, 3, 32, 32)  # CIFAR-10 like data
        y = torch.randint(0, 10, (self.batch_size,))  # 10 classes
        
        self.current_batch += 1
        return x, y
    
    def __len__(self):
        return self.num_batches


class RaftFederatedLearningNode:
    """RAFT node with actual federated learning capabilities."""
    
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
        
        # Initialize federated learning components
        self._setup_fl_components()
        
        # Create RAFT components
        self._setup_raft_components()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.logger.info(f"RAFT Federated Learning Node {self.node_id} initialized")
    
    def _setup_fl_components(self):
        """Setup federated learning components."""
        # Create a simple model (adjust based on your needs)
        if self.args.model == 'simple_cnn':
            import torch.nn as nn
            self.model = nn.Sequential(
                nn.Conv2d(3, 32, 3, padding=1),
                nn.ReLU(),
                nn.AdaptiveAvgPool2d((1, 1)),
                nn.Flatten(),
                nn.Linear(32, 10)
            )
        else:
            # You can add more sophisticated models here
            from torchvision.models import resnet18
            self.model = resnet18(num_classes=10)
        
        # Move model to appropriate device
        self.device = torch.device('cuda' if torch.cuda.is_available() and self.args.use_cuda else 'cpu')
        self.model.to(self.device)
        
        # Create model trainer
        self.model_trainer = MyModelTrainer(self.model, self.device, self.args)
        self.model_trainer.set_id(self.node_id)
        
        # Create mock data loaders
        self.train_data_local = MockDataLoader(self.node_id, self.args.batch_size, 50)
        self.test_data_local = MockDataLoader(self.node_id, self.args.batch_size, 10)
        
        # Mock data dictionaries
        self.train_data_local_dict = {self.node_id: self.train_data_local}
        self.test_data_local_dict = {self.node_id: self.test_data_local}
        self.train_data_local_num_dict = {self.node_id: 1000}  # Mock sample count
        
        # Create topology manager
        self.topology_manager = SAPSTopologyManager(self.args)
        
        # Create timer and metrics
        self.timer = Timer(verbosity_level=1)
        self.metrics = Metrics([1], task='classification')
        
        # Create federated learning worker
        self.worker = DecentralizedWorker(
            worker_index=self.node_id,
            topology_manager=self.topology_manager,
            train_data_global=None,  # Not used in decentralized setting
            test_data_global=None,   # Not used in decentralized setting
            train_data_num=1000,     # Mock total training data
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
    
    def _setup_raft_components(self):
        """Setup RAFT consensus components."""
        # Create gRPC communication manager
        self.comm_manager = DynamicGRPCCommManager(
            host=self.args.ip_address,
            port=self.args.port,
            node_id=self.node_id,
            client_num=self.args.total_nodes,
            topic="raft_fl",
            service_discovery_host=self.args.discovery_host,
            service_discovery_port=self.args.discovery_port,
            capabilities=["raft", "fedml", "gossip"],
            metadata={"model": self.args.model, "dataset": self.args.dataset},
            use_service_discovery=True
        )
        
        # Create RAFT node
        self.raft_node = RaftNode(self.node_id, self.args)
        
        # Create RAFT consensus
        self.raft_consensus = RaftConsensus(
            raft_node=self.raft_node,
            worker_manager=None,  # Will be set after RaftWorkerManager is created
            args=self.args,
            bandwidth_manager=None,  # Optional for now
            topology_manager=self.topology_manager
        )
        
        # Create RAFT worker manager (this is where the magic happens!)
        self.worker_manager = RaftWorkerManager(
            args=self.args,
            comm=self.comm_manager,
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
        
        # Complete the circular references
        self.raft_consensus.worker_manager = self.worker_manager
        
        # Register message handlers
        self.worker_manager.register_message_receive_handlers()
        
        self.logger.info("RAFT consensus components initialized")
    
    def start(self):
        """Start the federated learning node with RAFT coordination."""
        try:
            self.running = True
            self.logger.info(f"Starting RAFT Federated Learning Node {self.node_id}")
            
            # Start the worker manager - this handles everything!
            # The worker manager will:
            # 1. Start RAFT consensus
            # 2. Handle cluster joining
            # 3. Coordinate federated learning
            # 4. Manage topology through RAFT
            self.worker_manager.run()
            
        except Exception as e:
            self.logger.error(f"Error starting federated learning node: {e}")
            import traceback
            traceback.print_exc()
            raise
    
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
            if hasattr(self, 'worker_manager'):
                self.worker_manager.finish()
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
        
        self.logger.info(f"Node {self.node_id} shutdown complete")


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
    
    # Optimizer configuration
    parser.add_argument('--client-optimizer', default='sgd', help='Client optimizer')
    parser.add_argument('--wd', type=float, default=1e-4, help='Weight decay')
    parser.add_argument('--momentum', type=float, default=0.9, help='Momentum')
    parser.add_argument('--nesterov', action='store_true', help='Nesterov momentum')
    
    # Additional configuration
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])
    parser.add_argument('--warmup-epochs', type=int, default=0, help='Warmup epochs')
    parser.add_argument('--task', default='classification', help='Task type')
    
    args = parser.parse_args()
    
    # Auto-assign port if not specified
    if args.port is None:
        args.port = 9000 + args.node_id
    
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
    print("  - Nodes will form RAFT consensus cluster")
    print("  - Leader will coordinate federated learning rounds")
    print("  - Topology will be managed through RAFT consensus")
    print("  - Training will proceed with SAPS-FL gossip communication")
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