import logging
import numpy as np
from mpi4py import MPI

from utils.timer_with_cuda import Timer
from utils.metrics import Metrics
from utils.logger import Logger

from algorithms.SAPS_FL.decentralized_worker import DecentralizedWorker
from algorithms.SAPS_FL.MyModelTrainer import MyModelTrainer

from .raft_node import RaftNode
from .raft_consensus import RaftConsensus
from .raft_topology_manager import RaftTopologyManager
from .raft_bandwidth_manager import RaftBandwidthManager
from .raft_worker_manager import RaftWorkerManager

track_time = True


def FedML_init():
    """Initialize MPI communication."""
    comm = MPI.COMM_WORLD
    process_id = comm.Get_rank()
    worker_number = comm.Get_size()
    return comm, process_id, worker_number


def FedML_RAFT_GossipFL(process_id, worker_number, device, comm, model, train_data_num, train_data_global, test_data_global,
                        train_data_local_num_dict, train_data_local_dict, test_data_local_dict, args, model_trainer=None):
    """
    Run RAFT-enhanced GossipFL federated learning.
    
    This function integrates RAFT consensus with GossipFL to provide:
    - Dynamic leader election and fault tolerance
    - Consistent topology and bandwidth synchronization
    - Dynamic node membership management
    - Parameter consistency across all nodes
    
    Args:
        process_id (int): Rank of this process
        worker_number (int): Total number of processes
        device: Training device (CPU/GPU)
        comm: MPI communicator
        model: The neural network model
        train_data_num (int): Total number of training samples
        train_data_global: Global training dataset
        test_data_global: Global test dataset
        train_data_local_num_dict (dict): Local training sample counts per worker
        train_data_local_dict (dict): Local training datasets per worker
        test_data_local_dict (dict): Local test datasets per worker
        args: Configuration arguments
        model_trainer: Optional model trainer (will create if None)
    """
    # Initialize model trainer if not provided
    if model_trainer is None:
        model_trainer = MyModelTrainer(model, device, args)
    model_trainer.set_id(process_id)

    # Set random seed for reproducibility
    np.random.seed(args.client_index + 2023)

    # Configure timer
    timer = Timer(
        verbosity_level=1 if track_time else 0,
        log_fn=Logger.log_timer
    )
    
    # Configure metrics
    metrics = Metrics([1], task=args.task)
    
    # Initialize RAFT node
    raft_node = RaftNode(process_id, args)
    
    # Initialize RAFT-aware topology manager
    # This manager will synchronize topology via RAFT consensus
    topology_manager = RaftTopologyManager(args, None)  # raft_consensus set later
    
    # Initialize RAFT-aware bandwidth manager
    # This manager will synchronize bandwidth matrices via RAFT consensus
    bandwidth_manager = RaftBandwidthManager(args, None)  # raft_consensus set later
    
    # Initialize the decentralized worker
    # This uses the same worker logic as SAPS_FL but with RAFT-synchronized topology
    worker_index = process_id
    worker = DecentralizedWorker(worker_index, topology_manager, train_data_global, test_data_global, train_data_num,
                                train_data_local_dict, test_data_local_dict, train_data_local_num_dict, worker_number, 
                                device, model, args, model_trainer, timer, metrics)
    
    # Initialize the RAFT worker manager
    # This extends DecentralizedWorkerManager with RAFT consensus capabilities
    # Note: We need to create this after raft_consensus, so we'll do a two-step initialization
    worker_manager = RaftWorkerManager(
        args, comm, process_id, worker_number, worker, topology_manager, 
        model_trainer, timer, metrics, None, bandwidth_manager  # raft_consensus set below
    )
    
    # Initialize RAFT consensus coordinator
    # This handles leader election, log replication, and state synchronization
    raft_consensus = RaftConsensus(raft_node, worker_manager, args, bandwidth_manager, topology_manager)
    
    # Set cross-references for RAFT integration
    topology_manager.raft_consensus = raft_consensus
    bandwidth_manager.raft_consensus = raft_consensus
    worker_manager.raft_consensus = raft_consensus
    
    logging.info(f"RAFT-GossipFL initialized for node {process_id} with {worker_number} total nodes")
    logging.info(f"RAFT configuration: election_timeout={getattr(args, 'max_election_timeout', 300)}ms, "
                f"heartbeat_interval={getattr(args, 'heartbeat_interval', 50)}ms")
    
    # Start the RAFT-enabled federated learning process
    worker_manager.run()
    
    # Wait for all processes to complete
    comm.Barrier()
    logging.info(f"RAFT-GossipFL process {process_id} completed successfully")


def add_raft_args(parser):
    """
    Add RAFT-specific command line arguments.
    
    Args:
        parser: ArgumentParser object
    
    Returns:
        parser: Updated ArgumentParser object
    """
    # RAFT consensus parameters
    parser.add_argument('--min_election_timeout', type=int, default=150,
                       help='Minimum election timeout in milliseconds')
    parser.add_argument('--max_election_timeout', type=int, default=300,
                       help='Maximum election timeout in milliseconds')
    parser.add_argument('--heartbeat_interval', type=int, default=50,
                       help='Heartbeat interval in milliseconds')
    
    # Node joining parameters
    parser.add_argument('--join_existing_cluster', action='store_true',
                       help='Join an existing cluster instead of starting a new one')
    parser.add_argument('--known_leader_id', type=int, default=None,
                       help='ID of a known leader when joining existing cluster')
    
    # RAFT behavior parameters
    parser.add_argument('--raft_log_compaction_threshold', type=int, default=1000,
                       help='Number of log entries before triggering compaction')
    parser.add_argument('--raft_snapshot_interval', type=int, default=100,
                       help='Interval between state snapshots')
    
    return parser
