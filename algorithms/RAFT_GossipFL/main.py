import logging
import numpy as np
import argparse
import os
import sys
import time
from mpi4py import MPI

from algorithms.SAPS_FL.compressor import SAPS_FLCompressor
from algorithms.SAPS_FL.decentralized_worker import DecentralizedWorker
from algorithms.RAFT_GossipFL.raft_node import RaftNode
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
from algorithms.RAFT_GossipFL.raft_topology_manager import RaftTopologyManager
from algorithms.RAFT_GossipFL.raft_bandwidth_manager import RaftBandwidthManager
from algorithms.RAFT_GossipFL.raft_worker_manager import RaftWorkerManager

from utils.timer import Timer
from utils.metrics import Metrics
from utils.timer_with_cuda import Timer

def add_args(parser):
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
    
    return parser

def init_processes(args, rank, size):
    """
    Initialize the RAFT-GossipFL processes.
    
    Args:
        args: Command line arguments
        rank: Rank of this process
        size: Total number of processes
    """
    # Set the random seed
    np.random.seed(args.client_index + 2023)
    
    # Initialize the RAFT node
    raft_node = RaftNode(rank, args)
    
    # Initialize topology and bandwidth managers
    if args.timing:
        timer = Timer()
    else:
        timer = Timer()
    
    # Create the metrics object
    metrics = Metrics([])
    
    # Initialize the worker
    worker = DecentralizedWorker(args, None, None, rank, size, timer, metrics)
    
    # Initialize the bandwidth manager
    bandwidth_manager = RaftBandwidthManager(args, None)  # Will set raft_consensus later
    
    # Initialize the topology manager
    topology_manager = RaftTopologyManager(args, None)  # Will set raft_consensus later
    
    # Initialize the worker manager
    worker_manager = RaftWorkerManager(
        args, MPI.COMM_WORLD, rank, size, worker, topology_manager, 
        None, timer, metrics, None, bandwidth_manager
    )
    
    # Initialize the RAFT consensus
    raft_consensus = RaftConsensus(raft_node, worker_manager, args)
    
    # Set the raft_consensus in the topology and bandwidth managers
    topology_manager.raft_consensus = raft_consensus
    bandwidth_manager.raft_consensus = raft_consensus
    
    # Set the raft_consensus in the worker manager
    worker_manager.raft_consensus = raft_consensus
    
    # Run the worker manager
    worker_manager.run()
    
    # Wait for process completion
    MPI.COMM_WORLD.Barrier()
    logging.info(f"Process {rank} finished")

def run_raft_gossipfl(args):
    """
    Run RAFT-GossipFL.
    
    Args:
        args: Command line arguments
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    # Set logging level
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize and run the process
    init_processes(args, rank, size)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    
    # Add RAFT-specific arguments
    parser = add_args(parser)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run RAFT-GossipFL
    run_raft_gossipfl(args)
