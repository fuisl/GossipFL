# RAFT Consensus Demo

This directory contains a demonstration of the RAFT consensus algorithm using MPI for communication.

## Prerequisites

- Python 3.7+
- MPI implementation (e.g., MPICH, Open MPI)
- Python packages: mpi4py, flask, requests

You can install the required packages using:

```bash
pip install mpi4py flask requests
```

## Components

The demo consists of several components:

1. **Service Discovery Server** (`service_discovery.py`): HTTP server that tracks node registration and leader status
2. **RAFT Node** (`raft_node.py`): MPI-based RAFT node that participates in the consensus protocol
3. **Web Monitor** (`web_monitor.py`): Web interface to monitor and interact with the RAFT cluster
4. **Launch Script** (`launch_cluster.py`): Script to launch the entire RAFT cluster

## Running the Demo

The easiest way to run the demo is using the launch script:

```bash
python demo/launch_cluster.py --nodes 3
```

This will start:

- A service discovery server on port 5000
- A web monitor on port 8080
- 3 RAFT nodes using MPI, each with a web UI on ports 7000, 7001, and 7002

You can customize the launch with these options:

- `--nodes N`: Number of RAFT nodes to launch (default: 3)
- `--discovery_port P`: Service discovery port (default: 5000)
- `--monitor_port P`: Web monitor port (default: 8080)
- `--min_election_timeout MS`: Minimum election timeout in milliseconds (default: 2000)
- `--max_election_timeout MS`: Maximum election timeout in milliseconds (default: 4000)
- `--heartbeat_interval MS`: Heartbeat interval in milliseconds (default: 750)

## Web Interfaces

- **Cluster Monitor**: `http://localhost:8080`
  - Shows the status of all nodes in the cluster
  - Allows triggering elections for individual nodes
  - Displays node logs

- **Individual Node UIs**: `http://localhost:700X` (where X is the node rank)
  - Shows detailed status for a specific node
  - Displays node logs
  - Allows sending commands to the node

## Node Commands

Each node supports these commands:

- `trigger_election`: Force the node to start an election (only works if not in INITIAL state)
- `become_follower`: Transition a node from INITIAL to FOLLOWER state
- `show_known_nodes`: Display the nodes known to this node
- `sync_known_nodes`: Force the node to sync its peer list with service discovery
- `check_timers`: Show election timeout and heartbeat information
- `force_timeout`: Force election timeout by setting last heartbeat time to 0
- `state_details`: Display detailed node state information

## RAFT Protocol Implementation

The demo uses the actual RAFT implementation from `algorithms/RAFT_GossipFL`, including:

- Leader election with randomized timeouts
- Heartbeat messages from leader to followers
- Log replication (although we don't submit actual commands in this demo)
- Term-based protocol with proper state transitions

The MPI Worker Manager handles sending and receiving RAFT protocol messages between nodes.

## Stopping the Demo

Press Ctrl+C in the terminal where you launched the cluster to stop all processes.
