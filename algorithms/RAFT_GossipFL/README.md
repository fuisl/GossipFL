# RAFT-GossipFL

This implementation integrates the RAFT consensus algorithm with the GossipFL framework to provide a fault-tolerant, dynamic, and scalable federated learning system.

## Overview

RAFT-GossipFL addresses several limitations of the original SAPS_FL implementation:

1. **Fault Tolerance**: Replaces the static coordinator with a dynamically elected leader through RAFT consensus.
2. **Dynamic Topology**: Provides consensus on network topology changes.
3. **Dynamic Bandwidth**: Ensures consistent bandwidth information across all nodes.
4. **Membership Management**: Supports dynamic node join/leave operations.
5. **Scalability**: Optimizes communication through sparse/delta updates and batching.

## Architecture

RAFT-GossipFL consists of the following components:

1. **RaftNode**: Implements the core RAFT state machine with leader election, log replication, and state transitions.
2. **RaftConsensus**: Coordinates RAFT operations and provides high-level APIs for the worker manager.
3. **RaftTopologyManager**: Extends SAPSTopologyManager to use RAFT for topology consensus.
4. **RaftBandwidthManager**: Manages bandwidth information using RAFT for consensus.
5. **RaftWorkerManager**: Extends DecentralizedWorkerManager to integrate with RAFT.

## Communication Flow

### RAFT Communication

1. **Leader Election**: Nodes elect a leader through RAFT's voting mechanism.
2. **Log Replication**: Leaders replicate log entries (topology updates, bandwidth changes) to followers.
3. **Heartbeats**: Leaders send periodic heartbeats to maintain leadership.

### GossipFL Communication

1. **Training Coordination**: The RAFT leader (coordinator) initiates rounds and notifies workers.
2. **Model Exchange**: Workers exchange models based on the topology.
3. **Result Reporting**: Workers report metrics to the leader at the end of rounds.

## Integration Points

The integration between RAFT and GossipFL occurs at these points:

1. **Leadership -> Coordinator**: RAFT leadership transitions trigger coordinator role changes.
2. **Log Entries -> State Updates**: Committed log entries update topology and bandwidth.
3. **Message Flow**: RAFT messages and GossipFL messages are handled by the same worker manager.

## Usage

To run RAFT-GossipFL:

```bash
mpirun -np <num_nodes> python -m algorithms.RAFT_GossipFL.main [args]
```

## Configuration

RAFT-specific parameters:

- `--min_election_timeout`: Minimum election timeout in milliseconds (default: 150)
- `--max_election_timeout`: Maximum election timeout in milliseconds (default: 300)
- `--heartbeat_interval`: Heartbeat interval in milliseconds (default: 50)

## Implementation Details

### Thread Management

1. **Election Thread**: Monitors timeouts and initiates elections.
2. **Heartbeat Thread**: Sends periodic heartbeats (leader only).
3. **Replication Thread**: Replicates log entries to followers (leader only).
4. **Training Thread**: Handles local training and model exchange.
5. **Coordinator Thread**: Manages round coordination (leader only).
6. **Supervisor Thread**: Monitors worker threads and restarts them if they fail.

### Log Entry Types

1. **Topology**: Network topology updates with round number.
2. **Bandwidth**: Bandwidth matrix updates with timestamp.
3. **Membership**: Node join/leave operations.
4. **Coordinator**: Coordinator selection (if different from RAFT leader).
5. **No-op**: Used to commit previous entries without making state changes.

## Scalability Considerations

- **Sparse Updates**: Only changed elements of matrices are included in log entries.
- **Delta Encoding**: Changes are represented as differences from previous state.
- **Batched Updates**: Multiple changes can be combined into a single log entry.
- **Hierarchical Clustering**: For very large clusters, RAFT nodes can be organized hierarchically.
