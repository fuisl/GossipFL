# RAFT with Service Discovery Testing Guide

This guide shows you how to test the complete RAFT consensus system with service discovery integration.

## Prerequisites

Make sure you have all dependencies installed and the project is set up correctly.

## Quick Start

### 1. Start Service Discovery Server

In one terminal:
```bash
cd /workspaces/GossipFL/algorithms/RAFT_GossipFL
python test_raft_cluster.py --mode server
```

### 2. Start RAFT Nodes

In separate terminals, start multiple RAFT nodes:

Terminal 2:
```bash
cd /workspaces/GossipFL/algorithms/RAFT_GossipFL
python raft_node_with_bridge.py --node-id 1
```

Terminal 3:
```bash
cd /workspaces/GossipFL/algorithms/RAFT_GossipFL
python raft_node_with_bridge.py --node-id 2
```

Terminal 4:
```bash
cd /workspaces/GossipFL/algorithms/RAFT_GossipFL
python raft_node_with_bridge.py --node-id 3
```

### 3. Test Cluster Operations

Once nodes are running, you can:

- Type `status` in any node terminal to see current status
- Type `election` to trigger a RAFT election
- Watch the logs to see consensus operations

## Automated Testing

### Run Complete Test Suite

```bash
cd /workspaces/GossipFL/algorithms/RAFT_GossipFL
python test_raft_cluster.py --mode test --num-nodes 3
```

### Start Cluster with Multiple Nodes

```bash
cd /workspaces/GossipFL/algorithms/RAFT_GossipFL
python test_raft_cluster.py --mode cluster --num-nodes 5
```

## What to Look For

### Service Discovery Integration

1. **Node Registration**: Watch for nodes registering with service discovery
2. **Discovery Events**: See bridge processing node discovery/loss events
3. **Dynamic Membership**: RAFT cluster adjusts as nodes join/leave

### RAFT Consensus

1. **Leader Election**: One node becomes leader, others become followers
2. **Heartbeats**: Leader sends regular heartbeats to followers
3. **Log Replication**: Log entries replicated across cluster
4. **Term Management**: Terms increase with each election

### Bridge Operations

1. **Event Processing**: Bridge converts service discovery events to RAFT operations
2. **Membership Changes**: New nodes added through RAFT consensus
3. **State Synchronization**: New nodes receive current cluster state

## Example Output

When everything is working, you should see:

```
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO - RAFT node 1 started successfully!
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO - === RAFT Node Status ===
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO -   node_id: 1
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO -   running: True
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO -   raft_state: FOLLOWER
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO -   raft_term: 0
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO -   known_nodes: [1, 2, 3]
2025-07-05 10:30:15 - RaftNodeWithBridge-1 - INFO -   is_leader: False
```

## Troubleshooting

### Service Discovery Issues
- Make sure discovery server is running on correct port (8090)
- Check firewall settings
- Verify nodes can reach discovery server

### RAFT Issues
- Check node IDs are unique
- Verify nodes are discovering each other
- Look for election timeout messages

### Bridge Issues
- Ensure bridge is registered with both comm manager and consensus
- Check for capability validation errors
- Verify RAFT leadership for membership changes

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RAFT Node 1   │    │   RAFT Node 2   │    │   RAFT Node 3   │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ RAFT        │ │    │ │ RAFT        │ │    │ │ RAFT        │ │
│ │ Consensus   │ │    │ │ Consensus   │ │    │ │ Consensus   │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Service     │ │    │ │ Service     │ │    │ │ Service     │ │
│ │ Discovery   │ │    │ │ Discovery   │ │    │ │ Discovery   │ │
│ │ Bridge      │ │    │ │ Bridge      │ │    │ │ Bridge      │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ gRPC Comm   │ │    │ │ gRPC Comm   │ │    │ │ gRPC Comm   │ │
│ │ Manager     │ │    │ │ Manager     │ │    │ │ Manager     │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Service         │
                    │ Discovery       │
                    │ Server          │
                    │ (Port 8090)     │
                    └─────────────────┘
```

## Key Features Demonstrated

1. **Dynamic Node Discovery**: Nodes automatically discover each other through service discovery
2. **Consensus-Based Membership**: All membership changes go through RAFT consensus
3. **Bridge Integration**: Service discovery events converted to RAFT operations
4. **State Synchronization**: New nodes receive complete cluster state
5. **Fault Tolerance**: System handles node failures and network partitions
6. **Leader Election**: Automatic leader election when needed
7. **Log Replication**: Consistent log replication across all nodes

This integration provides a robust foundation for dynamic federated learning with automatic node management!
