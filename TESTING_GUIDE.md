# RAFT Consensus Manual Testing Guide

This directory contains standalone scripts for manually testing the RAFT consensus implementation with service discovery in the GossipFL project.

## 🎯 Overview

The manual testing setup includes:
- **Service Discovery Server**: Bootstrap coordination and node registry
- **Standalone RAFT Nodes**: Mock federated learning nodes with full RAFT implementation
- **Test Runner**: Automated scenario testing
- **Real-time Monitoring**: Web dashboard for cluster state observation

## 📁 Files

- `standalone_service_discovery.py` - Service discovery server with monitoring
- `standalone_raft_node.py` - Individual RAFT node with mocked FL components
- `test_runner.py` - Automated test scenarios
- `TESTING_GUIDE.md` - This guide

## 🚀 Quick Start

### 1. Start Service Discovery (Terminal 1)
```bash
python standalone_service_discovery.py --port 8080 --monitor-port 8081
```

### 2. Start Bootstrap Node (Terminal 2)
```bash
python standalone_raft_node.py --node-id 0 --bootstrap
```

### 3. Start Additional Nodes (Terminal 3, 4, ...)
```bash
python standalone_raft_node.py --node-id 1
python standalone_raft_node.py --node-id 2
```

### 4. Monitor Cluster State
Open browser: `http://localhost:8081`

## 🧪 Test Scenarios

### Scenario 1: Single Node Bootstrap
Tests bootstrap detection and immediate leader transition.

**Manual Steps:**
1. Start service discovery
2. Start single node with `--bootstrap` flag
3. Verify node becomes LEADER immediately

**Expected Behavior:**
- Node detects it's the first node
- Transitions directly to LEADER state
- Service discovery shows bootstrap initiated

**Command:**
```bash
python test_runner.py scenario1
```

### Scenario 2: Three Node Cluster Formation
Tests multi-node cluster formation and leader election.

**Manual Steps:**
1. Start service discovery
2. Start 3 nodes sequentially
3. Verify leader election occurs
4. Verify all nodes sync state

**Expected Behavior:**
- First node becomes leader
- Subsequent nodes join as followers
- All nodes maintain consistent state

**Command:**
```bash
python test_runner.py scenario2
```

### Scenario 3: Dynamic Node Joining
Tests joining nodes to an existing cluster.

**Manual Steps:**
1. Start service discovery
2. Start initial cluster (2 nodes)
3. Wait for cluster to stabilize
4. Add new nodes dynamically
5. Verify state synchronization

**Expected Behavior:**
- New nodes discover existing cluster
- Join as followers
- Receive complete state sync
- Communication channels established

**Command:**
```bash
python test_runner.py scenario3
```

### Scenario 4: Leader Election
Tests leader failure and re-election.

**Manual Steps:**
1. Start service discovery
2. Start 3-node cluster
3. Kill the leader node
4. Verify new leader election
5. Restart failed node
6. Verify it rejoins as follower

**Expected Behavior:**
- New leader elected after timeout
- Cluster continues operating
- Failed node rejoins as follower
- State remains consistent

**Command:**
```bash
python test_runner.py scenario4
```

## 📊 Monitoring and Verification

### Service Discovery Dashboard
- **URL**: `http://localhost:8081`
- **Features**:
  - Real-time node status
  - Bootstrap status
  - Registry version tracking
  - Node join/leave events
  - Auto-refresh every 5 seconds

### Node Status Output
Each node prints status every 5 seconds:
```
============================================================
Node 0 Status - 14:30:25
============================================================
RAFT State:       LEADER
Current Term:     3
Commit Index:     5
Known Nodes:      [0, 1, 2]
Connected Nodes:  [1, 2]
Training Round:   0
Is Training:      False
Is Leader:        True
Running:          True
============================================================
```

### API Endpoints
- `GET /api/stats` - Service discovery statistics
- `GET /api/nodes` - List of registered nodes
- `GET /api/bootstrap` - Bootstrap information

## 🔧 Configuration Options

### Service Discovery Server
```bash
python standalone_service_discovery.py \
  --port 8080 \              # gRPC service port
  --monitor-port 8081 \      # HTTP monitoring port
  --node-timeout 300 \       # Node timeout in seconds
  --cleanup-interval 60 \    # Cleanup interval in seconds
  --log-level INFO \         # Log level
  --bind-address 0.0.0.0     # Bind address
```

### RAFT Node
```bash
python standalone_raft_node.py \
  --node-id 0 \                    # Unique node ID
  --discovery-host localhost \     # Service discovery host
  --discovery-port 8080 \          # Service discovery port
  --ip-address localhost \         # Node IP address
  --port 9000 \                    # Node port (auto-assigned if not specified)
  --bootstrap \                    # Bootstrap mode (first node)
  --min-election-timeout 150 \     # Min election timeout (ms)
  --max-election-timeout 300 \     # Max election timeout (ms)
  --heartbeat-interval 50 \        # Heartbeat interval (ms)
  --log-level INFO \               # Log level
  --comm-round 100                 # Communication rounds
```

## 🐛 Troubleshooting

### Common Issues

1. **Service Discovery Not Starting**
   - Check port availability: `netstat -tlnp | grep 8080`
   - Verify no other process is using the port
   - Check firewall settings

2. **Nodes Not Joining Cluster**
   - Verify service discovery is running
   - Check node connectivity to service discovery
   - Verify different node IDs are used

3. **Leader Election Not Working**
   - Check election timeout settings
   - Verify majority of nodes are running
   - Check network connectivity between nodes

4. **State Synchronization Issues**
   - Verify all nodes are connected
   - Check log outputs for errors
   - Verify RAFT message handling

### Debug Commands
```bash
# Check service discovery health
curl http://localhost:8081/api/stats

# Check registered nodes
curl http://localhost:8081/api/nodes

# Check bootstrap status
curl http://localhost:8081/api/bootstrap

# Monitor node logs
tail -f node_0.log
```

## 🧩 Component Architecture

### Service Discovery Flow
```
New Node → Service Discovery → Bootstrap Detection → Node Registration → Cluster Info
```

### RAFT Consensus Flow
```
Node Start → Service Discovery → Cluster Join → State Sync → Training Ready
```

### Integration Points
- **Service Discovery ↔ RAFT**: Bridge pattern for discovery events
- **RAFT ↔ Communication**: Dynamic connection management
- **RAFT ↔ Training**: Coordinated training rounds
- **Leadership ↔ Coordination**: Leader-based training coordination

## 📈 Success Criteria

### Single Node Bootstrap
- ✅ Node detects bootstrap condition
- ✅ Node becomes LEADER immediately
- ✅ Service discovery shows bootstrap initiated
- ✅ Node ready for training coordination

### Multi-Node Cluster
- ✅ Leader election completes successfully
- ✅ All nodes have consistent state
- ✅ Communication channels established
- ✅ Service discovery tracks all nodes

### Dynamic Joining
- ✅ New nodes discover existing cluster
- ✅ State synchronization completes
- ✅ New nodes join as followers
- ✅ Communication manager refreshes

### Leader Election
- ✅ New leader elected after failure
- ✅ Cluster continues operating
- ✅ Failed node rejoins as follower
- ✅ State consistency maintained

## 🚨 Known Limitations

1. **Mock Components**: Training, model management, and data handling are mocked
2. **Network Simulation**: No actual network partition testing
3. **Persistence**: State is not persisted across restarts
4. **Scale Testing**: Limited to small clusters (< 10 nodes)

## 🔮 Future Enhancements

1. **Real FL Integration**: Connect with actual federated learning
2. **Network Simulation**: Add network partition and failure modes
3. **Performance Testing**: Add load testing and benchmarks
4. **Persistence**: Add state persistence and recovery
5. **Monitoring**: Enhanced metrics and alerting
6. **Automation**: Automated test suites and CI/CD integration

---

## 🎯 Quick Test Commands

```bash
# Run all scenarios
python test_runner.py scenario1
python test_runner.py scenario2
python test_runner.py scenario3
python test_runner.py scenario4

# Manual testing
python test_runner.py discovery  # Start service discovery only
```

This manual testing setup provides comprehensive verification of the RAFT consensus implementation with service discovery, enabling thorough testing of all major scenarios before integration with the full federated learning system.
