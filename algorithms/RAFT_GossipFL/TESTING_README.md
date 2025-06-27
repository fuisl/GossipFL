# RAFT Consensus Algorithm Testing Framework

This directory contains a comprehensive testing framework for the RAFT consensus algorithm implementation. The tests focus on **pure message passing** and core RAFT functionality without dependencies on GossipFL components.

## Files Overview

### Core RAFT Implementation

- `raft_node.py` - Main RAFT node implementation
- `raft_consensus.py` - RAFT consensus coordination logic  
- `raft_messages.py` - Message type definitions

### Testing Framework

- `run_raft_tests.py` - **Main test runner** (start here)
- `test_raft_simple.py` - Basic node functionality tests
- `demo_raft_messages.py` - Interactive message passing demonstration
- `test_raft_consensus.py` - Full consensus algorithm simulation

## Quick Start

Run all tests:

```bash
cd algorithms/RAFT_GossipFL
python run_raft_tests.py
```

Run specific test suites:

```bash
# Basic node operations
python run_raft_tests.py --test simple

# Message passing demo
python run_raft_tests.py --test messages

# Full consensus simulation
python run_raft_tests.py --test consensus
```

## Test Descriptions

### 1. Simple Tests (`test_raft_simple.py`)

Tests individual RAFT node operations:

- ✅ Node initialization and state management
- ✅ Election process (candidate → leader transition)
- ✅ Vote request/response handling
- ✅ Log entry addition and validation
- ✅ AppendEntries RPC processing
- ✅ Log consistency checking
- ✅ Commit and apply operations

### 2. Message Demo (`demo_raft_messages.py`)

Interactive demonstration showing:

- 📨 Vote request/response message flow
- 📨 AppendEntries and response messages
- 📨 Heartbeat communication
- 📨 Log conflict resolution process
- 📊 Complete message trace and statistics

### 3. Consensus Tests (`test_raft_consensus.py`)

Full consensus algorithm simulation with:

- 🗳️ **Leader Election** - Multiple candidates, majority voting
- 📋 **Log Replication** - Entry propagation to followers
- 💔 **Network Partitions** - Split-brain prevention
- 🔄 **Leader Failure** - Automatic re-election
- 🌐 **Network Simulation** - Controllable message delays and failures

## Key Testing Features

### Pure Message Passing

- No GossipFL dependencies
- Simulated network with controllable properties
- Message delays, loss, and ordering
- Network partition simulation

### RAFT Algorithm Verification

✅ Safety Properties:

- Election Safety (at most one leader per term)
- Leader Append-Only (leaders never overwrite log entries)
- Log Matching (if logs contain same entry, all preceding entries match)
- Leader Completeness (committed entries present in future leaders)
- State Machine Safety (nodes apply same sequence of commands)

✅ Liveness Properties:

- Leader Election (eventually elects leader in stable network)
- Progress (committed entries eventually applied)

### Network Scenarios Tested

- ✅ Normal operation (all nodes connected)
- ✅ Node failures and recovery
- ✅ Network partitions and healing
- ✅ Message delays and reordering
- ✅ Concurrent elections
- ✅ Log conflicts and resolution

## Example Test Output

```bash
RAFT Consensus Algorithm Testing Framework
======================================================================
Checking RAFT implementation files...
✓ Found raft_node.py
✓ Found raft_consensus.py  
✓ Found raft_messages.py
✓ All RAFT modules imported successfully

======================================================================
RUNNING SIMPLE RAFT NODE TESTS
======================================================================

==================================================
TEST: Node Initialization
==================================================
Node ID: 0
Initial State: INITIAL
Current Term: 0
Known Nodes: {0, 1, 2}
Total Nodes: 3
Majority: 2
✓ Node initialization test passed

==================================================
TEST: Election Start  
==================================================
Before election - State: FOLLOWER, Term: 0
After election - State: CANDIDATE, Term: 1
Voted for: 0
Votes received: {0}
Election start success: True
✓ Election start test passed

[... more test output ...]

==================================================
ALL TESTS PASSED! ✓
==================================================
```

## Understanding RAFT Through Testing

### 1. Leader Election Process

```python
# Node becomes candidate
node.start_election()  # Increments term, votes for self

# Sends vote requests to all other nodes
for target in other_nodes:
    send_vote_request(target, term, last_log_index, last_log_term)

# Processes vote responses
if votes_received >= majority:
    become_leader()
```

### 2. Log Replication

```python
# Leader adds entry
index = leader.add_log_entry(command)

# Replicates to followers  
for follower in followers:
    send_append_entries(follower, entries, prev_log_index, prev_log_term)

# Commits when replicated to majority
if replicated_count >= majority:
    commit_index = index
```

### 3. Message Flow Example

```bash
[MSG 001] 0 → 1: VOTE_REQUEST
    term: 1
    candidate_id: 0
    last_log_index: 0
    last_log_term: 0

[MSG 002] 1 → 0: VOTE_RESPONSE  
    term: 1
    vote_granted: True

[MSG 003] 0 → 1: APPEND_ENTRIES
    term: 1
    entries: [{'type': 'test', 'data': 'entry1'}]
    leader_commit: 0
```

## Troubleshooting

### Import Errors

If you see import errors, ensure you're running from the correct directory:

```bash
cd algorithms/RAFT_GossipFL
python run_raft_tests.py
```

### Test Failures

Common issues and solutions:

1. **Election timeouts** - Normal in concurrent scenarios
2. **Log inconsistencies** - Check append_entries logic
3. **Network partition issues** - Verify majority calculation
4. **Message ordering** - Review message delivery simulation

### Verbose Output

For detailed debugging:

```bash
python run_raft_tests.py --verbose
```

## Extending the Tests

### Adding New Test Cases

1. Create test function in appropriate file
2. Follow existing test patterns
3. Use assertions for verification
4. Add to test runner if needed

### Custom Network Scenarios

```python
# Create custom network conditions
network.create_partition([0, 1], [2, 3, 4])  # Split network
network.fail_node(leader_id)                  # Simulate node failure  
network.set_message_delay(0.1, 0.5)         # Add network latency
```

## Further Reading

- [RAFT Paper](https://raft.github.io/raft.pdf) - Original RAFT consensus algorithm
- [RAFT Visualization](http://thesecretlivesofdata.com/raft/) - Interactive RAFT demo
- Implementation based on RAFT specification sections 5-8

---

**Note**: This testing framework is designed for educational purposes and algorithm verification. For production use, additional considerations like persistence, snapshots, and configuration changes would be needed.
