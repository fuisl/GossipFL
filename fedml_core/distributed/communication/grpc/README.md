# Dynamic gRPC Communication Manager for GossipFL

## Overview

This directory contains the **Dynamic gRPC Communication Manager** - a sophisticated communication layer that enables dynamic node membership, service discovery, and robust messaging for the GossipFL federated learning framework.

## Key Features

✅ **Dynamic Node Membership**: Nodes can join and leave the cluster dynamically  
✅ **Gateway-Based Service Discovery**: Centralized service discovery with gRPC gateway  
✅ **Robust Error Handling**: Graceful handling of network failures and node disconnections  
✅ **Registry Refresh**: Automatic refresh of cluster membership information  
✅ **Thread-Safe Communication**: Concurrent message handling with proper synchronization  
✅ **Comprehensive Testing**: Multiple test scripts for various scenarios  

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Discovery     │    │     Node 1      │    │     Node 2      │
│   Gateway       │◄──►│  (Bootstrap)    │◄──►│   (Regular)     │
│   (Port 8090)   │    │  (Port 8901)    │    │  (Port 8902)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Core Components

### 1. Dynamic gRPC Communication Manager

- **File**: `dynamic_grpc_comm_manager.py`
- **Purpose**: Main communication manager with dynamic membership support
- **Key Features**:
  - Dynamic node registration/deregistration
  - Gateway-based service discovery
  - Registry refresh mechanism
  - Robust error handling and fallback

### 2. Communication Manager Factory

- **File**: `comm_manager_factory.py`
- **Purpose**: Factory pattern for creating communication managers
- **Benefits**: Centralized instantiation and configuration

### 3. Service Discovery Gateway

- **Files**: `grpc_gateway_server.py`, `grpc_gateway_client.py`
- **Purpose**: Centralized service discovery and node registry
- **Location**: `algorithms/RAFT_GossipFL/`

## Testing Framework

### Quick Verification

```bash
# Run quick test to verify system works
python quick_test.py
```

### Manual Testing

```bash
# Terminal 1: Start discovery server
python discovery_server.py

# Terminal 2: Start first node
python dynamic_node.py --node-id 1

# Terminal 3: Start second node  
python dynamic_node.py --node-id 2
```

### Automated Testing

```bash
# Run comprehensive automated tests
python step_by_step_test.py

# Run unit tests
python test_dynamic_comm_manager.py

# Run simple integration test
python simple_test.py
```

### Test Coordination

```bash
# Run coordinated multi-node test
python test_coordinator.py
```

## Usage Examples

### Basic Usage

```python
from fedml_core.distributed.communication.grpc.comm_manager_factory import CommManagerFactory

# Create communication manager
comm_manager = CommManagerFactory.create_comm_manager(
    args=args,
    host='127.0.0.1',
    port=8901,
    node_id=1,
    client_num=2,
    gateway_host='localhost',
    gateway_port=8090
)

# Send message
message = Message(msg_type=1, sender=1, receiver=2)
message.set_content("Hello from node 1")
comm_manager.send_message(message)
```

### Advanced Configuration

```python
# Create with custom configuration
comm_manager = DynamicGRPCCommManager(
    host='127.0.0.1',
    port=8901,
    node_id=1,
    client_num=2,
    gateway_host='localhost',
    gateway_port=8090,
    max_connection_num=100,
    backend="gRPC",
    is_mobile=0,
    grpc_max_send_message_length=1024*1024,
    grpc_max_receive_message_length=1024*1024
)

# Register message handlers
comm_manager.register_message_receive_handler(msg_type=1, handler=my_handler)
```

## File Structure

```text
fedml_core/distributed/communication/grpc/
├── README.md                           # This file
├── __init__.py                         # Package initialization
├── dynamic_grpc_comm_manager.py        # Main communication manager
├── comm_manager_factory.py             # Factory for creating managers
├── test_dynamic_comm_manager.py        # Unit tests
├── simple_test.py                      # Simple integration test
├── discovery_server.py                 # Discovery server runner
├── dynamic_node.py                     # Node runner script
├── test_coordinator.py                 # Multi-node test coordinator
├── manual_test_guide.py                # Manual testing guide
├── quick_test.py                       # Quick verification test
└── step_by_step_test.py                # Comprehensive automated test
```

## Message Types

The system supports various message types:

- **Type 1**: Regular communication messages
- **Type 2**: System/control messages
- **Type 3**: Heartbeat/ping messages
- **Type 999**: Connection ready notifications

## Error Handling

The system includes comprehensive error handling:

- **Gateway Discovery Failures**: Fallback to bootstrap mode
- **Connection Failures**: Automatic retry and recovery
- **Node Disconnections**: Graceful cleanup and notification
- **Message Delivery Failures**: Retry mechanisms and error logging

## Configuration

### Environment Variables

```bash
# Optional: Set custom gateway address
export GOSSIPFL_GATEWAY_HOST=localhost
export GOSSIPFL_GATEWAY_PORT=8090

# Optional: Set logging level
export GOSSIPFL_LOG_LEVEL=INFO
```

### Command Line Arguments

```bash
# Node configuration
python dynamic_node.py \
    --node-id 1 \
    --host 127.0.0.1 \
    --port 8901 \
    --gateway-host localhost \
    --gateway-port 8090 \
    --client-num 2
```

## Troubleshooting

### Common Issues

1. **Gateway Connection Failed**
   - Ensure discovery server is running
   - Check firewall settings
   - Verify host/port configuration

2. **Node Registration Failed**
   - Check node ID uniqueness
   - Verify network connectivity
   - Check gateway logs

3. **Message Delivery Failed**
   - Verify receiver node is online
   - Check message format
   - Review error logs

### Debug Mode

```bash
# Enable debug logging
export GOSSIPFL_LOG_LEVEL=DEBUG
python dynamic_node.py --node-id 1
```

## Performance Considerations

- **Connection Pooling**: Reuses connections for efficiency
- **Message Batching**: Supports batched message delivery
- **Thread Pool**: Configurable thread pool for message handling
- **Memory Management**: Proper cleanup of resources

## Future Enhancements

- **Load Balancing**: Distribute messages across multiple gateways
- **Security**: Add authentication and encryption
- **Monitoring**: Enhanced metrics and monitoring
- **Fault Tolerance**: Advanced failure recovery mechanisms

## Contributing

When contributing to this communication layer:

1. Follow the existing code style
2. Add comprehensive tests
3. Update documentation
4. Ensure backward compatibility
5. Test with multiple nodes

## License

This is part of the GossipFL project. See the main project LICENSE file for details.

```bash
# Terminal 2
python dynamic_node.py --node-id 1
```

### 3. Start Second Node

```bash
# Terminal 3
python dynamic_node.py --node-id 2
```

### 4. Test Communication

In any node terminal:

```
Node-1> list                    # List all nodes
Node-1> send 2 Hello Node 2!    # Send message to Node 2
Node-1> broadcast Hello everyone! # Broadcast to all nodes
Node-1> status                  # Show node status
Node-1> help                    # Show all commands
```

## Advanced Usage

### Auto-generate Node IDs and Ports

```bash
python dynamic_node.py  # Will auto-generate node ID and port
```

### Custom Configuration

```bash
python dynamic_node.py --node-id 5 --port 8905 --gateway-host localhost --gateway-port 8090
```

### Background Mode (non-interactive)

```bash
python dynamic_node.py --node-id 10 --background
```

## Automated Test Scenarios

### Scenario 1: Basic 2-Node Communication

```bash
python test_coordinator.py scenario1
```

### Scenario 2: Multi-Node Join/Leave

```bash
python test_coordinator.py scenario2
```

### Scenario 3: Message Broadcasting

```bash
python test_coordinator.py scenario3
```

### Network Monitoring

```bash
python test_coordinator.py monitor
```

## Node Commands

Once a node is running in interactive mode, you can use these commands:

- `list` or `l` - List all nodes in the network
- `send <node_id> <message>` - Send a message to a specific node
- `broadcast <message>` - Send a message to all nodes
- `leader` - Show current cluster leader
- `status` or `s` - Show node status
- `refresh` - Refresh node registry
- `help` or `h` - Show help
- `quit` or `q` - Exit the node

## Testing Dynamic Membership

1. **Start discovery server** in Terminal 1
2. **Start Node 1** in Terminal 2: `python dynamic_node.py --node-id 1`
3. **Start Node 2** in Terminal 3: `python dynamic_node.py --node-id 2`
4. **Verify nodes see each other**: Type `list` in any node terminal
5. **Send messages**: Type `send 2 Hello!` in Node 1 terminal
6. **Add more nodes**: Start Node 3 in Terminal 4: `python dynamic_node.py --node-id 3`
7. **Verify network growth**: Type `list` in any node terminal
8. **Test broadcasting**: Type `broadcast Hello everyone!`
9. **Remove nodes**: Press Ctrl+C in any node terminal
10. **Verify network shrinks**: Type `list` in remaining nodes

## Testing Network Resilience

- **Gateway failure**: Stop the discovery server and verify existing nodes can still communicate
- **Node failure**: Kill nodes and verify others detect the failure
- **Network partition**: Test how the system handles partial connectivity

## Features Demonstrated

- ✅ **Dynamic node discovery**: Nodes automatically find each other
- ✅ **Runtime membership changes**: Add/remove nodes at runtime
- ✅ **Message passing**: Send targeted and broadcast messages
- ✅ **Service discovery integration**: Uses gRPC gateway for coordination
- ✅ **Bootstrap coordination**: First node becomes bootstrap/leader
- ✅ **Registry refresh**: Periodic updates of node membership
- ✅ **Graceful shutdown**: Clean resource cleanup on exit
- ✅ **Network monitoring**: Real-time network status monitoring

## Troubleshooting

1. **"Import failed"**: Make sure you're running from the correct directory
2. **"Gateway connection failed"**: Ensure discovery server is running first
3. **"Port already in use"**: Use different port numbers or wait for cleanup
4. **"Node not seeing others"**: Wait a few seconds for registry refresh

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Discovery     │     │     Node 1      │     │     Node 2      │
│    Server       │◄────┤  (Bootstrap)    │◄────┤   (Follower)    │
│  (Gateway)      │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                        │                        │
        │                        │                        │
        └────────────────────────┼────────────────────────┘
                                 │
                        ┌─────────────────┐
                        │     Node 3      │
                        │   (Follower)    │
                        │                 │
                        └─────────────────┘
```

The system demonstrates a complete dynamic membership management solution suitable for distributed systems like federated learning, where nodes can join and leave at runtime while maintaining communication capabilities.
