# Gateway Server Implementation

This directory contains the gateway server implementation for dynamic node discovery in the RAFT+GossipFL system.

## Overview

The gateway server provides a lightweight service discovery mechanism that allows nodes to dynamically join the cluster without requiring static configuration. It follows the principle of being a non-critical discovery service that doesn't interfere with cluster operations once nodes are connected.

## Components

### 1. gRPC Gateway Server (`grpc_gateway_server.py`)

The main gRPC-based gateway server implementation that provides:

- **Node Registration**: Allows nodes to register themselves with the cluster
- **Node Discovery**: Provides lists of active nodes to new joiners
- **Bootstrap Coordination**: Handles the first node scenario and leader assignment
- **Health Monitoring**: Tracks node health through heartbeats
- **gRPC API**: High-performance gRPC service for all gateway operations

#### Key Features

- High-performance gRPC-based communication
- Thread-safe node registry with RLock protection
- Automatic cleanup of inactive nodes
- Bootstrap detection and coordination
- Leader tracking and updates
- Integration with existing gRPC infrastructure

### 2. gRPC Gateway Client (`grpc_gateway_client.py`)

Client library for nodes to interact with the gateway server:

- **Node Registration**: Register node with gateway
- **Cluster Discovery**: Discover existing cluster nodes
- **Heartbeat Management**: Automatic heartbeat to maintain registration
- **Leader Updates**: Update leader information in gateway
- **Mixin Support**: `GRPCGatewayDiscoveryMixin` for easy integration

#### Key Features

- Automatic heartbeat management
- Connection retry logic with exponential backoff
- Simple API for common operations
- Thread-safe implementation
- Event callbacks for registration and connection events

### 3. Test Suite (`test_grpc_gateway.py`)

Comprehensive test suite that validates:

- gRPC Gateway server functionality
- gRPC Gateway client operations
- Multi-node scenarios
- Bootstrap detection
- Heartbeat functionality
- Discovery mixin integration

## API Reference

### gRPC Gateway Service

The gateway service is defined in `grpc_comm_manager.proto` and provides the following RPC methods:

#### Health Check

```protobuf
rpc HealthCheck (HealthCheckRequest) returns (HealthCheckResponse);
```

Returns gateway health status, timestamp, and version information.

#### Node Registration

```protobuf
rpc RegisterNode (RegisterNodeRequest) returns (RegisterNodeResponse);
```

Registers a node with the gateway and returns cluster information.

#### Get Nodes

```protobuf
rpc GetNodes (GetNodesRequest) returns (GetNodesResponse);
```

Returns list of active nodes (excluding the requesting node if specified).

#### Get Leader

```protobuf
rpc GetLeader (GetLeaderRequest) returns (GetLeaderResponse);
```

Returns current leader information.

#### Update Leader

```protobuf
rpc UpdateLeader (UpdateLeaderRequest) returns (UpdateLeaderResponse);
```

Updates leader information in the gateway.

#### Heartbeat

```protobuf
rpc Heartbeat (HeartbeatRequest) returns (HeartbeatResponse);
```

Sends heartbeat to maintain node registration.

#### Get Statistics

```protobuf
rpc GetStats (GetStatsRequest) returns (GetStatsResponse);
```

Returns gateway statistics including node counts, leader info, and uptime.

## Usage Examples

### Starting the gRPC Gateway Server

```python
from grpc_gateway_server import GRPCGatewayServer

# Start gRPC gateway server
gateway = GRPCGatewayServer(host='0.0.0.0', port=8090)
gateway.start()

# Gateway runs in background threads
# Stop when done
gateway.stop()
```

### Using the gRPC Gateway Client

```python
from grpc_gateway_client import GRPCGatewayClient

# Create client
client = GRPCGatewayClient('localhost', 8090)

# Discover cluster and register
is_bootstrap, cluster_info = client.discover_cluster(
    node_id=1,
    ip_address='127.0.0.1',
    port=5001,
    capabilities=['raft', 'gossip']
)

if is_bootstrap:
    print("This is the first node - bootstrap mode")
else:
    print(f"Found {len(cluster_info['nodes'])} existing nodes")

# Client automatically manages heartbeats
# Shutdown when done
client.shutdown()
```

### Integration with Existing Classes

```python
from grpc_gateway_client import GRPCGatewayDiscoveryMixin

class MyNode(GRPCGatewayDiscoveryMixin):
    def __init__(self, node_id, ip, port):
        super().__init__()
        
        # Initialize gateway discovery
        self.initialize_gateway_discovery(
            gateway_host='localhost',
            gateway_port=8090,
            node_id=node_id,
            ip_address=ip,
            port=port,
            capabilities=['raft', 'gossip']
        )
        
        # Check if bootstrap
        if self.is_bootstrap_node():
            print("Starting as bootstrap node")
        else:
            nodes = self.get_cluster_nodes()
            print(f"Joining cluster with {len(nodes)} nodes")

# Usage
node = MyNode(1, '127.0.0.1', 5001)
```

## Running Tests

```bash
# Run all tests
python test_grpc_gateway.py

# Run specific test scenarios
python -c "from test_grpc_gateway import test_grpc_gateway_server; test_grpc_gateway_server()"
```

## Command Line Usage

### Start gRPC Gateway Server

```bash
python grpc_gateway_server.py --host 0.0.0.0 --port 8090 --log-level INFO
```

### Test gRPC Gateway Client

```bash
python grpc_gateway_client.py --gateway-host localhost --gateway-port 8090 --node-id 1 --ip 127.0.0.1 --port 5001
```

## Configuration

### gRPC Gateway Server Configuration

The gRPC gateway server supports the following configuration options:

- `host`: Bind address (default: 'localhost')
- `port`: Port number (default: 8090)
- `node_timeout`: Node timeout in seconds (default: 300)
- `heartbeat_interval`: Heartbeat check interval (default: 30)
- `max_workers`: Maximum gRPC worker threads (default: 10)

### gRPC Gateway Client Configuration

The gRPC gateway client supports:

- `gateway_host`: Gateway server hostname/IP
- `gateway_port`: Gateway server port
- `timeout`: Request timeout in seconds (default: 10.0)
- `max_retry_attempts`: Maximum retry attempts (default: 3)
- `heartbeat_interval`: Heartbeat interval in seconds (default: 30.0)

## Integration with RAFT+GossipFL

The gRPC gateway implementation is designed to integrate seamlessly with the existing RAFT consensus and GossipFL training components:

1. **Bootstrap Detection**: The gateway identifies the first node and marks it for bootstrap mode
2. **Leader Discovery**: New nodes can discover the current RAFT leader through gRPC
3. **Topology Management**: Provides initial peer lists for gossip network formation
4. **Fault Tolerance**: Gateway failure doesn't affect cluster operation
5. **gRPC Integration**: Uses the same gRPC infrastructure as the main communication layer

## Error Handling

The gRPC implementation includes comprehensive error handling:

- **Connection Failures**: Retry logic with exponential backoff
- **gRPC Errors**: Proper gRPC status code handling
- **Node Failures**: Automatic cleanup of inactive nodes
- **Gateway Failures**: Graceful degradation for client operations
- **Timeout Handling**: Configurable timeouts for all operations

## Dependencies

- Python 3.6+
- gRPC libraries (`grpcio`, `grpcio-tools`)
- Protobuf (`protobuf`)
- Threading for concurrent operations
- Integration with existing `fedml_core` gRPC infrastructure

## Performance Considerations

- **High Throughput**: gRPC provides better performance than HTTP REST
- **Connection Pooling**: Reuses gRPC connections for efficiency
- **Concurrent Operations**: Thread-safe implementation supports concurrent requests
- **Memory Efficiency**: Protobuf serialization is more efficient than JSON
- **Stream Support**: Ready for future streaming operations if needed

## Future Enhancements

Potential improvements for the gRPC gateway implementation:

1. **SSL/TLS Support**: Add secure gRPC communication
2. **Authentication**: Add node authentication and authorization
3. **Load Balancing**: Support multiple gateway instances with load balancing
4. **Persistence**: Add optional state persistence for gateway restarts
5. **Monitoring**: Enhanced metrics and monitoring capabilities
6. **Configuration Management**: Dynamic configuration updates
7. **Streaming**: Use gRPC streaming for real-time updates
8. **Service Discovery**: Integration with service mesh technologies

## Architecture

The gRPC gateway follows a clean architecture:

```text
┌─────────────────────────────────────────────────────────────┐
│                    gRPC Gateway Server                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │  Node Registry  │  │  Leader Track   │  │  Bootstrap  │  │
│  │   (Thread-Safe) │  │   (Thread-Safe) │  │   Manager   │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              gRPC Service Layer                        │  │
│  │  RegisterNode | GetNodes | GetLeader | Heartbeat      │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                                │
                                │ gRPC Communication
                                │
┌─────────────────────────────────────────────────────────────┐
│                    gRPC Gateway Client                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐  │
│  │  Connection     │  │  Retry Logic    │  │  Heartbeat  │  │
│  │  Management     │  │  (Exponential)  │  │  Manager    │  │
│  └─────────────────┘  └─────────────────┘  └─────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │              Discovery Mixin                          │  │
│  │  Bootstrap Detection | Cluster Info | Leader Updates  │  │
│  └─────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Migration from HTTP Gateway

If you're migrating from the HTTP-based gateway:

1. **Update imports**: Change from `gateway_client` to `grpc_gateway_client`
2. **Class names**: Use `GRPCGatewayClient` instead of `GatewayClient`
3. **Mixin**: Use `GRPCGatewayDiscoveryMixin` instead of `GatewayDiscoveryMixin`
4. **Dependencies**: Ensure gRPC libraries are installed
5. **Configuration**: Update port numbers (default 8090 instead of 8080)
6. **Testing**: Use `test_grpc_gateway.py` for validation

The API remains largely the same, making migration straightforward.
