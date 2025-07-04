# Gateway Server Implementation

This directory contains the gateway server implementation for dynamic node discovery in the RAFT+GossipFL system.

## Overview

The gateway server provides a lightweight service discovery mechanism that allows nodes to dynamically join the cluster without requiring static configuration. It follows the principle of being a non-critical discovery service that doesn't interfere with cluster operations once nodes are connected.

## Components

### 1. Gateway Server (`gateway_server.py`)

The main gateway server implementation that provides:

- **Node Registration**: Allows nodes to register themselves with the cluster
- **Node Discovery**: Provides lists of active nodes to new joiners
- **Bootstrap Coordination**: Handles the first node scenario and leader assignment
- **Health Monitoring**: Tracks node health through heartbeats
- **HTTP API**: RESTful API for all gateway operations

#### Key Features

- Lightweight HTTP-based communication
- Thread-safe node registry
- Automatic cleanup of inactive nodes
- Bootstrap detection and coordination
- Leader tracking and updates

### 2. Gateway Client (`gateway_client.py`)

Client library for nodes to interact with the gateway server:

- **Node Registration**: Register node with gateway
- **Cluster Discovery**: Discover existing cluster nodes
- **Heartbeat Management**: Automatic heartbeat to maintain registration
- **Leader Updates**: Update leader information in gateway
- **Mixin Support**: `GatewayDiscoveryMixin` for easy integration

#### Key Features

- Automatic heartbeat management
- Connection retry logic
- Simple API for common operations
- Thread-safe implementation

### 3. Test Suite (`test_gateway.py`)

Comprehensive test suite that validates:

- Gateway server functionality
- Gateway client operations
- Multi-node scenarios
- Bootstrap detection
- Heartbeat functionality

## API Reference

### Gateway Server HTTP API

#### Health Check

```
GET /health
```

Returns gateway health status.

#### Node Registration

```
POST /register
{
    "node_id": 1,
    "ip_address": "127.0.0.1",
    "port": 5001,
    "capabilities": ["raft", "gossip"],
    "metadata": {"role": "worker"}
}
```

#### Get Nodes

```
GET /nodes?node_id=1
```

Returns list of active nodes (excluding the requesting node if node_id provided).

#### Get Leader

```
GET /leader
```

Returns current leader information.

#### Update Leader

```
POST /leader
{
    "leader_id": 2
}
```

#### Heartbeat

```
POST /heartbeat
{
    "node_id": 1
}
```

#### Get Statistics

```
GET /stats
```

Returns gateway statistics.

## Usage Examples

### Starting the Gateway Server

```python
from gateway_server import GatewayServer

# Start gateway server
gateway = GatewayServer(host='0.0.0.0', port=8080)
gateway.start()

# Gateway runs in background threads
# Stop when done
gateway.stop()
```

### Using the Gateway Client

```python
from gateway_client import GatewayClient

# Create client
client = GatewayClient('localhost', 8080)

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
from gateway_client import GatewayDiscoveryMixin

class MyNode(GatewayDiscoveryMixin):
    def __init__(self, node_id, ip, port):
        super().__init__()
        
        # Initialize gateway discovery
        self.initialize_gateway_discovery(
            gateway_host='localhost',
            gateway_port=8080,
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
python test_gateway.py

# Run specific test scenarios
python -c "from test_gateway import test_gateway_server; test_gateway_server()"
```

## Command Line Usage

### Start Gateway Server

```bash
python gateway_server.py --host 0.0.0.0 --port 8080 --log-level INFO
```

### Test Gateway Client

```bash
python gateway_client.py --gateway-host localhost --gateway-port 8080 --node-id 1 --ip 127.0.0.1 --port 5001
```

## Configuration

### Gateway Server Configuration

The gateway server supports the following configuration options:

- `host`: Bind address (default: '0.0.0.0')
- `port`: Port number (default: 8080)
- `node_timeout`: Node timeout in seconds (default: 300)
- `heartbeat_interval`: Heartbeat check interval (default: 30)

### Gateway Client Configuration

The gateway client supports:

- `gateway_host`: Gateway server hostname/IP
- `gateway_port`: Gateway server port
- `timeout`: Request timeout in seconds (default: 10)
- `heartbeat_interval`: Heartbeat interval in seconds (default: 30)

## Integration with RAFT+GossipFL

The gateway implementation is designed to integrate seamlessly with the existing RAFT consensus and GossipFL training components:

1. **Bootstrap Detection**: The gateway identifies the first node and marks it for bootstrap mode
2. **Leader Discovery**: New nodes can discover the current RAFT leader
3. **Topology Management**: Provides initial peer lists for gossip network formation
4. **Fault Tolerance**: Gateway failure doesn't affect cluster operation

## Error Handling

The implementation includes comprehensive error handling:

- **Connection Failures**: Retry logic with exponential backoff
- **Invalid Requests**: Proper HTTP error responses
- **Node Failures**: Automatic cleanup of inactive nodes
- **Gateway Failures**: Graceful degradation for client operations

## Dependencies

- Python 3.6+
- Standard library only (no external dependencies for core functionality)
- `requests` library for test suite
- `threading` for concurrent operations
- `http.server` for HTTP server implementation

## Future Enhancements

Potential improvements for the gateway implementation:

1. **SSL/TLS Support**: Add HTTPS support for secure communication
2. **Authentication**: Add node authentication and authorization
3. **Load Balancing**: Support multiple gateway instances
4. **Persistence**: Add optional state persistence
5. **Monitoring**: Enhanced metrics and monitoring capabilities
6. **Configuration Management**: Dynamic configuration updates
