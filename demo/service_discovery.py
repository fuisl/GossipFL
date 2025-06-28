# Lightweight service discovery for Raft demo
# Tracks node addresses and current leader

import threading
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

REGISTRY_FILE = './node_registry.json'

class NodeRegistry:
    def __init__(self, registry_file=REGISTRY_FILE):
        self.registry_file = registry_file
        self.lock = threading.Lock()
        self._init_registry()

    def _init_registry(self):
        try:
            with open(self.registry_file, 'r') as f:
                self.data = json.load(f)
        except Exception:
            self.data = {'nodes': {}, 'leader': None}
            self._save()

    def _save(self):
        os.makedirs(os.path.dirname(self.registry_file), exist_ok=True)
        with open(self.registry_file, 'w') as f:
            json.dump(self.data, f, indent=2)

    def register_node(self, node_id, address):
        with self.lock:
            self.data['nodes'][node_id] = address
            self._save()

    def set_leader(self, node_id):
        with self.lock:
            self.data['leader'] = node_id
            self._save()

    def reset(self):
        with self.lock:
            self.data = {'nodes': {}, 'leader': None}
            self._save()

    def get_state(self):
        with self.lock:
            return self.data.copy()

registry = NodeRegistry()

class ServiceDiscoveryHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/state':
            state = registry.get_state()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(state).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        content_length = int(self.headers['Content-Length']) if 'Content-Length' in self.headers else 0
        post_data = self.rfile.read(content_length) if content_length > 0 else b''
        data = json.loads(post_data) if post_data else {}
        if self.path == '/register':
            node_id = data['node_id']
            address = data['address']
            registry.register_node(node_id, address)
            self.send_response(200)
            self.end_headers()
        elif self.path == '/set_leader':
            node_id = data['node_id']
            registry.set_leader(node_id)
            self.send_response(200)
            self.end_headers()
        elif self.path == '/reset':
            registry.reset()
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

def run_service_discovery_server(port=5000):
    # Clear registry on startup
    registry.reset()
    server = HTTPServer(('0.0.0.0', port), ServiceDiscoveryHandler)
    print(f"Service discovery running on port {port}")
    server.serve_forever()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Run the service discovery server")
    parser.add_argument("--port", type=int, default=5000, help="Port to run the server on")
    args = parser.parse_args()
    run_service_discovery_server(args.port)
