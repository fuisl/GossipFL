#!/usr/bin/env python3
"""
RAFT Web Dashboard

A web-based visualization dashboard for RAFT consensus operations.
Provides real-time updates and interactive controls.
"""

import json
import time
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import webbrowser
import os
import sys

# Add parent directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from raft_visualizer import RaftVisualizer
from visual_raft_tests import VisualTestFramework
from raft_node import RaftState


class RaftWebHandler(SimpleHTTPRequestHandler):
    """HTTP handler for RAFT web dashboard."""
    
    def __init__(self, *args, visualizer=None, test_framework=None, **kwargs):
        self.visualizer = visualizer
        self.test_framework = test_framework
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests."""
        parsed_path = urlparse(self.path)
        
        if parsed_path.path == '/':
            self.serve_dashboard()
        elif parsed_path.path == '/api/state':
            self.serve_cluster_state()
        elif parsed_path.path == '/api/messages':
            self.serve_messages()
        elif parsed_path.path == '/api/timeline':
            self.serve_timeline()
        elif parsed_path.path == '/api/run_election':
            self.run_election()
        elif parsed_path.path == '/api/add_entry':
            self.add_log_entry()
        elif parsed_path.path == '/api/reset':
            self.reset_cluster()
        else:
            super().do_GET()
    
    def serve_dashboard(self):
        """Serve the main dashboard HTML."""
        html_content = """
<!DOCTYPE html>
<html>
<head>
    <title>RAFT Consensus Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f0f0f0; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { text-align: center; color: #333; margin-bottom: 30px; }
        .panel { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .node { 
            display: inline-block; margin: 10px; padding: 15px; border-radius: 8px; 
            min-width: 120px; text-align: center; color: white; font-weight: bold;
        }
        .node.INITIAL { background: #6c757d; }
        .node.FOLLOWER { background: #28a745; }
        .node.CANDIDATE { background: #ffc107; color: #000; }
        .node.LEADER { background: #dc3545; }
        .message { 
            padding: 8px 12px; margin: 5px 0; border-radius: 4px; 
            background: #e9ecef; border-left: 4px solid #007bff;
        }
        .timeline { max-height: 300px; overflow-y: auto; }
        .controls { text-align: center; margin: 20px 0; }
        .btn { 
            background: #007bff; color: white; border: none; padding: 10px 20px; 
            margin: 5px; border-radius: 4px; cursor: pointer; font-size: 14px;
        }
        .btn:hover { background: #0056b3; }
        .btn.success { background: #28a745; }
        .btn.warning { background: #ffc107; color: #000; }
        .btn.danger { background: #dc3545; }
        .network-diagram { text-align: center; margin: 20px 0; }
        .network-node { 
            display: inline-block; margin: 20px; padding: 20px; 
            border-radius: 50%; width: 60px; height: 60px; 
            text-align: center; line-height: 60px; color: white; font-weight: bold;
        }
        .stats { display: flex; justify-content: space-around; text-align: center; }
        .stat { flex: 1; }
        .stat-value { font-size: 24px; font-weight: bold; color: #007bff; }
        .auto-refresh { margin: 10px 0; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 RAFT Consensus Dashboard</h1>
            <p>Real-time visualization of distributed consensus</p>
        </div>
        
        <div class="panel">
            <h2>🎛️ Controls</h2>
            <div class="controls">
                <button class="btn" onclick="runElection()">Start Election</button>
                <button class="btn success" onclick="addLogEntry()">Add Log Entry</button>
                <button class="btn warning" onclick="simulateFailure()">Simulate Failure</button>
                <button class="btn danger" onclick="resetCluster()">Reset Cluster</button>
            </div>
            <div class="auto-refresh">
                <label>
                    <input type="checkbox" id="autoRefresh" checked> Auto-refresh (1s)
                </label>
            </div>
        </div>
        
        <div class="panel">
            <h2>🌐 Cluster Status</h2>
            <div class="stats">
                <div class="stat">
                    <div class="stat-value" id="nodeCount">-</div>
                    <div>Nodes</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="currentTerm">-</div>
                    <div>Current Term</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="leaderCount">-</div>
                    <div>Leaders</div>
                </div>
                <div class="stat">
                    <div class="stat-value" id="messageCount">-</div>
                    <div>Messages</div>
                </div>
            </div>
        </div>
        
        <div class="panel">
            <h2>🖥️ Node States</h2>
            <div id="nodes"></div>
        </div>
        
        <div class="panel">
            <h2>📊 Network Topology</h2>
            <div class="network-diagram" id="networkDiagram"></div>
        </div>
        
        <div class="panel">
            <h2>📨 Message Timeline</h2>
            <div class="timeline" id="messages"></div>
        </div>
        
        <div class="panel">
            <h2>📈 State Timeline</h2>
            <canvas id="timelineChart" width="800" height="200"></canvas>
        </div>
    </div>

    <script>
        let autoRefreshEnabled = true;
        
        async function fetchData(endpoint) {
            try {
                const response = await fetch(endpoint);
                return await response.json();
            } catch (error) {
                console.error('Error fetching data:', error);
                return null;
            }
        }
        
        async function updateDashboard() {
            const state = await fetchData('/api/state');
            const messages = await fetchData('/api/messages');
            
            if (state) {
                updateNodeStates(state);
                updateStats(state, messages);
                updateNetworkDiagram(state);
            }
            
            if (messages) {
                updateMessages(messages);
            }
        }
        
        function updateNodeStates(state) {
            const nodesDiv = document.getElementById('nodes');
            nodesDiv.innerHTML = '';
            
            for (const [nodeId, nodeState] of Object.entries(state.nodes)) {
                const nodeDiv = document.createElement('div');
                nodeDiv.className = `node ${nodeState.state}`;
                nodeDiv.innerHTML = `
                    <div>Node ${nodeId}</div>
                    <div>${nodeState.state}</div>
                    <div>Term: ${nodeState.term}</div>
                    <div>Log: ${nodeState.log_length}</div>
                    <div>Commit: ${nodeState.commit_index}</div>
                `;
                nodesDiv.appendChild(nodeDiv);
            }
        }
        
        function updateStats(state, messages) {
            document.getElementById('nodeCount').textContent = Object.keys(state.nodes).length;
            
            const terms = Object.values(state.nodes).map(n => n.term);
            document.getElementById('currentTerm').textContent = Math.max(...terms, 0);
            
            const leaders = Object.values(state.nodes).filter(n => n.state === 'LEADER').length;
            document.getElementById('leaderCount').textContent = leaders;
            
            document.getElementById('messageCount').textContent = messages ? messages.length : 0;
        }
        
        function updateNetworkDiagram(state) {
            const diagramDiv = document.getElementById('networkDiagram');
            diagramDiv.innerHTML = '';
            
            for (const [nodeId, nodeState] of Object.entries(state.nodes)) {
                const nodeDiv = document.createElement('div');
                nodeDiv.className = `network-node ${nodeState.state}`;
                nodeDiv.textContent = nodeId;
                nodeDiv.title = `Node ${nodeId}: ${nodeState.state} (Term ${nodeState.term})`;
                diagramDiv.appendChild(nodeDiv);
            }
        }
        
        function updateMessages(messages) {
            const messagesDiv = document.getElementById('messages');
            messagesDiv.innerHTML = '';
            
            const recentMessages = messages.slice(-10).reverse();
            
            for (const msg of recentMessages) {
                const msgDiv = document.createElement('div');
                msgDiv.className = 'message';
                
                const time = new Date(msg.timestamp * 1000).toLocaleTimeString();
                const success = msg.success !== null ? (msg.success ? '✅' : '❌') : '';
                
                msgDiv.innerHTML = `
                    <strong>${time}</strong> | 
                    ${msg.sender} → ${msg.receiver} | 
                    ${msg.msg_type} ${success}
                `;
                messagesDiv.appendChild(msgDiv);
            }
        }
        
        async function runElection() {
            await fetch('/api/run_election');
            updateDashboard();
        }
        
        async function addLogEntry() {
            await fetch('/api/add_entry');
            updateDashboard();
        }
        
        async function resetCluster() {
            await fetch('/api/reset');
            updateDashboard();
        }
        
        function simulateFailure() {
            alert('Failure simulation not yet implemented in demo');
        }
        
        // Auto-refresh functionality
        document.getElementById('autoRefresh').addEventListener('change', function(e) {
            autoRefreshEnabled = e.target.checked;
        });
        
        function autoRefresh() {
            if (autoRefreshEnabled) {
                updateDashboard();
            }
            setTimeout(autoRefresh, 1000);
        }
        
        // Initialize
        updateDashboard();
        autoRefresh();
    </script>
</body>
</html>
        """
        
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        self.wfile.write(html_content.encode())
    
    def serve_cluster_state(self):
        """Serve current cluster state as JSON."""
        if not self.visualizer or not self.test_framework:
            state = {'nodes': {}}
        else:
            state = {
                'nodes': {
                    str(node_id): {
                        'state': snapshot.state,
                        'term': snapshot.term,
                        'voted_for': snapshot.voted_for,
                        'log_length': snapshot.log_length,
                        'commit_index': snapshot.commit_index
                    }
                    for node_id, snapshot in self.visualizer.current_states.items()
                }
            }
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(state).encode())
    
    def serve_messages(self):
        """Serve message timeline as JSON."""
        if not self.visualizer:
            messages = []
        else:
            messages = [
                {
                    'timestamp': msg.timestamp,
                    'sender': msg.sender,
                    'receiver': msg.receiver,
                    'msg_type': msg.msg_type,
                    'content': msg.content,
                    'success': msg.success
                }
                for msg in self.visualizer.messages
            ]
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(messages).encode())
    
    def serve_timeline(self):
        """Serve complete timeline data."""
        if not self.visualizer:
            timeline = {'node_states': {}, 'messages': []}
        else:
            timeline = {
                'node_states': {
                    str(node_id): [
                        {
                            'timestamp': s.timestamp,
                            'state': s.state,
                            'term': s.term,
                            'voted_for': s.voted_for,
                            'log_length': s.log_length,
                            'commit_index': s.commit_index
                        }
                        for s in states
                    ]
                    for node_id, states in self.visualizer.node_states.items()
                },
                'messages': [
                    {
                        'timestamp': m.timestamp,
                        'sender': m.sender,
                        'receiver': m.receiver,
                        'msg_type': m.msg_type,
                        'success': m.success
                    }
                    for m in self.visualizer.messages
                ]
            }
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(timeline).encode())
    
    def run_election(self):
        """Trigger an election in the test framework."""
        if self.test_framework:
            # Find a follower to start election
            for node in self.test_framework.nodes.values():
                if node.state == RaftState.FOLLOWER:
                    node.start_election()
                    
                    # Quick vote collection for demo
                    last_log_index, last_log_term = node.get_last_log_info()
                    for target_id, target in self.test_framework.nodes.items():
                        if target_id != node.node_id:
                            current_term, vote_granted = target.receive_vote_request(
                                node.node_id, node.current_term, last_log_index, last_log_term
                            )
                            if vote_granted:
                                node.receive_vote_response(target_id, current_term, vote_granted)
                                if node.state == RaftState.LEADER:
                                    break
                    break
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'election_triggered'}).encode())
    
    def add_log_entry(self):
        """Add a log entry through the leader."""
        if self.test_framework:
            # Find the leader
            leader = None
            for node in self.test_framework.nodes.values():
                if node.state == RaftState.LEADER:
                    leader = node
                    break
            
            if leader:
                entry = {
                    'type': 'demo',
                    'data': f'entry_{int(time.time())}',
                    'timestamp': time.time()
                }
                leader.add_log_entry(entry)
                
                # Replicate to followers
                for follower_id, follower in self.test_framework.nodes.items():
                    if follower_id != leader.node_id:
                        prev_log_index = len(follower.log)
                        prev_log_term = 0
                        if prev_log_index > 0:
                            prev_log_term = follower.log[prev_log_index - 1]['term']
                        
                        entries_to_send = leader.log[len(follower.log):]
                        follower.append_entries(
                            leader.node_id, leader.current_term, prev_log_index, 
                            prev_log_term, entries_to_send, leader.commit_index
                        )
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'entry_added'}).encode())
    
    def reset_cluster(self):
        """Reset the cluster to initial state."""
        if self.test_framework:
            # Reset all nodes to followers
            for node in self.test_framework.nodes.values():
                node.state = RaftState.FOLLOWER
                node.current_term = 0
                node.voted_for = None
                node.log.clear()
                node.commit_index = 0
                node.last_applied = 0
                node.votes_received.clear()
                node._log_state_change()
        
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'cluster_reset'}).encode())


def create_web_handler(visualizer, test_framework):
    """Create a web handler with the given visualizer and test framework."""
    def handler(*args, **kwargs):
        return RaftWebHandler(*args, visualizer=visualizer, test_framework=test_framework, **kwargs)
    return handler


def start_web_dashboard(port=8080, num_nodes=5):
    """Start the web dashboard server."""
    print(f"🌐 Starting RAFT Web Dashboard on port {port}")
    
    # Create test framework
    test_framework = VisualTestFramework(num_nodes)
    visualizer = test_framework.visualizer
    
    # Create HTTP server
    handler = create_web_handler(visualizer, test_framework)
    httpd = HTTPServer(('localhost', port), handler)
    
    print(f"🚀 Dashboard URL: http://localhost:{port}")
    print("📊 The dashboard will show real-time RAFT consensus visualization")
    print("🎛️ Use the web interface to trigger elections and add log entries")
    
    # Try to open browser
    try:
        webbrowser.open(f'http://localhost:{port}')
    except:
        pass
    
    print("\nPress Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Stopping web dashboard...")
        httpd.shutdown()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="RAFT Web Dashboard")
    parser.add_argument('--port', type=int, default=8080, help='Port to run the web server on')
    parser.add_argument('--nodes', type=int, default=5, help='Number of nodes in the cluster')
    
    args = parser.parse_args()
    
    start_web_dashboard(args.port, args.nodes)
