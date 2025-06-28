# Web-based Raft cluster monitor
# Requires: pip install flask requests

from flask import Flask, render_template_string, jsonify, request
import requests
import threading
import time

# Default service discovery URL, can be overridden via command line
SERVICE_DISCOVERY_URL = 'http://localhost:5000'
NODE_API_BASE_PORT = 7000

app = Flask(__name__)

TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Raft Cluster Monitor</title>
    <style>
        body { font-family: Arial; }
        .node { border: 1px solid #ccc; margin: 10px; padding: 10px; display: inline-block; vertical-align: top; }
        .leader { background: #e0ffe0; }
        .follower { background: #f0f0f0; }
        .candidate { background: #fffbe0; }
        .initial { background: #e0e0ff; }
        pre { background: #222; color: #eee; padding: 5px; }
    </style>
    <script>
        function fetchStatus() {
            fetch('/api/cluster').then(r => r.json()).then(data => {
                let html = '';
                for (const node of data.nodes) {
                    let cls = 'node ' + (node.state || '').toLowerCase();
                    html += `<div class='${cls}'>`;
                    html += `<b>Node ${node.node_id}</b><br/>`;
                    html += `State: ${node.state}<br/>`;
                    html += `Term: ${node.term}<br/>`;
                    html += `Leader: ${node.leader_id}<br/>`;
                    html += `Log length: ${node.log_length}<br/>`;
                    html += `<button onclick="fetchLog('${node.api_url}')">Show Log</button> `;
                    html += `<button onclick="sendCommand('${node.api_url}', 'trigger_election')">Trigger Election</button>`;
                    html += `<div id='log_${node.node_id}'></div>`;
                    html += '</div>';
                }
                document.getElementById('nodes').innerHTML = html;
            });
        }
        function fetchLog(api_url) {
            fetch(api_url + '/log').then(r => r.json()).then(data => {
                let log = data.log.join('\n');
                let node_id = api_url.split(':').pop();
                document.getElementById('log_' + node_id).innerHTML = `<pre>${log}</pre>`;
            });
        }
        function sendCommand(api_url, cmd) {
            fetch(api_url + '/command', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({cmd: cmd})
            }).then(r => r.json()).then(data => {
                alert('Command result: ' + data.result);
            });
        }
        setInterval(fetchStatus, 2000);
        window.onload = fetchStatus;
    </script>
</head>
<body>
    <h1>Raft Cluster Monitor</h1>
    <div id='nodes'></div>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(TEMPLATE)

@app.route('/api/cluster')
def api_cluster():
    # Get node list from service discovery
    try:
        resp = requests.get(f'{SERVICE_DISCOVERY_URL}/state')
        state = resp.json()
        nodes = []
        for node_id, addr in state.get('nodes', {}).items():
            api_port = NODE_API_BASE_PORT + int(node_id)
            api_url = f'http://localhost:{api_port}'
            try:
                status = requests.get(f'{api_url}/status', timeout=0.5).json()
            except Exception:
                status = {'node_id': node_id, 'state': 'offline', 'term': None, 'leader_id': None, 'log_length': 0}
            status['api_url'] = api_url
            nodes.append(status)
        return jsonify({'nodes': nodes, 'leader': state.get('leader')})
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Run the RAFT web monitor")
    parser.add_argument("--port", type=int, default=8080, help="Port to run the monitor on")
    parser.add_argument("--discovery_url", type=str, default=SERVICE_DISCOVERY_URL, 
                        help="URL of the service discovery server")
    args = parser.parse_args()
    
    # Update the service discovery URL
    if args.discovery_url != SERVICE_DISCOVERY_URL:
        SERVICE_DISCOVERY_URL = args.discovery_url
    
    app.run(host='0.0.0.0', port=args.port, debug=True)
