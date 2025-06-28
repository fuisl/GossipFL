import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Raft node launcher for demo
# Uses RAFT_GossipFL components and MPI for communication
import requests
import socket
import uuid
import argparse
import threading
import time
import random
import json
import enum

from mpi4py import MPI
from algorithms.RAFT_GossipFL.raft_node import RaftNode as RealRaftNode
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus
from flask import Flask, jsonify, request
import logging

# Set up basic logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'raft_node_%d.log' % MPI.COMM_WORLD.Get_rank())
    ]
)

SERVICE_DISCOVERY_URL = "http://localhost:5000"

# Define MPI message tags for RAFT protocol
class MPITag(enum.IntEnum):
    VOTE_REQUEST = 1
    VOTE_RESPONSE = 2
    APPEND_ENTRIES = 3
    APPEND_RESPONSE = 4


# Helper to get local IP address
def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = "127.0.0.1"
    finally:
        s.close()
    return IP


def register_with_service_discovery(node_id, address):
    url = f"{SERVICE_DISCOVERY_URL}/register"
    data = {"node_id": node_id, "address": address}
    try:
        requests.post(url, json=data)
        print(f"Registered node {node_id} at {address}")
    except Exception as e:
        print(f"Failed to register node: {e}")


def deregister_from_service_discovery(node_id):
    url = f"{SERVICE_DISCOVERY_URL}/register"
    data = {"node_id": node_id, "address": None}
    try:
        requests.post(url, json=data)
        print(f"Deregistered node {node_id}")
    except Exception as e:
        print(f"Failed to deregister node: {e}")


def get_node_list():
    url = f"{SERVICE_DISCOVERY_URL}/state"
    try:
        resp = requests.get(url)
        state = resp.json()
        return state.get("nodes", {})
    except Exception as e:
        logging.error(f"Failed to fetch node list: {e}")
        return {}


def report_leader_to_service_discovery(node_id):
    url = f"{SERVICE_DISCOVERY_URL}/set_leader"
    data = {"node_id": node_id}
    try:
        requests.post(url, json=data)
        logging.info(f"Reported leader: {node_id}")
    except Exception as e:
        logging.error(f"Failed to report leader: {e}")


# Capture log output for web UI
class LogCapture(logging.Handler):
    def __init__(self, buffer, max_lines=100):
        super().__init__()
        self.buffer = buffer
        self.max_lines = max_lines
        
    def emit(self, record):
        log_entry = self.format(record)
        self.buffer.append(log_entry)
        # Keep buffer size limited
        while len(self.buffer) > self.max_lines:
            self.buffer.pop(0)


class MpiWorkerManager:
    """
    Worker manager implementation for RAFT that uses MPI for communication.
    
    This class implements the required methods for the RAFT consensus protocol to
    communicate between nodes using MPI. It handles sending and receiving vote
    requests/responses and append entries requests/responses.
    """
    
    def __init__(self, node_id, comm):
        """
        Initialize the MPI worker manager.
        
        Args:
            node_id (str): The ID of this node
            comm: The MPI communicator
        """
        self.node_id = node_id
        self.comm = comm
        self.rank = comm.Get_rank()
        self.size = comm.Get_size()
        self.raft_consensus = None  # Will be set after creation
        
        # Start receiving thread
        self.stop_event = threading.Event()
        self.receive_thread = threading.Thread(
            target=self._receive_thread_func,
            name=f"mpi-receive-{node_id}",
            daemon=True
        )
        self.receive_thread.start()
        
        logging.info(f"MpiWorkerManager initialized for node {node_id} (rank {self.rank})")
    
    def set_raft_consensus(self, raft_consensus):
        """Set the reference to the RAFT consensus manager."""
        self.raft_consensus = raft_consensus
    
    def stop(self):
        """Stop the worker manager."""
        self.stop_event.set()
        if self.receive_thread.is_alive():
            self.receive_thread.join(timeout=1.0)
    
    def _get_rank_from_node_id(self, node_id):
        """Convert node ID to MPI rank."""
        try:
            return int(node_id)
        except ValueError:
            logging.error(f"Cannot convert node_id '{node_id}' to rank")
            return -1
    
    def _receive_thread_func(self):
        """Thread to receive MPI messages."""
        try:
            while not self.stop_event.is_set():
                # Check for messages using a non-blocking probe
                if self.comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
                    status = MPI.Status()
                    data = self.comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    sender = status.Get_source()
                    tag = status.Get_tag()
                    
                    # Process message based on tag
                    self._handle_message(sender, tag, data)
                else:
                    # Sleep briefly if no messages
                    time.sleep(0.01)
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error in MPI receive thread: {str(e)}")
    
    def _handle_message(self, sender, tag, data):
        """
        Handle received MPI messages.
        
        Args:
            sender (int): MPI rank of sender
            tag (int): Message tag
            data (dict): Message data
        """
        try:
            sender_id = str(sender)  # Convert rank to node_id
            
            if tag == MPITag.VOTE_REQUEST:
                if self.raft_consensus:
                    self.raft_consensus.handle_vote_request(
                        sender_id,
                        data['term'],
                        data['last_log_index'],
                        data['last_log_term']
                    )
                    logging.debug(f"Node {self.node_id}: Processed vote request from {sender_id}")
            
            elif tag == MPITag.VOTE_RESPONSE:
                if self.raft_consensus:
                    self.raft_consensus.handle_vote_response(
                        sender_id,
                        data['term'],
                        data['vote_granted']
                    )
                    logging.debug(f"Node {self.node_id}: Processed vote response from {sender_id}")
            
            elif tag == MPITag.APPEND_ENTRIES:
                if self.raft_consensus:
                    # For append entries (heartbeats), update the node's last heartbeat time directly 
                    # for debugging purposes - this helps verify heartbeats are being received
                    if not data['entries']:  # Empty entries = heartbeat
                        self.raft_consensus.raft_node.update_heartbeat()
                        logging.debug(f"Node {self.node_id}: Received heartbeat from {sender_id}, term={data['term']}")
                    
                    self.raft_consensus.handle_append_entries(
                        sender_id,
                        data['term'],
                        data['prev_log_index'],
                        data['prev_log_term'],
                        data['entries'],
                        data['leader_commit']
                    )
            
            elif tag == MPITag.APPEND_RESPONSE:
                if self.raft_consensus:
                    self.raft_consensus.handle_append_response(
                        sender_id,
                        data['term'],
                        data['success'],
                        data['match_index']
                    )
                    logging.debug(f"Node {self.node_id}: Processed append response from {sender_id}")
            
            else:
                logging.warning(f"Node {self.node_id}: Received unknown message tag {tag} from {sender_id}")
                
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error handling message (tag={tag}) from {sender}: {str(e)}")
    
    def send_vote_request(self, node_id, term, last_log_index, last_log_term):
        """
        Send a vote request to another node.
        
        Args:
            node_id (str): ID of the target node
            term (int): Current term
            last_log_index (int): Index of last log entry
            last_log_term (int): Term of last log entry
        """
        try:
            rank = self._get_rank_from_node_id(node_id)
            if rank < 0:
                return
            
            data = {
                'term': term,
                'last_log_index': last_log_index,
                'last_log_term': last_log_term
            }
            
            self.comm.send(data, dest=rank, tag=MPITag.VOTE_REQUEST)
            logging.debug(f"Node {self.node_id}: Sent vote request to {node_id}")
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error sending vote request to {node_id}: {str(e)}")
    
    def send_vote_response(self, node_id, term, vote_granted):
        """
        Send a vote response to another node.
        
        Args:
            node_id (str): ID of the target node
            term (int): Current term
            vote_granted (bool): Whether the vote was granted
        """
        try:
            rank = self._get_rank_from_node_id(node_id)
            if rank < 0:
                return
            
            data = {
                'term': term,
                'vote_granted': vote_granted
            }
            
            self.comm.send(data, dest=rank, tag=MPITag.VOTE_RESPONSE)
            logging.debug(f"Node {self.node_id}: Sent vote response to {node_id}: granted={vote_granted}")
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error sending vote response to {node_id}: {str(e)}")
    
    def send_append_entries(self, node_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        """
        Send an append entries request to another node.
        
        Args:
            node_id (str): ID of the target node
            term (int): Current term
            prev_log_index (int): Index of log entry immediately preceding new ones
            prev_log_term (int): Term of prev_log_index entry
            entries (list): Log entries to append
            leader_commit (int): Leader's commit index
        """
        try:
            rank = self._get_rank_from_node_id(node_id)
            if rank < 0:
                return
            
            data = {
                'term': term,
                'prev_log_index': prev_log_index,
                'prev_log_term': prev_log_term,
                'entries': entries,
                'leader_commit': leader_commit
            }
            
            self.comm.send(data, dest=rank, tag=MPITag.APPEND_ENTRIES)
            log_msg = "heartbeat" if not entries else f"{len(entries)} entries"
            logging.debug(f"Node {self.node_id}: Sent append entries ({log_msg}) to {node_id}")
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error sending append entries to {node_id}: {str(e)}")
    
    def send_append_response(self, node_id, term, success, match_index):
        """
        Send an append entries response to another node.
        
        Args:
            node_id (str): ID of the target node
            term (int): Current term
            success (bool): Whether the append was successful
            match_index (int): Index of highest log entry known to be replicated
        """
        try:
            rank = self._get_rank_from_node_id(node_id)
            if rank < 0:
                return
            
            data = {
                'term': term,
                'success': success,
                'match_index': match_index
            }
            
            self.comm.send(data, dest=rank, tag=MPITag.APPEND_RESPONSE)
            logging.debug(f"Node {self.node_id}: Sent append response to {node_id}: success={success}")
        except Exception as e:
            logging.error(f"Node {self.node_id}: Error sending append response to {node_id}: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description="Run a Raft node for the demo (MPI-based)."
    )
    parser.add_argument(
        "--min_election_timeout",
        type=int,
        default=2000,
        help="Minimum election timeout in ms",
    )
    parser.add_argument(
        "--max_election_timeout",
        type=int,
        default=4000,
        help="Maximum election timeout in ms",
    )
    parser.add_argument(
        "--heartbeat_interval", type=int, default=750, help="Heartbeat interval in ms"
    )
    parser.add_argument(
        "--http_port",
        type=int,
        default=None,
        help="Port for node HTTP API (default: 7000+rank)",
    )
    parser.add_argument(
        "--client_num_in_total",
        type=int,
        required=True,
        help="Total number of clients/nodes in the cluster",
    )
    parser.add_argument(
        "--discovery_poll_interval",
        type=int,
        default=5,
        help="Interval (seconds) for polling service discovery",
    )
    args = parser.parse_args()

    # Initialize MPI
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    node_id = str(rank)  # Use rank as node_id for MPI-based demo
    
    # Set up logging with a buffer for the web UI
    global log_buffer
    log_buffer = []
    handler = LogCapture(log_buffer)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logging.getLogger().addHandler(handler)
    
    # Initialize command history
    global cmd_history
    cmd_history = []
    
    # Register with service discovery
    ip = get_local_ip()
    port = args.http_port if args.http_port else 7000 + rank
    address = f"{ip}:{port}"
    register_with_service_discovery(node_id, address)
    logging.info(f"Node {node_id} (rank {rank}) running at {address}")

    # Create the worker manager for MPI communication
    worker_manager = MpiWorkerManager(node_id, comm)
    
    # Instantiate the real RaftNode and RaftConsensus
    raft_node = RealRaftNode(node_id, args)
    raft_consensus = RaftConsensus(
        raft_node, worker_manager, args
    )
    
    # Set the reference to raft_consensus in the worker_manager
    worker_manager.set_raft_consensus(raft_consensus)
    
    # Start the consensus operations
    raft_consensus.start()
    
    # Force transition from INITIAL to FOLLOWER state to participate in elections
    if raft_node.state.name == "INITIAL":
        logging.info(f"Node {node_id}: Force transition from INITIAL to FOLLOWER state")
        raft_node.become_follower(0)
    
    # Log initial state and timeout settings
    logging.info(f"Node {node_id}: Initialized with settings:")
    logging.info(f"  - Min election timeout: {args.min_election_timeout}ms")
    logging.info(f"  - Max election timeout: {args.max_election_timeout}ms")  
    logging.info(f"  - Heartbeat interval: {args.heartbeat_interval}ms")
    logging.info(f"  - Initial state: {raft_node.state}")
    
    # Set up callback for leadership changes
    def on_leadership_change(leader_id):
        logging.info(f"Node {node_id}: Leadership changed to {leader_id}")
        report_leader_to_service_discovery(leader_id)
    
    raft_consensus.on_leadership_change = on_leadership_change

    # --- Flask HTTP API and Web UI for status/log interaction ---
    from flask import render_template_string

    NODE_UI_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Raft Node {{ node_id }}</title>
        <style>
            body { font-family: Arial; margin: 20px; }
            .state { font-size: 1.2em; margin-bottom: 10px; }
            .leader { color: green; }
            .follower { color: #333; }
            .candidate { color: orange; }
            .initial { color: blue; }
            .log { background: #222; color: #eee; padding: 10px; margin-top: 10px; height: 300px; overflow-y: auto; }
            .cmd-history { background: #f8f8f8; padding: 10px; margin-top: 10px; }
        </style>
        <script>
            function fetchStatus() {
                fetch('/status').then(r => r.json()).then(data => {
                    document.getElementById('state').innerText = data.state;
                    document.getElementById('term').innerText = data.term;
                    document.getElementById('leader').innerText = data.leader_id || 'None';
                    document.getElementById('log_length').innerText = data.log_length;
                    
                    // Apply the appropriate class based on state
                    document.getElementById('node_info').className = data.state.toLowerCase();
                });
            }
            function fetchLog() {
                fetch('/log').then(r => r.json()).then(data => {
                    document.getElementById('log').innerText = data.log.join('\\n');
                    // Auto-scroll to bottom
                    var logDiv = document.getElementById('log_container');
                    logDiv.scrollTop = logDiv.scrollHeight;
                });
            }
            function fetchCmdHistory() {
                fetch('/cmd_history').then(r => r.json()).then(data => {
                    document.getElementById('cmd_history').innerText = data.history.join('\\n');
                });
            }
            function sendCommand() {
                let cmd = document.getElementById('cmd_input').value;
                fetch('/command', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({cmd: cmd})
                }).then(r => r.json()).then(data => {
                    alert('Command result: ' + data.result);
                    fetchCmdHistory();
                });
            }
            setInterval(fetchStatus, 2000);
            setInterval(fetchLog, 2000);
            setInterval(fetchCmdHistory, 2000);
            window.onload = function() { fetchStatus(); fetchLog(); fetchCmdHistory(); };
        </script>
    </head>
    <body>
        <h1>Raft Node {{ node_id }}</h1>
        <div id="node_info">
            <div class="state">State: <span id="state"></span></div>
            <div>Term: <span id="term"></span></div>
            <div>Leader: <span id="leader"></span></div>
            <div>Log length: <span id="log_length"></span></div>
        </div>
        <div class="cmd-history">
            <b>Command History:</b>
            <pre id="cmd_history"></pre>
        </div>
        <div>
            <input id="cmd_input" placeholder="Command (e.g. trigger_election)"/>
            <button onclick="sendCommand()">Send Command</button>
        </div>
        <div class="log" id="log_container">
            <b>Log Output:</b>
            <pre id="log"></pre>
        </div>
    </body>
    </html>
    """

    app = Flask(__name__)

    @app.route("/")
    def node_ui():
        return render_template_string(NODE_UI_TEMPLATE, node_id=node_id)

    @app.route("/cmd_history")
    def get_cmd_history():
        return jsonify({"history": cmd_history})

    @app.route("/status")
    def status():
        return jsonify(
            {
                "node_id": node_id,
                "state": str(getattr(raft_node, "state", "unknown")).replace("RaftState.", ""),
                "term": getattr(raft_node, "current_term", None),
                "leader_id": getattr(raft_node, "leader_id", None),
                "log_length": len(getattr(raft_node, "log", [])),
            }
        )

    @app.route("/log")
    def get_log():
        return jsonify({"log": log_buffer})

    @app.route("/command", methods=["POST"])
    def command():
        cmd = request.json.get("cmd")
        cmd_history.append(f"{time.strftime('%H:%M:%S')} - {cmd}")
        # Process commands
        if cmd == "trigger_election" and hasattr(raft_node, "start_election"):
            raft_node.start_election()
            return jsonify({"result": "election triggered"})
        elif cmd == "show_known_nodes" and hasattr(raft_node, "known_nodes"):
            return jsonify({"result": f"Known nodes: {list(raft_node.known_nodes)}"})
        elif cmd == "sync_known_nodes":
            sync_nodes_from_service_discovery(raft_node)
            return jsonify({"result": f"Synced nodes: {list(raft_node.known_nodes)}"})
        # Add command to manually transition from INITIAL to FOLLOWER
        elif cmd == "become_follower" and hasattr(raft_node, "become_follower") and raft_node.state.name == "INITIAL":
            raft_node.become_follower(raft_node.current_term)
            return jsonify({"result": "Transitioned to FOLLOWER state"})
        # Add command to force node to become leader (for testing)
        elif cmd == "force_timeout":
            # Force election timeout by setting last heartbeat time far in the past
            raft_node.last_heartbeat_time = 0
            return jsonify({"result": "Forced election timeout"})
        # Add command to view state details
        elif cmd == "state_details":
            details = {
                "state": str(getattr(raft_node, "state", "unknown")),
                "term": getattr(raft_node, "current_term", None),
                "voted_for": getattr(raft_node, "voted_for", None),
                "leader_id": getattr(raft_node, "leader_id", None),
                "log_length": len(getattr(raft_node, "log", [])),
                "known_nodes": list(getattr(raft_node, "known_nodes", [])),
                "votes_received": list(getattr(raft_node, "votes_received", [])),
                "election_timeout_ms": getattr(raft_node, "election_timeout", 0) * 1000,
                "time_since_heartbeat_ms": (time.time() - getattr(raft_node, "last_heartbeat_time", 0)) * 1000
            }
            return jsonify({"result": str(details)})
        return jsonify({"result": "unknown command"})

    def run_flask():
        port = args.http_port if args.http_port else 7000 + rank
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Periodically check for new nodes and update raft_node's known_nodes
    def sync_nodes_from_service_discovery(raft_node):
        nodes = get_node_list()
        node_ids = set(int(nid) for nid in nodes.keys())
        
        if node_ids != raft_node.known_nodes:
            # Update the node's knowledge of the cluster
            logging.info(f"Node {node_id}: Updating known nodes from {raft_node.known_nodes} to {node_ids}")
            raft_node.update_known_nodes(len(node_ids))

    def node_discovery_thread():
        while True:
            try:
                sync_nodes_from_service_discovery(raft_node)
                time.sleep(args.discovery_poll_interval)
            except Exception as e:
                logging.error(f"Error in node discovery: {e}")
                time.sleep(5)  # Longer wait on error
    
    discovery_thread = threading.Thread(target=node_discovery_thread, daemon=True)
    discovery_thread.start()

    # Start a thread to handle automatic transition from INITIAL to FOLLOWER state
    def auto_init_follower():
        # Wait a short time for initialization
        time.sleep(3)
        if raft_node.state.name == "INITIAL":
            logging.info(f"Node {node_id}: Auto-transitioning from INITIAL to FOLLOWER state")
            raft_node.become_follower(0)  # Term 0 for initial transition
    
    init_thread = threading.Thread(target=auto_init_follower, daemon=True)
    init_thread.start()

    # Monitor and report leader to service discovery
    try:
        while True:
            current_leader = getattr(raft_node, "leader_id", None)
            if current_leader:
                report_leader_to_service_discovery(current_leader)
            
            # Log election timeout debug info periodically
            if raft_node.state.name in ["FOLLOWER", "CANDIDATE"]:
                time_since_heartbeat = time.time() - raft_node.last_heartbeat_time
                timeout_percentage = (time_since_heartbeat / raft_node.election_timeout) * 100
                if timeout_percentage > 50:  # Log when more than 50% of timeout has elapsed
                    logging.debug(f"Node {node_id}: Election timeout {timeout_percentage:.1f}% elapsed " +
                                 f"({time_since_heartbeat:.2f}s / {raft_node.election_timeout:.2f}s)")
            
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info(f"Node {node_id}: Shutting down")
    finally:
        # Clean up on exit
        deregister_from_service_discovery(node_id)
        worker_manager.stop()
        logging.info(f"Node {node_id}: Shutdown complete")


if __name__ == "__main__":
    main()
