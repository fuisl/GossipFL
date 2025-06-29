import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import logging
import random
import threading
import time
from types import SimpleNamespace

from algorithms.RAFT_GossipFL.raft_node import RaftNode
from algorithms.RAFT_GossipFL.raft_consensus import RaftConsensus


class LocalTransport:
    """Simple in-memory transport for RAFT messages."""

    def __init__(self):
        self.managers = {}
        self.lock = threading.Lock()

    def register(self, manager):
        with self.lock:
            self.managers[manager.node_id] = manager

    def unregister(self, node_id):
        with self.lock:
            self.managers.pop(node_id, None)

    def get(self, node_id):
        with self.lock:
            return self.managers.get(node_id)


class LocalRaftManager:
    """Mimics RaftWorkerManager network functions using LocalTransport."""

    def __init__(self, node_id, transport: LocalTransport):
        self.node_id = node_id
        self.transport = transport
        self.consensus = None
        self.transport.register(self)

    # --- message sending helpers ---
    def _deliver(self, receiver_id, handler_name, *args):
        dest = self.transport.get(receiver_id)
        if dest and dest.consensus:
            handler = getattr(dest.consensus, handler_name)
            handler(*args)

    def send_vote_request(self, receiver_id, term, last_log_index, last_log_term):
        self._deliver(receiver_id, "handle_vote_request", self.node_id, term, last_log_index, last_log_term)

    def send_prevote_request(self, receiver_id, term, last_log_index, last_log_term):
        self._deliver(receiver_id, "handle_prevote_request", self.node_id, term, last_log_index, last_log_term)

    def send_vote_response(self, receiver_id, term, vote_granted):
        self._deliver(receiver_id, "handle_vote_response", self.node_id, term, vote_granted)

    def send_prevote_response(self, receiver_id, term, prevote_granted):
        self._deliver(receiver_id, "handle_prevote_response", self.node_id, term, prevote_granted)

    def send_append_entries(self, receiver_id, term, prev_log_index, prev_log_term, entries, leader_commit):
        self._deliver(
            receiver_id,
            "handle_append_entries",
            self.node_id,
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        )

    def send_append_response(self, receiver_id, term, success, match_index):
        self._deliver(receiver_id, "handle_append_response", self.node_id, term, success, match_index)

    def send_install_snapshot(self, receiver_id, term, last_index, last_term, offset, data, done):
        self._deliver(
            receiver_id,
            "handle_install_snapshot",
            self.node_id,
            term,
            last_index,
            last_term,
            offset,
            data,
            done,
        )

    def send_state_snapshot(self, receiver_id, term, log, commit_index):
        self._deliver(receiver_id, "handle_state_snapshot", term, log, commit_index)

    def send_model_params(self, receiver_id, params):
        # Not needed for basic tests
        pass

    def send_state_request(self, receiver_id):
        self._deliver(receiver_id, "handle_state_request", {"sender": self.node_id})

    def send_leader_redirect(self, receiver_id, leader_id):
        # Not used in tests
        pass

    def send_param_request(self, receiver_id):
        pass

    def send_model_params_with_metadata(self, receiver_id, params, raft_metadata):
        pass


def create_consensus(node_id, args, transport):
    node = RaftNode(node_id, args)
    manager = LocalRaftManager(node_id, transport)
    consensus = RaftConsensus(node, manager, args)
    manager.consensus = consensus
    return consensus


def print_status(consensuses):
    info = []
    for c in consensuses:
        st = c.get_status()
        info.append(
            f"N{st['node_id']} state={st['node_state']} term={st['current_term']} commit={st['commit_index']} log={st['log_length']}"
        )
    logging.info(" | ".join(info))


def run_simulation(num_nodes=3, runtime=10):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = SimpleNamespace(
        min_election_timeout=150,
        max_election_timeout=300,
        heartbeat_interval=100,
        client_num_in_total=num_nodes,
    )
    transport = LocalTransport()
    consensuses = [create_consensus(i, args, transport) for i in range(num_nodes)]

    for c in consensuses:
        c.start()

    start = time.time()
    leader = None
    while time.time() - start < runtime:
        print_status(consensuses)
        for c in consensuses:
            if c.is_leader():
                leader = c
                break
        if leader:
            # Leader appends random topology update every second
            data = {"round": random.randint(1, 100), "value": random.random()}
            leader.add_topology_update(data)
        time.sleep(1)
    for c in consensuses:
        c.stop()


if __name__ == "__main__":
    run_simulation()
