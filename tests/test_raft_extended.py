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
        self.worker = None

    def _deliver(self, receiver_id, handler_name, *args):
        dest = self.transport.get(receiver_id)
        if dest and dest.consensus:
            getattr(dest.consensus, handler_name)(*args)

    # all the send_* methods ...
    def send_vote_request(self,      r, t,i,j): self._deliver(r, "handle_vote_request",      self.node_id, t, i, j)
    def send_prevote_request(self,   r, t,i,j): self._deliver(r, "handle_prevote_request",   self.node_id, t, i, j)
    def send_vote_response(self,     r, t,vg ): self._deliver(r, "handle_vote_response",     self.node_id, t, vg)
    def send_prevote_response(self,  r, t,pg ): self._deliver(r, "handle_prevote_response",  self.node_id, t, pg)
    def send_append_entries(self,    r, t,p,pt,es,lc):
        self._deliver(r, "handle_append_entries", self.node_id, t, p, pt, es, lc)
    def send_append_response(self,   r, t,s,mi): self._deliver(r, "handle_append_response",   self.node_id, t, s, mi)
    def send_install_snapshot(self,  r, t,li,lt,o,d,do):
        self._deliver(r, "handle_install_snapshot", self.node_id, t, li, lt, o, d, do)
    def send_state_snapshot(self,    r, t,log,ci):
        self._deliver(r, "handle_state_snapshot",   t, log, ci)
    def send_model_params(self,      *args): pass
    def send_state_request(self,     r):        self._deliver(r, "handle_node_join_request", self.node_id)
    def send_leader_redirect(self,    *args): pass
    def send_param_request(self,      *args): pass
    def send_model_params_with_metadata(self, *args): pass
    def on_membership_change(self, new_nodes, round_num=0):
        pass
    def on_coordinator_change(self, *args, **kwargs):
        pass
    def on_become_coordinator(self, *args, **kwargs):
        pass


def create_consensus(node_id, args, transport):
    node = RaftNode(node_id, args)
    mgr  = LocalRaftManager(node_id, transport)
    c    = RaftConsensus(node, mgr, args)
    mgr.consensus = c
    return c


def print_status(consensuses):
    info = []
    for c in consensuses:
        st = c.get_status()
        info.append(
            f"N{st['node_id']} state={st['node_state']} term={st['current_term']} "
            f"commit={st['commit_index']} log={st['log_length']}"
        )
    logging.info(" | ".join(info))


def run_extended_simulation():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = SimpleNamespace(
        min_election_timeout=150,
        max_election_timeout=300,
        heartbeat_interval=100,
        client_num_in_total=3,
        bootstrap=True,
    )

    transport = LocalTransport()
    # --- Phase 1: start 3-node cluster ---
    consensuses = [create_consensus(i, args, transport) for i in range(3)]
    for c in consensuses:
        c.start()
    time.sleep(3)
    print_status(consensuses)

    # find the current leader
    leader = next(c for c in consensuses if c.is_leader())

    # --- Phase 2: kill node 1 ---
    logging.info("=== Simulating failure of node 1 ===")
    consensuses[1].stop()
    transport.unregister(1)
    time.sleep(5)
    print_status(consensuses)

    # --- Phase 3: bring back node 1 via snapshot join-request ---
    logging.info("=== Re-adding node 1 ===")
    args_without_boot = SimpleNamespace (
        min_election_timeout=150,
        max_election_timeout=300,
        heartbeat_interval=100,
        client_num_in_total=3,
    )
    c1 = create_consensus(1, args_without_boot, transport)
    consensuses[1] = c1
    c1.start()
    # ask the leader to send its full state to 1
    transport.get(1).send_state_request(leader.get_leader_id())
    time.sleep(2)
    print_status(consensuses)

    # --- Phase 4: add a completely new node 3 via membership-change + snapshot ---
    logging.info("=== Adding new node 3 ===")
    # bump the total in args so RaftConsensus on 3 thinks cluster size=4
    args.client_num_in_total = 4
    # tell the existing leader to append a membership-change → this will replicate to all 3
    leader.add_membership_change('add', 3)
    time.sleep(2)   # wait for that log entry to commit on 0,1,2
    # now spin up node-3
    c3 = create_consensus(3, args, transport)
    consensuses.append(c3)
    c3.start()
    # fresh node-3 asks leader for a full snapshot
    transport.get(3).send_state_request(leader.get_leader_id())
    time.sleep(3)
    print_status(consensuses)

    # --- tear down ---
    for c in consensuses:
        c.stop()


if __name__ == "__main__":
    run_extended_simulation()
