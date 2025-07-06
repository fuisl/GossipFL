import logging
import threading
import time
from .raft_messages import RaftMessage
from .raft_consensus import RaftConsensus

class RaftServiceDiscoveryBridge:
    """
    Bridge between DynamicGRPCCommManager and the new RaftConsensus.
    Hooks:
      • consensus → comms: send RPCs  
      • comms → consensus: handle RPCs  
      • discovery events → membership proposals
      • membership commits → comm manager registry updates
    """
    def __init__(self, consensus: RaftConsensus, comm_manager):
        self.consensus    = consensus
        self.comm_manager = comm_manager
        self.lock = threading.RLock()

        # 1) Register ourselves with the comm layer
        comm_manager.register_service_discovery_bridge(self)

        # 2) Wire consensus → comms
        self._wire_consensus_to_comm()

        # 3) Wire comms → consensus
        self._wire_comm_to_consensus()

        # 4) Hook comm_manager membership callbacks
        comm_manager.on_node_discovered = self._on_node_discovered
        comm_manager.on_node_lost       = self._on_node_lost

        # 5) Hook consensus commit callbacks
        consensus.raft_node.on_membership_change = self._on_commit_membership
        consensus.raft_node.on_commit = self._on_commit_any

        logging.info("RaftServiceDiscoveryBridge initialized")

    def _wire_consensus_to_comm(self):
        cm = self.comm_manager.send_raft_message
        rn = self.consensus.raft_node

        rn.on_send_prevote   = lambda cid, term, lli, llt: cm(_msg(RaftMessage.MSG_TYPE_PREVOTE_REQUEST,
                                                                   term=term,
                                                                   candidate_id=cid,
                                                                   last_log_index=lli,
                                                                   last_log_term=llt))
        rn.on_send_vote      = lambda cid, term, lli, llt: cm(_msg(RaftMessage.MSG_TYPE_REQUEST_VOTE,
                                                                   term=term,
                                                                   candidate_id=cid,
                                                                   last_log_index=lli,
                                                                   last_log_term=llt))
        rn.on_send_append    = lambda peer, term, lid, pli, plt, entries, lcommit: cm(
                                   _msg(RaftMessage.MSG_TYPE_APPEND_ENTRIES,
                                        leader_id=lid,
                                        term=term,
                                        prev_log_index=pli,
                                        prev_log_term=plt,
                                        entries=entries,
                                        leader_commit=lcommit),
                                   receiver=peer)
        rn.on_send_snapshot  = lambda peer, term, lid, idx, term0, data: cm(
                                   _msg(RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT,
                                        leader_id=lid,
                                        term=term,
                                        last_included_index=idx,
                                        last_included_term=term0,
                                        data=data),
                                   receiver=peer)

        # And the responses...
        rn.on_send_prevote_response = lambda peer, term, granted: cm(
                                          _msg(RaftMessage.MSG_TYPE_PREVOTE_RESPONSE,
                                               term=term,
                                               vote_granted=granted),
                                          receiver=peer)
        rn.on_send_vote_response   = lambda peer, term, granted: cm(
                                          _msg(RaftMessage.MSG_TYPE_VOTE_RESPONSE,
                                               term=term,
                                               vote_granted=granted),
                                          receiver=peer)
        rn.on_send_append_response = lambda peer, term, success, match_idx: cm(
                                          _msg(RaftMessage.MSG_TYPE_APPEND_RESPONSE,
                                               term=term,
                                               success=success,
                                               match_index=match_idx),
                                          receiver=peer)

    def _wire_comm_to_consensus(self):
        # whenever comm manager sees an envelope, it will call us
        cm = self.comm_manager
        # hook your own filtering in comm_manager.send_raft_message handler
        cm.add_raft_handler(RaftMessage.MSG_TYPE_PREVOTE_REQUEST,
                            lambda p: self.consensus.handle_prevote_request(**p))
        cm.add_raft_handler(RaftMessage.MSG_TYPE_PREVOTE_RESPONSE,
                            lambda p: self.consensus.handle_prevote_response(**p))
        cm.add_raft_handler(RaftMessage.MSG_TYPE_REQUEST_VOTE,
                            lambda p: self.consensus.handle_vote_request(**p))
        cm.add_raft_handler(RaftMessage.MSG_TYPE_VOTE_RESPONSE,
                            lambda p: self.consensus.handle_vote_response(**p))
        cm.add_raft_handler(RaftMessage.MSG_TYPE_APPEND_ENTRIES,
                            lambda p: self.consensus.handle_append_entries(**p))
        cm.add_raft_handler(RaftMessage.MSG_TYPE_APPEND_RESPONSE,
                            lambda p: self.consensus.handle_append_response(**p))
        cm.add_raft_handler(RaftMessage.MSG_TYPE_INSTALL_SNAPSHOT,
                            lambda p: self.consensus.handle_install_snapshot(**p))

    def _on_node_discovered(self, node_id, node_info):
        # skip self
        if node_id == self.consensus.raft_node.node_id:
            return
        logging.info(f"Bridge: node discovered {node_id}")
        self.consensus.propose_membership_change(
            action='add',
            node_id=node_id,
            node_info=node_info,
            reason='service_discovery'
        )

    def _on_node_lost(self, node_id):
        if node_id == self.consensus.raft_node.node_id:
            return
        logging.info(f"Bridge: node lost {node_id}")
        self.consensus.propose_membership_change(
            action='remove',
            node_id=node_id,
            reason='service_discovery_lost'
        )

    def _on_commit_membership(self, new_nodes: set, round_num=0):
        # consensus has applied a membership log entry
        # notify comm manager to rebuild its registry
        node_list = []
        for nid in new_nodes:
            info = self.comm_manager.get_node_info(nid)
            node_list.append({
                'node_id': nid,
                'ip_address': info.ip_address,
                'port': info.port,
                'capabilities': info.capabilities,
                'metadata': info.metadata
            })
        # full update
        self.comm_manager._on_bridge_node_registry_update(node_list)
        
        # Chain to the worker manager's on_membership_change if it exists
        # This ensures both comm registry and FL topology get updated
        worker_manager = getattr(self.comm_manager, 'worker_manager', None)
        if worker_manager and hasattr(worker_manager, 'on_membership_change'):
            try:
                worker_manager.on_membership_change(new_nodes, round_num)
            except Exception as e:
                logging.error(f"Error calling worker_manager.on_membership_change: {e}", exc_info=True)

    def _on_commit_any(self, entry):
        # if it's a coordinator entry, notify worker manager, etc.
        # leave defaults in RaftConsensus
        pass

def _msg(msg_type: int, receiver=None, **kwargs):
    envelope = { RaftMessage.ARG_TYPE: msg_type }
    envelope.update(kwargs)
    # allow override of "receiver" for per-peer RPCs
    if receiver is not None:
        envelope[RaftMessage.ARG_RECEIVER] = receiver
    return envelope
