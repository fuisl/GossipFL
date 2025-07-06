class RaftMessage:
    """
    Defines message types and argument keys for RAFT communication.
    """
    # --- Core RAFT RPC types ---
    MSG_TYPE_REQUEST_VOTE       = 101
    MSG_TYPE_VOTE_RESPONSE      = 102
    MSG_TYPE_APPEND_ENTRIES     = 103
    MSG_TYPE_APPEND_RESPONSE    = 104
    MSG_TYPE_STATE_SNAPSHOT     = 105
    MSG_TYPE_STATE_REQUEST      = 106
    MSG_TYPE_PARAM_REQUEST      = 107
    MSG_TYPE_PARAM_RESPONSE     = 108
    MSG_TYPE_LEADER_REDIRECT    = 109
    MSG_TYPE_PREVOTE_REQUEST    = 110
    MSG_TYPE_PREVOTE_RESPONSE   = 111
    MSG_TYPE_INSTALL_SNAPSHOT   = 112

    # --- Cluster join protocol ---
    MSG_TYPE_JOIN_REQUEST       = 120
    MSG_TYPE_JOIN_RESPONSE      = 121

    # --- Standard message envelope args ---
    ARG_SENDER                  = "sender"
    ARG_RECEIVER                = "receiver"
    ARG_TYPE                    = "msg_type"
    ARG_ERROR                   = "error_msg"

    # --- RAFT protocol args ---
    ARG_TERM                    = "term"
    ARG_CANDIDATE_ID            = "candidate_id"
    ARG_LAST_LOG_INDEX          = "last_log_index"
    ARG_LAST_LOG_TERM           = "last_log_term"
    ARG_VOTE_GRANTED            = "vote_granted"
    ARG_ENTRIES                 = "entries"
    ARG_PREV_LOG_INDEX          = "prev_log_index"
    ARG_PREV_LOG_TERM           = "prev_log_term"
    ARG_LEADER_COMMIT           = "leader_commit"
    ARG_SUCCESS                 = "success"
    ARG_MATCH_INDEX             = "match_index"
    ARG_COMMIT_INDEX            = "commit_index"

    # --- Snapshot args ---
    ARG_LAST_INCLUDED_INDEX     = "last_included_index"
    ARG_LAST_INCLUDED_TERM      = "last_included_term"
    ARG_OFFSET                  = "offset"
    ARG_DATA                    = "data"
    ARG_DONE                    = "done"

    # --- Initialization / Parameter exchange ---
    ARG_MODEL_PARAMS            = "model_params"
    ARG_RAFT_METADATA           = "raft_metadata"
    ARG_STATE_PACKAGE           = "state_package"
    ARG_LEADER_ID               = "leader_id"

    # --- Membership / Coordinator fields ---
    ARG_ACTION                  = "action"            # 'add' | 'remove'
    ARG_NODE_ID                 = "node_id"
    ARG_NODE_INFO               = "node_info"         # dict with ip, port, capabilities, timestamp
    ARG_CURRENT_NODES           = "current_nodes"     # snapshot of cluster node IDs
    ARG_CURRENT_NODES_INFO      = "current_nodes_info"# snapshot of nodes' info
    ARG_REASON                  = "reason"
    ARG_ROUND                   = "round"
    ARG_TIMESTAMP               = "timestamp"
    ARG_JOIN_APPROVED           = "join_approved"


class LogEntryType:
    """
    Defines log entry types for RAFT consensus.
    """
    # Standard RAFT entries
    NORMAL           = "normal"
    NO_OP            = "no_op"

    # Membership changes
    MEMBERSHIP_ADD    = "membership_add"
    MEMBERSHIP_REMOVE = "membership_remove"

    # Coordinator election metadata
    COORDINATOR       = "coordinator"

    # Read-index barrier for linearizable reads
    READ_BARRIER      = "read_barrier"
