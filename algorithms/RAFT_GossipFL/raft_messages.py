class RaftMessage:
    """
    Defines message types and argument keys for RAFT communication.
    """
    
    # Core RAFT message types
    MSG_TYPE_RAFT_REQUEST_VOTE = 101
    MSG_TYPE_RAFT_VOTE_RESPONSE = 102
    MSG_TYPE_RAFT_APPEND_ENTRIES = 103
    MSG_TYPE_RAFT_APPEND_RESPONSE = 104
    MSG_TYPE_RAFT_STATE_SNAPSHOT = 105
    MSG_TYPE_RAFT_STATE_REQUEST = 106
    MSG_TYPE_RAFT_PARAM_REQUEST = 107
    MSG_TYPE_RAFT_PARAM_RESPONSE = 108
    MSG_TYPE_RAFT_LEADER_REDIRECT = 109
    MSG_TYPE_RAFT_PREVOTE_REQUEST = 110
    MSG_TYPE_RAFT_PREVOTE_RESPONSE = 111
    MSG_TYPE_RAFT_INSTALL_SNAPSHOT = 112
    
    MSG_TYPE_RAFT_JOIN_REQUEST = 120
    MSG_TYPE_RAFT_JOIN_RESPONSE = 121

    # Message arguments
    MSG_ARG_KEY_TYPE = "msg_type"
    MSG_ARG_KEY_SENDER = "sender"
    MSG_ARG_KEY_RECEIVER = "receiver"
    
    # RAFT specific arguments
    MSG_ARG_TERM = "term"
    MSG_ARG_CANDIDATE_ID = "candidate_id"
    MSG_ARG_LAST_LOG_INDEX = "last_log_index"
    MSG_ARG_LAST_LOG_TERM = "last_log_term"
    MSG_ARG_VOTE_GRANTED = "vote_granted"
    MSG_ARG_ENTRIES = "entries"
    MSG_ARG_PREV_LOG_INDEX = "prev_log_index"
    MSG_ARG_PREV_LOG_TERM = "prev_log_term"
    MSG_ARG_LEADER_COMMIT = "leader_commit"
    MSG_ARG_SUCCESS = "success"
    MSG_ARG_MATCH_INDEX = "match_index"
    MSG_ARG_LOG = "log"
    MSG_ARG_COMMIT_INDEX = "commit_index"
    MSG_ARG_ERROR_MSG = "error_msg"

    # Install snapshot arguments
    MSG_ARG_LAST_INCLUDED_INDEX = "last_included_index"
    MSG_ARG_LAST_INCLUDED_TERM = "last_included_term"
    MSG_ARG_OFFSET = "offset"
    MSG_ARG_DATA = "data"
    MSG_ARG_DONE = "done"

    # Initialization / parameter exchange
    MSG_ARG_MODEL_PARAMS = "model_params"
    MSG_ARG_RAFT_METADATA = "raft_metadata"
    MSG_ARG_STATE_PACKAGE = "state_package"
    MSG_ARG_LEADER_ID = "leader_id"
    
    # Entry types
    MSG_ARG_ENTRY_TYPE = "entry_type"
    MSG_ARG_ENTRY_DATA = "entry_data"
    
    # Join protocol arguments (Phase 1)
    MSG_ARG_NODE_INFO = "node_info"
    MSG_ARG_JOIN_APPROVED = "join_approved"


class LogEntryType:
    """
    Defines log entry types for RAFT consensus.
    """
    
    # Standard RAFT log entry types
    NORMAL = "normal"
    NO_OP = "no_op"
    
    # Membership change log entry types (Phase 1)
    MEMBERSHIP_ADD = "membership_add"
    MEMBERSHIP_REMOVE = "membership_remove"
