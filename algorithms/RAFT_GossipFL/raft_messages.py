class RaftMessage:
    """
    Defines message types and argument keys for RAFT communication.
    """
    
    # RAFT message types
    MSG_TYPE_RAFT_REQUEST_VOTE = 101
    MSG_TYPE_RAFT_VOTE_RESPONSE = 102
    MSG_TYPE_RAFT_APPEND_ENTRIES = 103
    MSG_TYPE_RAFT_APPEND_RESPONSE = 104
    MSG_TYPE_RAFT_STATE_SNAPSHOT = 105
    
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
    
    # Entry types
    MSG_ARG_ENTRY_TYPE = "entry_type"
    MSG_ARG_ENTRY_DATA = "entry_data"
