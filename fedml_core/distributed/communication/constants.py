

class CommunicationConstants:
    MSG_TYPE_CONNECTION_IS_READY = 0
    MSG_CLIENT_STATUS_OFFLINE = "OFFLINE"
    MSG_CLIENT_STATUS_IDLE = "IDLE"
    CLIENT_TOP_LAST_WILL_MSG = "flclient/last_will_msg"
    CLIENT_TOP_ACTIVE_MSG = "flclient/active"
    SERVER_TOP_LAST_WILL_MSG = "flserver/last_will_msg"
    SERVER_TOP_ACTIVE_MSG = "flserver/active"
    GRPC_BASE_PORT = 8890
    
    # Gateway service message types
    MSG_TYPE_GATEWAY_REGISTER = "gateway_register"
    MSG_TYPE_GATEWAY_GET_NODES = "gateway_get_nodes"
    MSG_TYPE_GATEWAY_GET_LEADER = "gateway_get_leader"
    MSG_TYPE_GATEWAY_UPDATE_LEADER = "gateway_update_leader"
    MSG_TYPE_GATEWAY_HEARTBEAT = "gateway_heartbeat"
    MSG_TYPE_GATEWAY_HEALTH_CHECK = "gateway_health_check"
    MSG_TYPE_GATEWAY_REMOVE_NODE = "gateway_remove_node"
    MSG_TYPE_GATEWAY_GET_STATS = "gateway_get_stats"
    
    # Gateway response types
    MSG_TYPE_GATEWAY_REGISTER_RESPONSE = "gateway_register_response"
    MSG_TYPE_GATEWAY_NODES_RESPONSE = "gateway_nodes_response"
    MSG_TYPE_GATEWAY_LEADER_RESPONSE = "gateway_leader_response"
    MSG_TYPE_GATEWAY_UPDATE_LEADER_RESPONSE = "gateway_update_leader_response"
    MSG_TYPE_GATEWAY_HEARTBEAT_RESPONSE = "gateway_heartbeat_response"
    MSG_TYPE_GATEWAY_HEALTH_RESPONSE = "gateway_health_response"
    MSG_TYPE_GATEWAY_REMOVE_RESPONSE = "gateway_remove_response"
    MSG_TYPE_GATEWAY_STATS_RESPONSE = "gateway_stats_response"
    
    # Gateway configuration
    GATEWAY_BASE_PORT = 8090