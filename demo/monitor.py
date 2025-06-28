# Cluster monitor for Raft demo
# Visualizes and interacts with the cluster
import requests
import time

SERVICE_DISCOVERY_URL = 'http://localhost:5000'

def get_cluster_state():
    try:
        resp = requests.get(f"{SERVICE_DISCOVERY_URL}/state")
        return resp.json()
    except Exception as e:
        print(f"Error fetching cluster state: {e}")
        return None

def print_cluster_state(state):
    if not state:
        print("No state available.")
        return
    print("\n--- Cluster State ---")
    print(f"Leader: {state.get('leader')}")
    print("Nodes:")
    for node_id, addr in state.get('nodes', {}).items():
        print(f"  {node_id}: {addr}")
    print("---------------------\n")

def main():
    print("Monitoring Raft cluster. Press Ctrl+C to exit.")
    try:
        while True:
            state = get_cluster_state()
            print_cluster_state(state)
            time.sleep(2)
    except KeyboardInterrupt:
        print("Exiting monitor.")

if __name__ == '__main__':
    main()
