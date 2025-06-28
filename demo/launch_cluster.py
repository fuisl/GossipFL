import os
import sys
import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description="Launch RAFT cluster demo")
    parser.add_argument("--nodes", type=int, default=3, help="Number of RAFT nodes to launch")
    parser.add_argument("--discovery_port", type=int, default=5000, help="Service discovery port")
    parser.add_argument("--monitor_port", type=int, default=8080, help="Web monitor port")
    parser.add_argument("--min_election_timeout", type=int, default=2000, help="Min election timeout (ms)")
    parser.add_argument("--max_election_timeout", type=int, default=4000, help="Max election timeout (ms)")
    parser.add_argument("--heartbeat_interval", type=int, default=750, help="Heartbeat interval (ms)")
    args = parser.parse_args()

    # Start the service discovery server in a separate process
    print(f"Starting service discovery server on port {args.discovery_port}...")
    discovery_process = subprocess.Popen(
        [sys.executable, "demo/service_discovery.py", "--port", str(args.discovery_port)]
    )

    # Start the web monitor in a separate process
    print(f"Starting web monitor on port {args.monitor_port}...")
    monitor_process = subprocess.Popen(
        [sys.executable, "demo/web_monitor.py", "--port", str(args.monitor_port), 
         "--discovery_url", f"http://localhost:{args.discovery_port}"]
    )

    # Build the MPI command to launch the RAFT nodes
    mpi_cmd = [
        "mpiexec", "-n", str(args.nodes),
        sys.executable, "demo/raft_node.py",
        "--client_num_in_total", str(args.nodes),
        "--min_election_timeout", str(args.min_election_timeout),
        "--max_election_timeout", str(args.max_election_timeout),
        "--heartbeat_interval", str(args.heartbeat_interval)
    ]

    # Set discovery URL as environment variable
    os.environ["SERVICE_DISCOVERY_URL"] = f"http://localhost:{args.discovery_port}"

    print(f"Launching {args.nodes} RAFT nodes with MPI...")
    print(f"Web monitor URL: http://localhost:{args.monitor_port}")
    print(f"Individual node URLs: http://localhost:700x (where x is the node rank)")
    print("Press Ctrl+C to stop all processes")

    try:
        # Start the MPI processes and wait for them to finish
        raft_process = subprocess.Popen(mpi_cmd)
        raft_process.wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Clean up all processes
        raft_process.terminate() if 'raft_process' in locals() else None
        monitor_process.terminate()
        discovery_process.terminate()
        print("All processes terminated")

if __name__ == "__main__":
    main()
