#!/usr/bin/env python3
"""
Test Runner for RAFT Manual Testing

This script provides easy commands to test different scenarios with the
standalone RAFT implementation.

Usage:
    python test_runner.py scenario1    # Single node bootstrap
    python test_runner.py scenario2    # Three node cluster
    python test_runner.py scenario3    # Dynamic joining
    python test_runner.py scenario4    # Leader election
    python test_runner.py discovery    # Start service discovery only
"""

import sys
import os
import subprocess
import time
import signal
import threading
from typing import List, Dict, Any

def run_command(cmd: List[str], name: str, wait: bool = False) -> subprocess.Popen:
    """Run a command and return the process."""
    print(f"🚀 Starting {name}...")
    print(f"Command: {' '.join(cmd)}")
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    if wait:
        # Wait for process to complete
        process.wait()
    else:
        # Start a thread to print output
        def print_output():
            for line in iter(process.stdout.readline, ''):
                print(f"[{name}] {line.strip()}")
        
        thread = threading.Thread(target=print_output, daemon=True)
        thread.start()
    
    return process


def scenario1():
    """Single node bootstrap test."""
    print("=" * 80)
    print("📋 Scenario 1: Single Node Bootstrap")
    print("=" * 80)
    print("This test demonstrates:")
    print("  - Service discovery startup")
    print("  - Single node bootstrap detection")
    print("  - Node becoming leader immediately")
    print()
    
    processes = []
    
    try:
        # Start service discovery
        discovery_process = run_command([
            sys.executable, 'standalone_service_discovery.py',
            '--port', '8080',
            '--monitor-port', '8081'
        ], 'ServiceDiscovery')
        processes.append(discovery_process)
        
        # Wait for service discovery to start
        print("⏳ Waiting for service discovery to start...")
        time.sleep(3)
        
        # Start single node
        node_process = run_command([
            sys.executable, 'standalone_raft_node.py',
            '--node-id', '0',
            '--bootstrap',
            '--discovery-host', 'localhost',
            '--discovery-port', '8080',
            '--log-level', 'INFO'
        ], 'Node0')
        processes.append(node_process)
        
        print()
        print("✅ Test Started!")
        print("Monitor: http://localhost:8081")
        print("Expected: Node 0 should become LEADER immediately")
        print("Press Ctrl+C to stop all processes...")
        
        # Wait for interrupt
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping processes...")
            
    finally:
        for process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()


def scenario2():
    """Three node cluster formation."""
    print("=" * 80)
    print("📋 Scenario 2: Three Node Cluster Formation")
    print("=" * 80)
    print("This test demonstrates:")
    print("  - Multi-node cluster formation")
    print("  - Leader election among multiple nodes")
    print("  - Service discovery coordination")
    print("  - Node joining existing cluster")
    print()
    
    processes = []
    
    try:
        # Start service discovery
        discovery_process = run_command([
            sys.executable, 'standalone_service_discovery.py',
            '--port', '8080',
            '--monitor-port', '8081'
        ], 'ServiceDiscovery')
        processes.append(discovery_process)
        
        # Wait for service discovery to start
        print("⏳ Waiting for service discovery to start...")
        time.sleep(3)
        
        # Start nodes sequentially
        for i in range(3):
            node_process = run_command([
                sys.executable, 'standalone_raft_node.py',
                '--node-id', str(i),
                '--bootstrap' if i == 0 else '',
                '--discovery-host', 'localhost',
                '--discovery-port', '8080',
                '--log-level', 'INFO'
            ], f'Node{i}')
            processes.append(node_process)
            
            if i < 2:  # Don't wait after last node
                print(f"⏳ Waiting before starting next node...")
                time.sleep(5)
        
        print()
        print("✅ Test Started!")
        print("Monitor: http://localhost:8081")
        print("Expected: One node should become LEADER, others FOLLOWERS")
        print("Press Ctrl+C to stop all processes...")
        
        # Wait for interrupt
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping processes...")
            
    finally:
        for process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()


def scenario3():
    """Dynamic node joining test."""
    print("=" * 80)
    print("📋 Scenario 3: Dynamic Node Joining")
    print("=" * 80)
    print("This test demonstrates:")
    print("  - Initial cluster formation")
    print("  - Dynamic node joining after cluster is established")
    print("  - State synchronization for new nodes")
    print("  - Communication refresh")
    print()
    
    processes = []
    
    try:
        # Start service discovery
        discovery_process = run_command([
            sys.executable, 'standalone_service_discovery.py',
            '--port', '8080',
            '--monitor-port', '8081'
        ], 'ServiceDiscovery')
        processes.append(discovery_process)
        
        # Wait for service discovery to start
        print("⏳ Waiting for service discovery to start...")
        time.sleep(3)
        
        # Start initial cluster (2 nodes)
        for i in range(2):
            node_process = run_command([
                sys.executable, 'standalone_raft_node.py',
                '--node-id', str(i),
                '--bootstrap' if i == 0 else '',
                '--discovery-host', 'localhost',
                '--discovery-port', '8080',
                '--log-level', 'INFO'
            ], f'Node{i}')
            processes.append(node_process)
            
            if i < 1:
                print(f"⏳ Waiting before starting next node...")
                time.sleep(5)
        
        print()
        print("✅ Initial cluster started!")
        print("Monitor: http://localhost:8081")
        print("⏳ Waiting 15 seconds before adding dynamic nodes...")
        time.sleep(15)
        
        # Add nodes dynamically
        for i in range(2, 5):
            print(f"🔄 Adding Node {i} dynamically...")
            node_process = run_command([
                sys.executable, 'standalone_raft_node.py',
                '--node-id', str(i),
                '--discovery-host', 'localhost',
                '--discovery-port', '8080',
                '--log-level', 'INFO'
            ], f'Node{i}')
            processes.append(node_process)
            
            if i < 4:
                print(f"⏳ Waiting before adding next node...")
                time.sleep(10)
        
        print()
        print("✅ Dynamic joining test running!")
        print("Expected: New nodes should join as FOLLOWERS")
        print("Press Ctrl+C to stop all processes...")
        
        # Wait for interrupt
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping processes...")
            
    finally:
        for process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()


def scenario4():
    """Leader election test."""
    print("=" * 80)
    print("📋 Scenario 4: Leader Election Test")
    print("=" * 80)
    print("This test demonstrates:")
    print("  - Initial cluster formation")
    print("  - Leader failure simulation")
    print("  - New leader election")
    print("  - Failed leader rejoining as follower")
    print()
    
    processes = []
    
    try:
        # Start service discovery
        discovery_process = run_command([
            sys.executable, 'standalone_service_discovery.py',
            '--port', '8080',
            '--monitor-port', '8081'
        ], 'ServiceDiscovery')
        processes.append(discovery_process)
        
        # Wait for service discovery to start
        print("⏳ Waiting for service discovery to start...")
        time.sleep(3)
        
        # Start 3 nodes
        for i in range(3):
            node_process = run_command([
                sys.executable, 'standalone_raft_node.py',
                '--node-id', str(i),
                '--bootstrap' if i == 0 else '',
                '--discovery-host', 'localhost',
                '--discovery-port', '8080',
                '--log-level', 'INFO'
            ], f'Node{i}')
            processes.append(node_process)
            
            if i < 2:
                print(f"⏳ Waiting before starting next node...")
                time.sleep(5)
        
        print()
        print("✅ Initial cluster started!")
        print("Monitor: http://localhost:8081")
        print("⏳ Waiting 20 seconds for cluster to stabilize...")
        time.sleep(20)
        
        # Kill the leader (assumed to be Node 0)
        print("💀 Simulating leader failure - killing Node 0...")
        if len(processes) > 1:
            processes[1].terminate()  # Node 0 is processes[1]
            processes[1].wait(timeout=5)
        
        print("⏳ Waiting for new leader election...")
        time.sleep(15)
        
        # Restart the failed node
        print("🔄 Restarting failed node as follower...")
        node_process = run_command([
            sys.executable, 'standalone_raft_node.py',
            '--node-id', '0',
            '--discovery-host', 'localhost',
            '--discovery-port', '8080',
            '--log-level', 'INFO'
        ], 'Node0-Restart')
        processes.append(node_process)
        
        print()
        print("✅ Leader election test running!")
        print("Expected: New leader elected, old leader rejoins as follower")
        print("Press Ctrl+C to stop all processes...")
        
        # Wait for interrupt
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping processes...")
            
    finally:
        for process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()


def discovery_only():
    """Start only service discovery for manual testing."""
    print("=" * 80)
    print("📋 Service Discovery Only")
    print("=" * 80)
    print("Starting service discovery server for manual testing.")
    print("You can start nodes manually using:")
    print("  python standalone_raft_node.py --node-id 0 --bootstrap")
    print("  python standalone_raft_node.py --node-id 1")
    print("  python standalone_raft_node.py --node-id 2")
    print()
    
    try:
        discovery_process = run_command([
            sys.executable, 'standalone_service_discovery.py',
            '--port', '8080',
            '--monitor-port', '8081'
        ], 'ServiceDiscovery')
        
        print("✅ Service Discovery Started!")
        print("Monitor: http://localhost:8081")
        print("Press Ctrl+C to stop...")
        
        # Wait for interrupt
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping service discovery...")
            
    finally:
        try:
            discovery_process.terminate()
            discovery_process.wait(timeout=5)
        except:
            discovery_process.kill()


def main():
    """Main function."""
    if len(sys.argv) != 2:
        print("Usage: python test_runner.py <scenario>")
        print()
        print("Available scenarios:")
        print("  scenario1  - Single node bootstrap")
        print("  scenario2  - Three node cluster formation")
        print("  scenario3  - Dynamic node joining")
        print("  scenario4  - Leader election test")
        print("  discovery  - Start service discovery only")
        return 1
    
    scenario = sys.argv[1]
    
    if scenario == 'scenario1':
        scenario1()
    elif scenario == 'scenario2':
        scenario2()
    elif scenario == 'scenario3':
        scenario3()
    elif scenario == 'scenario4':
        scenario4()
    elif scenario == 'discovery':
        discovery_only()
    else:
        print(f"Unknown scenario: {scenario}")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
