#!/usr/bin/env python3
"""
RAFT Consensus Test Runner

This script provides a comprehensive testing suite for the RAFT consensus
implementation. It runs various tests to verify the correctness of the
RAFT algorithm implementation.

Usage:
    python run_raft_tests.py [--test TEST_NAME] [--verbose]

Available tests:
    - simple: Basic node functionality tests
    - messages: Message passing demonstration
    - consensus: Full consensus algorithm tests
    - all: Run all tests (default)
"""

import argparse
import sys
import importlib.util
import os
from pathlib import Path


def setup_path():
    """Setup the Python path to import RAFT modules."""
    # Get the directory containing this script
    script_dir = Path(__file__).parent
    parent_dir = script_dir.parent
    
    # Add the current directory and parent directory to Python path
    if str(script_dir) not in sys.path:
        sys.path.insert(0, str(script_dir))
    if str(parent_dir) not in sys.path:
        sys.path.insert(0, str(parent_dir))


def import_test_module(module_name: str):
    """Import a test module by name."""
    try:
        module = importlib.import_module(module_name)
        return module
    except ImportError as e:
        print(f"Failed to import {module_name}: {e}")
        return None


def run_simple_tests():
    """Run simple RAFT node tests."""
    print("\n" + "="*70)
    print("RUNNING SIMPLE RAFT NODE TESTS")
    print("="*70)
    
    try:
        from test_raft_simple import main as simple_main
        
        simple_main()
        return True
    except Exception as e:
        print(f"❌ Simple tests failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_message_demo():
    """Run RAFT message passing demonstration."""
    print("\n" + "="*70)
    print("RUNNING RAFT MESSAGE PASSING DEMO")
    print("="*70)
    
    try:
        from demo_raft_messages import main as demo_main
        
        demo_main()
        return True
    except Exception as e:
        print(f"❌ Message demo failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_consensus_tests():
    """Run full consensus algorithm tests."""
    print("\n" + "="*70)
    print("RUNNING FULL RAFT CONSENSUS TESTS")
    print("="*70)
    
    try:
        from test_raft_consensus import main as consensus_main
        
        consensus_main()
        return True
    except Exception as e:
        print(f"❌ Consensus tests failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def check_raft_implementation():
    """Check if RAFT implementation files exist and are importable."""
    print("Checking RAFT implementation files...")
    
    required_files = [
        'raft_node.py',
        'raft_consensus.py',
        'raft_messages.py'
    ]
    
    script_dir = Path(__file__).parent
    parent_dir = script_dir.parent
    missing_files = []
    
    for file_name in required_files:
        file_path = parent_dir / file_name
        if not file_path.exists():
            missing_files.append(file_name)
        else:
            print(f"✓ Found {file_name}")
    
    if missing_files:
        print(f"❌ Missing required files: {missing_files}")
        return False
    
    # Try to import the main modules
    try:
        from raft_node import RaftNode, RaftState
        from raft_consensus import RaftConsensus
        from raft_messages import RaftMessage
        
        print("✓ All RAFT modules imported successfully")
        return True
    except ImportError as e:
        print(f"❌ Failed to import RAFT modules: {e}")
        return False


def print_usage():
    """Print usage information."""
    print("""
RAFT Consensus Test Runner

This tool tests the RAFT consensus algorithm implementation with pure message passing.

Available Tests:
  1. Simple Tests    - Basic node operations (initialization, elections, log operations)
  2. Message Demo    - Interactive demonstration of message passing between nodes  
  3. Consensus Tests - Full consensus algorithm with network simulation
  4. All Tests       - Run all tests in sequence

Test Features:
  • Leader election simulation
  • Log replication verification
  • Network partition testing
  • Node failure and recovery
  • Message delivery and ordering
  • Consensus safety and liveness properties

The tests run without GossipFL dependencies using pure message passing simulation.
""")


def main():
    """Main function to run RAFT tests."""
    parser = argparse.ArgumentParser(
        description="Test the RAFT consensus algorithm implementation",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--test', 
        choices=['simple', 'messages', 'consensus', 'all'],
        default='all',
        help='Which test to run (default: all)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--help-detailed',
        action='store_true',
        help='Show detailed help and exit'
    )
    
    args = parser.parse_args()
    
    if args.help_detailed:
        print_usage()
        return
    
    # Setup Python path
    setup_path()
    
    print("RAFT Consensus Algorithm Testing Framework")
    print("=" * 70)
    
    # Check implementation
    if not check_raft_implementation():
        print("\n❌ RAFT implementation check failed. Cannot run tests.")
        return 1
    
    # Track test results
    results = {}
    
    # Run selected tests
    if args.test in ['simple', 'all']:
        results['simple'] = run_simple_tests()
    
    if args.test in ['messages', 'all']:
        results['messages'] = run_message_demo()
    
    if args.test in ['consensus', 'all']:
        results['consensus'] = run_consensus_tests()
    
    # Print summary
    print("\n" + "="*70)
    print("TEST RESULTS SUMMARY")
    print("="*70)
    
    total_tests = len(results)
    passed_tests = sum(1 for result in results.values() if result)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "❌ FAILED"
        print(f"{test_name.upper()} TESTS: {status}")
    
    print(f"\nTotal: {passed_tests}/{total_tests} test suites passed")
    
    if passed_tests == total_tests:
        print("\n🎉 All tests passed! The RAFT implementation is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total_tests - passed_tests} test suite(s) failed. Check the output above for details.")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
