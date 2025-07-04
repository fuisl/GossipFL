#!/usr/bin/env python3
"""
Test Runner for gRPC Service Discovery

This script provides a comprehensive test runner for the gRPC service discovery system.
It allows running different test suites and provides detailed reporting.
"""

import os
import sys
import time
import argparse
import subprocess
from typing import List, Dict, Any

# Add paths
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL')
sys.path.insert(0, '/home/fuisloy/data1tb/GossipFL/algorithms/RAFT_GossipFL')

class TestRunner:
    """Test runner for gRPC service discovery tests."""
    
    def __init__(self):
        self.test_suites = {
            'basic': {
                'file': 'test_grpc_service_discovery.py',
                'description': 'Basic gRPC service discovery tests'
            },
            'integration': {
                'file': 'test_gateway_integration.py',
                'description': 'Gateway integration tests'
            },
            'comprehensive': {
                'file': 'test_comprehensive_grpc_service_discovery.py',
                'description': 'Comprehensive service discovery tests'
            }
        }
        
        self.test_dir = '/home/fuisloy/data1tb/GossipFL/fedml_core/distributed/communication/grpc'
    
    def run_test_suite(self, suite_name: str) -> Dict[str, Any]:
        """Run a specific test suite."""
        if suite_name not in self.test_suites:
            raise ValueError(f"Unknown test suite: {suite_name}")
        
        suite_info = self.test_suites[suite_name]
        test_file = os.path.join(self.test_dir, suite_info['file'])
        
        if not os.path.exists(test_file):
            raise FileNotFoundError(f"Test file not found: {test_file}")
        
        print(f"🧪 Running {suite_info['description']}...")
        print(f"📁 Test file: {test_file}")
        print("-" * 80)
        
        start_time = time.time()
        
        try:
            # Run the test file
            result = subprocess.run(
                [sys.executable, test_file],
                cwd=self.test_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            return {
                'suite': suite_name,
                'description': suite_info['description'],
                'duration': duration,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'success': result.returncode == 0
            }
            
        except subprocess.TimeoutExpired:
            return {
                'suite': suite_name,
                'description': suite_info['description'],
                'duration': 300,
                'returncode': -1,
                'stdout': '',
                'stderr': 'Test suite timed out after 5 minutes',
                'success': False
            }
        except Exception as e:
            return {
                'suite': suite_name,
                'description': suite_info['description'],
                'duration': 0,
                'returncode': -1,
                'stdout': '',
                'stderr': str(e),
                'success': False
            }
    
    def run_all_suites(self) -> List[Dict[str, Any]]:
        """Run all test suites."""
        results = []
        
        for suite_name in self.test_suites:
            print(f"\n{'='*100}")
            print(f"🚀 Starting test suite: {suite_name.upper()}")
            print(f"{'='*100}")
            
            result = self.run_test_suite(suite_name)
            results.append(result)
            
            if result['success']:
                print(f"✅ Test suite '{suite_name}' PASSED in {result['duration']:.2f}s")
            else:
                print(f"❌ Test suite '{suite_name}' FAILED after {result['duration']:.2f}s")
                if result['stderr']:
                    print(f"Error: {result['stderr']}")
        
        return results
    
    def print_summary(self, results: List[Dict[str, Any]]):
        """Print test summary."""
        print("\n" + "="*100)
        print("📊 COMPREHENSIVE TEST SUMMARY")
        print("="*100)
        
        total_suites = len(results)
        passed_suites = sum(1 for r in results if r['success'])
        failed_suites = total_suites - passed_suites
        total_duration = sum(r['duration'] for r in results)
        
        print(f"🧪 Total test suites: {total_suites}")
        print(f"✅ Passed: {passed_suites}")
        print(f"❌ Failed: {failed_suites}")
        print(f"⏱️  Total execution time: {total_duration:.2f} seconds")
        
        if failed_suites > 0:
            print("\n❌ FAILED SUITES:")
            for result in results:
                if not result['success']:
                    print(f"  • {result['suite']}: {result['description']}")
                    if result['stderr']:
                        print(f"    Error: {result['stderr']}")
        
        print("\n📋 DETAILED RESULTS:")
        for result in results:
            status = "✅ PASSED" if result['success'] else "❌ FAILED"
            print(f"  {result['suite']:<15} | {status:<10} | {result['duration']:.2f}s | {result['description']}")
        
        print("\n" + "="*100)
        if failed_suites == 0:
            print("🎉 ALL TEST SUITES PASSED! gRPC Service Discovery system is fully validated.")
        else:
            print(f"⚠️  {failed_suites} test suite(s) failed. Please review the failures above.")
        print("="*100)
    
    def run_specific_tests(self, test_patterns: List[str]) -> bool:
        """Run specific tests matching patterns."""
        print(f"🎯 Running specific tests matching: {', '.join(test_patterns)}")
        
        # For now, run the comprehensive suite with all patterns
        # In a more advanced implementation, we could filter specific test methods
        result = self.run_test_suite('comprehensive')
        
        if result['success']:
            print("✅ Specific tests PASSED")
        else:
            print("❌ Specific tests FAILED")
            if result['stderr']:
                print(f"Error: {result['stderr']}")
        
        return result['success']


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(
        description='Test runner for gRPC Service Discovery',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_runner.py                           # Run all test suites
  python test_runner.py --suite basic            # Run basic tests only
  python test_runner.py --suite comprehensive    # Run comprehensive tests
  python test_runner.py --list                   # List available test suites
  python test_runner.py --pattern gateway        # Run tests matching 'gateway'
        """
    )
    
    parser.add_argument(
        '--suite', 
        choices=['basic', 'integration', 'comprehensive'],
        help='Run specific test suite'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available test suites'
    )
    
    parser.add_argument(
        '--pattern',
        nargs='+',
        help='Run tests matching specific patterns'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output'
    )
    
    args = parser.parse_args()
    
    runner = TestRunner()
    
    if args.list:
        print("Available test suites:")
        for name, info in runner.test_suites.items():
            print(f"  {name:<15} - {info['description']}")
        return
    
    print("🧪 gRPC Service Discovery Test Runner")
    print("=" * 100)
    
    if args.pattern:
        success = runner.run_specific_tests(args.pattern)
        sys.exit(0 if success else 1)
    
    elif args.suite:
        result = runner.run_test_suite(args.suite)
        
        if args.verbose:
            print(f"\nSTDOUT:\n{result['stdout']}")
            if result['stderr']:
                print(f"\nSTDERR:\n{result['stderr']}")
        
        if result['success']:
            print(f"\n✅ Test suite '{args.suite}' completed successfully!")
        else:
            print(f"\n❌ Test suite '{args.suite}' failed!")
            if result['stderr']:
                print(f"Error: {result['stderr']}")
        
        sys.exit(0 if result['success'] else 1)
    
    else:
        # Run all suites
        results = runner.run_all_suites()
        runner.print_summary(results)
        
        success = all(r['success'] for r in results)
        sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
