#!/usr/bin/env python3
"""
Test runner script for deltacat with different test categories.

This script provides easy commands to run different categories of tests,
including the ability to reproduce the distributed Daft runner errors.
"""

import subprocess
import sys
import argparse


def run_command(cmd, description):
    """Run a command and display results."""
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running command: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run deltacat tests")
    parser.add_argument(
        "test_type",
        choices=[
            "unit", 
            "integration", 
            "distributed_daft", 
            "failing_daft", 
            "ci", 
            "all"
        ],
        help="Type of tests to run"
    )
    parser.add_argument(
        "-v", "--verbose", 
        action="store_true", 
        help="Verbose output"
    )
    parser.add_argument(
        "--tb", 
        choices=["short", "long", "line", "native", "no"],
        default="short",
        help="Traceback format"
    )
    
    args = parser.parse_args()
    
    base_cmd = [sys.executable, "-m", "pytest"]
    if args.verbose:
        base_cmd.append("-v")
    base_cmd.extend(["--tb", args.tb])
    
    if args.test_type == "unit":
        cmd = base_cmd + ["-m", "not distributed_daft"]
        description = "Running unit and integration tests (excluding distributed_daft)"
        
    elif args.test_type == "integration":
        cmd = base_cmd + ["-m", "integration"]
        description = "Running integration tests (including non-distributed_daft integration tests)"
        
    elif args.test_type == "distributed_daft":
        cmd = base_cmd + ["-m", "distributed_daft"]
        description = "Running distributed Daft tests (these WILL fail due to runner conflicts when run together)"
        
    elif args.test_type == "failing_daft":
        # Specific failing tests mentioned in the issue
        failing_tests = [
            "deltacat/tests/storage/main/test_main_storage.py::TestDelta::test_download_delta_distributed_daft_basic",
            "deltacat/tests/storage/main/test_main_storage.py::TestDelta::test_download_delta_distributed_daft_with_delta_locator", 
            "deltacat/tests/storage/main/test_main_storage.py::TestDelta::test_download_delta_distributed_daft_vs_ray_consistency",
            "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_accepts_custom_kwargs",
            "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_accepts_io_config",
            "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_read_local_files_all_columns",
            "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_read_local_files_does_not_materialize_by_default",
            "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_read_local_files_with_column_selection",
            "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_supports_gzip_content_encoding",
            "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_supports_unescaped_tsv_content_type",
            "deltacat/tests/experimental/converter_agent/test_table_monitor.py::TestTableMonitorEndToEnd::test_table_monitor_with_shared_catalog"
        ]
        cmd = base_cmd + failing_tests
        description = "Running specific failing distributed Daft tests (WILL show runner conflict errors)"
        
    elif args.test_type == "ci":
        cmd = base_cmd + ["-m", "not distributed_daft", "--benchmark-json", "output.json"]
        description = "Running CI tests (same as GitHub Actions)"
        
    elif args.test_type == "all":
        cmd = base_cmd
        description = "Running all tests (including distributed_daft - WILL fail due to runner conflicts)"
    
    success = run_command(cmd, description)
    
    if args.test_type == "distributed_daft" or args.test_type == "failing_daft" or args.test_type == "all":
        print(f"\n{'='*60}")
        print("EXPECTED BEHAVIOR:")
        print("- These tests WILL fail when run together due to Daft runner conflicts")
        print("- This is the exact issue we're trying to solve!")
        print("- Expected errors:")
        print("  * 'DaftError::InternalError Cannot set runner more than once'")
        print("  * 'Failed to submit task to actor ActorID(...)'")
        print("")
        print("TO REPRODUCE ERRORS:")
        print("- Run: python -m pytest -m distributed_daft")
        print("- Or: python run_tests.py distributed_daft")
        print("")
        print("FOR INDIVIDUAL TESTING (should work):")
        print("- Run each test file separately:")
        print("  python -m pytest deltacat/tests/storage/main/test_main_storage.py -k distributed_daft -v")
        print("  python -m pytest deltacat/tests/utils/test_daft.py::TestFilesToDataFrame -v")
        print("- Use: ./test_individual_daft.sh")
        print(f"{'='*60}")
    
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)