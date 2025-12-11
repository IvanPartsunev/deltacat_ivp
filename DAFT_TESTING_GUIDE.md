# Daft Distributed Runner Testing Guide

This guide explains the test setup for handling Daft distributed runner conflicts and how to reproduce the reported errors.

## Problem

The issue occurs when switching between Daft local native runner and Daft Ray distributed runners within the same test session. This causes errors like:

- `DaftError::InternalError Cannot set runner more than once`
- `Failed to submit task to actor ActorID(...) due to "Can't find actor ... It might be dead or it's from a different cluster"`

## Solution

We've implemented a simple approach to isolate distributed Daft tests:

1. **Separate test markers**: Added `@pytest.mark.distributed_daft` marker for tests requiring distributed Daft Ray runner
2. **Removed integration markers**: Distributed Daft tests no longer have `@pytest.mark.integration` so they run by default and reproduce errors
3. **CI exclusion**: Updated GitHub Actions to skip only distributed Daft tests during CI/CD
4. **Local testing tools**: Provided scripts to reproduce and test individual cases locally

## Test Markers

### New Marker: `distributed_daft`

Tests that require distributed Daft Ray runner are now marked with:
```python
@pytest.mark.distributed_daft
def test_download_delta_distributed_daft_basic(self):
    # Test implementation - WILL fail when run with other distributed_daft tests
```

### Integration Tests

Regular integration tests that don't use distributed Daft keep their integration marker:
```python
@pytest.mark.integration
def test_some_integration_feature(self):
    # Test implementation
```

## Affected Test Files

The following test files have been updated with `distributed_daft` markers (integration markers removed):

1. **deltacat/tests/storage/main/test_main_storage.py**
   - `test_download_delta_distributed_daft_basic`
   - `test_download_delta_distributed_daft_with_delta_locator`
   - `test_download_delta_distributed_daft_vs_ray_consistency`
   - `test_download_delta_distributed_daft_error_handling`

2. **deltacat/tests/utils/test_daft.py**
   - Entire `TestFilesToDataFrame` class (all methods)

3. **deltacat/tests/experimental/converter_agent/test_table_monitor.py**
   - Entire `TestTableMonitorEndToEnd` class

## Running Tests

### CI/CD (GitHub Actions)
```bash
# This is what runs in CI - excludes only distributed_daft tests
python -m pytest -m "not distributed_daft" --benchmark-json output.json
```

### Local Development

#### 1. All Tests Except Distributed Daft (Recommended for development)
```bash
python -m pytest -m "not distributed_daft"
# OR use the helper script
python run_tests.py unit
```

#### 2. Integration Tests Only
```bash
python -m pytest -m "integration"
# OR use the helper script
python run_tests.py integration
```

#### 3. Reproduce Distributed Daft Errors (WILL FAIL - this is expected!)
```bash
python -m pytest -m "distributed_daft"
# OR use the helper script
python run_tests.py distributed_daft
```

#### 4. Reproduce Specific Failing Tests (WILL FAIL - this is expected!)
```bash
# Use the helper script to run the exact failing tests from the issue
python run_tests.py failing_daft

# Or run them manually
python -m pytest \
  "deltacat/tests/storage/main/test_main_storage.py::TestDelta::test_download_delta_distributed_daft_basic" \
  "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame::test_read_local_files_all_columns" \
  -v
```

#### 5. All Tests (WILL FAIL due to distributed Daft conflicts)
```bash
python -m pytest
# OR use the helper script
python run_tests.py all
```

## Reproducing Errors Locally

### Method 1: Quick reproduction (shows the exact errors)
```bash
# This WILL fail and show the runner conflict errors
python -m pytest -m "distributed_daft" -v
```

### Method 2: Use the reproduction script
```bash
python reproduce_daft_errors.py
```

### Method 3: Use the interactive test script
```bash
./test_individual_daft.sh
```

### Method 4: Run tests by file (better isolation - may work individually)
```bash
# Storage tests (may work individually)
python -m pytest "deltacat/tests/storage/main/test_main_storage.py" -k "test_download_delta_distributed_daft" -v

# Utils tests (may work individually)
python -m pytest "deltacat/tests/utils/test_daft.py::TestFilesToDataFrame" -v

# Table monitor test (may work individually)
python -m pytest "deltacat/tests/experimental/converter_agent/test_table_monitor.py::TestTableMonitorEndToEnd::test_table_monitor_with_shared_catalog" -v
```

### Method 5: Individual test execution (best for debugging)
```bash
python -m pytest "deltacat/tests/storage/main/test_main_storage.py::TestDelta::test_download_delta_distributed_daft_basic" -v --tb=long
```

## Configuration Files Updated

1. **pytest.ini**: Added `distributed_daft` marker definition
2. **.github/workflows/ci.yml**: Updated to exclude only `distributed_daft` tests (not integration)
3. **New files created**:
   - `reproduce_daft_errors.py`: Automated reproduction script
   - `run_tests.py`: Flexible test runner with different categories
   - `test_individual_daft.sh`: Interactive script for individual test execution
   - `DAFT_TESTING_GUIDE.md`: This documentation

## Expected Behavior

- **CI/CD**: Passes without distributed Daft runner conflicts (skips distributed_daft tests)
- **Local default tests**: Run all tests including distributed_daft, WILL show runner conflicts (this reproduces the issue!)
- **Local unit tests**: Pass without distributed Daft runner conflicts when using `-m "not distributed_daft"`
- **Local integration tests**: Pass and include non-distributed_daft integration tests
- **Local distributed Daft tests**: WILL fail when run together due to runner conflicts, but may pass when run individually

## Key Changes from Previous Approach

1. **Removed `@pytest.mark.integration`** from distributed Daft tests
2. **Tests now run by default** and reproduce the errors
3. **CI only excludes `distributed_daft`** tests, not all integration tests
4. **Easier error reproduction** - just run `pytest` or `python -m pytest -m distributed_daft`

## Best Practices

1. **For CI/CD**: Use `-m "not distributed_daft"` to avoid runner conflicts
2. **For reproducing errors**: Run `python -m pytest -m distributed_daft` or `python run_tests.py distributed_daft`
3. **For local development**: Use `-m "not distributed_daft"` for clean test runs
4. **For debugging individual tests**: Run each test file or method separately
5. **For comprehensive testing**: Run test categories separately rather than all at once

## Troubleshooting

If you encounter runner conflicts:

1. **This is expected!** The distributed_daft tests are designed to show the runner conflict errors
2. **Run tests individually**: Each test file separately or even individual test methods
3. **Restart your Python session**: Between different test runs
4. **Use the provided scripts**: They handle isolation better than running pytest directly
5. **Check Ray cluster state**: Ensure Ray is properly shut down between test runs

## Future Improvements

Consider implementing test fixtures that properly manage Daft runner lifecycle to allow running distributed Daft tests together in the future.