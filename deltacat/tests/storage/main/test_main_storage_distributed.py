import shutil
import tempfile
import uuid

import pytest
import pyarrow as pa
import pandas as pd
import ray

from deltacat.exceptions import (
    SchemaValidationError,
)
from deltacat.storage import (
    metastore,
    LifecycleState,
    Schema,
    DeltaType,
)
from deltacat.types.media import (
    ContentType,
    DatasetType,
    StorageType,
    DistributedDatasetType,
)

from deltacat.tests.test_utils.storage import (
    create_test_namespace,
)
from deltacat.catalog import CatalogProperties
from daft import DataFrame as DaftDataFrame


class TestDeltaDistributed:
  @classmethod
  def setup_method(cls):
    ray.shutdown()
    from daft.runners import flotilla
    flotilla.FLOTILLA_RUNNER_NAMESPACE = f'daft-job-{uuid.uuid4()}'

    cls.tmpdir = tempfile.mkdtemp()
    cls.catalog = CatalogProperties(root=cls.tmpdir)

    # Create and commit namespace
    cls.namespace = create_test_namespace()
    metastore.create_namespace(
      namespace=cls.namespace.locator.namespace,
      catalog=cls.catalog,
    )

    # Create a schema for the table version
    arrow_schema = pa.schema(
      [
        ("id", pa.int64()),
        ("name", pa.string()),
        ("age", pa.int64()),
        ("city", pa.string()),
      ]
    )
    schema = Schema.of(
      schema=arrow_schema,
      schema_id=1,
    )

    # Create and commit table version with schema
    cls.table, cls.table_version, cls.stream = metastore.create_table_version(
      namespace=cls.namespace.locator.namespace,
      table_name="test_table_daft",
      table_version="v.1",
      schema=schema,
      catalog=cls.catalog,
    )

    # Make the table version active
    metastore.update_table_version(
      namespace=cls.namespace.locator.namespace,
      table_name="test_table_daft",
      table_version="v.1",
      lifecycle_state=LifecycleState.ACTIVE,
      catalog=cls.catalog,
    )

    # Stage and commit partition
    cls.partition = metastore.stage_partition(
      stream=cls.stream,
      catalog=cls.catalog,
    )
    cls.partition = metastore.commit_partition(
      partition=cls.partition,
      catalog=cls.catalog,
    )
    # Get the committed partition to ensure we have the latest state
    cls.partition = metastore.get_partition_by_id(
      stream_locator=cls.partition.stream_locator,
      partition_id=cls.partition.partition_id,
      catalog=cls.catalog,
    )

  @classmethod
  def teardown_method(cls):
    shutil.rmtree(cls.tmpdir)

  # ========== DAFT DISTRIBUTED TESTS ==========
  def test_download_delta_distributed_daft_basic(self):
    """Test basic distributed download with DAFT dataset type."""

    # Create test data
    test_data = pd.DataFrame(
      {
        "id": [12001, 12002, 12003, 12004, 12005],
        "name": ["DAFT_A", "DAFT_B", "DAFT_C", "DAFT_D", "DAFT_E"],
        "age": [21, 22, 23, 24, 25],
        "city": [
          "DaftCity1",
          "DaftCity2",
          "DaftCity3",
          "DaftCity4",
          "DaftCity5",
        ],
      }
    )

    staged_delta = metastore.stage_delta(
      data=test_data,
      partition=self.partition,
      catalog=self.catalog,
      content_type=ContentType.PARQUET,
      delta_type=DeltaType.UPSERT,
    )

    committed_delta = metastore.commit_delta(
      delta=staged_delta,
      catalog=self.catalog,
    )

    # Download using DAFT distributed dataset type
    distributed_dataset = metastore.download_delta(
      delta_like=committed_delta,
      table_type=DatasetType.PYARROW,
      storage_type=StorageType.DISTRIBUTED,
      distributed_dataset_type=DistributedDatasetType.DAFT,
      catalog=self.catalog,
    )

    # Verify the result is a DAFT DataFrame
    assert isinstance(distributed_dataset, DaftDataFrame), "Expected DAFT DataFrame"

    # Convert to pandas and verify data integrity
    downloaded_df = (
      distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
    )
    expected_df = test_data.sort_values("id").reset_index(drop=True)

    assert len(downloaded_df) == len(expected_df), "Row count mismatch"
    assert set(downloaded_df.columns) == set(
      expected_df.columns
    ), "Column names mismatch"
    pd.testing.assert_frame_equal(downloaded_df, expected_df)

  def test_download_delta_distributed_daft_with_delta_locator(self):
    """Test DAFT distributed download using DeltaLocator instead of Delta object."""

    test_data = pd.DataFrame(
      {
        "id": [12101, 12102, 12103],
        "name": ["DAFT_Locator_A", "DAFT_Locator_B", "DAFT_Locator_C"],
        "age": [30, 31, 32],
        "city": ["DaftLocCity1", "DaftLocCity2", "DaftLocCity3"],
      }
    )

    staged_delta = metastore.stage_delta(
      data=test_data,
      partition=self.partition,
      content_type=ContentType.PARQUET,
      delta_type=DeltaType.UPSERT,
      catalog=self.catalog,
    )
    committed_delta = metastore.commit_delta(
      delta=staged_delta,
      catalog=self.catalog,
    )

    # Download using DeltaLocator with DAFT
    distributed_dataset = metastore.download_delta(
      delta_like=committed_delta.locator,
      table_type=DatasetType.PYARROW,
      storage_type=StorageType.DISTRIBUTED,
      distributed_dataset_type=DistributedDatasetType.DAFT,
      catalog=self.catalog,
    )

    downloaded_df = (
      distributed_dataset.to_pandas().sort_values("id").reset_index(drop=True)
    )
    expected_df = test_data.sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(downloaded_df, expected_df)

  def test_download_delta_distributed_daft_vs_ray_consistency(self):
    """Test that DAFT and Ray distributed downloads return the same data."""

    test_data = pd.DataFrame(
      {
        "id": [12501, 12502, 12503, 12504],
        "name": [
          "Consistency_A",
          "Consistency_B",
          "Consistency_C",
          "Consistency_D",
        ],
        "age": [60, 61, 62, 63],
        "city": [
          "ConsistencyCity1",
          "ConsistencyCity2",
          "ConsistencyCity3",
          "ConsistencyCity4",
        ],
      }
    )

    staged_delta = metastore.stage_delta(
      data=test_data,
      partition=self.partition,
      catalog=self.catalog,
      content_type=ContentType.PARQUET,
      delta_type=DeltaType.UPSERT,
    )
    committed_delta = metastore.commit_delta(
      delta=staged_delta,
      catalog=self.catalog,
    )

    # Download using DAFT
    daft_dataset = metastore.download_delta(
      delta_like=committed_delta,
      table_type=DatasetType.PYARROW,
      storage_type=StorageType.DISTRIBUTED,
      distributed_dataset_type=DistributedDatasetType.DAFT,
      catalog=self.catalog,
    )

    # Download using Ray
    ray_dataset = metastore.download_delta(
      delta_like=committed_delta,
      table_type=DatasetType.PYARROW,
      storage_type=StorageType.DISTRIBUTED,
      distributed_dataset_type=DistributedDatasetType.RAY_DATASET,
      catalog=self.catalog,
    )

    # Compare results
    daft_df = daft_dataset.to_pandas().sort_values("id").reset_index(drop=True)
    ray_df = ray_dataset.to_pandas().sort_values("id").reset_index(drop=True)
    pd.testing.assert_frame_equal(daft_df, ray_df)

  def test_download_delta_distributed_daft_error_handling(self):
    """Test error handling in DAFT distributed downloads."""

    test_data = pd.DataFrame(
      {
        "id": [12601, 12602, 12603],
        "name": ["DaftError_A", "DaftError_B", "DaftError_C"],
        "age": [70, 71, 72],
        "city": ["DaftErrorCity1", "DaftErrorCity2", "DaftErrorCity3"],
      }
    )

    staged_delta = metastore.stage_delta(
      data=test_data,
      partition=self.partition,
      catalog=self.catalog,
      content_type=ContentType.PARQUET,
      delta_type=DeltaType.UPSERT,
    )
    committed_delta = metastore.commit_delta(
      delta=staged_delta,
      catalog=self.catalog,
    )

    # Test with invalid column names using DAFT
    with pytest.raises(
      SchemaValidationError,
      match="One or more columns .* are not present in table version columns",
    ):
      metastore.download_delta(
        delta_like=committed_delta,
        table_type=DatasetType.PYARROW,
        storage_type=StorageType.DISTRIBUTED,
        columns=["id", "invalid_column"],
        distributed_dataset_type=DistributedDatasetType.DAFT,
        catalog=self.catalog,
      )