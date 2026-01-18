"""Unit tests for TPC-DI ETL batch processing.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from benchbox.core.tpcdi.etl.batch import BatchProcessor, BatchStatus, BatchType

pytestmark = pytest.mark.fast


class TestBatchProcessorHistoricalLoad:
    """Test BatchProcessor.process_historical_load() method."""

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_calls_process_batch(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load calls process_batch with correct parameters."""
        # Setup
        processor = BatchProcessor()
        source_config = {"data_dir": "/path/to/source"}
        target_config = {"connection": "db_connection"}

        # Mock the process_batch return value
        expected_status = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )
        mock_process_batch.return_value = expected_status

        # Execute
        result = processor.process_historical_load(source_config, target_config)

        # Verify process_batch was called with correct arguments
        mock_process_batch.assert_called_once()
        call_args = mock_process_batch.call_args

        assert call_args.kwargs["batch_id"] == 1
        assert call_args.kwargs["batch_type"] == BatchType.HISTORICAL
        assert call_args.kwargs["target_config"] == target_config

        # Verify source_config was modified correctly
        source_config_arg = call_args.kwargs["source_config"]
        assert source_config_arg["load_type"] == "historical"
        assert source_config_arg["full_refresh"] is True
        assert source_config_arg["scd_processing"] is False
        assert source_config_arg["batch_date"] is None

        # Verify return value
        assert result == expected_status

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_does_not_modify_original_config(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load does not modify the original source_config."""
        # Setup
        processor = BatchProcessor()
        source_config = {"data_dir": "/path/to/source", "existing_key": "existing_value"}
        target_config = {"connection": "db_connection"}

        original_source_config = source_config.copy()

        # Mock return value
        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify original config was not modified
        assert source_config == original_source_config
        assert "load_type" not in source_config
        assert "full_refresh" not in source_config

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_preserves_original_config_values(self, mock_process_batch: MagicMock) -> None:
        """Test that historical config preserves values from original source_config."""
        # Setup
        processor = BatchProcessor()
        source_config = {
            "data_dir": "/path/to/source",
            "encoding": "utf-8",
            "delimiter": "|",
        }
        target_config = {"connection": "db_connection"}

        # Mock return value
        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify historical config includes original values plus new ones
        call_args = mock_process_batch.call_args
        historical_config = call_args.kwargs["source_config"]

        assert historical_config["data_dir"] == "/path/to/source"
        assert historical_config["encoding"] == "utf-8"
        assert historical_config["delimiter"] == "|"
        assert historical_config["load_type"] == "historical"
        assert historical_config["full_refresh"] is True

    @patch.object(BatchProcessor, "process_batch")
    @patch("benchbox.core.tpcdi.etl.batch.logger")
    def test_process_historical_load_logs_start_message(
        self, mock_logger: MagicMock, mock_process_batch: MagicMock
    ) -> None:
        """Test that process_historical_load logs appropriate start message."""
        # Setup
        processor = BatchProcessor()
        source_config = {"data_dir": "/path/to/source"}
        target_config = {"connection": "db_connection"}

        # Mock return value
        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify logger was called
        mock_logger.info.assert_called_once_with("Starting TPC-DI historical load (Batch 1)")

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_returns_batch_status(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load returns BatchStatus from process_batch."""
        # Setup
        processor = BatchProcessor()
        source_config = {"data_dir": "/path/to/source"}
        target_config = {"connection": "db_connection"}

        # Create a specific BatchStatus to return
        expected_status = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )
        expected_status.records_processed = 50000
        expected_status.end_time = datetime.now()

        mock_process_batch.return_value = expected_status

        # Execute
        result = processor.process_historical_load(source_config, target_config)

        # Verify
        assert result is expected_status
        assert result.batch_id == 1
        assert result.batch_type == BatchType.HISTORICAL
        assert result.records_processed == 50000

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_batch_type_is_historical(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load uses HISTORICAL batch type."""
        # Setup
        processor = BatchProcessor()
        source_config = {}
        target_config = {}

        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify batch_type is HISTORICAL
        call_args = mock_process_batch.call_args
        assert call_args.kwargs["batch_type"] == BatchType.HISTORICAL

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_batch_id_is_one(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load uses batch_id=1 (TPC-DI Batch 1)."""
        # Setup
        processor = BatchProcessor()
        source_config = {}
        target_config = {}

        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify batch_id is 1
        call_args = mock_process_batch.call_args
        assert call_args.kwargs["batch_id"] == 1

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_scd_processing_disabled(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load disables SCD processing for initial load."""
        # Setup
        processor = BatchProcessor()
        source_config = {}
        target_config = {}

        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify SCD processing is disabled
        call_args = mock_process_batch.call_args
        historical_config = call_args.kwargs["source_config"]
        assert historical_config["scd_processing"] is False

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_full_refresh_enabled(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load enables full_refresh for initial load."""
        # Setup
        processor = BatchProcessor()
        source_config = {}
        target_config = {}

        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify full_refresh is enabled
        call_args = mock_process_batch.call_args
        historical_config = call_args.kwargs["source_config"]
        assert historical_config["full_refresh"] is True

    @patch.object(BatchProcessor, "process_batch")
    def test_process_historical_load_batch_date_is_none(self, mock_process_batch: MagicMock) -> None:
        """Test that process_historical_load sets batch_date to None (not applicable for historical)."""
        # Setup
        processor = BatchProcessor()
        source_config = {}
        target_config = {}

        mock_process_batch.return_value = BatchStatus(
            batch_id=1,
            batch_type=BatchType.HISTORICAL,
            start_time=datetime.now(),
        )

        # Execute
        processor.process_historical_load(source_config, target_config)

        # Verify batch_date is None
        call_args = mock_process_batch.call_args
        historical_config = call_args.kwargs["source_config"]
        assert historical_config["batch_date"] is None
