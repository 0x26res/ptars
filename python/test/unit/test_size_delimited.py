"""Tests for size-delimited protobuf reading."""

import io
from pathlib import Path

import pyarrow as pa
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from ptars import HandlerPool, PtarsConfig
from ptars_protos.simple_pb2 import DESCRIPTOR, SearchRequest, WithTimestamp


def _write_varint(stream: io.BytesIO, value: int) -> None:
    """Write a varint to a stream."""
    while value > 0x7F:
        stream.write(bytes([0x80 | (value & 0x7F)]))
        value >>= 7
    stream.write(bytes([value]))


def _create_size_delimited_bytes(messages: list[bytes]) -> bytes:
    """Create size-delimited protobuf bytes from a list of message bytes."""
    stream = io.BytesIO()
    for message_bytes in messages:
        _write_varint(stream, len(message_bytes))
        stream.write(message_bytes)
    return stream.getvalue()


class TestReadSizeDelimitedFile:
    """Tests for HandlerPool.read_size_delimited_file method."""

    def test_single_message(self, tmp_path: Path):
        """Read a single size-delimited message."""
        msg = SearchRequest(query="test", page_number=1, result_per_page=10)
        data = _create_size_delimited_bytes([msg.SerializeToString()])

        file_path = tmp_path / "messages.bin"
        file_path.write_bytes(data)

        pool = HandlerPool([DESCRIPTOR])
        batch = pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 1
        assert batch["query"].to_pylist() == ["test"]
        assert batch["page_number"].to_pylist() == [1]
        assert batch["result_per_page"].to_pylist() == [10]

    def test_multiple_messages(self, tmp_path: Path):
        """Read multiple size-delimited messages."""
        msgs = [
            SearchRequest(query="hello", page_number=1, result_per_page=10),
            SearchRequest(query="world", page_number=2, result_per_page=20),
            SearchRequest(query="test", page_number=3, result_per_page=30),
        ]
        data = _create_size_delimited_bytes([m.SerializeToString() for m in msgs])

        file_path = tmp_path / "messages.bin"
        file_path.write_bytes(data)

        pool = HandlerPool([DESCRIPTOR])
        batch = pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 3
        assert batch["query"].to_pylist() == ["hello", "world", "test"]
        assert batch["page_number"].to_pylist() == [1, 2, 3]
        assert batch["result_per_page"].to_pylist() == [10, 20, 30]

    def test_empty_file(self, tmp_path: Path):
        """Empty file returns empty RecordBatch."""
        file_path = tmp_path / "empty.bin"
        file_path.write_bytes(b"")

        pool = HandlerPool([DESCRIPTOR])
        batch = pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

        assert isinstance(batch, pa.RecordBatch)
        assert batch.num_rows == 0

    def test_string_path(self, tmp_path: Path):
        """Read using a string path."""
        msg = SearchRequest(query="test")
        data = _create_size_delimited_bytes([msg.SerializeToString()])

        file_path = tmp_path / "messages.bin"
        file_path.write_bytes(data)

        pool = HandlerPool([DESCRIPTOR])
        batch = pool.read_size_delimited_file(str(file_path), SearchRequest.DESCRIPTOR)

        assert batch.num_rows == 1

    def test_large_message(self, tmp_path: Path):
        """Handle messages with sizes requiring multi-byte varints."""
        # Create a message larger than 127 bytes
        msg = SearchRequest(query="x" * 200)
        data = _create_size_delimited_bytes([msg.SerializeToString()])

        file_path = tmp_path / "messages.bin"
        file_path.write_bytes(data)

        pool = HandlerPool([DESCRIPTOR])
        batch = pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

        assert batch.num_rows == 1
        assert batch["query"].to_pylist() == ["x" * 200]

    def test_file_not_found(self, tmp_path: Path):
        """Non-existent file raises error."""
        pool = HandlerPool([DESCRIPTOR])
        with pytest.raises(OSError):
            pool.read_size_delimited_file(
                tmp_path / "nonexistent.bin", SearchRequest.DESCRIPTOR
            )

    def test_truncated_message(self, tmp_path: Path):
        """Truncated message data raises error."""
        # Varint says 10 bytes, but only 5 are present
        file_path = tmp_path / "truncated.bin"
        file_path.write_bytes(bytes([10]) + b"hello")

        pool = HandlerPool([DESCRIPTOR])
        with pytest.raises(OSError):
            pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

    def test_truncated_varint(self, tmp_path: Path):
        """Truncated varint (continuation bit set but EOF) raises error."""
        # 0x80 has continuation bit set, but no following byte
        file_path = tmp_path / "truncated_varint.bin"
        file_path.write_bytes(bytes([0x80]))

        pool = HandlerPool([DESCRIPTOR])
        with pytest.raises(OSError, match="unexpected EOF"):
            pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

    def test_varint_too_large(self, tmp_path: Path):
        """Varint exceeding 64 bits raises error."""
        # 10 bytes with continuation bits set = > 64 bits
        file_path = tmp_path / "large_varint.bin"
        file_path.write_bytes(bytes([0x80] * 10 + [0x01]))

        pool = HandlerPool([DESCRIPTOR])
        with pytest.raises(OSError, match="varint too large"):
            pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

    def test_message_size_exceeds_limit(self, tmp_path: Path):
        """Message size exceeding limit raises error."""
        # Write a varint representing a huge size (100MB > 64MB limit)
        file_path = tmp_path / "huge_size.bin"
        stream = io.BytesIO()
        _write_varint(stream, 100 * 1024 * 1024)  # 100 MB
        file_path.write_bytes(stream.getvalue())

        pool = HandlerPool([DESCRIPTOR])
        with pytest.raises(OSError, match="exceeds maximum"):
            pool.read_size_delimited_file(file_path, SearchRequest.DESCRIPTOR)

    def test_config_is_respected(self, tmp_path: Path):
        """Verify that read_size_delimited_file uses the handler's config."""
        # Create a timestamp message
        msg = WithTimestamp(
            timestamp=Timestamp(seconds=1704067200, nanos=0)  # 2024-01-01 00:00:00 UTC
        )
        data = _create_size_delimited_bytes([msg.SerializeToString()])

        file_path = tmp_path / "timestamps.bin"
        file_path.write_bytes(data)

        # Test with default config (nanoseconds, UTC timezone)
        pool_default = HandlerPool([DESCRIPTOR])
        batch_default = pool_default.read_size_delimited_file(
            file_path, WithTimestamp.DESCRIPTOR
        )
        ts_type_default = batch_default.schema.field("timestamp").type
        assert ts_type_default == pa.timestamp("ns", tz="UTC")

        # Test with custom config (seconds, America/New_York timezone)
        config = PtarsConfig(timestamp_unit="s", timestamp_tz="America/New_York")
        pool_custom = HandlerPool([DESCRIPTOR], config=config)
        batch_custom = pool_custom.read_size_delimited_file(
            file_path, WithTimestamp.DESCRIPTOR
        )
        ts_type_custom = batch_custom.schema.field("timestamp").type
        assert ts_type_custom == pa.timestamp("s", tz="America/New_York")

        # Verify the value is the same (just in different units)
        # Default (ns): 1704067200 * 1e9 = 1704067200000000000
        # Custom (s): 1704067200
        assert batch_default["timestamp"][0].as_py().timestamp() == pytest.approx(
            1704067200, abs=1
        )
        assert batch_custom["timestamp"][0].as_py().timestamp() == pytest.approx(
            1704067200, abs=1
        )
