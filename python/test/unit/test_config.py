import datetime
import zoneinfo

import pyarrow as pa
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.timeofday_pb2 import TimeOfDay

from ptars import HandlerPool, PtarsConfig
from ptars_protos.simple_pb2 import (
    DESCRIPTOR,
    WithDuration,
    WithMap,
    WithTimeOfDay,
    WithTimestamp,
)


class TestTimestampConfig:
    """Test timestamp time unit configuration."""

    def test_timestamp_nanoseconds_default(self):
        """Default config uses nanoseconds."""
        pool = HandlerPool([DESCRIPTOR])
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert batch.schema.field("timestamp").type == pa.timestamp("ns", tz="UTC")
        # Roundtrip should preserve full precision
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back == messages

    def test_timestamp_nanoseconds_explicit(self):
        """Explicit nanosecond config."""
        config = PtarsConfig(timestamp_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert batch.schema.field("timestamp").type == pa.timestamp("ns", tz="UTC")
        # Roundtrip should preserve full precision
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back == messages

    def test_timestamp_microseconds(self):
        """Microsecond timestamp config."""
        config = PtarsConfig(timestamp_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert batch.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert batch["timestamp"].to_pylist() == [
            datetime.datetime(
                1970, 1, 1, 0, 0, 1, 500000, tzinfo=zoneinfo.ZoneInfo(key="UTC")
            )
        ]
        # Roundtrip preserves microsecond precision
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back == messages

    def test_timestamp_milliseconds(self):
        """Millisecond timestamp config."""
        config = PtarsConfig(timestamp_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # Use a value that has exact millisecond precision
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert batch.schema.field("timestamp").type == pa.timestamp("ms", tz="UTC")
        # Roundtrip preserves millisecond precision
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back == messages

    def test_timestamp_seconds(self):
        """Second timestamp config."""
        config = PtarsConfig(timestamp_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # Use a value with only second precision for exact roundtrip
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=0))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert batch.schema.field("timestamp").type == pa.timestamp("s", tz="UTC")
        # Roundtrip preserves second precision
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back == messages

    def test_timestamp_no_timezone(self):
        """Timestamp without timezone."""
        config = PtarsConfig(timestamp_tz=None, timestamp_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=0))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert batch.schema.field("timestamp").type == pa.timestamp("ns")
        assert batch.schema.field("timestamp").type.tz is None
        # Roundtrip should preserve the value
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back == messages

    def test_timestamp_custom_timezone(self):
        """Timestamp with custom timezone."""
        config = PtarsConfig(timestamp_tz="America/New_York")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=0))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert batch.schema.field("timestamp").type == pa.timestamp(
            "ns", tz="America/New_York"
        )
        # Roundtrip should preserve the value
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back == messages


class TestDurationConfig:
    """Test duration time unit configuration."""

    def test_duration_nanoseconds_default(self):
        """Default config uses nanoseconds."""
        pool = HandlerPool([DESCRIPTOR])
        messages = [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        assert batch.schema.field("duration").type == pa.duration("ns")
        # Roundtrip should preserve full precision
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back == messages

    def test_duration_nanoseconds_explicit(self):
        """Explicit nanosecond config."""
        config = PtarsConfig(duration_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        assert batch.schema.field("duration").type == pa.duration("ns")
        # Roundtrip should preserve full precision
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back == messages

    def test_duration_microseconds(self):
        """Microsecond duration config."""
        config = PtarsConfig(duration_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        assert batch.schema.field("duration").type == pa.duration("us")
        # Roundtrip preserves microsecond precision
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back == messages

    def test_duration_milliseconds(self):
        """Millisecond duration config."""
        config = PtarsConfig(duration_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # Use a value with exact millisecond precision
        messages = [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        assert batch.schema.field("duration").type == pa.duration("ms")
        # Roundtrip preserves millisecond precision
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back == messages

    def test_duration_seconds(self):
        """Second duration config."""
        config = PtarsConfig(duration_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # Use a value with only second precision for exact roundtrip
        messages = [WithDuration(duration=Duration(seconds=1, nanos=0))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        assert batch.schema.field("duration").type == pa.duration("s")
        # Roundtrip preserves second precision
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back == messages

    def test_duration_value_conversion_nanoseconds(self):
        """Test that duration values are correctly converted to nanoseconds."""
        pool = HandlerPool([DESCRIPTOR])
        messages = [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        # 1.5 seconds = 1,500,000,000 nanoseconds
        assert batch["duration"].to_pylist() == [
            datetime.timedelta(seconds=1, microseconds=500_000)
        ]
        # Roundtrip
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back == messages

    def test_duration_value_conversion_seconds(self):
        """Test that duration values are correctly converted to seconds."""
        config = PtarsConfig(duration_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [WithDuration(duration=Duration(seconds=3600, nanos=0))]  # 1 hour
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        assert batch["duration"].to_pylist() == [datetime.timedelta(hours=1)]
        # Roundtrip
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back == messages

    @pytest.mark.parametrize(
        ("duration_unit", "expected_type"),
        [
            ("s", pa.duration("s")),
            ("ms", pa.duration("ms")),
            ("us", pa.duration("us")),
            ("ns", pa.duration("ns")),
        ],
    )
    def test_duration_arrow_type(self, duration_unit, expected_type):
        """Verify correct duration type for each unit."""
        config = PtarsConfig(duration_unit=duration_unit)
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=1))],
            WithDuration.DESCRIPTOR,
        )
        assert batch.schema.field("duration").type == expected_type


class TestTimeOfDayConfig:
    """Test time of day time unit configuration."""

    def test_time_of_day_nanoseconds_default(self):
        """Default config uses nanoseconds."""
        pool = HandlerPool([DESCRIPTOR])
        messages = [
            WithTimeOfDay(
                time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000)
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithTimeOfDay.DESCRIPTOR)
        assert batch.schema.field("time_of_day").type == pa.time64("ns")
        # Roundtrip should preserve full precision
        messages_back = pool.record_batch_to_messages(batch, WithTimeOfDay.DESCRIPTOR)
        assert messages_back == messages

    def test_time_of_day_nanoseconds_explicit(self):
        """Explicit nanosecond config."""
        config = PtarsConfig(time_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [
            WithTimeOfDay(
                time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000)
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithTimeOfDay.DESCRIPTOR)
        assert batch.schema.field("time_of_day").type == pa.time64("ns")
        # Roundtrip should preserve full precision
        messages_back = pool.record_batch_to_messages(batch, WithTimeOfDay.DESCRIPTOR)
        assert messages_back == messages

    def test_time_of_day_microseconds(self):
        """Microsecond time config."""
        config = PtarsConfig(time_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        messages = [
            WithTimeOfDay(
                time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000)
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithTimeOfDay.DESCRIPTOR)
        assert batch.schema.field("time_of_day").type == pa.time64("us")
        # Roundtrip preserves microsecond precision
        messages_back = pool.record_batch_to_messages(batch, WithTimeOfDay.DESCRIPTOR)
        assert messages_back == messages

    def test_time_of_day_milliseconds(self):
        """Millisecond time config."""
        config = PtarsConfig(time_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # Use a value with exact millisecond precision
        messages = [
            WithTimeOfDay(
                time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000)
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithTimeOfDay.DESCRIPTOR)
        assert batch.schema.field("time_of_day").type == pa.time32("ms")
        # Roundtrip preserves millisecond precision
        messages_back = pool.record_batch_to_messages(batch, WithTimeOfDay.DESCRIPTOR)
        assert messages_back == messages

    def test_time_of_day_seconds(self):
        """Second time config."""
        config = PtarsConfig(time_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # Use a value with only second precision for exact roundtrip
        messages = [
            WithTimeOfDay(time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=0))
        ]
        batch = pool.messages_to_record_batch(messages, WithTimeOfDay.DESCRIPTOR)
        assert batch.schema.field("time_of_day").type == pa.time32("s")
        # Roundtrip preserves second precision
        messages_back = pool.record_batch_to_messages(batch, WithTimeOfDay.DESCRIPTOR)
        assert messages_back == messages

    def test_time_of_day_value_conversion(self):
        """Test that time values are correctly converted."""
        # Test with a simple value: 01:02:03.5
        time_of_day = TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000)
        expected_time = datetime.time(
            1, 2, 3, 500_000
        )  # microsecond precision in Python

        pool = HandlerPool([DESCRIPTOR])
        messages = [WithTimeOfDay(time_of_day=time_of_day)]
        batch = pool.messages_to_record_batch(messages, WithTimeOfDay.DESCRIPTOR)
        # PyArrow converts to datetime.time
        assert batch["time_of_day"].to_pylist() == [expected_time]
        # Roundtrip
        messages_back = pool.record_batch_to_messages(batch, WithTimeOfDay.DESCRIPTOR)
        assert messages_back == messages

    @pytest.mark.parametrize(
        ("time_unit", "expected_type"),
        [
            ("s", pa.time32("s")),
            ("ms", pa.time32("ms")),
            ("us", pa.time64("us")),
            ("ns", pa.time64("ns")),
        ],
    )
    def test_time_of_day_arrow_type(self, time_unit, expected_type):
        """Verify time32 is used for s/ms and time64 for us/ns."""
        config = PtarsConfig(time_unit=time_unit)
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=TimeOfDay(hours=12, minutes=30, seconds=45))],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == expected_type


class TestTimestampTruncation:
    """Test timestamp truncation with coarser units."""

    def test_timestamp_truncation_to_seconds(self):
        """Timestamp with 999ms is truncated to 1 second, not rounded to 2."""
        config = PtarsConfig(timestamp_unit="s", timestamp_tz=None)
        pool = HandlerPool([DESCRIPTOR], config=config)
        # 1 second + 999,999,999 nanoseconds = 1.999999999 seconds
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=999_999_999))],
            WithTimestamp.DESCRIPTOR,
        )
        # Should be truncated to 1 second, not rounded to 2
        assert batch["timestamp"].to_pylist() == [
            datetime.datetime(1970, 1, 1, 0, 0, 1)
        ]

    def test_timestamp_truncation_to_milliseconds(self):
        """Timestamp with 999us is truncated, not rounded."""
        config = PtarsConfig(timestamp_unit="ms", timestamp_tz=None)
        pool = HandlerPool([DESCRIPTOR], config=config)
        # 1 second + 999,999,999 nanoseconds
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=999_999_999))],
            WithTimestamp.DESCRIPTOR,
        )
        # Should be 1999ms (1.999s), losing the 999,999 nanoseconds
        expected = datetime.datetime(1970, 1, 1, 0, 0, 1, 999_000)
        assert batch["timestamp"].to_pylist() == [expected]

    def test_timestamp_truncation_to_microseconds(self):
        """Timestamp with 999ns is truncated, not rounded."""
        config = PtarsConfig(timestamp_unit="us", timestamp_tz=None)
        pool = HandlerPool([DESCRIPTOR], config=config)
        # 1 second + 999,999,999 nanoseconds
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=999_999_999))],
            WithTimestamp.DESCRIPTOR,
        )
        # Should be 1,999,999us (1.999999s), losing the 999 nanoseconds
        expected = datetime.datetime(1970, 1, 1, 0, 0, 1, 999_999)
        assert batch["timestamp"].to_pylist() == [expected]


class TestTimeOfDayTruncation:
    """Test time of day truncation with coarser units."""

    def test_time_of_day_truncation_to_seconds(self):
        """Time with 999ms is truncated to whole seconds, not rounded."""
        config = PtarsConfig(time_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # 01:02:03.999999999
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=999_999_999
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        # Should be truncated to 01:02:03, not rounded to 01:02:04
        assert batch["time_of_day"].to_pylist() == [datetime.time(1, 2, 3)]

    def test_time_of_day_truncation_to_milliseconds(self):
        """Time with 999us is truncated, not rounded."""
        config = PtarsConfig(time_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # 01:02:03.999999999
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=999_999_999
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        # Should be 01:02:03.999, losing the 999,999 nanoseconds
        assert batch["time_of_day"].to_pylist() == [datetime.time(1, 2, 3, 999_000)]

    def test_time_of_day_truncation_to_microseconds(self):
        """Time with 999ns is truncated, not rounded."""
        config = PtarsConfig(time_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        # 01:02:03.999999999
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=999_999_999
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        # Should be 01:02:03.999999, losing the 999 nanoseconds
        assert batch["time_of_day"].to_pylist() == [datetime.time(1, 2, 3, 999_999)]


class TestConfigValidation:
    """Test PtarsConfig validation."""

    def test_valid_config(self):
        """Valid config should not raise."""
        config = PtarsConfig(
            timestamp_tz="UTC",
            timestamp_unit="us",
            time_unit="ms",
            duration_unit="s",
            list_value_name="element",
            list_nullable=True,
            map_nullable=True,
            list_value_nullable=True,
            map_value_nullable=True,
        )
        assert config.timestamp_unit == "us"

    def test_valid_config_no_timezone(self):
        """Config with None timezone should not raise."""
        config = PtarsConfig(timestamp_tz=None)
        assert config.timestamp_tz is None

    @pytest.mark.parametrize(
        ("kwargs", "error_type", "error_match"),
        [
            (
                {"timestamp_unit": "invalid"},
                ValueError,
                "timestamp_unit must be one of",
            ),
            ({"time_unit": "hours"}, ValueError, "time_unit must be one of"),
            ({"duration_unit": "days"}, ValueError, "duration_unit must be one of"),
            ({"timestamp_tz": 123}, TypeError, "timestamp_tz must be str"),
            ({"list_value_name": 123}, TypeError, "list_value_name must be str"),
            ({"list_nullable": "true"}, TypeError, "list_nullable must be bool"),
            ({"map_nullable": 1}, TypeError, "map_nullable must be bool"),
            (
                {"list_value_nullable": "false"},
                TypeError,
                "list_value_nullable must be bool",
            ),
            ({"map_value_nullable": 0}, TypeError, "map_value_nullable must be bool"),
        ],
    )
    def test_invalid_config(self, kwargs, error_type, error_match):
        """Invalid config values should raise appropriate errors."""
        with pytest.raises(error_type, match=error_match):
            PtarsConfig(**kwargs)


class TestListConfig:
    """Test list field name configuration."""

    def test_list_value_name_default(self):
        """Default list item name is 'item'."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamps=[Timestamp(seconds=1)])],
            WithTimestamp.DESCRIPTOR,
        )
        list_type = batch.schema.field("timestamps").type
        assert isinstance(list_type, pa.ListType)
        assert list_type.value_field.name == "item"

    def test_list_value_name_custom(self):
        """Custom list item name."""
        config = PtarsConfig(list_value_name="element")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamps=[Timestamp(seconds=1)])],
            WithTimestamp.DESCRIPTOR,
        )
        list_type = batch.schema.field("timestamps").type
        assert isinstance(list_type, pa.ListType)
        assert list_type.value_field.name == "element"

    def test_list_value_nullable_default(self):
        """Default list value is not nullable."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamps=[Timestamp(seconds=1)])],
            WithTimestamp.DESCRIPTOR,
        )
        list_type = batch.schema.field("timestamps").type
        assert list_type.value_field.nullable is False

    def test_list_value_nullable_true(self):
        """List value can be set to nullable."""
        config = PtarsConfig(list_value_nullable=True)
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamps=[Timestamp(seconds=1)])],
            WithTimestamp.DESCRIPTOR,
        )
        list_type = batch.schema.field("timestamps").type
        assert list_type.value_field.nullable is True

    def test_list_nullable_default(self):
        """Default list field is not nullable."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamps=[Timestamp(seconds=1)])],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamps").nullable is False

    def test_list_nullable_true(self):
        """List field can be set to nullable."""
        config = PtarsConfig(list_nullable=True)
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamps=[Timestamp(seconds=1)])],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamps").nullable is True


class TestMapConfig:
    """Test map field configuration."""

    def test_map_value_name_is_always_value(self):
        """Map value name is always 'value' (not configurable in Python).

        Note: The Rust API supports customizing this via `map_value_name`,
        but this cannot be passed through PyArrow's C data interface.
        """
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithMap(string_to_double={"key": 1.0})],
            WithMap.DESCRIPTOR,
        )
        map_type = batch.schema.field("string_to_double").type
        assert isinstance(map_type, pa.MapType)
        assert map_type.item_field.name == "value"

    def test_map_value_nullable_default(self):
        """Default map value is not nullable."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithMap(string_to_double={"key": 1.0})],
            WithMap.DESCRIPTOR,
        )
        map_type = batch.schema.field("string_to_double").type
        assert map_type.item_field.nullable is False

    def test_map_value_nullable_true(self):
        """Map value can be set to nullable."""
        config = PtarsConfig(map_value_nullable=True)
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithMap(string_to_double={"key": 1.0})],
            WithMap.DESCRIPTOR,
        )
        map_type = batch.schema.field("string_to_double").type
        assert map_type.item_field.nullable is True

    def test_map_nullable_default(self):
        """Default map field is not nullable."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithMap(string_to_double={"key": 1.0})],
            WithMap.DESCRIPTOR,
        )
        assert batch.schema.field("string_to_double").nullable is False

    def test_map_nullable_true(self):
        """Map field can be set to nullable."""
        config = PtarsConfig(map_nullable=True)
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithMap(string_to_double={"key": 1.0})],
            WithMap.DESCRIPTOR,
        )
        assert batch.schema.field("string_to_double").nullable is True


class TestLargeValueOverflow:
    """Test that large timestamp/duration values that would overflow."""

    def test_timestamp_large_value_second_unit_roundtrip(self):
        """Large timestamp (year 2500) should roundtrip with second unit."""
        config = PtarsConfig(timestamp_unit="s", timestamp_tz=None)
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Year 2500 timestamp: ~16.7 billion seconds from epoch
        # This would overflow i64 if multiplied by 1e9 to convert to nanoseconds
        large_seconds = 16_725_225_600

        messages = [WithTimestamp(timestamp=Timestamp(seconds=large_seconds, nanos=0))]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)

        # Verify the Arrow type
        assert batch.schema.field("timestamp").type == pa.timestamp("s")

        # Verify the value is correct
        ts_array = batch.column("timestamp")
        assert ts_array[0].as_py().timestamp() == large_seconds

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back[0].timestamp.seconds == large_seconds
        assert messages_back[0].timestamp.nanos == 0

    def test_timestamp_large_value_millisecond_unit_roundtrip(self):
        """Large timestamp should roundtrip with millisecond unit."""
        config = PtarsConfig(timestamp_unit="ms", timestamp_tz=None)
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Large timestamp that fits in milliseconds
        large_seconds = 9_000_000_000  # ~year 2255
        nanos = 123_000_000  # 123 milliseconds

        messages = [
            WithTimestamp(timestamp=Timestamp(seconds=large_seconds, nanos=nanos))
        ]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)

        # Verify the Arrow type
        assert batch.schema.field("timestamp").type == pa.timestamp("ms")

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert messages_back[0].timestamp.seconds == large_seconds
        assert messages_back[0].timestamp.nanos == nanos

    def test_duration_large_value_second_unit_roundtrip(self):
        """Large duration (500 years) should roundtrip with second unit."""
        config = PtarsConfig(duration_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Large duration: 500 years in seconds
        # This would overflow i64 if multiplied by 1e9
        large_seconds = 15_768_000_000

        messages = [WithDuration(duration=Duration(seconds=large_seconds, nanos=0))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)

        # Verify the Arrow type
        assert batch.schema.field("duration").type == pa.duration("s")

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back[0].duration.seconds == large_seconds
        assert messages_back[0].duration.nanos == 0

    def test_duration_large_value_millisecond_unit_roundtrip(self):
        """Large duration should roundtrip with millisecond unit."""
        config = PtarsConfig(duration_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Large duration that fits in milliseconds
        large_seconds = 8_000_000_000  # ~253 years
        nanos = 456_000_000  # 456 milliseconds

        messages = [WithDuration(duration=Duration(seconds=large_seconds, nanos=nanos))]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)

        # Verify the Arrow type
        assert batch.schema.field("duration").type == pa.duration("ms")

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert messages_back[0].duration.seconds == large_seconds
        assert messages_back[0].duration.nanos == nanos

    def test_repeated_timestamp_large_value_roundtrip(self):
        """Repeated large timestamps should roundtrip with second unit."""
        config = PtarsConfig(timestamp_unit="s", timestamp_tz=None)
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Create timestamps with large values
        large_seconds_1 = 16_725_225_600  # ~year 2500
        large_seconds_2 = 20_000_000_000  # ~year 2604

        messages = [
            WithTimestamp(
                timestamps=[
                    Timestamp(seconds=large_seconds_1, nanos=0),
                    Timestamp(seconds=large_seconds_2, nanos=0),
                ]
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert len(messages_back[0].timestamps) == 2
        assert messages_back[0].timestamps[0].seconds == large_seconds_1
        assert messages_back[0].timestamps[1].seconds == large_seconds_2

    def test_repeated_duration_large_value_roundtrip(self):
        """Repeated large durations should roundtrip with second unit."""
        config = PtarsConfig(duration_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Create durations with large values
        large_seconds_1 = 15_768_000_000  # ~500 years
        large_seconds_2 = 31_536_000_000  # ~1000 years

        messages = [
            WithDuration(
                durations=[
                    Duration(seconds=large_seconds_1, nanos=0),
                    Duration(seconds=large_seconds_2, nanos=0),
                ]
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert len(messages_back[0].durations) == 2
        assert messages_back[0].durations[0].seconds == large_seconds_1
        assert messages_back[0].durations[1].seconds == large_seconds_2


class TestMapValueTimeUnits:
    """Test map values with non-default time units."""

    def test_map_timestamp_second_unit_roundtrip(self):
        """Map timestamp values should roundtrip with second unit."""
        config = PtarsConfig(timestamp_unit="s", timestamp_tz=None)
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Large timestamp that would overflow if converted to nanoseconds
        large_seconds = 16_725_225_600  # ~year 2500

        messages = [
            WithTimestamp(
                int_to_timestamp={1: Timestamp(seconds=large_seconds, nanos=0)}
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
        assert len(messages_back[0].int_to_timestamp) == 1
        assert messages_back[0].int_to_timestamp[1].seconds == large_seconds

    def test_map_duration_roundtrip(self):
        """Map duration values should roundtrip with default (nanosecond) unit."""
        pool = HandlerPool([DESCRIPTOR])

        messages = [
            WithDuration(
                int_to_duration={
                    1: Duration(seconds=3600, nanos=500_000_000),  # 1 hour + 0.5s
                    2: Duration(seconds=7200, nanos=0),  # 2 hours
                }
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert len(messages_back[0].int_to_duration) == 2
        assert messages_back[0].int_to_duration[1].seconds == 3600
        assert messages_back[0].int_to_duration[1].nanos == 500_000_000
        assert messages_back[0].int_to_duration[2].seconds == 7200

    def test_map_duration_second_unit_roundtrip(self):
        """Map duration values should roundtrip with second unit."""
        config = PtarsConfig(duration_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)

        # Large duration that would overflow if converted to nanoseconds
        large_seconds = 15_768_000_000  # ~500 years

        messages = [
            WithDuration(int_to_duration={1: Duration(seconds=large_seconds, nanos=0)})
        ]
        batch = pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithDuration.DESCRIPTOR)
        assert len(messages_back[0].int_to_duration) == 1
        assert messages_back[0].int_to_duration[1].seconds == large_seconds

    def test_map_time_of_day_millisecond_unit_roundtrip(self):
        """Map time of day values should roundtrip with millisecond unit."""
        config = PtarsConfig(time_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)

        messages = [
            WithTimeOfDay(
                int_to_time_of_day={
                    1: TimeOfDay(hours=12, minutes=30, seconds=45, nanos=500_000_000)
                }
            )
        ]
        batch = pool.messages_to_record_batch(messages, WithTimeOfDay.DESCRIPTOR)

        # Roundtrip back to protobuf
        messages_back = pool.record_batch_to_messages(batch, WithTimeOfDay.DESCRIPTOR)
        assert len(messages_back[0].int_to_time_of_day) == 1
        tod = messages_back[0].int_to_time_of_day[1]
        assert tod.hours == 12
        assert tod.minutes == 30
        assert tod.seconds == 45
        # Nanos preserved at millisecond precision
        assert tod.nanos == 500_000_000


class TestLargeStringBinaryConfig:
    """Test large string and binary config options."""

    def test_use_large_string_default_is_false(self):
        """Default config uses regular Utf8."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        pool = HandlerPool([BENCH_DESCRIPTOR])
        messages = [ExampleMessage(string_value="test")]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        assert batch.schema.field("string_value").type == pa.string()

    def test_use_large_string_true(self):
        """Config with use_large_string=True uses LargeUtf8."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        config = PtarsConfig(use_large_string=True)
        pool = HandlerPool([BENCH_DESCRIPTOR], config=config)
        messages = [ExampleMessage(string_value="test")]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        assert batch.schema.field("string_value").type == pa.large_string()

    def test_use_large_binary_default_is_false(self):
        """Default config uses regular Binary."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        pool = HandlerPool([BENCH_DESCRIPTOR])
        messages = [ExampleMessage(bytes_value=b"test")]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        assert batch.schema.field("bytes_value").type == pa.binary()

    def test_use_large_binary_true(self):
        """Config with use_large_binary=True uses LargeBinary."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        config = PtarsConfig(use_large_binary=True)
        pool = HandlerPool([BENCH_DESCRIPTOR], config=config)
        messages = [ExampleMessage(bytes_value=b"test")]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        assert batch.schema.field("bytes_value").type == pa.large_binary()

    def test_use_large_string_repeated(self):
        """Config with use_large_string=True uses LargeUtf8 for repeated fields."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        config = PtarsConfig(use_large_string=True)
        pool = HandlerPool([BENCH_DESCRIPTOR], config=config)
        messages = [ExampleMessage(string_values=["a", "b", "c"])]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        list_type = batch.schema.field("string_values").type
        assert isinstance(list_type, pa.ListType)
        assert list_type.value_type == pa.large_string()

    def test_use_large_binary_repeated(self):
        """Config with use_large_binary=True uses LargeBinary for repeated fields."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        config = PtarsConfig(use_large_binary=True)
        pool = HandlerPool([BENCH_DESCRIPTOR], config=config)
        messages = [ExampleMessage(bytes_values=[b"a", b"b", b"c"])]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        list_type = batch.schema.field("bytes_values").type
        assert isinstance(list_type, pa.ListType)
        assert list_type.value_type == pa.large_binary()

    def test_use_large_string_roundtrip(self):
        """Large string values should roundtrip correctly."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        config = PtarsConfig(use_large_string=True)
        pool = HandlerPool([BENCH_DESCRIPTOR], config=config)
        messages = [ExampleMessage(string_value="test string")]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        messages_back = pool.record_batch_to_messages(batch, ExampleMessage.DESCRIPTOR)
        assert messages_back == messages

    def test_use_large_binary_roundtrip(self):
        """Large binary values should roundtrip correctly."""
        from ptars_protos.bench_pb2 import DESCRIPTOR as BENCH_DESCRIPTOR
        from ptars_protos.bench_pb2 import ExampleMessage

        config = PtarsConfig(use_large_binary=True)
        pool = HandlerPool([BENCH_DESCRIPTOR], config=config)
        messages = [ExampleMessage(bytes_value=b"test bytes")]
        batch = pool.messages_to_record_batch(messages, ExampleMessage.DESCRIPTOR)
        messages_back = pool.record_batch_to_messages(batch, ExampleMessage.DESCRIPTOR)
        assert messages_back == messages
