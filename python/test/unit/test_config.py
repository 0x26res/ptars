import datetime

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
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamp").type == pa.timestamp("ns", tz="UTC")

    def test_timestamp_nanoseconds_explicit(self):
        """Explicit nanosecond config."""
        config = PtarsConfig(timestamp_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamp").type == pa.timestamp("ns", tz="UTC")

    def test_timestamp_microseconds(self):
        """Microsecond timestamp config."""
        config = PtarsConfig(timestamp_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")

    def test_timestamp_milliseconds(self):
        """Millisecond timestamp config."""
        config = PtarsConfig(timestamp_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamp").type == pa.timestamp("ms", tz="UTC")

    def test_timestamp_seconds(self):
        """Second timestamp config."""
        config = PtarsConfig(timestamp_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamp").type == pa.timestamp("s", tz="UTC")

    def test_timestamp_no_timezone(self):
        """Timestamp without timezone."""
        config = PtarsConfig(timestamp_tz=None, timestamp_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=0))],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamp").type == pa.timestamp("ns")
        assert batch.schema.field("timestamp").type.tz is None

    def test_timestamp_custom_timezone(self):
        """Timestamp with custom timezone."""
        config = PtarsConfig(timestamp_tz="America/New_York")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=0))],
            WithTimestamp.DESCRIPTOR,
        )
        assert batch.schema.field("timestamp").type == pa.timestamp(
            "ns", tz="America/New_York"
        )


class TestDurationConfig:
    """Test duration time unit configuration."""

    def test_duration_nanoseconds_default(self):
        """Default config uses nanoseconds."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))],
            WithDuration.DESCRIPTOR,
        )
        assert batch.schema.field("duration").type == pa.duration("ns")

    def test_duration_nanoseconds_explicit(self):
        """Explicit nanosecond config."""
        config = PtarsConfig(duration_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))],
            WithDuration.DESCRIPTOR,
        )
        assert batch.schema.field("duration").type == pa.duration("ns")

    def test_duration_microseconds(self):
        """Microsecond duration config."""
        config = PtarsConfig(duration_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))],
            WithDuration.DESCRIPTOR,
        )
        assert batch.schema.field("duration").type == pa.duration("us")

    def test_duration_milliseconds(self):
        """Millisecond duration config."""
        config = PtarsConfig(duration_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))],
            WithDuration.DESCRIPTOR,
        )
        assert batch.schema.field("duration").type == pa.duration("ms")

    def test_duration_seconds(self):
        """Second duration config."""
        config = PtarsConfig(duration_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))],
            WithDuration.DESCRIPTOR,
        )
        assert batch.schema.field("duration").type == pa.duration("s")

    def test_duration_value_conversion_nanoseconds(self):
        """Test that duration values are correctly converted to nanoseconds."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=1, nanos=500_000_000))],
            WithDuration.DESCRIPTOR,
        )
        # 1.5 seconds = 1,500,000,000 nanoseconds
        assert batch["duration"].to_pylist() == [
            datetime.timedelta(seconds=1, microseconds=500_000)
        ]

    def test_duration_value_conversion_seconds(self):
        """Test that duration values are correctly converted to seconds."""
        config = PtarsConfig(duration_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithDuration(duration=Duration(seconds=3600, nanos=0))],  # 1 hour
            WithDuration.DESCRIPTOR,
        )
        assert batch["duration"].to_pylist() == [datetime.timedelta(hours=1)]

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
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=500_000_000
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time64("ns")

    def test_time_of_day_nanoseconds_explicit(self):
        """Explicit nanosecond config."""
        config = PtarsConfig(time_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=500_000_000
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time64("ns")

    def test_time_of_day_microseconds(self):
        """Microsecond time config."""
        config = PtarsConfig(time_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=500_000_000
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time64("us")

    def test_time_of_day_milliseconds(self):
        """Millisecond time config."""
        config = PtarsConfig(time_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=500_000_000
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time32("ms")

    def test_time_of_day_seconds(self):
        """Second time config."""
        config = PtarsConfig(time_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [
                WithTimeOfDay(
                    time_of_day=TimeOfDay(
                        hours=1, minutes=2, seconds=3, nanos=500_000_000
                    )
                )
            ],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time32("s")

    def test_time_of_day_value_conversion(self):
        """Test that time values are correctly converted."""
        # Test with a simple value: 01:02:03.5
        time_of_day = TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000)
        expected_time = datetime.time(
            1, 2, 3, 500_000
        )  # microsecond precision in Python

        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=time_of_day)],
            WithTimeOfDay.DESCRIPTOR,
        )
        # PyArrow converts to datetime.time
        assert batch["time_of_day"].to_pylist() == [expected_time]

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

    def test_map_value_name_default(self):
        """Default map value name is 'value'."""
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
