import datetime

import pyarrow as pa
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.timeofday_pb2 import TimeOfDay

from ptars import HandlerPool, PtarsConfig
from ptars_protos.simple_pb2 import DESCRIPTOR, WithTimeOfDay, WithTimestamp


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
        assert batch.schema.field("timestamp").type == pa.timestamp("ns", tz="America/New_York")


class TestTimeOfDayConfig:
    """Test time of day time unit configuration."""

    def test_time_of_day_nanoseconds_default(self):
        """Default config uses nanoseconds."""
        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000))],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time64("ns")

    def test_time_of_day_nanoseconds_explicit(self):
        """Explicit nanosecond config."""
        config = PtarsConfig(time_unit="ns")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000))],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time64("ns")

    def test_time_of_day_microseconds(self):
        """Microsecond time config."""
        config = PtarsConfig(time_unit="us")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000))],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time64("us")

    def test_time_of_day_milliseconds(self):
        """Millisecond time config."""
        config = PtarsConfig(time_unit="ms")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000))],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time32("ms")

    def test_time_of_day_seconds(self):
        """Second time config."""
        config = PtarsConfig(time_unit="s")
        pool = HandlerPool([DESCRIPTOR], config=config)
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000))],
            WithTimeOfDay.DESCRIPTOR,
        )
        assert batch.schema.field("time_of_day").type == pa.time32("s")

    def test_time_of_day_value_conversion(self):
        """Test that time values are correctly converted."""
        # Test with a simple value: 01:02:03.5
        time_of_day = TimeOfDay(hours=1, minutes=2, seconds=3, nanos=500_000_000)
        expected_time = datetime.time(1, 2, 3, 500_000)  # microsecond precision in Python

        pool = HandlerPool([DESCRIPTOR])
        batch = pool.messages_to_record_batch(
            [WithTimeOfDay(time_of_day=time_of_day)],
            WithTimeOfDay.DESCRIPTOR,
        )
        # PyArrow converts to datetime.time
        assert batch["time_of_day"].to_pylist() == [expected_time]


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
