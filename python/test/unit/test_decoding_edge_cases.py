"""Decoding of values and wire data that used to abort or silently drop a batch.

Each class covers one behaviour: a malformed or extreme input now produces a
defined value instead of a panic, an error, or an empty result.
"""

import datetime

import pyarrow as pa
import pytest
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.date_pb2 import Date

from ptars import HandlerPool, PtarsConfig
from ptars_protos import bench_pb2, simple_pb2
from ptars_protos.bench_pb2 import ExampleEnum, ExampleMessage
from ptars_protos.simple_pb2 import (
    RecursiveMessage,
    SearchRequest,
    WithDate,
    WithDuration,
    WithFixedWidthKeyMaps,
    WithTimestamp,
)

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)

# Seconds for the two ends of the google.protobuf.Timestamp range. Both are used
# as sentinels in real feeds and neither fits in nanoseconds since the epoch.
YEAR_9999 = 253_402_300_799
YEAR_0001 = -62_135_596_800


@pytest.fixture
def simple_pool() -> HandlerPool:
    return HandlerPool([simple_pb2.DESCRIPTOR])


def _int64_values(batch: pa.RecordBatch, name: str) -> list[int]:
    """Underlying integers of a temporal column, bypassing datetime conversion."""
    return batch[name].cast(pa.int64()).to_pylist()


class TestTemporalSaturation:
    """Out-of-range temporal values clamp to the Arrow limits of the unit."""

    def test_nanosecond_timestamp_overflow_saturates(self, simple_pool):
        messages = [
            WithTimestamp(timestamp=Timestamp(seconds=YEAR_9999)),
            WithTimestamp(timestamp=Timestamp(seconds=YEAR_0001)),
        ]
        batch = simple_pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert _int64_values(batch, "timestamp") == [INT64_MAX, INT64_MIN]

    def test_coarser_unit_represents_the_same_values_exactly(self):
        pool = HandlerPool(
            [simple_pb2.DESCRIPTOR], config=PtarsConfig(timestamp_unit="us")
        )
        messages = [
            WithTimestamp(timestamp=Timestamp(seconds=YEAR_9999)),
            WithTimestamp(timestamp=Timestamp(seconds=YEAR_0001)),
        ]
        batch = pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert _int64_values(batch, "timestamp") == [
            YEAR_9999 * 10**6,
            YEAR_0001 * 10**6,
        ]

    def test_nanosecond_duration_overflow_saturates(self, simple_pool):
        # The maximum google.protobuf.Duration is 10000 years, ~34x the
        # nanosecond range.
        messages = [
            WithDuration(duration=Duration(seconds=315_576_000_000)),
            WithDuration(duration=Duration(seconds=-315_576_000_000)),
        ]
        batch = simple_pool.messages_to_record_batch(messages, WithDuration.DESCRIPTOR)
        assert _int64_values(batch, "duration") == [INT64_MAX, INT64_MIN]

    def test_in_range_values_are_untouched(self, simple_pool):
        messages = [WithTimestamp(timestamp=Timestamp(seconds=1, nanos=500_000_000))]
        batch = simple_pool.messages_to_record_batch(messages, WithTimestamp.DESCRIPTOR)
        assert _int64_values(batch, "timestamp") == [1_500_000_000]
        assert (
            simple_pool.record_batch_to_messages(batch, WithTimestamp.DESCRIPTOR)
            == messages
        )


class TestPartialDates:
    """google.type.Date allows partial dates; they normalise rather than fail."""

    @staticmethod
    def _date(pool: HandlerPool, **kwargs) -> datetime.date:
        batch = pool.messages_to_record_batch(
            [WithDate(date=Date(**kwargs))], WithDate.DESCRIPTOR
        )
        return batch["date"].to_pylist()[0]

    def test_complete_date_is_unchanged(self, simple_pool):
        assert self._date(simple_pool, year=2026, month=3, day=15) == datetime.date(
            2026, 3, 15
        )

    def test_unspecified_month_and_day_become_the_first_of_the_period(
        self, simple_pool
    ):
        # Zero means "not significant": a whole year, or a whole month.
        assert self._date(simple_pool, year=2026) == datetime.date(2026, 1, 1)
        assert self._date(simple_pool, year=2026, month=3) == datetime.date(2026, 3, 1)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},  # all zero
            {"year": 0, "month": 3, "day": 15},  # no year
            {"year": 2026, "month": 2, "day": 30},  # not a calendar date
            {"year": 2026, "month": 13, "day": 1},  # month out of range
        ],
    )
    def test_dates_without_a_calendar_day_fall_back_to_the_epoch(
        self, simple_pool, kwargs
    ):
        assert self._date(simple_pool, **kwargs) == datetime.date(1970, 1, 1)


class TestFixedWidthMapKeys:
    """Maps keyed by fixed32/fixed64/sfixed32/sfixed64 decode their entries."""

    def test_fixed_width_keys_are_not_dropped(self, simple_pool):
        message = WithFixedWidthKeyMaps(
            fixed32_key_map={7: "a"},
            fixed64_key_map={8: "b"},
            sfixed32_key_map={-9: "c"},
            sfixed64_key_map={-10: "d"},
        )
        batch = simple_pool.messages_to_record_batch(
            [message], WithFixedWidthKeyMaps.DESCRIPTOR
        )
        assert batch["fixed32_key_map"].to_pylist() == [[(7, "a")]]
        assert batch["fixed64_key_map"].to_pylist() == [[(8, "b")]]
        assert batch["sfixed32_key_map"].to_pylist() == [[(-9, "c")]]
        assert batch["sfixed64_key_map"].to_pylist() == [[(-10, "d")]]

    def test_signedness_of_the_key_type_is_preserved(self, simple_pool):
        batch = simple_pool.messages_to_record_batch(
            [WithFixedWidthKeyMaps()], WithFixedWidthKeyMaps.DESCRIPTOR
        )
        assert batch.schema.field("fixed32_key_map").type.key_type == pa.uint32()
        assert batch.schema.field("fixed64_key_map").type.key_type == pa.uint64()
        assert batch.schema.field("sfixed32_key_map").type.key_type == pa.int32()
        assert batch.schema.field("sfixed64_key_map").type.key_type == pa.int64()

    def test_multiple_rows_keep_their_own_entries(self, simple_pool):
        messages = [
            WithFixedWidthKeyMaps(fixed32_key_map={1: "a"}),
            WithFixedWidthKeyMaps(),
            WithFixedWidthKeyMaps(fixed32_key_map={2: "b", 3: "c"}),
        ]
        batch = simple_pool.messages_to_record_batch(
            messages, WithFixedWidthKeyMaps.DESCRIPTOR
        )
        assert [
            sorted(entries) for entries in batch["fixed32_key_map"].to_pylist()
        ] == [[(1, "a")], [], [(2, "b"), (3, "c")]]


def _tag(field_number: int, wire_type: int) -> bytes:
    return bytes([(field_number << 3) | wire_type])


# query="hello", page_number=7
_KNOWN_FIELDS = _tag(1, 2) + bytes([5]) + b"hello" + _tag(2, 0) + bytes([7])
_SPLIT = 7  # byte offset between the two known fields


class TestUnknownGroups:
    """Groups (wire types 3 and 4) are skipped like any other unknown field."""

    @staticmethod
    def _decode(pool: HandlerPool, payload: bytes) -> dict:
        handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
        return handler.list_to_record_batch([payload]).to_pylist()[0]

    def test_unknown_group_between_known_fields_is_skipped(self, simple_pool):
        group = _tag(9, 3) + _tag(1, 0) + bytes([1]) + _tag(9, 4)
        payload = _KNOWN_FIELDS[:_SPLIT] + group + _KNOWN_FIELDS[_SPLIT:]
        assert self._decode(simple_pool, payload) == {
            "query": "hello",
            "page_number": 7,
            "result_per_page": 0,
        }

    def test_nested_unknown_groups_are_skipped(self, simple_pool):
        inner = _tag(2, 3) + _tag(1, 0) + bytes([1]) + _tag(2, 4)
        group = _tag(9, 3) + inner + _tag(9, 4)
        payload = _KNOWN_FIELDS[:_SPLIT] + group + _KNOWN_FIELDS[_SPLIT:]
        assert self._decode(simple_pool, payload)["query"] == "hello"

    def test_end_group_without_a_start_is_rejected(self, simple_pool):
        with pytest.raises(ValueError, match="unexpected end-group tag"):
            self._decode(simple_pool, _KNOWN_FIELDS + _tag(9, 4))

    def test_unterminated_group_is_rejected(self, simple_pool):
        payload = _KNOWN_FIELDS + _tag(9, 3) + _tag(1, 0) + bytes([1])
        with pytest.raises(ValueError, match="unexpected EOF inside group"):
            self._decode(simple_pool, payload)


class TestRecursiveMessages:
    """A message type that contains itself decodes as raw protobuf bytes."""

    def test_recursive_fields_become_binary(self, simple_pool):
        batch = simple_pool.messages_to_record_batch(
            [RecursiveMessage()], RecursiveMessage.DESCRIPTOR
        )
        assert batch.schema.field("name").type == pa.string()
        assert batch.schema.field("child").type == pa.binary()
        assert batch.schema.field("children").type.value_type == pa.binary()

    def test_nested_payloads_are_preserved_verbatim(self, simple_pool):
        message = RecursiveMessage(
            name="root",
            child=RecursiveMessage(name="kid", child=RecursiveMessage(name="grandkid")),
            children=[RecursiveMessage(name="c1"), RecursiveMessage(name="c2")],
        )
        row = simple_pool.messages_to_record_batch(
            [message], RecursiveMessage.DESCRIPTOR
        ).to_pylist()[0]

        assert row["name"] == "root"
        assert RecursiveMessage.FromString(row["child"]) == message.child
        assert [RecursiveMessage.FromString(c) for c in row["children"]] == list(
            message.children
        )

    def test_absent_recursive_field_is_null(self, simple_pool):
        row = simple_pool.messages_to_record_batch(
            [RecursiveMessage(name="lonely")], RecursiveMessage.DESCRIPTOR
        ).to_pylist()[0]
        assert row["child"] is None
        assert row["children"] == []


class TestEnumNames:
    """String-represented enums resolve declared names and format unknown numbers."""

    @staticmethod
    def _pool() -> HandlerPool:
        return HandlerPool(
            [bench_pb2.DESCRIPTOR], config=PtarsConfig(enum_repr="string")
        )

    def test_declared_values_use_their_name(self):
        messages = [
            ExampleMessage(example_enum_value=ExampleEnum.EXAMPLE_ENUM_1),
            ExampleMessage(example_enum_value=ExampleEnum.EXAMPLE_ENUM_2),
            ExampleMessage(),
        ]
        batch = self._pool().messages_to_record_batch(
            messages, ExampleMessage.DESCRIPTOR
        )
        assert batch["example_enum_value"].to_pylist() == [
            "EXAMPLE_ENUM_1",
            "EXAMPLE_ENUM_2",
            "UNKNOWN_EXAMPLE_ENUM",
        ]

    def test_undeclared_numbers_render_as_the_number_formatted_as_a_string(self):
        # proto3 enums are open: a producer may send a number this build of the
        # schema does not know.
        batch = self._pool().messages_to_record_batch(
            [ExampleMessage(example_enum_value=4321)], ExampleMessage.DESCRIPTOR
        )
        assert batch["example_enum_value"].to_pylist() == ["4321"]

    def test_repeated_enums_resolve_the_same_way(self):
        message = ExampleMessage(
            example_enum_values=[ExampleEnum.EXAMPLE_ENUM_2, 4321, 0]
        )
        batch = self._pool().messages_to_record_batch(
            [message], ExampleMessage.DESCRIPTOR
        )
        assert batch["example_enum_values"].to_pylist() == [
            ["EXAMPLE_ENUM_2", "4321", "UNKNOWN_EXAMPLE_ENUM"]
        ]
