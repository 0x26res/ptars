import datetime

import protarrow
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from google._upb._message import MessageMeta
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.date_pb2 import Date
from google.type.timeofday_pb2 import TimeOfDay
from ptars._lib import MessageHandler

from ptars import HandlerPool
from ptars_protos import bench_pb2, simple_pb2
from ptars_protos.bench_pb2 import (
    ExampleMessage,
    NestedExampleMessage,
    SuperNestedExampleMessage,
)
from ptars_protos.simple_pb2 import (
    NestedMap,
    RepeatedNestedMessageSimple,
    ReturnCode,
    SearchRequest,
    SimpleMessage,
    TestEnum,
    WithDate,
    WithMap,
    WithMapOfDate,
    WithMapOfWrapper,
    WithRepeated,
)
from python.test.random_generator import generate_messages

MESSAGES = [ExampleMessage, NestedExampleMessage, SuperNestedExampleMessage]


@pytest.fixture()
def pool():
    return HandlerPool([simple_pb2.DESCRIPTOR, bench_pb2.DESCRIPTOR])


@pytest.fixture()
def simple_message_handler(pool: HandlerPool) -> MessageHandler:
    return pool.get_for_message(simple_pb2.SimpleMessage.DESCRIPTOR)


def test_generate_proto(simple_message_handler):
    SearchRequest()
    search_request = SearchRequest(query="hello", page_number=0, result_per_page=10)
    protos = [
        simple_pb2.SimpleMessage(
            int64_value=123,
            uint32_value=456,
        ),
        simple_pb2.SimpleMessage(
            int64_value=0,
            uint32_value=789,
            search_request={},
            search_requests=[search_request],
        ),
        simple_pb2.SimpleMessage(
            double_value=1.0,
            float_value=2.0,
            int32_value=3,
            int64_value=4,
            uint32_value=5,
            uint64_value=6,
            sint32_value=7,
            sint64_value=8,
            fixed32_value=9,
            fixed64_value=10,
            sfixed32_value=11,
            sfixed64_value=12,
            bool_value=True,
            string_value="14",
            bytes_value=b"15",
            enum_value=TestEnum.HELLO,
            int32_values=[1, 2, 3],
            int64_values=[1, 2, 3],
            bool_values=[True, False, True],
            search_request=search_request,
            string_values=["1", "B", "ABC"],
            bytes_values=[b"1", b"B", b"ABC", b""],
            search_requests=[{}, {}, {}, {}],
        ),
        simple_pb2.SimpleMessage(),
    ]
    message_payloads = [p.SerializeToString() for p in protos]
    table = simple_message_handler.list_to_record_batch(message_payloads)

    assert table["int64_value"].to_pylist() == [123, 0, 4, 0]
    assert table["uint32_value"].to_pylist() == [456, 789, 5, 0]
    assert table["bool_value"].to_pylist() == [False, False, True, False]
    assert table["string_value"].to_pylist() == ["", "", "14", ""]
    assert table["bytes_value"].to_pylist() == [b"", b"", b"15", b""]
    assert table["enum_value"].to_pylist() == [0, 0, 1, 0]
    assert table["int32_values"].to_pylist() == [[], [], [1, 2, 3], []]
    assert table["int64_values"].to_pylist() == [[], [], [1, 2, 3], []]
    assert table["bool_values"].to_pylist() == [[], [], [True, False, True], []]
    assert table["search_request"].to_pylist() == [
        None,
        {"query": "", "page_number": 0, "result_per_page": 0},
        {"query": "hello", "page_number": 0, "result_per_page": 10},
        None,
    ]
    assert table["string_values"].to_pylist() == [[], [], ["1", "B", "ABC"], []]
    assert table["bytes_values"].to_pylist() == [[], [], [b"1", b"B", b"ABC", b""], []]
    assert pa.types.is_list(table["search_requests"].type)
    assert table["search_requests"].to_pylist() == [
        [],
        [{"query": "hello", "page_number": 0, "result_per_page": 10}],
        [{"query": "", "page_number": 0, "result_per_page": 0}] * 4,
        [],
    ]


@pytest.mark.parametrize("message_type", MESSAGES)
def test_back_and_forth(message_type: MessageMeta):
    messages = generate_messages(message_type, 10)
    run_round_trip(messages, message_type)


def sort_map_by_key(map_array: pa.MapArray):
    key_array = pa.ListArray.from_arrays(
        offsets=map_array.offsets, values=map_array.keys
    )
    value_array = pa.ListArray.from_arrays(
        offsets=map_array.offsets, values=map_array.values
    )

    return pa.table(
        {
            "index": pc.list_parent_indices(key_array),
            "key": pc.list_flatten(key_array),
            "value": pc.list_flatten(value_array),
        }
    ).sort_by([("index", "ascending"), ("key", "ascending")])


@pytest.mark.parametrize("message_type", MESSAGES[:1])
def test_protarrow_parity(message_type: MessageMeta):
    messages = generate_messages(message_type, 10)
    payloads = [m.SerializeToString() for m in messages]
    pool = HandlerPool([message_type.DESCRIPTOR.file])

    handler = pool.get_for_message(message_type.DESCRIPTOR)
    record_batch = handler.list_to_record_batch(payloads)
    assert isinstance(record_batch, pa.RecordBatch)
    record_batch_protarrow = protarrow.messages_to_record_batch(messages, message_type)
    assert record_batch.schema == record_batch_protarrow.schema
    for field in record_batch.schema:
        if "date" in field.name and "map" in field.name:
            # TODO: there an issue with how defaults are handled in date
            pass
        elif pa.types.is_map(field.type):
            assert sort_map_by_key(record_batch[field.name]) == sort_map_by_key(
                record_batch_protarrow[field.name]
            )
        else:
            assert record_batch[field.name] == record_batch_protarrow[field.name]


def test_arrow_to_proto(pool):
    record_batch = pa.record_batch(
        [
            pa.array([33, 42, 17], pa.int32()),
            pa.array([133, 142, 117], pa.int64()),
            pa.array([True, False, None], pa.bool_()),
            pa.array(["ABC", "", None], pa.utf8()),
            pa.array([b"ABC", b"", None], pa.binary()),
        ],
        [
            "int32_value",
            "int64_value",
            "bool_value",
            "string_value",
            "bytes_value",
        ],
    )
    handler = pool.get_for_message(ExampleMessage.DESCRIPTOR)
    array = handler.record_batch_to_array(record_batch)
    assert isinstance(array, pa.Array)
    assert array.type == pa.binary()
    assert len(array) == 3

    messages = [ExampleMessage.FromString(b.as_py()) for b in array]
    assert messages == [
        ExampleMessage(
            int32_value=33,
            int64_value=133,
            bool_value=True,
            string_value="ABC",
            bytes_value=b"ABC",
        ),
        ExampleMessage(int32_value=42, int64_value=142),
        ExampleMessage(int32_value=17, int64_value=117),
    ]


def test_timestamp_missing(pool):
    handler = pool.get_for_message(simple_pb2.WithTimestamp.DESCRIPTOR)
    messages = [
        simple_pb2.WithTimestamp(
            timestamp=Timestamp(seconds=1712660619, nanos=123456789)
        ),
        simple_pb2.WithTimestamp(),
    ]
    payloads = [message.SerializeToString() for message in messages]
    record_batch = handler.list_to_record_batch(payloads)

    assert record_batch["timestamp"].type == pa.timestamp("ns", "UTC")
    assert record_batch["timestamp"].is_null().to_pylist() == [False, True]
    assert record_batch["timestamp"].cast(pa.int64()).to_pylist() == [
        1712660619123456789,
        None,
    ]


def test_date_missing(pool):
    handler = pool.get_for_message(WithDate.DESCRIPTOR)
    messages = [
        WithDate(
            date=Date(year=2024, month=4, day=9),
            dates=[Date(year=2024, month=4, day=10), Date()],
            int_to_date={
                1: Date(),
            },
        ),
        WithDate(
            date=Date(),
        ),
        WithDate(),
    ]
    payloads = [message.SerializeToString() for message in messages]
    record_batch = handler.list_to_record_batch(payloads)

    assert record_batch["date"].type == pa.date32()
    assert record_batch["date"].is_null().to_pylist() == [False, False, True]
    assert record_batch["date"].to_pylist() == [
        datetime.date(2024, 4, 9),
        datetime.date(1970, 1, 1),
        None,
    ]
    assert record_batch["dates"].to_pylist() == [
        [datetime.date(2024, 4, 10), datetime.date(1970, 1, 1)],
        [],
        [],
    ]
    assert record_batch["int_to_date"].to_pylist() == [
        [
            (1, datetime.date(1970, 1, 1)),
        ],
        [],
        [],
    ]


def test_repeated_date(pool):
    handler = pool.get_for_message(WithDate.DESCRIPTOR)
    messages = [
        WithDate(
            dates=[
                Date(year=2024, month=4, day=9),
                Date(year=2024, month=4, day=10),
            ]
        ),
        WithDate(dates=[]),
        WithDate(),
    ]
    payloads = [message.SerializeToString() for message in messages]
    record_batch = handler.list_to_record_batch(payloads)

    assert pa.types.is_list(record_batch["dates"].type)
    assert pa.types.is_date32(record_batch["dates"].type.value_type)

    assert record_batch["dates"].to_pylist() == [
        [
            datetime.date(2024, 4, 9),
            datetime.date(2024, 4, 10),
        ],
        [],
        [],
    ]


def test_time_of_day_missing(pool):
    handler = pool.get_for_message(simple_pb2.WithTimeOfDay.DESCRIPTOR)
    messages = [
        simple_pb2.WithTimeOfDay(
            time_of_day=TimeOfDay(hours=14, minutes=30, seconds=45, nanos=123456789)
        ),
        simple_pb2.WithTimeOfDay(),
    ]
    payloads = [message.SerializeToString() for message in messages]
    record_batch = handler.list_to_record_batch(payloads)

    assert record_batch["time_of_day"].type == pa.time64("ns")
    assert record_batch["time_of_day"].is_null().to_pylist() == [False, True]
    assert record_batch["time_of_day"].cast(pa.int64()).to_pylist() == [
        52245123456789,
        None,
    ]


def test_repeated_time_of_day(pool):
    handler = pool.get_for_message(simple_pb2.WithTimeOfDay.DESCRIPTOR)
    messages = [
        simple_pb2.WithTimeOfDay(
            time_of_days=[
                TimeOfDay(hours=9, minutes=0, seconds=0, nanos=0),
                TimeOfDay(hours=17, minutes=30, seconds=0, nanos=0),
            ]
        ),
        simple_pb2.WithTimeOfDay(time_of_days=[]),
        simple_pb2.WithTimeOfDay(),
    ]
    payloads = [message.SerializeToString() for message in messages]
    record_batch = handler.list_to_record_batch(payloads)

    assert pa.types.is_list(record_batch["time_of_days"].type)
    assert pa.types.is_time64(record_batch["time_of_days"].type.value_type)

    # 9:00:00 = 9*3_600_000_000_000 = 32_400_000_000_000 ns
    # 17:30:00 = 17*3_600_000_000_000 + 30*60_000_000_000 = 63_000_000_000_000 ns
    flattened = record_batch["time_of_days"].flatten().cast(pa.int64())
    assert flattened.to_pylist() == [32400000000000, 63000000000000]


def test_repeated():
    messages = generate_messages(WithRepeated, count=10)
    run_round_trip(messages, WithRepeated)


def test_nested_map():
    messages = generate_messages(NestedMap, count=10)
    run_round_trip(messages, NestedMap)


def test_map():
    messages = generate_messages(WithMap, count=10)
    record_batch = run_round_trip(messages, WithMap)
    assert record_batch.schema == pa.schema(
        [
            pa.field(
                "string_to_double",
                pa.map_(pa.string(), pa.field("value", pa.float64(), nullable=False)),
                nullable=False,
            )
        ]
    )


def test_map_of_wrapper():
    messages = generate_messages(WithMapOfWrapper, count=10)
    run_round_trip(messages, WithMapOfWrapper)


def test_map_of_date():
    messages = generate_messages(WithMapOfDate, count=10)
    run_round_trip(messages, WithMapOfDate)


def run_round_trip(messages, message_type) -> pa.RecordBatch:
    payloads = [message.SerializeToString() for message in messages]

    pool = HandlerPool([message_type.DESCRIPTOR.file])
    handler = pool.get_for_message(message_type.DESCRIPTOR)
    record_batch = handler.list_to_record_batch(payloads)
    assert isinstance(record_batch, pa.RecordBatch)
    array: pa.BinaryArray = handler.record_batch_to_array(record_batch)
    assert isinstance(array, pa.BinaryArray)
    messages_back = [message_type.FromString(s.as_py()) for s in array]
    assert messages_back == messages
    return record_batch


def test_round_trip():
    run_round_trip(
        [
            SimpleMessage(double_values=[1.0, 2.0], search_request={}),
            SimpleMessage(
                int32_values=[1, 2, 3, 4],
                bool_values=[True, False],
            ),
        ],
        SimpleMessage,
    )


def test_round_trip_repeated_nested_message():
    run_round_trip(
        [
            RepeatedNestedMessageSimple(
                search_results=[
                    simple_pb2.SearchResult(
                        return_code=ReturnCode.ERROR, message="HELLO"
                    ),
                    simple_pb2.SearchResult(return_code=ReturnCode.OK, message="WOLRD"),
                ]
            )
        ],
        RepeatedNestedMessageSimple,
    )


def test_example():
    messages = [
        SearchRequest(
            query="protobuf to arrow",
            page_number=0,
            result_per_page=10,
        ),
        SearchRequest(
            query="protobuf to arrow",
            page_number=1,
            result_per_page=10,
        ),
    ]
    payloads = [message.SerializeToString() for message in messages]

    pool = HandlerPool([simple_pb2.DESCRIPTOR])
    handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
    record_batch = handler.list_to_record_batch(payloads)

    array: pa.BinaryArray = handler.record_batch_to_array(record_batch)
    messages_back: list[SearchRequest] = [
        SearchRequest.FromString(s.as_py()) for s in array
    ]
    assert messages_back == messages


def test_array_to_record_batch(pool):
    """Test converting a binary array of serialized messages to a record batch."""
    handler = pool.get_for_message(SearchRequest.DESCRIPTOR)

    messages = [
        SearchRequest(query="hello", page_number=1, result_per_page=10),
        SearchRequest(query="world", page_number=2, result_per_page=20),
    ]
    payloads = [message.SerializeToString() for message in messages]

    # Create a binary array from the payloads
    binary_array = pa.array(payloads, type=pa.binary())

    # Convert to record batch
    record_batch = handler.array_to_record_batch(binary_array)

    assert isinstance(record_batch, pa.RecordBatch)
    assert record_batch.num_rows == 2
    assert record_batch["query"].to_pylist() == ["hello", "world"]
    assert record_batch["page_number"].to_pylist() == [1, 2]
    assert record_batch["result_per_page"].to_pylist() == [10, 20]


def test_array_to_record_batch_roundtrip(pool):
    """Test roundtrip: binary array -> record batch -> binary array."""
    handler = pool.get_for_message(SearchRequest.DESCRIPTOR)

    messages = [
        SearchRequest(query="test1", page_number=0, result_per_page=5),
        SearchRequest(query="test2", page_number=1, result_per_page=15),
        SearchRequest(query="", page_number=0, result_per_page=0),
    ]
    payloads = [message.SerializeToString() for message in messages]

    # Binary array -> Record batch -> Binary array
    binary_array = pa.array(payloads, type=pa.binary())
    record_batch = handler.array_to_record_batch(binary_array)
    result_array = handler.record_batch_to_array(record_batch)

    # Decode and compare
    messages_back = [SearchRequest.FromString(b.as_py()) for b in result_array]
    assert messages_back == messages


def test_array_to_record_batch_complex_message(pool):
    """Test array_to_record_batch with a complex message type."""
    handler = pool.get_for_message(SimpleMessage.DESCRIPTOR)

    messages = [
        SimpleMessage(
            int32_value=42,
            string_value="hello",
            bool_value=True,
            double_value=3.14,
        ),
        SimpleMessage(
            int32_value=0,
            string_value="",
            bool_value=False,
        ),
    ]
    payloads = [message.SerializeToString() for message in messages]

    binary_array = pa.array(payloads, type=pa.binary())
    record_batch = handler.array_to_record_batch(binary_array)

    assert record_batch.num_rows == 2
    assert record_batch["int32_value"].to_pylist() == [42, 0]
    assert record_batch["string_value"].to_pylist() == ["hello", ""]
    assert record_batch["bool_value"].to_pylist() == [True, False]


def test_nested_primitive():
    data = [
        NestedExampleMessage(example_message=ExampleMessage()),
        NestedExampleMessage(example_message=None),
    ]
    record_batch = run_round_trip(data, NestedExampleMessage)

    struct_array = record_batch["example_message"]
    array = struct_array.field(struct_array.type.get_field_index("double_value"))
    # When the parent message is null, child fields should have default values
    # (consistent with protobuf semantics where accessing a field from an
    # unset message returns the default value)
    assert array.to_pylist() == [0.0, 0.0]
    assert pc.struct_field(struct_array, "double_value").to_pylist() == [0.0, None]

    assert struct_array.type.field("double_value").nullable is False


def test_nested_list_of_primitive():
    messages = [
        NestedExampleMessage(example_message=ExampleMessage(double_values=[1.0, 2, 0])),
        NestedExampleMessage(example_message=None),
    ]
    record_batch = run_round_trip(messages, NestedExampleMessage)

    struct_array = record_batch["example_message"]
    array = struct_array.field(struct_array.type.get_field_index("double_values"))
    assert array.to_pylist() == [[1.0, 2.0, 0.0], []]
    assert pc.struct_field(struct_array, "double_values").to_pylist() == [
        [1.0, 2.0, 0.0],
        None,
    ]
    assert struct_array.type.field("double_values").nullable is False

    record_batch_protarrow = protarrow.messages_to_record_batch(
        messages, NestedExampleMessage
    )
    struct_array_protarrow = record_batch_protarrow["example_message"]
    array_protarrow = struct_array_protarrow.field(
        struct_array.type.get_field_index("double_values")
    )
    assert array_protarrow.to_pylist() == [[1.0, 2.0, 0.0], None]  # Bug in protarrow


def test_map_of_maps_complex():
    messages = [
        NestedExampleMessage(
            example_message=ExampleMessage(
                example_enum_values=[0, 1, 2], double_int32_map={123: 0.12345}
            ),
            repeated_example_message=[
                ExampleMessage(
                    example_enum_values=[0, 1, 2], double_int32_map={123: 0.12345}
                )
            ],
            example_message_int32_map={
                123: ExampleMessage(
                    example_enum_values=[0, 1, 2], double_int32_map={123: 0.12345}
                )
            },
            example_message_string_map={
                "FOO": ExampleMessage(
                    example_enum_values=[0, 1, 2], double_int32_map={123: 0.12345}
                )
            },
        ),
        NestedExampleMessage(example_message=None),
    ]
    record_batch = run_round_trip(messages, NestedExampleMessage)

    struct_array = record_batch["example_message"]
    array = struct_array.field(struct_array.type.get_field_index("example_enum_values"))

    assert array.to_pylist() == [[0, 1, 2], []]
    assert pc.struct_field(struct_array, "example_enum_values").to_pylist() == [
        [0, 1, 2],
        None,
    ]
    record_batch_protarrow = protarrow.messages_to_record_batch(
        messages, NestedExampleMessage
    )
    assert record_batch == record_batch_protarrow
