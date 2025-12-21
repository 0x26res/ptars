import datetime
import sys

import pyarrow as pa
import pytest
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.date_pb2 import Date
from ptars import HandlerPool
from ptars._lib import MessageHandler

from ptars_protos import bench_pb2, simple_pb2
from ptars_protos.bench_pb2 import ExampleMessage
from ptars_protos.simple_pb2 import RepeatedNestedMessageSimple, SimpleMessage, TestEnum, SearchRequest
from python.test.random_generator import generate_messages

MESSAGES = [ExampleMessage]


@pytest.fixture()
def pool():
    return HandlerPool([simple_pb2.DESCRIPTOR, bench_pb2.DESCRIPTOR])


@pytest.fixture()
def simple_message_handler(pool: HandlerPool) -> MessageHandler:
    return pool.get_for_message(simple_pb2.SimpleMessage.DESCRIPTOR)


def test_generate_proto(simple_message_handler):
    SearchRequest()
    search_request = SearchRequest(
        query="hello", page_number=0, result_per_page=10
    )
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
def test_back_and_forth(message_type: type[Message], pool):
    handler = pool.get_for_message(message_type.DESCRIPTOR)

    messages = generate_messages(message_type, 10)
    payloads = [message.SerializeToString() for message in messages]

    record_batch = handler.list_to_record_batch(payloads)

    assert isinstance(record_batch, pa.RecordBatch)
    # print(record_batch["date_value"].to_pylist())
    # print([m.date_value for m in messages])


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

    assert record_batch["timestamp"].is_null().to_pylist() == [False, True]
    assert record_batch["timestamp"].cast(pa.int64()).to_pylist() == [
        1712660619123456789,
        None,
    ]


def test_date_missing(pool):
    handler = pool.get_for_message(simple_pb2.WithDate.DESCRIPTOR)
    messages = [
        simple_pb2.WithDate(date=Date(year=2024, month=4, day=9)),
        simple_pb2.WithDate(),
    ]
    payloads = [message.SerializeToString() for message in messages]
    record_batch = handler.list_to_record_batch(payloads)

    assert record_batch["date"].is_null().to_pylist() == [False, True]
    assert record_batch["date"].to_pylist() == [
        datetime.date(2024, 4, 9),
        None,
    ]


def test_repeated_date(pool):
    handler = pool.get_for_message(simple_pb2.WithDate.DESCRIPTOR)
    messages = [
        simple_pb2.WithDate(
            dates=[
                Date(year=2024, month=4, day=9),
                Date(year=2024, month=4, day=10),
            ]
        ),
        simple_pb2.WithDate(dates=[]),
        simple_pb2.WithDate(),
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


def run_round_trip(messages, message_type):
    payloads = [message.SerializeToString() for message in messages]

    pool = HandlerPool([message_type.DESCRIPTOR.file])
    handler = pool.get_for_message(message_type.DESCRIPTOR)
    record_batch = handler.list_to_record_batch(payloads)
    assert isinstance(record_batch, pa.RecordBatch)
    array: pa.BinaryArray = handler.record_batch_to_array(record_batch)
    assert isinstance(array, pa.BinaryArray)
    messages_back = [message_type.FromString(s.as_py()) for s in array]
    assert messages_back == messages


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


@pytest.mark.skip(reason="WIP")
def test_round_trip_not_ready():
    run_round_trip(
        [
            RepeatedNestedMessageSimple(
                search_results=[
                    simple_pb2.SearchResult(),
                    simple_pb2.SearchResult(),
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
    try:
        record_batch.to_pandas().to_markdown(sys.stdout, index=False)
    except ImportError:
        pass

    array: pa.BinaryArray = handler.record_batch_to_array(record_batch)
    messages_back: list[SearchRequest] = [
        SearchRequest.FromString(s.as_py()) for s in array
    ]
    assert messages_back == messages
