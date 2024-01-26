from test.random_generator import generate_messages

import pyarrow as pa
import pytest
from google.protobuf.message import Message

from ptars import HandlerPool
from ptars_protos import simple_pb2
from ptars_protos.bench_pb2 import ExampleMessage

MESSAGES = [ExampleMessage]


def test_generate_proto():
    pool = HandlerPool()

    handler = pool.get_for_message(simple_pb2.SimpleMessage.DESCRIPTOR)
    simple_pb2.SearchRequest()
    search_request = simple_pb2.SearchRequest(
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
            enum_value=1,
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
    table = handler.list_to_record_batch(message_payloads)

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
def test_back_and_forth(message_type: type[Message]):
    messages = generate_messages(message_type, 10)
    payloads = [message.SerializeToString() for message in messages]

    pool = HandlerPool()
    handler = pool.get_for_message(message_type.DESCRIPTOR)
    record_batch = handler.list_to_record_batch(payloads)

    assert isinstance(record_batch, pa.RecordBatch)
    print(record_batch["date_value"].to_pylist())
    print([m.date_value for m in messages])


def test_arrow_to_proto():
    record_batch = pa.record_batch([[1, 2, 3]], ["col1"])
    pool = HandlerPool()
    handler = pool.get_for_message(ExampleMessage.DESCRIPTOR)
    array = handler.record_batch_to_array(record_batch)
    assert isinstance(array, pa.Array)
    assert array.type == pa.binary()
