import random
import secrets

import protarrow
import pyarrow as pa
import pytest
from google._upb._message import MessageMeta
from ptars._lib import MessageHandler
from pytest_benchmark.fixture import BenchmarkFixture

import ptars
from ptars_protos import benchmark_pb2
from ptars_protos.benchmark_pb2 import BenchmarkMessage

MESSAGE_COUNT = 10_000
STRING_SIZE = 10
MIN_INT = -2147483648
MAX_INT = 2147483647


@pytest.fixture
def messages() -> list[BenchmarkMessage]:
    return [
        BenchmarkMessage(
            query=secrets.token_urlsafe(random.randint(0, STRING_SIZE)),
            page_number=random.randint(MIN_INT, MAX_INT),
            result_per_page=random.randint(MIN_INT, MAX_INT),
        )
        for _ in range(MESSAGE_COUNT)
    ]


@pytest.fixture()
def payloads(messages: list[BenchmarkMessage]) -> list[bytes]:
    return [m.SerializeToString() for m in messages]


def run_protarrow_to_arrow(
    payloads: list[bytes], message_type: MessageMeta
) -> pa.RecordBatch:
    return protarrow.messages_to_record_batch(
        [BenchmarkMessage.FromString(p) for p in payloads],
        message_type,
    )


def test_same(payloads: list[bytes]):
    assert protarrow.messages_to_record_batch(
        [BenchmarkMessage.FromString(p) for p in payloads],
        BenchmarkMessage,
    ) == ptars.HandlerPool([benchmark_pb2.DESCRIPTOR]).get_for_message(
        BenchmarkMessage.DESCRIPTOR
    ).list_to_record_batch(payloads)


def run_protarrow_to_proto(record_batch, message_type):
    messages = protarrow.record_batch_to_messages(record_batch, message_type)
    return [m.SerializeToString() for m in messages]


def test_protarrow_to_arrow(benchmark: BenchmarkFixture, payloads: list[bytes]):
    benchmark.group = "to_arrow"
    benchmark(run_protarrow_to_arrow, payloads, BenchmarkMessage)


def test_ptars_to_arrow(benchmark: BenchmarkFixture, payloads: list[bytes]):
    benchmark.group = "to_arrow"
    pool = ptars.HandlerPool([benchmark_pb2.DESCRIPTOR])
    handler = pool.get_for_message(BenchmarkMessage.DESCRIPTOR)

    benchmark(handler.list_to_record_batch, payloads)


def test_protarrow_to_proto(benchmark: BenchmarkFixture, payloads: list[bytes]):
    benchmark.group = "to_proto"
    record_batch = run_protarrow_to_arrow(payloads, BenchmarkMessage)
    benchmark(run_protarrow_to_proto, record_batch, BenchmarkMessage)


def test_ptars_to_proto(benchmark: BenchmarkFixture, payloads: list[bytes]):
    benchmark.group = "to_proto"
    pool = ptars.HandlerPool([benchmark_pb2.DESCRIPTOR])
    handler = pool.get_for_message(BenchmarkMessage.DESCRIPTOR)
    record_batch = handler.list_to_record_batch(payloads)

    benchmark(handler.record_batch_to_array, record_batch)


def run_python_deserialize(payloads, message_type):
    [message_type.FromString(p) for p in payloads]


def test_python_deserialize(benchmark: BenchmarkFixture, payloads: list[bytes]):
    benchmark.group = "deserialize"
    benchmark(run_python_deserialize, payloads, BenchmarkMessage)


def test_rust_deserialize(benchmark: BenchmarkFixture, payloads: list[bytes]):
    benchmark.group = "deserialize"
    pool = ptars.HandlerPool([benchmark_pb2.DESCRIPTOR])
    handler: MessageHandler = pool.get_for_message(BenchmarkMessage.DESCRIPTOR)
    benchmark(handler.just_convert, payloads)
