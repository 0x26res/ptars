import pyarrow as pa
import pytest
from google.protobuf.descriptor import FileDescriptor
from ptars._lib import MessageHandler

from ptars import HandlerPool
from ptars_protos.bench_pb2 import DESCRIPTOR, ExampleMessage


def test_with_descriptor():
    pool = HandlerPool([DESCRIPTOR])
    assert pool._proto_cache.get_names() != []
    handler = pool.get_for_message(ExampleMessage.DESCRIPTOR)
    assert isinstance(handler, MessageHandler)
    same_handler = pool.get_for_message(ExampleMessage.DESCRIPTOR)
    assert handler is same_handler
    with_message = pool.get_for_message(ExampleMessage)
    assert with_message is handler

    other_pool = HandlerPool([DESCRIPTOR])
    other_handler = other_pool.get_for_message(ExampleMessage.DESCRIPTOR)
    assert other_handler is not handler


def test_with_wrong_types():
    pool = HandlerPool([DESCRIPTOR])

    with pytest.raises(TypeError, match="Expecting Descriptor"):
        pool.get_for_message(None)

    assert isinstance(ExampleMessage.DESCRIPTOR.file, FileDescriptor)
    with pytest.raises(TypeError, match="Expecting Descriptor"):
        pool.get_for_message(ExampleMessage.DESCRIPTOR.file)


def test_messages_to_record_batch():
    pool = HandlerPool([DESCRIPTOR])
    batch = pool.messages_to_record_batch(
        [
            ExampleMessage(double_value=1.0),
            ExampleMessage(int32_value=1),
        ],
        ExampleMessage.DESCRIPTOR,
    )
    assert isinstance(batch, pa.RecordBatch)
    assert batch["double_value"].to_pylist() == [1.0, 0.0]
    assert batch["int32_value"].to_pylist() == [0, 1]

    pool.record_batch_to_messages(batch, ExampleMessage.DESCRIPTOR)
