from protarrow_protos import simple_pb2
from protarrowrs import HandlerPool


def test_generate_proto():
    pool = HandlerPool()

    handler = pool.get_for_message(simple_pb2.SimpleMessage.DESCRIPTOR)
    protos = [
        simple_pb2.SimpleMessage(col_1=123, col_2=456),
        simple_pb2.SimpleMessage(col_1=0, col_2=789),
    ]
    message_payloads = [p.SerializeToString() for p in protos]
    table = handler.list_to_table(message_payloads)

    assert table["col_1"].to_pylist() == [123, 0]
    assert table["col_2"].to_pylist() == [456, 789]
