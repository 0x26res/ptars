from google.protobuf.descriptor import FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorProto

import protarrowrs
import protarrowrs._lib as lib
from protarrow_protos import simple_pb2


def test_get_a_table():
    protarrowrs.get_a_table()


def _file_descriptor_to_bytes(fd: FileDescriptor) -> bytes:
    file_descriptor = FileDescriptorProto()
    fd.CopyToProto(file_descriptor)
    return file_descriptor.SerializeToString()


def get_dependencies(
    file_descriptor: FileDescriptor, results: list[FileDescriptor] = None
) -> list[FileDescriptor]:
    if results is None:
        results = []
    results.append(file_descriptor)
    for dependency in file_descriptor.dependencies:
        if dependency not in results:
            get_dependencies(dependency, results)
    return results


def test_generate_proto():
    cache = lib.ProtoCache()

    file_descriptor = simple_pb2.DESCRIPTOR
    dependencies = get_dependencies(file_descriptor)
    payloads = [_file_descriptor_to_bytes(d) for d in dependencies]
    assert len(payloads) == 2

    message_handler = cache.create_for_message(
        "." + simple_pb2.TestMessage.DESCRIPTOR.full_name, payloads
    )
    print(message_handler.list_to_table())
