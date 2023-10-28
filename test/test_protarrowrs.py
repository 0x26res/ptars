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


def test_generate_proto():
    file_descriptor = simple_pb2.DESCRIPTOR
    payloads = [_file_descriptor_to_bytes(d) for d in file_descriptor.dependencies] + [
        _file_descriptor_to_bytes(file_descriptor)
    ]
    lib.py_create(payloads)
