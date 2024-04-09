from google.protobuf.descriptor import Descriptor, FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorProto

from ptars._lib import MessageHandler, ProtoCache


def _file_descriptor_to_bytes(fd: FileDescriptor) -> bytes:
    file_descriptor = FileDescriptorProto()
    fd.CopyToProto(file_descriptor)
    return file_descriptor.SerializeToString()


def _get_dependencies(
    file_descriptor: FileDescriptor, results: list[FileDescriptor] = None
) -> list[FileDescriptor]:
    """
    Return list of FileDescriptor that this file depends on, including this one.

    Results are in topological order (least dependent first).
    """
    if results is None:
        results = []
    for dependency in file_descriptor.dependencies:
        if dependency not in results:
            _get_dependencies(dependency, results)
    results.append(file_descriptor)
    return results[::-1]


class HandlerPool:
    def __init__(self):
        self._proto_cache = ProtoCache()
        self._pool = {}

    def get_for_message(self, descriptor: Descriptor) -> MessageHandler:
        assert isinstance(descriptor, Descriptor)
        try:
            self._pool[descriptor.full_name]
        except KeyError:
            file_descriptor = descriptor.file

            dependencies = _get_dependencies(file_descriptor)
            payloads = [_file_descriptor_to_bytes(d) for d in dependencies]
            message_handler = self._proto_cache.create_for_message(
                "." + descriptor.full_name, payloads
            )
            self._pool[descriptor.full_name] = message_handler
            return message_handler
