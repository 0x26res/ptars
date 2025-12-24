import warnings

import pyarrow as pa
from google._upb._message import MessageMeta
from google.protobuf.descriptor import Descriptor, FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorProto
from google.protobuf.message import Message
from ptars._lib import MessageHandler, ProtoCache


def _file_descriptor_to_bytes(fd: FileDescriptor) -> bytes:
    file_descriptor = FileDescriptorProto()
    fd.CopyToProto(file_descriptor)
    return file_descriptor.SerializeToString()


def _get_dependencies(
    file_descriptor: FileDescriptor, results: list[FileDescriptor] | None = None
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
    return results


class HandlerPool:
    def __init__(self, file_descriptors: list[FileDescriptor]):
        all_descriptors = []
        for file_descriptor in file_descriptors:
            if not isinstance(file_descriptor, FileDescriptor):
                raise TypeError(f"Expecting {Descriptor.__name__}")

            if file_descriptor not in all_descriptors:
                new_descriptors = _get_dependencies(file_descriptor)
                for new_descriptor in new_descriptors:
                    if new_descriptor not in all_descriptors:
                        all_descriptors.append(new_descriptor)

        payloads = [_file_descriptor_to_bytes(d) for d in all_descriptors]

        self._proto_cache = ProtoCache(payloads)
        self._pool = {}

    def get_for_message(self, descriptor: Descriptor) -> MessageHandler:
        if isinstance(descriptor, MessageMeta):
            warnings.warn(
                f"Received {MessageMeta.__name__} instead of {Descriptor.__name__}"
            )
            descriptor = descriptor.DESCRIPTOR
        if not isinstance(descriptor, Descriptor):
            raise TypeError(f"Expecting {Descriptor.__name__}")

        assert isinstance(descriptor, Descriptor)
        try:
            return self._pool[descriptor.full_name]
        except KeyError:
            result = self._proto_cache.create_for_message(descriptor.full_name)
            self._pool[descriptor.full_name] = result
            return result

    def messages_to_record_batch(
        self, messages: list[Message], descriptor: Descriptor
    ) -> pa.RecordBatch:
        handler = self.get_for_message(descriptor)
        return handler.list_to_record_batch([m.SerializeToString() for m in messages])

    def record_batch_to_messages(
        self, record_batch: pa.RecordBatch, descriptor: Descriptor
    ) -> list[Message]:
        handler = self.get_for_message(descriptor)
        array = handler.record_batch_to_array(record_batch)
        return [descriptor._concrete_class.FromString(s.as_py()) for s in array]  # type: ignore[unresolved-attribute]
