"""Internal implementation of ptars Python bindings."""

import warnings

import google._upb._message
import pyarrow as pa
from google.protobuf.descriptor import Descriptor, FileDescriptor
from google.protobuf.descriptor_pb2 import FileDescriptorProto
from google.protobuf.message import Message
from ptars._lib import MessageHandler, ProtoCache  # type: ignore[unresolved-import]


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
    """A pool for managing protobuf message handlers.

    The HandlerPool caches message handlers for efficient conversion between
    protobuf messages and Arrow RecordBatches. It automatically resolves
    protobuf file descriptor dependencies.

    Args:
        file_descriptors: A list of protobuf FileDescriptor objects.
            All dependencies will be automatically resolved.

    Example:
        ```python
        from ptars import HandlerPool
        from your_proto_pb2 import YourMessage

        pool = HandlerPool([YourMessage.DESCRIPTOR.file])
        handler = pool.get_for_message(YourMessage.DESCRIPTOR)
        record_batch = handler.list_to_record_batch(payloads)
        ```
    """

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

    def get_for_message(
        self, descriptor: Descriptor | google._upb._message.Descriptor
    ) -> MessageHandler:
        """Get a message handler for the given protobuf descriptor.

        Args:
            descriptor: A protobuf message Descriptor.

        Returns:
            A MessageHandler that can convert messages of this type
            to/from Arrow format.

        Raises:
            TypeError: If descriptor is not a valid protobuf Descriptor.
        """
        if isinstance(descriptor, google._upb._message.MessageMeta):
            warnings.warn(
                f"Received {google._upb._message.MessageMeta.__class__.__name__}"
                f" instead of {Descriptor.__class__.__name__}"
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
        """Convert a list of protobuf messages to an Arrow RecordBatch.

        This is a convenience method that handles serialization internally.

        Args:
            messages: A list of protobuf message instances.
            descriptor: The protobuf Descriptor for the message type.

        Returns:
            A pyarrow.RecordBatch with one column per protobuf field.

        Example:
            ```python
            pool = HandlerPool([MyMessage.DESCRIPTOR.file])
            messages = [MyMessage(field="value")]
            record_batch = pool.messages_to_record_batch(
                messages, MyMessage.DESCRIPTOR
            )
            ```
        """
        handler = self.get_for_message(descriptor)
        return handler.list_to_record_batch([m.SerializeToString() for m in messages])

    def record_batch_to_messages(
        self, record_batch: pa.RecordBatch, descriptor: Descriptor
    ) -> list[Message]:
        """Convert an Arrow RecordBatch back to protobuf messages.

        This is a convenience method that handles deserialization internally.

        Args:
            record_batch: A pyarrow.RecordBatch with the same schema
                as produced by messages_to_record_batch.
            descriptor: The protobuf Descriptor for the message type.

        Returns:
            A list of protobuf message instances.

        Example:
            ```python
            pool = HandlerPool([MyMessage.DESCRIPTOR.file])
            messages = pool.record_batch_to_messages(
                record_batch, MyMessage.DESCRIPTOR
            )
            ```
        """
        handler = self.get_for_message(descriptor)
        array = handler.record_batch_to_array(record_batch)
        return [descriptor._concrete_class.FromString(s.as_py()) for s in array]  # type: ignore[unresolved-attribute]
