"""ptars: Protobuf to Arrow conversion using Rust.

This library provides fast conversion between Protocol Buffer messages
and Apache Arrow format, implemented in Rust with Python bindings.

Example:
    ```python
    from ptars import HandlerPool
    from your_proto_pb2 import YourMessage

    pool = HandlerPool([YourMessage.DESCRIPTOR.file])
    handler = pool.get_for_message(YourMessage.DESCRIPTOR)
    record_batch = handler.list_to_record_batch(payloads)
    ```
"""

from ptars.internal import HandlerPool

__all__ = ["HandlerPool"]
