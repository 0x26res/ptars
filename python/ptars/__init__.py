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

from dataclasses import dataclass
from typing import Literal

from ptars.internal import HandlerPool

TimeUnitLiteral = Literal["s", "ms", "us", "ns"]


@dataclass(frozen=True)
class PtarsConfig:
    """Configuration for protobuf to Arrow conversions.

    Attributes:
        timestamp_tz: Timezone for timestamp values. Use None for timezone-naive.
        timestamp_unit: Time unit for timestamps ("s", "ms", "us", "ns").
        time_unit: Time unit for time of day ("s", "ms", "us", "ns").
        duration_unit: Time unit for durations ("s", "ms", "us", "ns").
        list_value_name: Field name for list items in Arrow schema.
        map_value_name: Field name for map values in Arrow schema.
        list_nullable: Whether list fields can be null.
        map_nullable: Whether map fields can be null.
        list_value_nullable: Whether list elements can be null.
        map_value_nullable: Whether map values can be null.
    """

    timestamp_tz: str | None = "UTC"
    timestamp_unit: TimeUnitLiteral = "ns"
    time_unit: TimeUnitLiteral = "ns"
    duration_unit: TimeUnitLiteral = "ns"
    list_value_name: str = "item"
    map_value_name: str = "value"
    list_nullable: bool = False
    map_nullable: bool = False
    list_value_nullable: bool = False
    map_value_nullable: bool = False


__all__ = ["HandlerPool", "PtarsConfig"]
