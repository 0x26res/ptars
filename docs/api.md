# API Reference

## HandlerPool

::: ptars.HandlerPool
    options:
      members:
        - __init__
        - get_for_message
        - messages_to_record_batch
        - record_batch_to_messages
        - read_size_delimited_file

## MessageHandler

The `MessageHandler` class is returned by `HandlerPool.get_for_message()`
and provides low-level conversion methods.

### Methods

#### `list_to_record_batch(payloads: list[bytes]) -> pyarrow.RecordBatch`

Convert a list of serialized protobuf messages to an Arrow RecordBatch.

__Parameters:__

- `payloads`: A list of bytes, where each element is a serialized protobuf message.

__Returns:__

- A `pyarrow.RecordBatch` with one column per field in the protobuf message.

__Example:__

```python
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
payloads = [msg.SerializeToString() for msg in messages]
record_batch = handler.list_to_record_batch(payloads)
```

---

#### `record_batch_to_array(record_batch: pyarrow.RecordBatch) -> pyarrow.BinaryArray`

Convert an Arrow RecordBatch back to serialized protobuf messages.

__Parameters:__

- `record_batch`: A `pyarrow.RecordBatch` with the same schema as produced by `list_to_record_batch`.

__Returns:__

- A `pyarrow.BinaryArray` where each element is a serialized protobuf message.

__Example:__

```python
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
binary_array = handler.record_batch_to_array(record_batch)
messages = [SearchRequest.FromString(s.as_py()) for s in binary_array]
```

---

#### `array_to_record_batch(array: pyarrow.BinaryArray) -> pyarrow.RecordBatch`

Convert a binary array of serialized protobuf messages to a RecordBatch.

__Parameters:__

- `array`: A `pyarrow.BinaryArray` where each element is a serialized protobuf message.

__Returns:__

- A `pyarrow.RecordBatch` with one column per field in the protobuf message.

__Example:__

```python
import pyarrow as pa

handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
binary_array = pa.array([msg.SerializeToString() for msg in messages], type=pa.binary())
record_batch = handler.array_to_record_batch(binary_array)
```

## PtarsConfig

::: ptars.PtarsConfig
    options:
      members:
        - timestamp_tz
        - timestamp_unit
        - time_unit
        - duration_unit
        - list_value_name
        - map_value_name
        - list_nullable
        - map_nullable
        - list_value_nullable
        - map_value_nullable

### Configuration Options

| Option                | Type                             | Default   | Description                                                              |
|-----------------------|----------------------------------|-----------|--------------------------------------------------------------------------|
| `timestamp_tz`        | `str \| None`                    | `"UTC"`   | Timezone for `google.protobuf.Timestamp`. Use `None` for timezone-naive. |
| `timestamp_unit`      | `Literal["s", "ms", "us", "ns"]` | `"ns"`    | Time unit for timestamps.                                                |
| `time_unit`           | `Literal["s", "ms", "us", "ns"]` | `"ns"`    | Time unit for `google.type.TimeOfDay`.                                   |
| `duration_unit`       | `Literal["s", "ms", "us", "ns"]` | `"ns"`    | Time unit for `google.protobuf.Duration`.                                |
| `list_value_name`     | `str`                            | `"item"`  | Field name for list items in Arrow schema.                               |
| `map_value_name`      | `str`                            | `"value"` | Field name for map values in Arrow schema.                               |
| `list_nullable`       | `bool`                           | `False`   | Whether list fields can be null.                                         |
| `map_nullable`        | `bool`                           | `False`   | Whether map fields can be null.                                          |
| `list_value_nullable` | `bool`                           | `False`   | Whether list elements can be null.                                       |
| `map_value_nullable`  | `bool`                           | `False`   | Whether map values can be null.                                          |

!!! warning "Precision Loss with Coarser Time Units"
    When converting timestamps, time of day, or duration values to coarser time units
    (e.g., `"s"` instead of `"ns"`), sub-unit precision is __truncated__ (not rounded).
    For example:

    - A timestamp at `1.999` seconds with `timestamp_unit="s"` becomes `1` second
    - A time of day `01:02:03.500` with `time_unit="s"` becomes `01:02:03`

    Choose the appropriate unit based on your precision requirements.

### Example

```python
from ptars import HandlerPool, PtarsConfig

# Use microsecond precision for timestamps
config = PtarsConfig(
    timestamp_unit="us",
    timestamp_tz="America/New_York",
    list_value_name="element",
)

pool = HandlerPool([MyMessage.DESCRIPTOR.file], config=config)
```

---

#### `read_size_delimited_file(path: str) -> pyarrow.RecordBatch`

Read size-delimited protobuf messages from a file and convert to a RecordBatch.

Size-delimited format means each message is preceded by its size encoded as a varint.
This is a common format for storing multiple protobuf messages in a single file.

__Parameters:__

- `path`: Path to the file containing size-delimited protobuf messages.

__Returns:__

- A `pyarrow.RecordBatch` with one column per field in the protobuf message.

__Example:__

```python
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
record_batch = handler.read_size_delimited_file("messages.bin")
```

## Type Mappings

ptars converts protobuf types to Arrow types as follows:

### Scalar Types

| Protobuf Type | Arrow Type |
|---------------|------------|
| `double`      | `float64`  |
| `float`       | `float32`  |
| `int32`       | `int32`    |
| `int64`       | `int64`    |
| `uint32`      | `uint32`   |
| `uint64`      | `uint64`   |
| `sint32`      | `int32`    |
| `sint64`      | `int64`    |
| `fixed32`     | `uint32`   |
| `fixed64`     | `uint64`   |
| `sfixed32`    | `int32`    |
| `sfixed64`    | `int64`    |
| `bool`        | `bool`     |
| `string`      | `utf8`     |
| `bytes`       | `binary`   |
| `enum`        | `int32`    |

### Composite Types

| Protobuf Type | Arrow Type  |
|---------------|-------------|
| `message`     | `struct`    |
| `repeated T`  | `list<T>`   |
| `map<K, V>`   | `map<K, V>` |

### Well-Known Types

| Protobuf Type                | Arrow Type                        | Notes                                            |
|------------------------------|-----------------------------------|--------------------------------------------------|
| `google.protobuf.Timestamp`  | `timestamp[unit, tz]`             | Unit and timezone configurable via `PtarsConfig` |
| `google.type.Date`           | `date32`                          |                                                  |
| `google.type.TimeOfDay`      | `time32[s]`, `time32[ms]`, `time64[us]`, or `time64[ns]` | Unit configurable via `PtarsConfig` (see below)  |

**TimeOfDay type mapping by unit:**

| `time_unit` | Arrow Type   |
|-------------|--------------|
| `"s"`       | `time32[s]`  |
| `"ms"`      | `time32[ms]` |
| `"us"`      | `time64[us]` |
| `"ns"`      | `time64[ns]` |

### Wrapper Types

Wrapper types are converted to their corresponding Arrow types with nullability.
These are useful for representing nullable scalars in proto3.

| Protobuf Type                 | Arrow Type           |
|-------------------------------|----------------------|
| `google.protobuf.DoubleValue` | `float64` (nullable) |
| `google.protobuf.FloatValue`  | `float32` (nullable) |
| `google.protobuf.Int64Value`  | `int64` (nullable)   |
| `google.protobuf.UInt64Value` | `uint64` (nullable)  |
| `google.protobuf.Int32Value`  | `int32` (nullable)   |
| `google.protobuf.UInt32Value` | `uint32` (nullable)  |
| `google.protobuf.BoolValue`   | `bool` (nullable)    |
| `google.protobuf.StringValue` | `utf8` (nullable)    |
| `google.protobuf.BytesValue`  | `binary` (nullable)  |
