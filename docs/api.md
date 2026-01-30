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

| Protobuf Type | Arrow Type  |
| ------------- | ----------- |
| `double`      | `float64`   |
| `float`       | `float32`   |
| `int32`       | `int32`     |
| `int64`       | `int64`     |
| `uint32`      | `uint32`    |
| `uint64`      | `uint64`    |
| `sint32`      | `int32`     |
| `sint64`      | `int64`     |
| `fixed32`     | `uint32`    |
| `fixed64`     | `uint64`    |
| `sfixed32`    | `int32`     |
| `sfixed64`    | `int64`     |
| `bool`        | `bool`      |
| `string`      | `utf8`      |
| `bytes`       | `binary`    |
| `enum`        | `int32`     |
| `message`     | `struct`    |
| `repeated T`  | `list<T>`   |
| `map<K, V>`   | `map<K, V>` |
