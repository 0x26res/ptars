# API Reference

## HandlerPool

::: ptars.HandlerPool
    options:
      members:
        - __init__
        - get_for_message
        - messages_to_record_batch
        - record_batch_to_messages

## MessageHandler

The `MessageHandler` class is returned by `HandlerPool.get_for_message()` and provides low-level conversion methods.

### Methods

#### `list_to_record_batch(payloads: list[bytes]) -> pyarrow.RecordBatch`

Convert a list of serialized protobuf messages to an Arrow RecordBatch.

**Parameters:**

- `payloads`: A list of bytes, where each element is a serialized protobuf message.

**Returns:**

- A `pyarrow.RecordBatch` with one column per field in the protobuf message.

**Example:**

```python
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
payloads = [msg.SerializeToString() for msg in messages]
record_batch = handler.list_to_record_batch(payloads)
```

---

#### `record_batch_to_array(record_batch: pyarrow.RecordBatch) -> pyarrow.BinaryArray`

Convert an Arrow RecordBatch back to serialized protobuf messages.

**Parameters:**

- `record_batch`: A `pyarrow.RecordBatch` with the same schema as produced by `list_to_record_batch`.

**Returns:**

- A `pyarrow.BinaryArray` where each element is a serialized protobuf message.

**Example:**

```python
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
binary_array = handler.record_batch_to_array(record_batch)
messages = [SearchRequest.FromString(s.as_py()) for s in binary_array]
```

---

#### `array_to_record_batch(array: pyarrow.BinaryArray) -> pyarrow.RecordBatch`

Convert a binary array of serialized protobuf messages to a RecordBatch.

**Parameters:**

- `array`: A `pyarrow.BinaryArray` where each element is a serialized protobuf message.

**Returns:**

- A `pyarrow.RecordBatch` with one column per field in the protobuf message.

**Example:**

```python
import pyarrow as pa

handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
binary_array = pa.array([msg.SerializeToString() for msg in messages], type=pa.binary())
record_batch = handler.array_to_record_batch(binary_array)
```

## Type Mappings

ptars converts protobuf types to Arrow types as follows:

| Protobuf Type | Arrow Type |
|---------------|------------|
| `double` | `float64` |
| `float` | `float32` |
| `int32` | `int32` |
| `int64` | `int64` |
| `uint32` | `uint32` |
| `uint64` | `uint64` |
| `sint32` | `int32` |
| `sint64` | `int64` |
| `fixed32` | `uint32` |
| `fixed64` | `uint64` |
| `sfixed32` | `int32` |
| `sfixed64` | `int64` |
| `bool` | `bool` |
| `string` | `utf8` |
| `bytes` | `binary` |
| `enum` | `int32` |
| `message` | `struct` |
| `repeated T` | `list<T>` |
| `map<K, V>` | `map<K, V>` |
