# Getting Started

This guide will help you get started with ptars for converting
Protocol Buffer messages to Apache Arrow format.

## Installation

Install ptars using pip:

```bash
pip install ptars
```

You'll need Python 3.10 or higher and a protobuf schema (`.proto` file)
compiled to Python. The `protobuf` and `pyarrow` packages are installed
automatically with ptars.

## Basic Usage

Given a protobuf schema, `search.proto`, compiled with
`protoc --python_out=. search.proto`:

```protobuf
syntax = "proto3";

message SearchRequest {
  string query = 1;
  int32 page_number = 2;
  int32 result_per_page = 3;
}
```

Convert messages to an Arrow RecordBatch and back:

```python
from ptars import HandlerPool
from search_pb2 import SearchRequest

messages = [
    SearchRequest(query="hello", page_number=1, result_per_page=10),
    SearchRequest(query="world", page_number=2, result_per_page=20),
]

pool = HandlerPool([SearchRequest.DESCRIPTOR.file])

# Proto to Arrow
record_batch = pool.messages_to_record_batch(messages, SearchRequest.DESCRIPTOR)

print(record_batch.to_pandas())
#    query  page_number  result_per_page
# 0  hello            1               10
# 1  world            2               20

# Arrow to Proto
messages_back = pool.record_batch_to_messages(record_batch, SearchRequest.DESCRIPTOR)
```

Nested messages, repeated fields, and maps are converted to Arrow structs,
lists, and maps respectively — see [Type Mappings](api.md#type-mappings).

## Working with Serialized Messages

If your messages are already serialized (e.g. read from Kafka or a database),
use a handler to skip the protobuf object layer entirely:

```python
payloads = [msg.SerializeToString() for msg in messages]

handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
record_batch = handler.list_to_record_batch(payloads)

# and back to serialized messages
binary_array = handler.record_batch_to_array(record_batch)
```

`handler.array_to_record_batch` accepts a `pyarrow.BinaryArray` of
serialized messages directly, which is faster when your payloads are
already in Arrow.

## Getting the Arrow Schema

Use `get_schema` to get the Arrow schema for a message type without converting
any data, for example to pre-declare the schema of a dataset or stream:

```python
from ptars import PtarsConfig, get_schema
from search_pb2 import SearchRequest

schema = get_schema(SearchRequest.DESCRIPTOR)

# The schema reflects the configuration
schema = get_schema(SearchRequest.DESCRIPTOR, PtarsConfig(use_large_string=True))
```

If you already have a handler, `handler.schema()` returns the same schema
without building a new pool.

## Reading Size-Delimited Files

ptars can read files of size-delimited protobuf messages (each message
preceded by its varint-encoded size, as written by Java's
`writeDelimitedTo()` and similar functions):

```python
record_batch = pool.read_size_delimited_file("messages.bin", SearchRequest.DESCRIPTOR)
```

## Configuration

Use `PtarsConfig` to customize Arrow type mappings — timestamp precision and
timezone, large types, nullability, enum representation, and stripping of the
Confluent Schema Registry wire format:

```python
from ptars import HandlerPool, PtarsConfig

config = PtarsConfig(
    timestamp_unit="us",  # microseconds instead of nanoseconds
    timestamp_tz="America/New_York",  # custom timezone
)

pool = HandlerPool([MyMessage.DESCRIPTOR.file], config=config)
```

See the [API Reference](api.md#ptarsconfig) for the full list of options.
