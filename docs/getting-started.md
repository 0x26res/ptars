# Getting Started

This guide will help you get started with ptars for converting
Protocol Buffer messages to Apache Arrow format.

## Installation

Install ptars using pip:

```bash
pip install ptars
```

## Prerequisites

You'll need:

- Python 3.10 or higher
- A protobuf schema (`.proto` file) compiled to Python
- `protobuf` and `pyarrow` packages (installed automatically with ptars)

## Basic Usage

### Define Your Protobuf Schema

First, create a protobuf schema. For example, `search.proto`:

```protobuf
syntax = "proto3";

message SearchRequest {
  string query = 1;
  int32 page_number = 2;
  int32 result_per_page = 3;
}
```

Compile it to Python using `protoc`:

```bash
protoc --python_out=. search.proto
```

### Convert Protobuf to Arrow

```python
from ptars import HandlerPool
from search_pb2 import SearchRequest

# Create some messages
messages = [
    SearchRequest(query="hello", page_number=1, result_per_page=10),
    SearchRequest(query="world", page_number=2, result_per_page=20),
]

# Serialize messages to bytes
payloads = [msg.SerializeToString() for msg in messages]

# Create a handler pool with the file descriptor
pool = HandlerPool([SearchRequest.DESCRIPTOR.file])

# Get a handler for the specific message type
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)

# Convert to Arrow RecordBatch
record_batch = handler.list_to_record_batch(payloads)

print(record_batch.to_pandas())
#    query  page_number  result_per_page
# 0  hello            1               10
# 1  world            2               20
```

### Convert Arrow to Protobuf

```python
# Convert RecordBatch back to serialized protobuf
array = handler.record_batch_to_array(record_batch)

# Deserialize back to protobuf messages
messages_back = [SearchRequest.FromString(s.as_py()) for s in array]

for msg in messages_back:
    print(f"query={msg.query}, page={msg.page_number}")
```

## Using the High-Level API

The `HandlerPool` class provides convenience methods for direct message conversion:

```python
from ptars import HandlerPool
from search_pb2 import SearchRequest

pool = HandlerPool([SearchRequest.DESCRIPTOR.file])

# Convert messages directly (without manual serialization)
messages = [
    SearchRequest(query="hello", page_number=1, result_per_page=10),
    SearchRequest(query="world", page_number=2, result_per_page=20),
]

# Proto to Arrow
record_batch = pool.messages_to_record_batch(messages, SearchRequest.DESCRIPTOR)

# Arrow to Proto
messages_back = pool.record_batch_to_messages(record_batch, SearchRequest.DESCRIPTOR)
```

## Working with Nested Messages

ptars supports nested protobuf messages:

```protobuf
syntax = "proto3";

message Address {
  string street = 1;
  string city = 2;
}

message Person {
  string name = 1;
  Address address = 2;
}
```

```python
from ptars import HandlerPool
from person_pb2 import Person, Address

messages = [
    Person(name="Alice", address=Address(street="123 Main St", city="NYC")),
    Person(name="Bob", address=Address(street="456 Oak Ave", city="LA")),
]

pool = HandlerPool([Person.DESCRIPTOR.file])
record_batch = pool.messages_to_record_batch(messages, Person.DESCRIPTOR)
```

The nested `Address` message will be converted to a nested Arrow struct type.

## Working with Repeated Fields

Repeated fields are converted to Arrow list types:

```protobuf
syntax = "proto3";

message Order {
  string order_id = 1;
  repeated string items = 2;
}
```

```python
from ptars import HandlerPool
from order_pb2 import Order

messages = [
    Order(order_id="001", items=["apple", "banana"]),
    Order(order_id="002", items=["orange"]),
]

pool = HandlerPool([Order.DESCRIPTOR.file])
record_batch = pool.messages_to_record_batch(messages, Order.DESCRIPTOR)
```

## Working with Maps

Protobuf maps are also supported:

```protobuf
syntax = "proto3";

message Config {
  map<string, string> settings = 1;
}
```

```python
from ptars import HandlerPool
from config_pb2 import Config

messages = [
    Config(settings={"key1": "value1", "key2": "value2"}),
]

pool = HandlerPool([Config.DESCRIPTOR.file])
record_batch = pool.messages_to_record_batch(messages, Config.DESCRIPTOR)
```

## Binary Array Input

For better performance when you already have a `pyarrow.BinaryArray`
of serialized messages:

```python
import pyarrow as pa
from ptars import HandlerPool
from search_pb2 import SearchRequest

# If you have serialized messages as a BinaryArray
payloads = [msg.SerializeToString() for msg in messages]
binary_array = pa.array(payloads, type=pa.binary())

pool = HandlerPool([SearchRequest.DESCRIPTOR.file])
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)

# Convert directly from BinaryArray
record_batch = handler.array_to_record_batch(binary_array)
```
