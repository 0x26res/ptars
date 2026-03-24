# ptars

Protobuf to Arrow, using Rust.

[![PyPI Version](https://img.shields.io/pypi/v/ptars)](https://pypi.org/project/ptars/)
[![Python Version](https://img.shields.io/pypi/pyversions/ptars)](https://pypi.org/project/ptars/)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/0x26res/ptars/blob/master/LICENSE)

ptars is a high-performance library for converting Protocol Buffer messages
to Apache Arrow format and back.
It's implemented in Rust with Python bindings via PyO3.

Ptars converts directly between the protobuf wire format and Arrow columnar arrays.
No intermediate message objects (like `DynamicMessage`) are created.
Serialized protobuf bytes are parsed straight into Arrow builders.
And Arrow arrays are encoded directly to protobuf wire format, avoiding per-row object allocation entirely.

## Features

- Convert serialized protobuf messages directly to `pyarrow.RecordBatch`
- Convert `pyarrow.RecordBatch` back to serialized protobuf messages
- Direct wire format encoding/decoding — no intermediate message objects
- Support for nested messages, repeated fields, and maps
- Configurable type mappings (timestamp precision, timezone, nullability)
- High performance through Rust implementation

## Installation

```bash
pip install ptars
```

## Quick Example

```python
from ptars import HandlerPool

# Your protobuf message class (generated from .proto file)
from your_proto_pb2 import SearchRequest

messages = [
    SearchRequest(
        query="protobuf to arrow",
        page_number=0,
        result_per_page=10,
    ),
    SearchRequest(
        query="protobuf to arrow",
        page_number=1,
        result_per_page=10,
    ),
]
payloads = [message.SerializeToString() for message in messages]

pool = HandlerPool([SearchRequest.DESCRIPTOR.file])
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
record_batch = handler.list_to_record_batch(payloads)
```

See the [Getting Started](getting-started.md) guide for more details.

## Performance

ptars is a Rust implementation of
[protarrow](https://github.com/tradewelltech/protarrow),
which is implemented in plain Python.
By encoding and decoding directly between protobuf wire format and Arrow arrays, ptars is:

- **7x+ faster** when converting from proto to Arrow
- **30x+ faster** when converting from Arrow to proto

```text
---- benchmark 'to_arrow': 2 tests ----
Name (time in us)        Mean
---------------------------------------
ptars_to_arrow          659 (1.0)
protarrow_to_arrow    5,037 (7.65)
---------------------------------------

---- benchmark 'to_proto': 2 tests -----
Name (time in us)         Mean
----------------------------------------
ptars_to_proto           397 (1.0)
protarrow_to_proto    12,534 (31.61)
----------------------------------------
```
