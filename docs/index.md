# ptars

Protobuf to Arrow, using Rust.

[![PyPI Version](https://img.shields.io/pypi/v/ptars)](https://pypi.org/project/ptars/)
[![Python Version](https://img.shields.io/pypi/pyversions/ptars)](https://pypi.org/project/ptars/)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/0x26res/ptars/blob/master/LICENSE)

ptars is a high-performance library for converting Protocol Buffer messages
to Apache Arrow format and back.
It's implemented in Rust with Python bindings via PyO3.

## Features

- Convert serialized protobuf messages directly to `pyarrow.RecordBatch`
- Convert `pyarrow.RecordBatch` back to serialized protobuf messages
- Support for nested messages, repeated fields, and maps
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
which is implemented in plain Python. Benchmarks show:

- **2.5x faster** when converting from proto to arrow
- **3x faster** when converting from arrow to proto

```text
---- benchmark 'to_arrow': 2 tests ----
Name (time in ms)        Mean
---------------------------------------
protarrow_to_arrow     9.4863 (2.63)
ptars_to_arrow         3.6009 (1.0)
---------------------------------------

---- benchmark 'to_proto': 2 tests -----
Name (time in ms)         Mean
----------------------------------------
protarrow_to_proto     20.8297 (3.20)
ptars_to_proto          6.5013 (1.0)
----------------------------------------
```
