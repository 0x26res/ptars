# FAQ

## Why convert from protobuf to arrow?

You need the right tool for the right job.
**Apache Arrow** is optimized for analytical tasks.
Whereas **protobuf** is optimized for transactional tasks.

**ptars** allows you to convert from one format to the other seamlessly,
deterministically and without data loss.

Here are a few use cases:

### Unified realtime and batch data processing

Transactional, real time services run using grpc or protobuf over kafka.
At the end of the day you want to run some analytical batch jobs
using the same data. ptars can convert protobuf data to arrow.
It can also help you convert parquet data generated from kafka connect
back to protobuf.

### Build realtime analytical and ML services using kafka and protobuf

You can use kafka to publish protobuf messages in real time.
These messages can then be polled and processed in micro batches.
These batches can be converted to arrow tables seamlessly
to run analytics or ML workloads.
Later the data can be converted back to protobuf and published on kafka.

### Unit Tests

For unit tests relying on data samples, you can save your protobuf
as json (or jsonl).
This data can then be parsed with high fidelity using the protobuf library,
and converted to arrow RecordBatch.

### Convert parquet data back to protobuf

If you use kafka-connect, your kafka topic data is archived as parquet files.
To run tests or replay your data you may want to convert
this parquet data to protobuf.

## Why not use `pa.Table.from_pylist` and `MessageToDict`?

You could convert protobuf messages to arrow out of the box:

```python
import pyarrow as pa
from google.protobuf.json_format import MessageToDict
from your_proto_pb2 import MyProto

my_protos = [
    MyProto(name="foo", id=1, values=[1, 2, 4]),
    MyProto(name="bar", id=2, values=[3, 4, 5]),
]

jsons = [MessageToDict(message) for message in my_protos]

table = pa.Table.from_pylist(jsons)
```

This works, but it has several drawbacks:

- It can't guess the types for missing values, empty lists, empty maps,
  or empty input.
- Special types like date and timestamp are not supported.
- Integers and floats will be cast to their 64-bit representation,
  which is inefficient.
- When representing enums as strings you'd want to use dictionary encoding
  to save memory.

ptars solves all of these issues by using the protobuf descriptor to create
a properly typed Arrow schema.

## Why ptars over protarrow?

ptars is a Rust implementation of
[protarrow](https://github.com/tradewelltech/protarrow).
While protarrow is implemented in pure Python, ptars uses Rust for the core
conversion logic, resulting in significant performance improvements:

- **2.5x faster** when converting from proto to arrow
- **3x faster** when converting from arrow to proto

If performance is critical for your use case, ptars is the better choice.

## Are there other rust library doing the same thing?

Not that we know of, but there are many places
where this conversion happens in the wild:

- [tansu](https://github.com/tansu-io/tansu) converts proto to arrow
  in order to save them to iceberg.
- [arroyo](https://github.com/ArroyoSystems/arroyo) converts proto to arrow
  to leverage the columnar format.

## Why prost over other Rust protobuf libraries?

ptars uses [prost](https://github.com/tokio-rs/prost)
for protobuf handling in Rust.
We chose prost over alternatives like
[rust-protobuf](https://github.com/stepancheg/rust-protobuf) because:

- **Performance**: prost is faster for both serialization and deserialization.
- **Maintenance**: prost is actively maintained by the Tokio team, ensuring
  long-term support and compatibility with the Rust ecosystem.
- **Dynamic Message Support**: prost-reflect provides runtime reflection
  capabilities, allowing ptars to handle protobuf messages
  without knowing the schema at compile time.
