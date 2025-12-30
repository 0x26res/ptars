# ptars

[![Crates.io](https://img.shields.io/crates/v/ptars.svg)](https://crates.io/crates/ptars)
[![Documentation](https://docs.rs/ptars/badge.svg)](https://docs.rs/ptars)
[![License](https://img.shields.io/crates/l/ptars.svg)](https://github.com/0x26res/ptars/blob/main/LICENSE)

Fast conversion between Protocol Buffers and Apache Arrow in Rust.

## Features

- Convert protobuf messages to Arrow `RecordBatch`
- Convert Arrow `RecordBatch` back to serialized protobuf messages
- Support for nested messages, repeated fields, and maps
- Special handling for well-known types:
  - `google.protobuf.Timestamp` → `timestamp[ns]`
  - `google.type.Date` → `date32`
  - `google.type.TimeOfDay` → `time64[ns]`
  - Wrapper types (`DoubleValue`, `Int32Value`, etc.) → nullable primitives

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
ptars = "0.0.9"
prost-reflect = "0.16"
```

### Converting Protobuf to Arrow

```rust
use ptars::messages_to_record_batch;
use prost_reflect::{DescriptorPool, DynamicMessage};

// Load your protobuf descriptor
let pool = DescriptorPool::decode(include_bytes!("descriptor.bin").as_ref()).unwrap();
let message_descriptor = pool.get_message_by_name("my.package.MyMessage").unwrap();

// Create some messages
let mut msg = DynamicMessage::new(message_descriptor.clone());
msg.set_field_by_name("id", prost_reflect::Value::I32(42));
msg.set_field_by_name("name", prost_reflect::Value::String("example".into()));

let messages = vec![msg];

// Convert to Arrow RecordBatch
let record_batch = messages_to_record_batch(&messages, &message_descriptor);
```

### Converting Binary Array to Arrow

If you have serialized protobuf messages in an Arrow `BinaryArray`:

```rust
use ptars::binary_array_to_record_batch;
use arrow_array::BinaryArray;

let binary_array: BinaryArray = /* your serialized messages */;
let record_batch = binary_array_to_record_batch(&binary_array, &message_descriptor).unwrap();
```

### Converting Arrow back to Protobuf

```rust
use ptars::record_batch_to_array;

// Convert RecordBatch to a BinaryArray of serialized messages
let binary_array = record_batch_to_array(&record_batch, &message_descriptor);

// Decode individual messages
for i in 0..binary_array.len() {
    let msg = DynamicMessage::decode(message_descriptor.clone(), binary_array.value(i)).unwrap();
}
```

## License

Apache-2.0
