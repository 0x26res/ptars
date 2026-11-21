# ptars

[![Crates.io](https://img.shields.io/crates/v/ptars.svg)](https://crates.io/crates/ptars)
[![Documentation](https://docs.rs/ptars/badge.svg)](https://docs.rs/ptars)
[![License](https://img.shields.io/crates/l/ptars.svg)](https://github.com/0x26res/ptars/blob/main/LICENSE)

Fast conversion between Protocol Buffers and Apache Arrow in Rust —
**without pinning you to an arrow version**.

This crate's public API contains no arrow (or prost) types. Arrow data crosses
the API boundary through the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html),
whose ABI is frozen by the Arrow specification. You can depend on `ptars`
alongside **any** arrow-rs version and exchange record batches zero-copy.

If you prefer a conventional arrow-rs-typed API (and are happy to match ptars'
arrow major version), use the [`ptars-core`](https://crates.io/crates/ptars-core)
crate instead — it contains the actual implementation.

## Usage

```toml
[dependencies]
ptars = "0.1"
arrow = "58"  # any version you like — ptars does not care
```

```rust
use ptars::{Handler, PtarsConfig};

// Descriptors are passed as serialized bytes (protoc --descriptor_set_out),
// so no prost version needs to match either.
let handler = Handler::try_new(
    &descriptor_set_bytes,
    "my.package.MyMessage",
    PtarsConfig::default(),
)?;

// Decode serialized protobuf messages into a record batch. The result comes
// back as an Arrow C Data Interface pair...
let (array, schema) = handler.decode_bytes(&[Some(&message_bytes)])?;

// ...which you import with *your* arrow version, zero-copy. ptars' structs
// and arrow's FFI structs implement the same frozen C ABI:
let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { std::mem::transmute(array) };
let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema)? };
let batch: arrow::record_batch::RecordBatch = arrow::array::StructArray::from(data).into();
```

The reverse direction works the same way: `Handler::encode` takes a record
batch (as a struct-typed C Data Interface pair) and returns a Binary array of
serialized protobuf messages.

## How the version independence is enforced

- The `ptars::ffi::ArrowArray`/`ArrowSchema` structs are hand-written from the
  Arrow specification; compile-time assertions check they stay
  layout-compatible with the arrow version used internally.
- CI runs an integration test that consumes ptars from a crate pinned to a
  *different* arrow major version (`tests/arrow-version-independence`).
- CI diffs the public API (`cargo public-api`) and fails if any `arrow*::` or
  `prost*::` path ever appears in it.

## License

Apache-2.0
