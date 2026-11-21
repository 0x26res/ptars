//! # ptars
//!
//! Fast conversion from Protocol Buffers to Apache Arrow and back.
//!
//! This crate is **arrow-version-independent**: its public API contains no
//! arrow-rs (or prost) types. Arrow data crosses the API boundary through the
//! [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! ([`ffi::ArrowArray`]/[`ffi::ArrowSchema`]), whose ABI is frozen by the
//! Arrow specification. You can therefore depend on `ptars` together with any
//! arrow version and exchange data zero-copy.
//!
//! The implementation lives in the `ptars-core` crate, which exposes a
//! conventional arrow-rs-typed API if you prefer to pin the same arrow version
//! as ptars.
//!
//! ## Example
//!
//! ```ignore
//! use ptars::{Handler, PtarsConfig};
//!
//! // Descriptors are passed as serialized bytes (protoc --descriptor_set_out).
//! let handler = Handler::try_new(&descriptor_set_bytes, "my.Message", PtarsConfig::default())?;
//!
//! // Decode serialized messages without arrow on your side at all:
//! let (array, schema) = handler.decode_bytes(&[Some(&message_bytes)])?;
//!
//! // Import the result with *your* arrow version (any major), zero-copy.
//! // Both struct definitions implement the same frozen C ABI, so they can be
//! // transmuted into one another:
//! let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { std::mem::transmute(array) };
//! let ffi_schema: arrow::ffi::FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
//! let data = unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema)? };
//! let batch: arrow::record_batch::RecordBatch =
//!     arrow::array::StructArray::from(data).into();
//! ```

pub mod ffi;

mod error;
mod handler;

pub use error::PtarsError;
pub use handler::Handler;
// Plain configuration types, free of arrow/prost types, shared with ptars-core.
pub use ptars_core::{ConfluentWirePolicy, EnumRepr, PtarsConfig, TimeUnit};
