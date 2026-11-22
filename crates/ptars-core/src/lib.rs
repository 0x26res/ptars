//! # ptars-core
//!
//! Fast conversion from Protocol Buffers to Apache Arrow and back.
//!
//! This crate provides efficient conversion between Protocol Buffer messages
//! and Apache Arrow record batches, enabling high-performance data processing
//! pipelines that bridge the protobuf and Arrow ecosystems.
//!
//! Its public API exposes arrow-rs and prost-reflect types, which means users
//! must depend on the same major version of arrow and prost-reflect as this
//! crate. If you want to exchange Arrow data without matching those versions,
//! use the `ptars` crate instead: it exposes only the
//! [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! for zero-copy data exchange.
//!
//! ## Features
//!
//! - Convert protobuf messages to Arrow RecordBatch
//! - Convert Arrow RecordBatch back to protobuf messages
//! - Support for nested messages, repeated fields, and maps
//! - Special handling for well-known types (Timestamp, Date, TimeOfDay, wrapper types)
//!
//! ## Example
//!
//! ```ignore
//! use ptars_core::{binary_array_to_record_batch_direct, PtarsConfig};
//! use arrow_array::BinaryArray;
//! use prost_reflect::DescriptorPool;
//!
//! // Load your protobuf descriptor
//! let pool = DescriptorPool::decode(descriptor_bytes).unwrap();
//! let message_descriptor = pool.get_message_by_name("my.Message").unwrap();
//!
//! // Decode serialized protobuf messages directly to Arrow
//! let binary_array: BinaryArray = /* your serialized messages */;
//! let config = PtarsConfig::default();
//! let record_batch = binary_array_to_record_batch_direct(&binary_array, &message_descriptor, &config).unwrap();
//! ```

pub mod arrow_to_proto;
pub mod config;
pub mod proto_to_arrow;

#[cfg(test)]
mod converter;

// Re-export commonly used items
pub use arrow_to_proto::record_batch_to_array;
pub use config::{ConfluentWirePolicy, EnumRepr, PtarsConfig, TimeUnit};
pub use proto_to_arrow::{
    binary_array_to_messages, binary_array_to_record_batch_direct, messages_to_record_batch,
    messages_to_record_batch_with_config,
};
