//! # ptars
//!
//! Fast conversion from Protocol Buffers to Apache Arrow and back.
//!
//! This crate provides efficient conversion between Protocol Buffer messages
//! and Apache Arrow record batches, enabling high-performance data processing
//! pipelines that bridge the protobuf and Arrow ecosystems.
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
//! use ptars::{binary_array_to_record_batch_direct, PtarsConfig};
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
pub use config::{ConfluentWirePolicy, EnumRepr, PtarsConfig};
pub use proto_to_arrow::{
    binary_array_to_messages, binary_array_to_record_batch_direct, messages_to_record_batch,
    messages_to_record_batch_with_config,
};
