use std::fmt;

/// Error type for ptars operations.
///
/// Carries only plain strings so that no arrow or prost types appear in this
/// crate's public API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PtarsError {
    /// The protobuf descriptors could not be parsed, or the message was not found.
    Descriptor(String),
    /// Serialized protobuf messages could not be decoded.
    Decode(String),
    /// Arrow data could not be encoded back to protobuf messages.
    Encode(String),
    /// Arrow data could not be imported or exported through the C Data Interface.
    Arrow(String),
}

impl fmt::Display for PtarsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PtarsError::Descriptor(msg) => write!(f, "descriptor error: {}", msg),
            PtarsError::Decode(msg) => write!(f, "decode error: {}", msg),
            PtarsError::Encode(msg) => write!(f, "encode error: {}", msg),
            PtarsError::Arrow(msg) => write!(f, "arrow error: {}", msg),
        }
    }
}

impl std::error::Error for PtarsError {}
