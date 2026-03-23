use arrow::array::ArrayData;
use arrow_array::builder::BinaryBuilder;
use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type};
use arrow_array::{
    Array, BinaryArray, BooleanArray, LargeBinaryArray, LargeStringArray, PrimitiveArray,
    RecordBatch, StringArray, StructArray,
};
use arrow_schema::DataType;
use prost::encoding::{encode_key, encode_varint, WireType};

use prost_reflect::{EnumDescriptor, FieldDescriptor, Kind, MessageDescriptor};

use crate::arrow_to_proto::enum_number_from_name;

/// Reference to a string column, abstracting over Regular and Large variants.
enum StringColumnRef<'a> {
    Regular(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl<'a> StringColumnRef<'a> {
    fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Regular(a) => a.is_null(idx),
            Self::Large(a) => a.is_null(idx),
        }
    }

    fn value(&self, idx: usize) -> &str {
        match self {
            Self::Regular(a) => a.value(idx),
            Self::Large(a) => a.value(idx),
        }
    }
}

/// Reference to a binary column, abstracting over Regular and Large variants.
enum BinaryColumnRef<'a> {
    Regular(&'a BinaryArray),
    Large(&'a LargeBinaryArray),
}

impl<'a> BinaryColumnRef<'a> {
    fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Regular(a) => a.is_null(idx),
            Self::Large(a) => a.is_null(idx),
        }
    }

    fn value(&self, idx: usize) -> &[u8] {
        match self {
            Self::Regular(a) => a.value(idx),
            Self::Large(a) => a.value(idx),
        }
    }
}

/// Encoder for a single protobuf field, holding a downcasted array reference.
enum FieldEncoder<'a> {
    Double {
        tag: u32,
        arr: &'a PrimitiveArray<Float64Type>,
        has_presence: bool,
    },
    Float {
        tag: u32,
        arr: &'a PrimitiveArray<Float32Type>,
        has_presence: bool,
    },
    Int32 {
        tag: u32,
        arr: &'a PrimitiveArray<Int32Type>,
        has_presence: bool,
    },
    Int64 {
        tag: u32,
        arr: &'a PrimitiveArray<Int64Type>,
        has_presence: bool,
    },
    UInt32 {
        tag: u32,
        arr: &'a PrimitiveArray<UInt32Type>,
        has_presence: bool,
    },
    UInt64 {
        tag: u32,
        arr: &'a PrimitiveArray<UInt64Type>,
        has_presence: bool,
    },
    Sint32 {
        tag: u32,
        arr: &'a PrimitiveArray<Int32Type>,
        has_presence: bool,
    },
    Sint64 {
        tag: u32,
        arr: &'a PrimitiveArray<Int64Type>,
        has_presence: bool,
    },
    Sfixed32 {
        tag: u32,
        arr: &'a PrimitiveArray<Int32Type>,
        has_presence: bool,
    },
    Sfixed64 {
        tag: u32,
        arr: &'a PrimitiveArray<Int64Type>,
        has_presence: bool,
    },
    Fixed32 {
        tag: u32,
        arr: &'a PrimitiveArray<UInt32Type>,
        has_presence: bool,
    },
    Fixed64 {
        tag: u32,
        arr: &'a PrimitiveArray<UInt64Type>,
        has_presence: bool,
    },
    Bool {
        tag: u32,
        arr: &'a BooleanArray,
        has_presence: bool,
    },
    String {
        tag: u32,
        col: StringColumnRef<'a>,
        has_presence: bool,
    },
    Bytes {
        tag: u32,
        col: BinaryColumnRef<'a>,
        has_presence: bool,
    },
    EnumInt32 {
        tag: u32,
        arr: &'a PrimitiveArray<Int32Type>,
        has_presence: bool,
    },
    EnumString {
        tag: u32,
        col: StringColumnRef<'a>,
        enum_descriptor: EnumDescriptor,
        has_presence: bool,
    },
    EnumBinary {
        tag: u32,
        col: BinaryColumnRef<'a>,
        enum_descriptor: EnumDescriptor,
        has_presence: bool,
    },
    Message {
        tag: u32,
        struct_arr: &'a StructArray,
        sub_encoder: MessageEncoder<'a>,
    },
}

impl<'a> FieldEncoder<'a> {
    fn encode_at(&self, idx: usize, buf: &mut Vec<u8>) {
        match self {
            Self::Double {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0.0 {
                    return;
                }
                prost::encoding::double::encode(*tag, &val, buf);
            }
            Self::Float {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0.0 {
                    return;
                }
                prost::encoding::float::encode(*tag, &val, buf);
            }
            Self::Int32 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::int32::encode(*tag, &val, buf);
            }
            Self::Int64 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::int64::encode(*tag, &val, buf);
            }
            Self::UInt32 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::uint32::encode(*tag, &val, buf);
            }
            Self::UInt64 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::uint64::encode(*tag, &val, buf);
            }
            Self::Sint32 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::sint32::encode(*tag, &val, buf);
            }
            Self::Sint64 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::sint64::encode(*tag, &val, buf);
            }
            Self::Sfixed32 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::sfixed32::encode(*tag, &val, buf);
            }
            Self::Sfixed64 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::sfixed64::encode(*tag, &val, buf);
            }
            Self::Fixed32 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::fixed32::encode(*tag, &val, buf);
            }
            Self::Fixed64 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::fixed64::encode(*tag, &val, buf);
            }
            Self::Bool {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && !val {
                    return;
                }
                prost::encoding::bool::encode(*tag, &val, buf);
            }
            Self::String {
                tag,
                col,
                has_presence,
            } => {
                if col.is_null(idx) {
                    return;
                }
                let val = col.value(idx);
                if !has_presence && val.is_empty() {
                    return;
                }
                encode_key(*tag, WireType::LengthDelimited, buf);
                encode_varint(val.len() as u64, buf);
                buf.extend_from_slice(val.as_bytes());
            }
            Self::Bytes {
                tag,
                col,
                has_presence,
            } => {
                if col.is_null(idx) {
                    return;
                }
                let val = col.value(idx);
                if !has_presence && val.is_empty() {
                    return;
                }
                encode_key(*tag, WireType::LengthDelimited, buf);
                encode_varint(val.len() as u64, buf);
                buf.extend_from_slice(val);
            }
            Self::EnumInt32 {
                tag,
                arr,
                has_presence,
            } => {
                if arr.is_null(idx) {
                    return;
                }
                let val = arr.value(idx);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::int32::encode(*tag, &val, buf);
            }
            Self::EnumString {
                tag,
                col,
                enum_descriptor,
                has_presence,
            } => {
                if col.is_null(idx) {
                    return;
                }
                let name = col.value(idx);
                let val = enum_number_from_name(name, enum_descriptor);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::int32::encode(*tag, &val, buf);
            }
            Self::EnumBinary {
                tag,
                col,
                enum_descriptor,
                has_presence,
            } => {
                if col.is_null(idx) {
                    return;
                }
                let bytes = col.value(idx);
                let name = std::str::from_utf8(bytes).unwrap();
                let val = enum_number_from_name(name, enum_descriptor);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::int32::encode(*tag, &val, buf);
            }
            Self::Message {
                tag,
                struct_arr,
                sub_encoder,
            } => {
                if !struct_arr.is_valid(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                sub_encoder.encode_row(idx, &mut tmp);
                encode_key(*tag, WireType::LengthDelimited, buf);
                encode_varint(tmp.len() as u64, buf);
                buf.extend_from_slice(&tmp);
            }
        }
    }
}

/// Encodes rows directly from Arrow arrays to protobuf wire format.
pub struct MessageEncoder<'a> {
    encoders: Vec<FieldEncoder<'a>>,
}

impl<'a> MessageEncoder<'a> {
    /// Build a MessageEncoder from a RecordBatch (top-level).
    pub fn from_record_batch(descriptor: &MessageDescriptor, batch: &'a RecordBatch) -> Self {
        let mut encoders = Vec::new();
        for field in descriptor.fields() {
            // Skip map and repeated fields (deferred)
            if field.is_map() || field.is_list() {
                continue;
            }
            if let Some(column) = batch.column_by_name(field.name()) {
                if let Some(enc) = build_field_encoder(&field, column.as_ref()) {
                    encoders.push(enc);
                }
            }
        }
        Self { encoders }
    }

    /// Build a MessageEncoder from a StructArray (nested messages).
    fn from_struct_array(descriptor: &MessageDescriptor, struct_arr: &'a StructArray) -> Self {
        let mut encoders = Vec::new();
        for field in descriptor.fields() {
            if field.is_map() || field.is_list() {
                continue;
            }
            if let Some(column) = struct_arr.column_by_name(field.name()) {
                if let Some(enc) = build_field_encoder(&field, column.as_ref()) {
                    encoders.push(enc);
                }
            }
        }
        Self { encoders }
    }

    /// Encode a single row into the buffer.
    fn encode_row(&self, idx: usize, buf: &mut Vec<u8>) {
        for encoder in &self.encoders {
            encoder.encode_at(idx, buf);
        }
    }
}

/// Build a FieldEncoder for a given field descriptor and array.
fn build_field_encoder<'a>(
    field: &FieldDescriptor,
    array: &'a dyn Array,
) -> Option<FieldEncoder<'a>> {
    let tag: u32 = field.number();
    let has_presence = field.supports_presence();

    match field.kind() {
        Kind::Double => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()?;
            Some(FieldEncoder::Double {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Float => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()?;
            Some(FieldEncoder::Float {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Int32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::Int32 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Int64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()?;
            Some(FieldEncoder::Int64 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Uint32 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            Some(FieldEncoder::UInt32 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Uint64 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            Some(FieldEncoder::UInt64 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Sint32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::Sint32 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Sint64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()?;
            Some(FieldEncoder::Sint64 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Sfixed32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::Sfixed32 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Sfixed64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()?;
            Some(FieldEncoder::Sfixed64 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Fixed32 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            Some(FieldEncoder::Fixed32 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Fixed64 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            Some(FieldEncoder::Fixed64 {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::Bool => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            Some(FieldEncoder::Bool {
                tag,
                arr,
                has_presence,
            })
        }
        Kind::String => {
            let col = if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
                StringColumnRef::Regular(a)
            } else if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
                StringColumnRef::Large(a)
            } else {
                return None;
            };
            Some(FieldEncoder::String {
                tag,
                col,
                has_presence,
            })
        }
        Kind::Bytes => {
            let col = if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
                BinaryColumnRef::Regular(a)
            } else if let Some(a) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                BinaryColumnRef::Large(a)
            } else {
                return None;
            };
            Some(FieldEncoder::Bytes {
                tag,
                col,
                has_presence,
            })
        }
        Kind::Enum(enum_desc) => build_enum_encoder(tag, has_presence, array, &enum_desc),
        Kind::Message(msg_desc) => {
            // Skip well-known types for now (deferred)
            if msg_desc.full_name().starts_with("google.protobuf.") {
                return None;
            }
            let struct_arr = array.as_any().downcast_ref::<StructArray>()?;
            let sub_encoder = MessageEncoder::from_struct_array(&msg_desc, struct_arr);
            Some(FieldEncoder::Message {
                tag,
                struct_arr,
                sub_encoder,
            })
        }
    }
}

/// Build an enum field encoder, auto-detecting the Arrow representation.
fn build_enum_encoder<'a>(
    tag: u32,
    has_presence: bool,
    array: &'a dyn Array,
    enum_descriptor: &EnumDescriptor,
) -> Option<FieldEncoder<'a>> {
    match array.data_type() {
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::EnumInt32 {
                tag,
                arr,
                has_presence,
            })
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>()?;
            Some(FieldEncoder::EnumString {
                tag,
                col: StringColumnRef::Regular(a),
                enum_descriptor: enum_descriptor.clone(),
                has_presence,
            })
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>()?;
            Some(FieldEncoder::EnumString {
                tag,
                col: StringColumnRef::Large(a),
                enum_descriptor: enum_descriptor.clone(),
                has_presence,
            })
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>()?;
            Some(FieldEncoder::EnumBinary {
                tag,
                col: BinaryColumnRef::Regular(a),
                enum_descriptor: enum_descriptor.clone(),
                has_presence,
            })
        }
        DataType::LargeBinary => {
            let a = array.as_any().downcast_ref::<LargeBinaryArray>()?;
            Some(FieldEncoder::EnumBinary {
                tag,
                col: BinaryColumnRef::Large(a),
                enum_descriptor: enum_descriptor.clone(),
                has_presence,
            })
        }
        _ => None,
    }
}

/// Encode a RecordBatch directly to protobuf wire format bytes.
///
/// Returns a BinaryArray (as ArrayData) where each element is the protobuf
/// encoding of the corresponding row. This skips DynamicMessage allocation
/// and writes wire format directly from Arrow arrays.
///
/// Currently supports scalar fields, enums, and nested messages.
/// Repeated fields, maps, and well-known types are not yet supported
/// and will be silently skipped.
pub fn record_batch_to_bytes(batch: &RecordBatch, descriptor: &MessageDescriptor) -> ArrayData {
    let encoder = MessageEncoder::from_record_batch(descriptor, batch);
    let mut results = BinaryBuilder::new();
    let mut row_buf = Vec::new();

    for idx in 0..batch.num_rows() {
        row_buf.clear();
        encoder.encode_row(idx, &mut row_buf);
        results.append_value(&row_buf);
    }

    results.finish().to_data()
}
