use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow_array::builder::ArrayBuilder;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, LargeBinaryBuilder, LargeStringBuilder, PrimitiveBuilder,
    StringBuilder,
};
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrowPrimitiveType, BinaryArray, MapArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::Datelike;

use prost_reflect::{EnumDescriptor, FieldDescriptor, Kind, MessageDescriptor};

use crate::config::{ConfluentWirePolicy, EnumRepr, PtarsConfig};

// ---------------------------------------------------------------------------
// Wire format decoding primitives
// ---------------------------------------------------------------------------

#[allow(deprecated)]
fn decode_error(msg: &str) -> prost::DecodeError {
    prost::DecodeError::new(msg.to_string())
}

fn decode_varint(buf: &[u8]) -> Result<(u64, usize), prost::DecodeError> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in buf.iter().enumerate() {
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err(decode_error("varint too large"));
        }
    }
    Err(decode_error("unexpected EOF in varint"))
}

fn decode_tag(buf: &[u8]) -> Result<(u32, u8, usize), prost::DecodeError> {
    let (key, n) = decode_varint(buf)?;
    let wire_type = (key & 0x07) as u8;
    let field_number = (key >> 3) as u32;
    if field_number == 0 {
        return Err(decode_error("invalid field number 0"));
    }
    Ok((field_number, wire_type, n))
}

fn skip_field(wire_type: u8, buf: &[u8]) -> Result<usize, prost::DecodeError> {
    match wire_type {
        0 => {
            let (_, n) = decode_varint(buf)?;
            Ok(n)
        }
        1 => {
            if buf.len() < 8 {
                return Err(decode_error("unexpected EOF"));
            }
            Ok(8)
        }
        2 => {
            let (len, n) = decode_varint(buf)?;
            let total = n + len as usize;
            if buf.len() < total {
                return Err(decode_error("unexpected EOF"));
            }
            Ok(total)
        }
        5 => {
            if buf.len() < 4 {
                return Err(decode_error("unexpected EOF"));
            }
            Ok(4)
        }
        _ => Err(decode_error("unsupported wire type")),
    }
}

/// Read a length-delimited field, returning (data_slice, bytes_consumed).
fn read_length_delimited(buf: &[u8]) -> Result<(&[u8], usize), prost::DecodeError> {
    let (len, n) = decode_varint(buf)?;
    let len = len as usize;
    let total = n + len;
    if buf.len() < total {
        return Err(decode_error("unexpected EOF"));
    }
    Ok((&buf[n..total], total))
}

/// Strip the Confluent Schema Registry wire format prefix from a message.
fn strip_confluent_prefix(
    buf: &[u8],
    policy: ConfluentWirePolicy,
) -> Result<&[u8], prost::DecodeError> {
    match policy {
        ConfluentWirePolicy::Raw => Ok(buf),
        ConfluentWirePolicy::Standard => {
            if buf.len() < 5 {
                return Err(decode_error(
                    "message too short for Confluent wire format header",
                ));
            }
            Ok(&buf[5..])
        }
        ConfluentWirePolicy::Protobuf => {
            if buf.len() < 5 {
                return Err(decode_error(
                    "message too short for Confluent wire format header",
                ));
            }
            let remaining = &buf[5..];
            // Read varint-encoded count of message indexes
            let (count, mut offset) = decode_varint(remaining)?;
            // Skip `count` varints (the message indexes themselves)
            for _ in 0..count {
                let (_, n) = decode_varint(&remaining[offset..])?;
                offset += n;
            }
            Ok(&remaining[offset..])
        }
    }
}

#[inline]
fn decode_zigzag32(v: u64) -> i32 {
    let v = v as u32;
    ((v >> 1) as i32) ^ (-((v & 1) as i32))
}

#[inline]
fn decode_zigzag64(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

fn convert_seconds_nanos_to_unit(seconds: i64, nanos: i32, unit: TimeUnit, type_name: &str) -> i64 {
    match unit {
        TimeUnit::Second => seconds,
        TimeUnit::Millisecond => seconds
            .checked_mul(1_000)
            .and_then(|s| s.checked_add(i64::from(nanos) / 1_000_000))
            .unwrap_or_else(|| panic!("{type_name} overflow")),
        TimeUnit::Microsecond => seconds
            .checked_mul(1_000_000)
            .and_then(|s| s.checked_add(i64::from(nanos) / 1_000))
            .unwrap_or_else(|| panic!("{type_name} overflow")),
        TimeUnit::Nanosecond => seconds
            .checked_mul(1_000_000_000)
            .and_then(|s| s.checked_add(i64::from(nanos)))
            .unwrap_or_else(|| panic!("{type_name} overflow")),
    }
}

static CE_OFFSET: i32 = 719163;

fn enum_name(enum_descriptor: &EnumDescriptor, number: i32) -> String {
    match enum_descriptor.get_value(number) {
        Some(v) => v.name().to_string(),
        None => number.to_string(),
    }
}

// ---------------------------------------------------------------------------
// String/Binary builder inner enums
// ---------------------------------------------------------------------------

enum StringBuilderInner {
    Regular(StringBuilder),
    Large(LargeStringBuilder),
}

impl StringBuilderInner {
    fn new(use_large: bool) -> Self {
        if use_large {
            Self::Large(LargeStringBuilder::new())
        } else {
            Self::Regular(StringBuilder::new())
        }
    }
    fn append_value(&mut self, v: &str) {
        match self {
            Self::Regular(b) => b.append_value(v),
            Self::Large(b) => b.append_value(v),
        }
    }
    fn append_null(&mut self) {
        match self {
            Self::Regular(b) => b.append_null(),
            Self::Large(b) => b.append_null(),
        }
    }
    fn append_default(&mut self) {
        self.append_value("");
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            Self::Regular(b) => Arc::new(std::mem::take(b).finish()),
            Self::Large(b) => Arc::new(std::mem::take(b).finish()),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Regular(b) => ArrayBuilder::len(b),
            Self::Large(b) => ArrayBuilder::len(b),
        }
    }
}

enum BinaryBuilderInner {
    Regular(BinaryBuilder),
    Large(LargeBinaryBuilder),
}

impl BinaryBuilderInner {
    fn new(use_large: bool) -> Self {
        if use_large {
            Self::Large(LargeBinaryBuilder::new())
        } else {
            Self::Regular(BinaryBuilder::new())
        }
    }
    fn append_value(&mut self, v: &[u8]) {
        match self {
            Self::Regular(b) => b.append_value(v),
            Self::Large(b) => b.append_value(v),
        }
    }
    fn append_null(&mut self) {
        match self {
            Self::Regular(b) => b.append_null(),
            Self::Large(b) => b.append_null(),
        }
    }
    fn append_default(&mut self) {
        self.append_value(b"");
    }
    fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            Self::Regular(b) => Arc::new(std::mem::take(b).finish()),
            Self::Large(b) => Arc::new(std::mem::take(b).finish()),
        }
    }
    fn len(&self) -> usize {
        match self {
            Self::Regular(b) => ArrayBuilder::len(b),
            Self::Large(b) => ArrayBuilder::len(b),
        }
    }
}

// ---------------------------------------------------------------------------
// ListOffsets
// ---------------------------------------------------------------------------

enum ListOffsets {
    Regular(Vec<i32>),
    Large(Vec<i64>),
}

impl ListOffsets {
    fn new(use_large: bool) -> Self {
        if use_large {
            Self::Large(vec![0])
        } else {
            Self::Regular(vec![0])
        }
    }
    fn push(&mut self, value: usize) {
        match self {
            Self::Regular(v) => v.push(value as i32),
            Self::Large(v) => v.push(value as i64),
        }
    }
    fn finish(self, values: Arc<dyn Array>, name: &str, nullable: bool) -> Arc<dyn Array> {
        let field = Arc::new(Field::new(name, values.data_type().clone(), nullable));
        match self {
            Self::Regular(offsets) => {
                let buf = Buffer::from_vec(offsets);
                let data = ArrayData::builder(DataType::List(field))
                    .len(buf.len() / 4 - 1)
                    .add_buffer(buf)
                    .add_child_data(values.to_data())
                    .build()
                    .unwrap();
                Arc::new(arrow_array::ListArray::from(data))
            }
            Self::Large(offsets) => {
                let buf = Buffer::from_vec(offsets);
                let data = ArrayData::builder(DataType::LargeList(field))
                    .len(buf.len() / 8 - 1)
                    .add_buffer(buf)
                    .add_child_data(values.to_data())
                    .build()
                    .unwrap();
                Arc::new(arrow_array::LargeListArray::from(data))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// RepeatedInner enum — repeated field value storage
// ---------------------------------------------------------------------------

enum RepeatedInner {
    Int32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    Int64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    UInt32 {
        values_builder: PrimitiveBuilder<UInt32Type>,
    },
    UInt64 {
        values_builder: PrimitiveBuilder<UInt64Type>,
    },
    Float {
        values_builder: PrimitiveBuilder<Float32Type>,
    },
    Double {
        values_builder: PrimitiveBuilder<Float64Type>,
    },
    Bool {
        values_builder: BooleanBuilder,
    },
    String {
        values_builder: StringBuilderInner,
    },
    Bytes {
        values_builder: BinaryBuilderInner,
    },
    Sint32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    Sint64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    Sfixed32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    Sfixed64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    Fixed32 {
        values_builder: PrimitiveBuilder<UInt32Type>,
    },
    Fixed64 {
        values_builder: PrimitiveBuilder<UInt64Type>,
    },
    EnumInt32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    EnumString {
        values_builder: StringBuilderInner,
        enum_descriptor: EnumDescriptor,
    },
    EnumBinary {
        values_builder: BinaryBuilderInner,
        enum_descriptor: EnumDescriptor,
    },
    Message {
        sub_decoder: MessageDecoder,
    },
    Timestamp {
        values_builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
        tz: Option<Arc<str>>,
    },
    Duration {
        values_builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },
    Date {
        values_builder: PrimitiveBuilder<Date32Type>,
    },
    TimeOfDay {
        values_builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },
    WrapperDouble {
        values_builder: PrimitiveBuilder<Float64Type>,
    },
    WrapperFloat {
        values_builder: PrimitiveBuilder<Float32Type>,
    },
    WrapperInt64 {
        values_builder: PrimitiveBuilder<Int64Type>,
    },
    WrapperUInt64 {
        values_builder: PrimitiveBuilder<UInt64Type>,
    },
    WrapperInt32 {
        values_builder: PrimitiveBuilder<Int32Type>,
    },
    WrapperUInt32 {
        values_builder: PrimitiveBuilder<UInt32Type>,
    },
    WrapperBool {
        values_builder: BooleanBuilder,
    },
    WrapperString {
        values_builder: StringBuilderInner,
    },
    WrapperBytes {
        values_builder: BinaryBuilderInner,
    },
}

impl RepeatedInner {
    fn decode(&mut self, wire_type: u8, buf: &[u8]) -> Result<usize, prost::DecodeError> {
        match self {
            Self::Int32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as i32)
            }
            Self::Int64 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as i64)
            }
            Self::UInt32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as u32)
            }
            Self::UInt64 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v)
            }
            Self::EnumInt32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, |v| v as i32)
            }
            Self::Sint32 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, decode_zigzag32)
            }
            Self::Sint64 { values_builder, .. } => {
                decode_repeated_varint(wire_type, buf, values_builder, decode_zigzag64)
            }
            Self::Sfixed32 { values_builder, .. } => decode_repeated_fixed::<Int32Type, 4>(
                wire_type,
                5,
                buf,
                values_builder,
                i32::from_le_bytes,
            ),
            Self::Sfixed64 { values_builder, .. } => decode_repeated_fixed::<Int64Type, 8>(
                wire_type,
                1,
                buf,
                values_builder,
                i64::from_le_bytes,
            ),
            Self::Fixed32 { values_builder, .. } => decode_repeated_fixed::<UInt32Type, 4>(
                wire_type,
                5,
                buf,
                values_builder,
                u32::from_le_bytes,
            ),
            Self::Fixed64 { values_builder, .. } => decode_repeated_fixed::<UInt64Type, 8>(
                wire_type,
                1,
                buf,
                values_builder,
                u64::from_le_bytes,
            ),
            Self::Float { values_builder, .. } => decode_repeated_fixed::<Float32Type, 4>(
                wire_type,
                5,
                buf,
                values_builder,
                f32::from_le_bytes,
            ),
            Self::Double { values_builder, .. } => decode_repeated_fixed::<Float64Type, 8>(
                wire_type,
                1,
                buf,
                values_builder,
                f64::from_le_bytes,
            ),
            Self::Bool { values_builder, .. } => {
                if wire_type == 2 {
                    let (data, total) = read_length_delimited(buf)?;
                    let mut p = 0;
                    while p < data.len() {
                        let (v, n) = decode_varint(&data[p..])?;
                        values_builder.append_value(v != 0);
                        p += n;
                    }
                    Ok(total)
                } else if wire_type == 0 {
                    let (v, n) = decode_varint(buf)?;
                    values_builder.append_value(v != 0);
                    Ok(n)
                } else {
                    skip_field(wire_type, buf)
                }
            }
            Self::String { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let s = std::str::from_utf8(data).map_err(|_| decode_error("invalid UTF-8"))?;
                values_builder.append_value(s);
                Ok(total)
            }
            Self::Bytes { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                values_builder.append_value(data);
                Ok(total)
            }
            Self::EnumString {
                values_builder,
                enum_descriptor,
                ..
            } => {
                if wire_type == 2 {
                    let (data, total) = read_length_delimited(buf)?;
                    let mut p = 0;
                    while p < data.len() {
                        let (v, n) = decode_varint(&data[p..])?;
                        values_builder.append_value(&enum_name(enum_descriptor, v as i32));
                        p += n;
                    }
                    Ok(total)
                } else if wire_type == 0 {
                    let (v, n) = decode_varint(buf)?;
                    values_builder.append_value(&enum_name(enum_descriptor, v as i32));
                    Ok(n)
                } else {
                    skip_field(wire_type, buf)
                }
            }
            Self::EnumBinary {
                values_builder,
                enum_descriptor,
                ..
            } => {
                if wire_type == 2 {
                    let (data, total) = read_length_delimited(buf)?;
                    let mut p = 0;
                    while p < data.len() {
                        let (v, n) = decode_varint(&data[p..])?;
                        values_builder
                            .append_value(enum_name(enum_descriptor, v as i32).as_bytes());
                        p += n;
                    }
                    Ok(total)
                } else if wire_type == 0 {
                    let (v, n) = decode_varint(buf)?;
                    values_builder.append_value(enum_name(enum_descriptor, v as i32).as_bytes());
                    Ok(n)
                } else {
                    skip_field(wire_type, buf)
                }
            }
            Self::Message { sub_decoder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                sub_decoder.decode_row(data)?;
                Ok(total)
            }
            Self::Timestamp {
                values_builder,
                unit,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                values_builder.append_value(convert_seconds_nanos_to_unit(
                    vals[0],
                    vals[1] as i32,
                    *unit,
                    "Timestamp",
                ));
                Ok(total)
            }
            Self::Duration {
                values_builder,
                unit,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                values_builder.append_value(convert_seconds_nanos_to_unit(
                    vals[0],
                    vals[1] as i32,
                    *unit,
                    "Duration",
                ));
                Ok(total)
            }
            Self::Date { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 3)?;
                let (y, m, d) = (vals[0] as i32, vals[1] as i32, vals[2] as i32);
                if y == 0 && m == 0 && d == 0 {
                    values_builder.append_value(0);
                } else {
                    values_builder.append_value(
                        chrono::NaiveDate::from_ymd_opt(y, m as u32, d as u32)
                            .unwrap()
                            .num_days_from_ce()
                            - CE_OFFSET,
                    );
                }
                Ok(total)
            }
            Self::TimeOfDay {
                values_builder,
                unit,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 4)?;
                let total_seconds = vals[0] * 3600 + vals[1] * 60 + vals[2];
                values_builder.append_value(convert_seconds_nanos_to_unit(
                    total_seconds,
                    vals[3] as i32,
                    *unit,
                    "TimeOfDay",
                ));
                Ok(total)
            }
            Self::WrapperDouble { values_builder, .. } => {
                decode_repeated_wrapper_fixed64(wire_type, buf, values_builder, f64::from_le_bytes)
            }
            Self::WrapperFloat { values_builder, .. } => {
                decode_repeated_wrapper_fixed32(wire_type, buf, values_builder, f32::from_le_bytes)
            }
            Self::WrapperInt64 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v as i64)
            }
            Self::WrapperUInt64 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v)
            }
            Self::WrapperInt32 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v as i32)
            }
            Self::WrapperUInt32 { values_builder, .. } => {
                decode_repeated_wrapper_varint(wire_type, buf, values_builder, |v| v as u32)
            }
            Self::WrapperBool { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                values_builder.append_value(v != 0);
                Ok(total)
            }
            Self::WrapperString { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, found) = decode_wrapper_string(data)?;
                if found {
                    values_builder.append_value(unsafe { std::str::from_utf8_unchecked(&v) });
                } else {
                    values_builder.append_value("");
                }
                Ok(total)
            }
            Self::WrapperBytes { values_builder, .. } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, found) = decode_wrapper_bytes(data)?;
                if found {
                    values_builder.append_value(&v);
                } else {
                    values_builder.append_value(b"");
                }
                Ok(total)
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Int32 { values_builder, .. }
            | Self::Sint32 { values_builder, .. }
            | Self::Sfixed32 { values_builder, .. }
            | Self::EnumInt32 { values_builder, .. }
            | Self::WrapperInt32 { values_builder, .. } => values_builder.len(),
            Self::Int64 { values_builder, .. }
            | Self::Sint64 { values_builder, .. }
            | Self::Sfixed64 { values_builder, .. }
            | Self::Timestamp { values_builder, .. }
            | Self::Duration { values_builder, .. }
            | Self::WrapperInt64 { values_builder, .. }
            | Self::TimeOfDay { values_builder, .. } => values_builder.len(),
            Self::UInt32 { values_builder, .. }
            | Self::Fixed32 { values_builder, .. }
            | Self::WrapperUInt32 { values_builder, .. } => values_builder.len(),
            Self::UInt64 { values_builder, .. }
            | Self::Fixed64 { values_builder, .. }
            | Self::WrapperUInt64 { values_builder, .. } => values_builder.len(),
            Self::Float { values_builder, .. } | Self::WrapperFloat { values_builder, .. } => {
                values_builder.len()
            }
            Self::Double { values_builder, .. } | Self::WrapperDouble { values_builder, .. } => {
                values_builder.len()
            }
            Self::Bool { values_builder, .. } | Self::WrapperBool { values_builder, .. } => {
                values_builder.len()
            }
            Self::String { values_builder, .. }
            | Self::EnumString { values_builder, .. }
            | Self::WrapperString { values_builder, .. } => values_builder.len(),
            Self::Bytes { values_builder, .. }
            | Self::EnumBinary { values_builder, .. }
            | Self::WrapperBytes { values_builder, .. } => values_builder.len(),
            Self::Message { sub_decoder, .. } => sub_decoder.row_count(),
            Self::Date { values_builder, .. } => values_builder.len(),
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match self {
            Self::Int32 { values_builder, .. }
            | Self::Sint32 { values_builder, .. }
            | Self::Sfixed32 { values_builder, .. }
            | Self::EnumInt32 { values_builder, .. }
            | Self::WrapperInt32 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Int64 { values_builder, .. }
            | Self::Sint64 { values_builder, .. }
            | Self::Sfixed64 { values_builder, .. }
            | Self::WrapperInt64 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::UInt32 { values_builder, .. }
            | Self::Fixed32 { values_builder, .. }
            | Self::WrapperUInt32 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::UInt64 { values_builder, .. }
            | Self::Fixed64 { values_builder, .. }
            | Self::WrapperUInt64 { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Float { values_builder, .. } | Self::WrapperFloat { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Double { values_builder, .. } | Self::WrapperDouble { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::Bool { values_builder, .. } | Self::WrapperBool { values_builder, .. } => {
                Arc::new(std::mem::take(values_builder).finish())
            }
            Self::String { values_builder, .. }
            | Self::EnumString { values_builder, .. }
            | Self::WrapperString { values_builder, .. } => values_builder.finish(),
            Self::Bytes { values_builder, .. }
            | Self::EnumBinary { values_builder, .. }
            | Self::WrapperBytes { values_builder, .. } => values_builder.finish(),
            Self::Message { sub_decoder, .. } => Arc::new(sub_decoder.build_struct_array(None)),
            Self::Timestamp {
                values_builder,
                unit,
                tz,
            } => finish_timestamp(values_builder, *unit, tz),
            Self::Duration {
                values_builder,
                unit,
            } => finish_duration(values_builder, *unit),
            Self::Date { values_builder, .. } => Arc::new(std::mem::take(values_builder).finish()),
            Self::TimeOfDay {
                values_builder,
                unit,
                ..
            } => finish_time_of_day(values_builder, *unit),
        }
    }
}

// ---------------------------------------------------------------------------
// FieldDecoder enum — all types
// ---------------------------------------------------------------------------

enum FieldDecoder {
    // --- Singular scalars (buffered) ---
    Int32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    Int64 {
        value: i64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    UInt32 {
        value: u32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt32Type>,
    },
    UInt64 {
        value: u64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt64Type>,
    },
    Sint32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    Sint64 {
        value: i64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    Sfixed32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    Sfixed64 {
        value: i64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    Fixed32 {
        value: u32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt32Type>,
    },
    Fixed64 {
        value: u64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<UInt64Type>,
    },
    Float {
        value: f32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Float32Type>,
    },
    Double {
        value: f64,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Float64Type>,
    },
    Bool {
        value: bool,
        has_value: bool,
        has_presence: bool,
        builder: BooleanBuilder,
    },
    String {
        value: Vec<u8>,
        has_value: bool,
        has_presence: bool,
        builder: StringBuilderInner,
    },
    Bytes {
        value: Vec<u8>,
        has_value: bool,
        has_presence: bool,
        builder: BinaryBuilderInner,
    },
    EnumInt32 {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    EnumString {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: StringBuilderInner,
        enum_descriptor: EnumDescriptor,
    },
    EnumBinary {
        value: i32,
        has_value: bool,
        has_presence: bool,
        builder: BinaryBuilderInner,
        enum_descriptor: EnumDescriptor,
    },

    // --- Well-known types (singular, buffered) ---
    Timestamp {
        seconds: i64,
        nanos: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
        tz: Option<Arc<str>>,
    },
    Duration {
        seconds: i64,
        nanos: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },
    Date {
        year: i32,
        month: i32,
        day: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Date32Type>,
    },
    TimeOfDay {
        hours: i32,
        minutes: i32,
        seconds_val: i32,
        nanos: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
        unit: TimeUnit,
    },

    // --- Wrapper types (singular) ---
    WrapperDouble {
        value: f64,
        has_value: bool,
        builder: PrimitiveBuilder<Float64Type>,
    },
    WrapperFloat {
        value: f32,
        has_value: bool,
        builder: PrimitiveBuilder<Float32Type>,
    },
    WrapperInt64 {
        value: i64,
        has_value: bool,
        builder: PrimitiveBuilder<Int64Type>,
    },
    WrapperUInt64 {
        value: u64,
        has_value: bool,
        builder: PrimitiveBuilder<UInt64Type>,
    },
    WrapperInt32 {
        value: i32,
        has_value: bool,
        builder: PrimitiveBuilder<Int32Type>,
    },
    WrapperUInt32 {
        value: u32,
        has_value: bool,
        builder: PrimitiveBuilder<UInt32Type>,
    },
    WrapperBool {
        value: bool,
        has_value: bool,
        builder: BooleanBuilder,
    },
    WrapperString {
        value: Vec<u8>,
        has_value: bool,
        builder: StringBuilderInner,
    },
    WrapperBytes {
        value: Vec<u8>,
        has_value: bool,
        builder: BinaryBuilderInner,
    },

    // --- Nested message ---
    Message {
        sub_decoder: MessageDecoder,
        has_value: bool,
        is_valid: BooleanBuilder,
    },

    // --- Repeated fields (all collapsed into one variant) ---
    Repeated {
        inner: RepeatedInner,
        offsets: ListOffsets,
        list_name: Arc<str>,
        list_nullable: bool,
    },

    // --- Map fields ---
    Map {
        key_decoder: Box<FieldDecoder>,
        value_decoder: Box<FieldDecoder>,
        offsets: Vec<i32>,
        map_value_name: Arc<str>,
        map_value_nullable: bool,
    },
}

// ---------------------------------------------------------------------------
// Helpers for decoding wire values inline
// ---------------------------------------------------------------------------

/// Decode fields of a well-known submessage with up to 4 int fields.
/// Returns (field1, field2, field3, field4) initialized to 0, scanning for field numbers 1..=max_field.
fn decode_wkt_submessage(buf: &[u8], max_field: u32) -> Result<[i64; 4], prost::DecodeError> {
    let mut vals = [0i64; 4];
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum >= 1 && fnum <= max_field && wt == 0 {
            let (v, n) = decode_varint(&buf[pos..])?;
            vals[(fnum - 1) as usize] = v as i64;
            pos += n;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok(vals)
}

/// Decode a wrapper submessage: field 1 with the given wire type.
/// For varint wrapper types.
fn decode_wrapper_varint(buf: &[u8]) -> Result<(u64, bool), prost::DecodeError> {
    let mut val = 0u64;
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 0 {
            let (v, n) = decode_varint(&buf[pos..])?;
            val = v;
            found = true;
            pos += n;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_fixed32(buf: &[u8]) -> Result<([u8; 4], bool), prost::DecodeError> {
    let mut val = [0u8; 4];
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 5 {
            if buf.len() < pos + 4 {
                return Err(decode_error("unexpected EOF"));
            }
            val.copy_from_slice(&buf[pos..pos + 4]);
            found = true;
            pos += 4;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_fixed64(buf: &[u8]) -> Result<([u8; 8], bool), prost::DecodeError> {
    let mut val = [0u8; 8];
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 1 {
            if buf.len() < pos + 8 {
                return Err(decode_error("unexpected EOF"));
            }
            val.copy_from_slice(&buf[pos..pos + 8]);
            found = true;
            pos += 8;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_string(buf: &[u8]) -> Result<(Vec<u8>, bool), prost::DecodeError> {
    let mut val = Vec::new();
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 2 {
            let (data, consumed) = read_length_delimited(&buf[pos..])?;
            std::str::from_utf8(data).map_err(|_| decode_error("invalid UTF-8"))?;
            val.clear();
            val.extend_from_slice(data);
            found = true;
            pos += consumed;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

fn decode_wrapper_bytes(buf: &[u8]) -> Result<(Vec<u8>, bool), prost::DecodeError> {
    let mut val = Vec::new();
    let mut found = false;
    let mut pos = 0;
    while pos < buf.len() {
        let (fnum, wt, n) = decode_tag(&buf[pos..])?;
        pos += n;
        if fnum == 1 && wt == 2 {
            let (data, consumed) = read_length_delimited(&buf[pos..])?;
            val.clear();
            val.extend_from_slice(data);
            found = true;
            pos += consumed;
        } else {
            pos += skip_field(wt, &buf[pos..])?;
        }
    }
    Ok((val, found))
}

// ---------------------------------------------------------------------------
// Macro for flush/finish boilerplate
// ---------------------------------------------------------------------------

macro_rules! flush_primitive {
    ($value:expr, $has_value:expr, $has_presence:expr, $builder:expr, $default:expr) => {
        if *$has_value {
            $builder.append_value(*$value);
        } else if *$has_presence {
            $builder.append_null();
        } else {
            $builder.append_value($default);
        }
        *$has_value = false;
        *$value = $default;
    };
}

fn finish_primitive<T: ArrowPrimitiveType>(builder: &mut PrimitiveBuilder<T>) -> Arc<dyn Array> {
    Arc::new(std::mem::take(builder).finish())
}

fn finish_timestamp(
    builder: &mut PrimitiveBuilder<Int64Type>,
    unit: TimeUnit,
    tz: &Option<Arc<str>>,
) -> Arc<dyn Array> {
    let values = std::mem::take(builder).finish();
    let dt = DataType::Timestamp(unit, tz.clone());
    let data = ArrayData::builder(dt)
        .len(values.len())
        .add_buffer(values.values().inner().clone())
        .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
        .build()
        .unwrap();
    arrow_array::make_array(data)
}

fn finish_duration(builder: &mut PrimitiveBuilder<Int64Type>, unit: TimeUnit) -> Arc<dyn Array> {
    let values = std::mem::take(builder).finish();
    let dt = DataType::Duration(unit);
    let data = ArrayData::builder(dt)
        .len(values.len())
        .add_buffer(values.values().inner().clone())
        .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
        .build()
        .unwrap();
    arrow_array::make_array(data)
}

fn finish_time_of_day(builder: &mut PrimitiveBuilder<Int64Type>, unit: TimeUnit) -> Arc<dyn Array> {
    let values = std::mem::take(builder).finish();
    let dt = match unit {
        TimeUnit::Second => DataType::Time32(TimeUnit::Second),
        TimeUnit::Millisecond => DataType::Time32(TimeUnit::Millisecond),
        TimeUnit::Microsecond => DataType::Time64(TimeUnit::Microsecond),
        TimeUnit::Nanosecond => DataType::Time64(TimeUnit::Nanosecond),
    };
    if matches!(unit, TimeUnit::Second | TimeUnit::Millisecond) {
        let i32_values: Vec<Option<i32>> = (0..values.len())
            .map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    let v = values.value(i);
                    Some(i32::try_from(v).unwrap_or(if v > 0 { i32::MAX } else { i32::MIN }))
                }
            })
            .collect();
        let i32_array = arrow_array::Int32Array::from(i32_values);
        let data = ArrayData::builder(dt)
            .len(i32_array.len())
            .add_buffer(i32_array.values().inner().clone())
            .null_bit_buffer(i32_array.nulls().map(|n| n.buffer().clone()))
            .build()
            .unwrap();
        arrow_array::make_array(data)
    } else {
        let data = ArrayData::builder(dt)
            .len(values.len())
            .add_buffer(values.values().inner().clone())
            .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
            .build()
            .unwrap();
        arrow_array::make_array(data)
    }
}

// ---------------------------------------------------------------------------
// FieldDecoder: decode + flush + finish
// ---------------------------------------------------------------------------

impl FieldDecoder {
    fn decode(&mut self, wire_type: u8, buf: &[u8]) -> Result<usize, prost::DecodeError> {
        match self {
            Self::Int32 {
                value, has_value, ..
            }
            | Self::EnumInt32 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as i32;
                *has_value = true;
                Ok(n)
            }
            Self::EnumString {
                value, has_value, ..
            }
            | Self::EnumBinary {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as i32;
                *has_value = true;
                Ok(n)
            }
            Self::Int64 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as i64;
                *has_value = true;
                Ok(n)
            }
            Self::UInt32 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v as u32;
                *has_value = true;
                Ok(n)
            }
            Self::UInt64 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v;
                *has_value = true;
                Ok(n)
            }
            Self::Sint32 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = decode_zigzag32(v);
                *has_value = true;
                Ok(n)
            }
            Self::Sint64 {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = decode_zigzag64(v);
                *has_value = true;
                Ok(n)
            }
            Self::Sfixed32 {
                value, has_value, ..
            } => {
                if wire_type != 5 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 4 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = i32::from_le_bytes(buf[..4].try_into().unwrap());
                *has_value = true;
                Ok(4)
            }
            Self::Sfixed64 {
                value, has_value, ..
            } => {
                if wire_type != 1 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 8 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = i64::from_le_bytes(buf[..8].try_into().unwrap());
                *has_value = true;
                Ok(8)
            }
            Self::Fixed32 {
                value, has_value, ..
            } => {
                if wire_type != 5 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 4 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = u32::from_le_bytes(buf[..4].try_into().unwrap());
                *has_value = true;
                Ok(4)
            }
            Self::Fixed64 {
                value, has_value, ..
            } => {
                if wire_type != 1 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 8 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = u64::from_le_bytes(buf[..8].try_into().unwrap());
                *has_value = true;
                Ok(8)
            }
            Self::Float {
                value, has_value, ..
            } => {
                if wire_type != 5 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 4 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = f32::from_le_bytes(buf[..4].try_into().unwrap());
                *has_value = true;
                Ok(4)
            }
            Self::Double {
                value, has_value, ..
            } => {
                if wire_type != 1 {
                    return skip_field(wire_type, buf);
                }
                if buf.len() < 8 {
                    return Err(decode_error("unexpected EOF"));
                }
                *value = f64::from_le_bytes(buf[..8].try_into().unwrap());
                *has_value = true;
                Ok(8)
            }
            Self::Bool {
                value, has_value, ..
            } => {
                if wire_type != 0 {
                    return skip_field(wire_type, buf);
                }
                let (v, n) = decode_varint(buf)?;
                *value = v != 0;
                *has_value = true;
                Ok(n)
            }
            Self::String {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                std::str::from_utf8(data).map_err(|_| decode_error("invalid UTF-8"))?;
                value.clear();
                value.extend_from_slice(data);
                *has_value = true;
                Ok(total)
            }
            Self::Bytes {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                value.clear();
                value.extend_from_slice(data);
                *has_value = true;
                Ok(total)
            }
            // Well-known types: decode submessage
            Self::Timestamp {
                seconds,
                nanos,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                *seconds = vals[0];
                *nanos = vals[1] as i32;
                *has_value = true;
                Ok(total)
            }
            Self::Duration {
                seconds,
                nanos,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 2)?;
                *seconds = vals[0];
                *nanos = vals[1] as i32;
                *has_value = true;
                Ok(total)
            }
            Self::Date {
                year,
                month,
                day,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 3)?;
                *year = vals[0] as i32;
                *month = vals[1] as i32;
                *day = vals[2] as i32;
                *has_value = true;
                Ok(total)
            }
            Self::TimeOfDay {
                hours,
                minutes,
                seconds_val,
                nanos,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let vals = decode_wkt_submessage(data, 4)?;
                *hours = vals[0] as i32;
                *minutes = vals[1] as i32;
                *seconds_val = vals[2] as i32;
                *nanos = vals[3] as i32;
                *has_value = true;
                Ok(total)
            }
            // Wrapper types
            Self::WrapperDouble {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (bytes, found) = decode_wrapper_fixed64(data)?;
                if found {
                    *value = f64::from_le_bytes(bytes);
                }
                *has_value = true;
                Ok(total)
            }
            Self::WrapperFloat {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (bytes, found) = decode_wrapper_fixed32(data)?;
                if found {
                    *value = f32::from_le_bytes(bytes);
                }
                *has_value = true;
                Ok(total)
            }
            Self::WrapperInt64 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v as i64;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperUInt64 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperInt32 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v as i32;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperUInt32 {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v as u32;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperBool {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_varint(data)?;
                *value = v != 0;
                *has_value = true;
                Ok(total)
            }
            Self::WrapperString {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_string(data)?;
                value.clear();
                value.extend_from_slice(&v);
                *has_value = true;
                Ok(total)
            }
            Self::WrapperBytes {
                value, has_value, ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                let (v, _) = decode_wrapper_bytes(data)?;
                value.clear();
                value.extend_from_slice(&v);
                *has_value = true;
                Ok(total)
            }
            // Nested message
            Self::Message {
                sub_decoder,
                has_value,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                // For singular messages, if seen multiple times, the spec says merge.
                // For simplicity (and matching DynamicMessage behavior), we decode fresh each time.
                // Reset sub_decoder if already decoded this row.
                if !*has_value {
                    *has_value = true;
                }
                sub_decoder.decode_message_bytes(data)?;
                Ok(total)
            }
            // Repeated fields — delegate to inner
            Self::Repeated { inner, .. } => inner.decode(wire_type, buf),
            // Map: each occurrence is a length-delimited entry submessage
            Self::Map {
                key_decoder,
                value_decoder,
                ..
            } => {
                if wire_type != 2 {
                    return skip_field(wire_type, buf);
                }
                let (data, total) = read_length_delimited(buf)?;
                // Parse entry submessage: field 1 = key, field 2 = value
                let mut pos = 0;
                while pos < data.len() {
                    let (fnum, wt, n) = decode_tag(&data[pos..])?;
                    pos += n;
                    if fnum == 1 {
                        pos += key_decoder.decode(wt, &data[pos..])?;
                    } else if fnum == 2 {
                        pos += value_decoder.decode(wt, &data[pos..])?;
                    } else {
                        pos += skip_field(wt, &data[pos..])?;
                    }
                }
                // Flush key and value (they're buffered singular decoders)
                key_decoder.flush();
                value_decoder.flush();
                Ok(total)
            }
        }
    }

    fn flush(&mut self) {
        match self {
            Self::Int32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sint32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sfixed32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::EnumInt32 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0i32);
            }
            Self::Int64 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sint64 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Sfixed64 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0i64);
            }
            Self::UInt32 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Fixed32 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0u32);
            }
            Self::UInt64 {
                value,
                has_value,
                has_presence,
                builder,
            }
            | Self::Fixed64 {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0u64);
            }
            Self::Float {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0.0f32);
            }
            Self::Double {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                flush_primitive!(value, has_value, has_presence, builder, 0.0f64);
            }
            Self::EnumString {
                value,
                has_value,
                has_presence,
                builder,
                enum_descriptor,
            } => {
                if *has_value {
                    builder.append_value(&enum_name(enum_descriptor, *value));
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_value(&enum_name(enum_descriptor, 0));
                }
                *has_value = false;
                *value = 0;
            }
            Self::EnumBinary {
                value,
                has_value,
                has_presence,
                builder,
                enum_descriptor,
            } => {
                if *has_value {
                    builder.append_value(enum_name(enum_descriptor, *value).as_bytes());
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_value(enum_name(enum_descriptor, 0).as_bytes());
                }
                *has_value = false;
                *value = 0;
            }
            Self::Bool {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_value(false);
                }
                *has_value = false;
                *value = false;
            }
            Self::String {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                if *has_value {
                    builder.append_value(unsafe { std::str::from_utf8_unchecked(value) });
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_default();
                }
                *has_value = false;
                value.clear();
            }
            Self::Bytes {
                value,
                has_value,
                has_presence,
                builder,
            } => {
                if *has_value {
                    builder.append_value(value.as_slice());
                } else if *has_presence {
                    builder.append_null();
                } else {
                    builder.append_default();
                }
                *has_value = false;
                value.clear();
            }
            // Well-known types
            Self::Timestamp {
                seconds,
                nanos,
                has_value,
                builder,
                unit,
                ..
            } => {
                if *has_value {
                    builder.append_value(convert_seconds_nanos_to_unit(
                        *seconds,
                        *nanos,
                        *unit,
                        "Timestamp",
                    ));
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *seconds = 0;
                *nanos = 0;
            }
            Self::Duration {
                seconds,
                nanos,
                has_value,
                builder,
                unit,
                ..
            } => {
                if *has_value {
                    builder.append_value(convert_seconds_nanos_to_unit(
                        *seconds, *nanos, *unit, "Duration",
                    ));
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *seconds = 0;
                *nanos = 0;
            }
            Self::Date {
                year,
                month,
                day,
                has_value,
                builder,
            } => {
                if *has_value {
                    if *year == 0 && *month == 0 && *day == 0 {
                        builder.append_value(0);
                    } else {
                        builder.append_value(
                            chrono::NaiveDate::from_ymd_opt(*year, *month as u32, *day as u32)
                                .unwrap()
                                .num_days_from_ce()
                                - CE_OFFSET,
                        );
                    }
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *year = 0;
                *month = 0;
                *day = 0;
            }
            Self::TimeOfDay {
                hours,
                minutes,
                seconds_val,
                nanos,
                has_value,
                builder,
                unit,
            } => {
                if *has_value {
                    let total_seconds = i64::from(*hours) * 3600
                        + i64::from(*minutes) * 60
                        + i64::from(*seconds_val);
                    builder.append_value(convert_seconds_nanos_to_unit(
                        total_seconds,
                        *nanos,
                        *unit,
                        "TimeOfDay",
                    ));
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *hours = 0;
                *minutes = 0;
                *seconds_val = 0;
                *nanos = 0;
            }
            // Wrapper types: present → value, absent → null
            Self::WrapperDouble {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0.0;
            }
            Self::WrapperFloat {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0.0;
            }
            Self::WrapperInt64 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperUInt64 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperInt32 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperUInt32 {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = 0;
            }
            Self::WrapperBool {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(*value);
                } else {
                    builder.append_null();
                }
                *has_value = false;
                *value = false;
            }
            Self::WrapperString {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(unsafe { std::str::from_utf8_unchecked(value) });
                } else {
                    builder.append_null();
                }
                *has_value = false;
                value.clear();
            }
            Self::WrapperBytes {
                value,
                has_value,
                builder,
            } => {
                if *has_value {
                    builder.append_value(value.as_slice());
                } else {
                    builder.append_null();
                }
                *has_value = false;
                value.clear();
            }
            // Nested message
            Self::Message {
                sub_decoder,
                has_value,
                is_valid,
            } => {
                if *has_value {
                    is_valid.append_value(true);
                    sub_decoder.flush_row();
                } else {
                    is_valid.append_value(false);
                    sub_decoder.flush_defaults();
                }
                *has_value = false;
            }
            // Repeated fields: push offset
            Self::Repeated { inner, offsets, .. } => {
                offsets.push(inner.len());
            }
            // Map: push offset based on key builder length
            Self::Map {
                key_decoder,
                offsets,
                ..
            } => {
                let count = match key_decoder.as_ref() {
                    FieldDecoder::Int32 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Int64 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::UInt32 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::UInt64 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Sint32 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Sint64 { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::Bool { builder, .. } => ArrayBuilder::len(builder),
                    FieldDecoder::String { builder, .. } => builder.len(),
                    _ => *offsets.last().unwrap() as usize,
                };
                offsets.push(count as i32);
            }
        }
    }

    fn finish(&mut self, nullable: bool) -> (Field, Arc<dyn Array>) {
        // This is called by MessageDecoder::finish, which provides the field name separately
        // We return a dummy field name here; the caller replaces it.
        let array: Arc<dyn Array> = match self {
            Self::Int32 { builder, .. } | Self::EnumInt32 { builder, .. } => {
                finish_primitive(builder)
            }
            Self::Int64 { builder, .. } => finish_primitive(builder),
            Self::UInt32 { builder, .. } => finish_primitive(builder),
            Self::UInt64 { builder, .. } => finish_primitive(builder),
            Self::Sint32 { builder, .. } | Self::Sfixed32 { builder, .. } => {
                finish_primitive(builder)
            }
            Self::Sint64 { builder, .. } | Self::Sfixed64 { builder, .. } => {
                finish_primitive(builder)
            }
            Self::Fixed32 { builder, .. } => finish_primitive(builder),
            Self::Fixed64 { builder, .. } => finish_primitive(builder),
            Self::Float { builder, .. } => finish_primitive(builder),
            Self::Double { builder, .. } => finish_primitive(builder),
            Self::Bool { builder, .. } => Arc::new(std::mem::take(builder).finish()),
            Self::String { builder, .. } | Self::EnumString { builder, .. } => builder.finish(),
            Self::Bytes { builder, .. } | Self::EnumBinary { builder, .. } => builder.finish(),
            Self::Timestamp {
                builder, unit, tz, ..
            } => finish_timestamp(builder, *unit, tz),
            Self::Duration { builder, unit, .. } => finish_duration(builder, *unit),
            Self::Date { builder, .. } => finish_primitive(builder),
            Self::TimeOfDay { builder, unit, .. } => finish_time_of_day(builder, *unit),
            Self::WrapperDouble { builder, .. } => finish_primitive(builder),
            Self::WrapperFloat { builder, .. } => finish_primitive(builder),
            Self::WrapperInt64 { builder, .. } => finish_primitive(builder),
            Self::WrapperUInt64 { builder, .. } => finish_primitive(builder),
            Self::WrapperInt32 { builder, .. } => finish_primitive(builder),
            Self::WrapperUInt32 { builder, .. } => finish_primitive(builder),
            Self::WrapperBool { builder, .. } => Arc::new(std::mem::take(builder).finish()),
            Self::WrapperString { builder, .. } => builder.finish(),
            Self::WrapperBytes { builder, .. } => builder.finish(),
            Self::Message {
                sub_decoder,
                is_valid,
                ..
            } => Arc::new(sub_decoder.build_struct_array(Some(std::mem::take(is_valid).finish()))),
            Self::Repeated {
                inner,
                offsets,
                list_name,
                list_nullable,
            } => {
                let vals = inner.finish();
                std::mem::replace(offsets, ListOffsets::new(false)).finish(
                    vals,
                    list_name,
                    *list_nullable,
                )
            }
            Self::Map {
                key_decoder,
                value_decoder,
                offsets,
                map_value_name,
                map_value_nullable,
            } => {
                let (_, key_array) = key_decoder.finish(false);
                let (_, value_array) = value_decoder.finish(*map_value_nullable);
                let key_field = Arc::new(Field::new("key", key_array.data_type().clone(), false));
                let value_field = Arc::new(Field::new(
                    &**map_value_name,
                    value_array.data_type().clone(),
                    *map_value_nullable,
                ));
                let entries_struct_type = DataType::Struct(
                    vec![key_field.as_ref().clone(), value_field.as_ref().clone()].into(),
                );
                let entry_struct =
                    StructArray::from(vec![(key_field, key_array), (value_field, value_array)]);
                let map_dt = DataType::Map(
                    Arc::new(Field::new("entries", entries_struct_type, false)),
                    false,
                );
                let len = offsets.len() - 1;
                let offsets_buf = Buffer::from_vec(std::mem::take(offsets));
                let map_data = ArrayData::builder(map_dt)
                    .len(len)
                    .add_buffer(offsets_buf)
                    .add_child_data(entry_struct.into_data())
                    .build()
                    .unwrap();
                Arc::new(MapArray::from(map_data))
            }
        };
        let field = Field::new("", array.data_type().clone(), nullable);
        (field, array)
    }
}

// ---------------------------------------------------------------------------
// Helpers for repeated varint/fixed decoding
// ---------------------------------------------------------------------------

fn decode_repeated_varint<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn(u64) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type == 2 {
        // packed
        let (data, total) = read_length_delimited(buf)?;
        let mut p = 0;
        while p < data.len() {
            let (v, n) = decode_varint(&data[p..])?;
            builder.append_value(convert(v));
            p += n;
        }
        Ok(total)
    } else if wire_type == 0 {
        let (v, n) = decode_varint(buf)?;
        builder.append_value(convert(v));
        Ok(n)
    } else {
        skip_field(wire_type, buf)
    }
}

fn decode_repeated_fixed<T: ArrowPrimitiveType, const WIDTH: usize>(
    wire_type: u8,
    expected_wt: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn([u8; WIDTH]) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type == 2 {
        let (data, total) = read_length_delimited(buf)?;
        let mut p = 0;
        while p + WIDTH <= data.len() {
            let mut bytes = [0u8; WIDTH];
            bytes.copy_from_slice(&data[p..p + WIDTH]);
            builder.append_value(convert(bytes));
            p += WIDTH;
        }
        Ok(total)
    } else if wire_type == expected_wt {
        if buf.len() < WIDTH {
            return Err(decode_error("unexpected EOF"));
        }
        let mut bytes = [0u8; WIDTH];
        bytes.copy_from_slice(&buf[..WIDTH]);
        builder.append_value(convert(bytes));
        Ok(WIDTH)
    } else {
        skip_field(wire_type, buf)
    }
}

fn decode_repeated_wrapper_varint<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn(u64) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type != 2 {
        return skip_field(wire_type, buf);
    }
    let (data, total) = read_length_delimited(buf)?;
    let (v, _) = decode_wrapper_varint(data)?;
    builder.append_value(convert(v));
    Ok(total)
}

fn decode_repeated_wrapper_fixed32<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn([u8; 4]) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type != 2 {
        return skip_field(wire_type, buf);
    }
    let (data, total) = read_length_delimited(buf)?;
    let (bytes, _) = decode_wrapper_fixed32(data)?;
    builder.append_value(convert(bytes));
    Ok(total)
}

fn decode_repeated_wrapper_fixed64<T: ArrowPrimitiveType>(
    wire_type: u8,
    buf: &[u8],
    builder: &mut PrimitiveBuilder<T>,
    convert: fn([u8; 8]) -> T::Native,
) -> Result<usize, prost::DecodeError> {
    if wire_type != 2 {
        return skip_field(wire_type, buf);
    }
    let (data, total) = read_length_delimited(buf)?;
    let (bytes, _) = decode_wrapper_fixed64(data)?;
    builder.append_value(convert(bytes));
    Ok(total)
}

// ---------------------------------------------------------------------------
// MessageDecoder
// ---------------------------------------------------------------------------

pub struct MessageDecoder {
    decoders: Vec<(FieldDecoder, FieldDescriptor)>,
    tag_map: Vec<Option<usize>>,
    list_nullable: bool,
    map_nullable: bool,
    num_rows: usize,
}

impl MessageDecoder {
    pub fn new(descriptor: &MessageDescriptor, config: &PtarsConfig) -> Self {
        let mut decoders = Vec::new();
        let mut max_field_number: u32 = 0;

        for field in descriptor.fields() {
            if let Some(decoder) = build_field_decoder(&field, config) {
                if field.number() > max_field_number {
                    max_field_number = field.number();
                }
                decoders.push((decoder, field));
            }
        }

        let mut tag_map = vec![
            None;
            if max_field_number == 0 {
                0
            } else {
                max_field_number as usize + 1
            }
        ];
        for (idx, (_, field)) in decoders.iter().enumerate() {
            let num = field.number() as usize;
            tag_map[num] = Some(idx);
        }

        Self {
            decoders,
            tag_map,
            list_nullable: config.list_nullable,
            map_nullable: config.map_nullable,
            num_rows: 0,
        }
    }

    fn decode_row(&mut self, buf: &[u8]) -> Result<(), prost::DecodeError> {
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, n) = decode_tag(&buf[pos..])?;
            pos += n;
            let idx = if (field_num as usize) < self.tag_map.len() {
                self.tag_map[field_num as usize]
            } else {
                None
            };
            if let Some(idx) = idx {
                pos += self.decoders[idx].0.decode(wire_type, &buf[pos..])?;
            } else {
                pos += skip_field(wire_type, &buf[pos..])?;
            }
        }
        for (decoder, _) in &mut self.decoders {
            decoder.flush();
        }
        self.num_rows += 1;
        Ok(())
    }

    /// Decode a submessage's bytes without flushing — used for singular message fields
    /// where the parent will call flush.
    fn decode_message_bytes(&mut self, buf: &[u8]) -> Result<(), prost::DecodeError> {
        let mut pos = 0;
        while pos < buf.len() {
            let (field_num, wire_type, n) = decode_tag(&buf[pos..])?;
            pos += n;
            let idx = if (field_num as usize) < self.tag_map.len() {
                self.tag_map[field_num as usize]
            } else {
                None
            };
            if let Some(idx) = idx {
                pos += self.decoders[idx].0.decode(wire_type, &buf[pos..])?;
            } else {
                pos += skip_field(wire_type, &buf[pos..])?;
            }
        }
        Ok(())
    }

    fn flush_row(&mut self) {
        for (decoder, _) in &mut self.decoders {
            decoder.flush();
        }
    }

    fn flush_defaults(&mut self) {
        for (decoder, _) in &mut self.decoders {
            decoder.flush(); // all absent → defaults
        }
    }

    fn decode_null_row(&mut self) {
        for (decoder, _) in &mut self.decoders {
            decoder.flush();
        }
        self.num_rows += 1;
    }

    fn row_count(&self) -> usize {
        self.num_rows
    }

    fn build_struct_array(&mut self, validity: Option<arrow_array::BooleanArray>) -> StructArray {
        if self.decoders.is_empty() {
            let len = validity.as_ref().map_or(self.num_rows, |v| v.len());
            return StructArray::new_empty_fields(
                len,
                validity.map(|v| arrow::buffer::NullBuffer::new(v.values().clone())),
            );
        }

        let (fields, columns): (Vec<_>, Vec<_>) = self
            .decoders
            .iter_mut()
            .map(|(decoder, field_desc)| {
                let nullable = if field_desc.is_list() {
                    self.list_nullable
                } else if field_desc.is_map() {
                    self.map_nullable
                } else {
                    field_desc.supports_presence()
                };
                let (_, array) = decoder.finish(nullable);
                let field = Field::new(field_desc.name(), array.data_type().clone(), nullable);
                (field, array)
            })
            .unzip();

        StructArray::new(
            arrow_schema::Fields::from(fields),
            columns,
            validity.map(|v| arrow::buffer::NullBuffer::new(v.values().clone())),
        )
    }

    pub fn finish(mut self) -> RecordBatch {
        if self.decoders.is_empty() {
            let schema = Arc::new(arrow_schema::Schema::empty());
            return RecordBatch::try_new_with_options(
                schema,
                vec![],
                &arrow_array::RecordBatchOptions::new().with_row_count(Some(self.num_rows)),
            )
            .unwrap();
        }
        let struct_array = self.build_struct_array(None);
        RecordBatch::from(struct_array)
    }
}

// ---------------------------------------------------------------------------
// build_field_decoder
// ---------------------------------------------------------------------------

fn build_field_decoder(field: &FieldDescriptor, config: &PtarsConfig) -> Option<FieldDecoder> {
    if field.is_map() {
        return build_map_decoder(field, config);
    }
    if field.is_list() {
        return build_repeated_decoder(field, config);
    }

    let has_presence = field.supports_presence();
    match field.kind() {
        Kind::Int32 => Some(FieldDecoder::Int32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Int64 => Some(FieldDecoder::Int64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Uint32 => Some(FieldDecoder::UInt32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Uint64 => Some(FieldDecoder::UInt64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sint32 => Some(FieldDecoder::Sint32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sint64 => Some(FieldDecoder::Sint64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sfixed32 => Some(FieldDecoder::Sfixed32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sfixed64 => Some(FieldDecoder::Sfixed64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Fixed32 => Some(FieldDecoder::Fixed32 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Fixed64 => Some(FieldDecoder::Fixed64 {
            value: 0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Float => Some(FieldDecoder::Float {
            value: 0.0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Double => Some(FieldDecoder::Double {
            value: 0.0,
            has_value: false,
            has_presence,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Bool => Some(FieldDecoder::Bool {
            value: false,
            has_value: false,
            has_presence,
            builder: BooleanBuilder::new(),
        }),
        Kind::String => Some(FieldDecoder::String {
            value: Vec::new(),
            has_value: false,
            has_presence,
            builder: StringBuilderInner::new(config.use_large_string),
        }),
        Kind::Bytes => Some(FieldDecoder::Bytes {
            value: Vec::new(),
            has_value: false,
            has_presence,
            builder: BinaryBuilderInner::new(config.use_large_binary),
        }),
        Kind::Enum(enum_desc) => match config.enum_repr {
            EnumRepr::Int32 => Some(FieldDecoder::EnumInt32 {
                value: 0,
                has_value: false,
                has_presence,
                builder: PrimitiveBuilder::new(),
            }),
            EnumRepr::String => Some(FieldDecoder::EnumString {
                value: 0,
                has_value: false,
                has_presence,
                builder: StringBuilderInner::new(config.use_large_string),
                enum_descriptor: enum_desc,
            }),
            EnumRepr::Binary => Some(FieldDecoder::EnumBinary {
                value: 0,
                has_value: false,
                has_presence,
                builder: BinaryBuilderInner::new(config.use_large_binary),
                enum_descriptor: enum_desc,
            }),
        },
        Kind::Message(msg_desc) => build_message_field_decoder(msg_desc, config),
    }
}

fn build_message_field_decoder(
    msg_desc: MessageDescriptor,
    config: &PtarsConfig,
) -> Option<FieldDecoder> {
    match msg_desc.full_name() {
        "google.protobuf.Timestamp" => Some(FieldDecoder::Timestamp {
            seconds: 0,
            nanos: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
            unit: config.timestamp_unit,
            tz: config.timestamp_tz.clone(),
        }),
        "google.protobuf.Duration" => Some(FieldDecoder::Duration {
            seconds: 0,
            nanos: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
            unit: config.duration_unit,
        }),
        "google.type.Date" => Some(FieldDecoder::Date {
            year: 0,
            month: 0,
            day: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
        }),
        "google.type.TimeOfDay" => Some(FieldDecoder::TimeOfDay {
            hours: 0,
            minutes: 0,
            seconds_val: 0,
            nanos: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
            unit: config.time_unit,
        }),
        "google.protobuf.DoubleValue" => Some(FieldDecoder::WrapperDouble {
            value: 0.0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
        }),
        "google.protobuf.FloatValue" => Some(FieldDecoder::WrapperFloat {
            value: 0.0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
        }),
        "google.protobuf.Int64Value" => Some(FieldDecoder::WrapperInt64 {
            value: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
        }),
        "google.protobuf.UInt64Value" => Some(FieldDecoder::WrapperUInt64 {
            value: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
        }),
        "google.protobuf.Int32Value" => Some(FieldDecoder::WrapperInt32 {
            value: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
        }),
        "google.protobuf.UInt32Value" => Some(FieldDecoder::WrapperUInt32 {
            value: 0,
            has_value: false,
            builder: PrimitiveBuilder::new(),
        }),
        "google.protobuf.BoolValue" => Some(FieldDecoder::WrapperBool {
            value: false,
            has_value: false,
            builder: BooleanBuilder::new(),
        }),
        "google.protobuf.StringValue" => Some(FieldDecoder::WrapperString {
            value: Vec::new(),
            has_value: false,
            builder: StringBuilderInner::new(config.use_large_string),
        }),
        "google.protobuf.BytesValue" => Some(FieldDecoder::WrapperBytes {
            value: Vec::new(),
            has_value: false,
            builder: BinaryBuilderInner::new(config.use_large_binary),
        }),
        _ => {
            let sub_decoder = MessageDecoder::new(&msg_desc, config);
            Some(FieldDecoder::Message {
                sub_decoder,
                has_value: false,
                is_valid: BooleanBuilder::new(),
            })
        }
    }
}

fn build_repeated_decoder(field: &FieldDescriptor, config: &PtarsConfig) -> Option<FieldDecoder> {
    let ln = config.list_value_name.clone();
    let lnb = config.list_value_nullable;
    let offsets = || ListOffsets::new(config.use_large_list);

    let inner = match field.kind() {
        Kind::Int32 => RepeatedInner::Int32 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Sint32 => RepeatedInner::Sint32 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Sfixed32 => RepeatedInner::Sfixed32 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Int64 => RepeatedInner::Int64 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Sint64 => RepeatedInner::Sint64 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Sfixed64 => RepeatedInner::Sfixed64 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Uint32 => RepeatedInner::UInt32 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Fixed32 => RepeatedInner::Fixed32 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Uint64 => RepeatedInner::UInt64 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Fixed64 => RepeatedInner::Fixed64 {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Float => RepeatedInner::Float {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Double => RepeatedInner::Double {
            values_builder: PrimitiveBuilder::new(),
        },
        Kind::Bool => RepeatedInner::Bool {
            values_builder: BooleanBuilder::new(),
        },
        Kind::String => RepeatedInner::String {
            values_builder: StringBuilderInner::new(config.use_large_string),
        },
        Kind::Bytes => RepeatedInner::Bytes {
            values_builder: BinaryBuilderInner::new(config.use_large_binary),
        },
        Kind::Enum(enum_desc) => match config.enum_repr {
            EnumRepr::Int32 => RepeatedInner::EnumInt32 {
                values_builder: PrimitiveBuilder::new(),
            },
            EnumRepr::String => RepeatedInner::EnumString {
                values_builder: StringBuilderInner::new(config.use_large_string),
                enum_descriptor: enum_desc,
            },
            EnumRepr::Binary => RepeatedInner::EnumBinary {
                values_builder: BinaryBuilderInner::new(config.use_large_binary),
                enum_descriptor: enum_desc,
            },
        },
        Kind::Message(msg_desc) => {
            return build_repeated_message_decoder(&msg_desc, config, offsets(), ln, lnb);
        }
    };

    Some(FieldDecoder::Repeated {
        inner,
        offsets: offsets(),
        list_name: ln,
        list_nullable: lnb,
    })
}

fn build_repeated_message_decoder(
    msg_desc: &MessageDescriptor,
    config: &PtarsConfig,
    offsets: ListOffsets,
    ln: Arc<str>,
    lnb: bool,
) -> Option<FieldDecoder> {
    let inner = match msg_desc.full_name() {
        "google.protobuf.Timestamp" => RepeatedInner::Timestamp {
            values_builder: PrimitiveBuilder::new(),
            unit: config.timestamp_unit,
            tz: config.timestamp_tz.clone(),
        },
        "google.protobuf.Duration" => RepeatedInner::Duration {
            values_builder: PrimitiveBuilder::new(),
            unit: config.duration_unit,
        },
        "google.type.Date" => RepeatedInner::Date {
            values_builder: PrimitiveBuilder::new(),
        },
        "google.type.TimeOfDay" => RepeatedInner::TimeOfDay {
            values_builder: PrimitiveBuilder::new(),
            unit: config.time_unit,
        },
        "google.protobuf.DoubleValue" => RepeatedInner::WrapperDouble {
            values_builder: PrimitiveBuilder::new(),
        },
        "google.protobuf.FloatValue" => RepeatedInner::WrapperFloat {
            values_builder: PrimitiveBuilder::new(),
        },
        "google.protobuf.Int64Value" => RepeatedInner::WrapperInt64 {
            values_builder: PrimitiveBuilder::new(),
        },
        "google.protobuf.UInt64Value" => RepeatedInner::WrapperUInt64 {
            values_builder: PrimitiveBuilder::new(),
        },
        "google.protobuf.Int32Value" => RepeatedInner::WrapperInt32 {
            values_builder: PrimitiveBuilder::new(),
        },
        "google.protobuf.UInt32Value" => RepeatedInner::WrapperUInt32 {
            values_builder: PrimitiveBuilder::new(),
        },
        "google.protobuf.BoolValue" => RepeatedInner::WrapperBool {
            values_builder: BooleanBuilder::new(),
        },
        "google.protobuf.StringValue" => RepeatedInner::WrapperString {
            values_builder: StringBuilderInner::new(config.use_large_string),
        },
        "google.protobuf.BytesValue" => RepeatedInner::WrapperBytes {
            values_builder: BinaryBuilderInner::new(config.use_large_binary),
        },
        _ => {
            let sub_decoder = MessageDecoder::new(msg_desc, config);
            RepeatedInner::Message { sub_decoder }
        }
    };

    Some(FieldDecoder::Repeated {
        inner,
        offsets,
        list_name: ln,
        list_nullable: lnb,
    })
}

fn build_map_decoder(field: &FieldDescriptor, config: &PtarsConfig) -> Option<FieldDecoder> {
    let map_entry = match field.kind() {
        Kind::Message(desc) => desc,
        _ => return None,
    };
    let key_field = map_entry.get_field_by_name("key")?;
    let value_field = map_entry.get_field_by_name("value")?;

    // Build singular decoders for key and value (they buffer per-entry, not per-row)
    let key_decoder = build_singular_decoder_for_map(&key_field, config)?;
    let value_decoder = build_singular_decoder_for_map(&value_field, config)?;

    Some(FieldDecoder::Map {
        key_decoder: Box::new(key_decoder),
        value_decoder: Box::new(value_decoder),
        offsets: vec![0],
        map_value_name: config.map_value_name.clone(),
        map_value_nullable: config.map_value_nullable,
    })
}

/// Build a singular decoder for use inside a map entry (no presence tracking).
fn build_singular_decoder_for_map(
    field: &FieldDescriptor,
    config: &PtarsConfig,
) -> Option<FieldDecoder> {
    // Map keys/values are never "optional" in protobuf sense — they use proto3 defaults
    match field.kind() {
        Kind::Int32 => Some(FieldDecoder::Int32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Int64 => Some(FieldDecoder::Int64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Uint32 => Some(FieldDecoder::UInt32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Uint64 => Some(FieldDecoder::UInt64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sint32 => Some(FieldDecoder::Sint32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sint64 => Some(FieldDecoder::Sint64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sfixed32 => Some(FieldDecoder::Sfixed32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Sfixed64 => Some(FieldDecoder::Sfixed64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Fixed32 => Some(FieldDecoder::Fixed32 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Fixed64 => Some(FieldDecoder::Fixed64 {
            value: 0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Float => Some(FieldDecoder::Float {
            value: 0.0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Double => Some(FieldDecoder::Double {
            value: 0.0,
            has_value: false,
            has_presence: false,
            builder: PrimitiveBuilder::new(),
        }),
        Kind::Bool => Some(FieldDecoder::Bool {
            value: false,
            has_value: false,
            has_presence: false,
            builder: BooleanBuilder::new(),
        }),
        Kind::String => Some(FieldDecoder::String {
            value: Vec::new(),
            has_value: false,
            has_presence: false,
            builder: StringBuilderInner::new(config.use_large_string),
        }),
        Kind::Bytes => Some(FieldDecoder::Bytes {
            value: Vec::new(),
            has_value: false,
            has_presence: false,
            builder: BinaryBuilderInner::new(config.use_large_binary),
        }),
        Kind::Enum(enum_desc) => match config.enum_repr {
            EnumRepr::Int32 => Some(FieldDecoder::EnumInt32 {
                value: 0,
                has_value: false,
                has_presence: false,
                builder: PrimitiveBuilder::new(),
            }),
            EnumRepr::String => Some(FieldDecoder::EnumString {
                value: 0,
                has_value: false,
                has_presence: false,
                builder: StringBuilderInner::new(config.use_large_string),
                enum_descriptor: enum_desc,
            }),
            EnumRepr::Binary => Some(FieldDecoder::EnumBinary {
                value: 0,
                has_value: false,
                has_presence: false,
                builder: BinaryBuilderInner::new(config.use_large_binary),
                enum_descriptor: enum_desc,
            }),
        },
        Kind::Message(msg_desc) => build_message_field_decoder(msg_desc, config),
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Decode a BinaryArray of serialized protobuf messages directly into a RecordBatch.
///
/// Parses protobuf wire format directly into Arrow builders — no intermediate
/// message objects are created.
pub fn binary_array_to_record_batch_direct(
    array: &BinaryArray,
    descriptor: &MessageDescriptor,
    config: &PtarsConfig,
) -> Result<RecordBatch, prost::DecodeError> {
    let mut decoder = MessageDecoder::new(descriptor, config);
    let policy = config.confluent_wire_policy;
    for i in 0..array.len() {
        if array.is_null(i) {
            decoder.decode_null_row();
        } else {
            let bytes = strip_confluent_prefix(array.value(i), policy)?;
            decoder.decode_row(bytes)?;
        }
    }
    Ok(decoder.finish())
}

/// Convert DynamicMessage instances to a RecordBatch using the default configuration.
///
/// Each message is serialized to protobuf wire format, then decoded directly
/// into Arrow arrays.
pub fn messages_to_record_batch(
    messages: &[prost_reflect::DynamicMessage],
    message_descriptor: &MessageDescriptor,
) -> RecordBatch {
    messages_to_record_batch_with_config(messages, message_descriptor, &PtarsConfig::default())
}

/// Convert DynamicMessage instances to a RecordBatch using the specified configuration.
///
/// Each message is serialized to protobuf wire format, then decoded directly
/// into Arrow arrays.
pub fn messages_to_record_batch_with_config(
    messages: &[prost_reflect::DynamicMessage],
    message_descriptor: &MessageDescriptor,
    config: &PtarsConfig,
) -> RecordBatch {
    use arrow_array::builder::BinaryBuilder;
    use prost::Message;

    let mut bin_builder = BinaryBuilder::new();
    for msg in messages {
        bin_builder.append_value(msg.encode_to_vec());
    }
    let binary_array = bin_builder.finish();
    binary_array_to_record_batch_direct(&binary_array, message_descriptor, config)
        .expect("failed to decode messages")
}

/// Decode a BinaryArray into a vector of DynamicMessage.
///
/// Each element in the binary array is expected to be a serialized protobuf message.
/// Null values in the array result in default (empty) messages.
pub fn binary_array_to_messages(
    array: &BinaryArray,
    message_descriptor: &MessageDescriptor,
) -> Result<Vec<prost_reflect::DynamicMessage>, prost::DecodeError> {
    let mut messages = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        let message = if array.is_null(i) {
            prost_reflect::DynamicMessage::new(message_descriptor.clone())
        } else {
            prost_reflect::DynamicMessage::decode(message_descriptor.clone(), array.value(i))?
        };
        messages.push(message);
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_confluent_prefix_raw() {
        let buf = b"\x00\x00\x00\x00\x01\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Raw).unwrap();
        assert_eq!(result, buf);
    }

    #[test]
    fn test_strip_confluent_prefix_standard() {
        // magic byte + 4-byte schema ID + payload
        let buf = b"\x00\x00\x00\x00\x01\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Standard).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_standard_too_short() {
        let buf = b"\x00\x01\x02";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Standard);
        assert!(result.is_err());
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_zero_indexes() {
        // magic byte + 4-byte schema ID + varint 0 (count=0) + payload
        let buf = b"\x00\x00\x00\x00\x01\x00\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_one_index() {
        // magic byte + 4-byte schema ID + varint 1 (count=1) + varint 0 (index) + payload
        let buf = b"\x00\x00\x00\x00\x01\x01\x00\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_two_indexes() {
        // magic byte + 4-byte schema ID + varint 2 (count) + varint 4 + varint 2 + payload
        let buf = b"\x00\x00\x00\x00\x01\x02\x04\x02\x08\x96\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf).unwrap();
        assert_eq!(result, b"\x08\x96\x01");
    }

    #[test]
    fn test_strip_confluent_prefix_protobuf_too_short() {
        let buf = b"\x00\x01";
        let result = strip_confluent_prefix(buf, ConfluentWirePolicy::Protobuf);
        assert!(result.is_err());
    }
}
