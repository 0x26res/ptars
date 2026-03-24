use arrow::array::ArrayData;
use arrow_array::builder::BinaryBuilder;
use arrow_array::types::{
    Date32Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Float32Type, Float64Type, Int32Type, Int64Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrowPrimitiveType, BinaryArray, BooleanArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, MapArray, PrimitiveArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate};
use prost::encoding::{encode_key, encode_varint, WireType};

use prost_reflect::{EnumDescriptor, FieldDescriptor, Kind, MessageDescriptor};

/// Look up enum number by name, returning 0 (proto3 default) for unknown names.
fn enum_number_from_name(name: &str, enum_descriptor: &EnumDescriptor) -> i32 {
    enum_descriptor
        .get_value_by_name(name)
        .map(|v| v.number())
        .unwrap_or(0)
}

// Days from CE epoch to Unix epoch (1970-01-01)
const CE_OFFSET: i32 = 719163;

// ---------------------------------------------------------------------------
// Helper column refs (abstract over Regular / Large array variants)
// ---------------------------------------------------------------------------

enum StringColumnRef<'a> {
    Regular(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl StringColumnRef<'_> {
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

enum BinaryColumnRef<'a> {
    Regular(&'a BinaryArray),
    Large(&'a LargeBinaryArray),
}

impl BinaryColumnRef<'_> {
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

/// Abstracts over ListArray / LargeListArray.
#[derive(Clone, Copy)]
enum GenericListArray<'a> {
    Regular(&'a ListArray),
    Large(&'a LargeListArray),
}

impl<'a> GenericListArray<'a> {
    fn from_array(array: &'a dyn Array) -> Option<Self> {
        array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(GenericListArray::Regular)
            .or_else(|| {
                array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .map(GenericListArray::Large)
            })
    }
    fn is_null(&self, i: usize) -> bool {
        match self {
            Self::Regular(a) => a.is_null(i),
            Self::Large(a) => a.is_null(i),
        }
    }
    fn value_offsets(&self, i: usize) -> (usize, usize) {
        match self {
            Self::Regular(a) => {
                let o = a.value_offsets();
                (o[i] as usize, o[i + 1] as usize)
            }
            Self::Large(a) => {
                let o = a.value_offsets();
                (o[i] as usize, o[i + 1] as usize)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Time conversion helpers (mirroring arrow_to_proto.rs logic)
// ---------------------------------------------------------------------------

fn time_unit_to_seconds_and_nanos(value: i64, unit: TimeUnit) -> (i64, i32) {
    match unit {
        TimeUnit::Second => (value, 0),
        TimeUnit::Millisecond => {
            let mut seconds = value / 1_000;
            let mut nanos = ((value % 1_000) * 1_000_000) as i32;
            if nanos < 0 {
                seconds -= 1;
                nanos += 1_000_000_000;
            }
            (seconds, nanos)
        }
        TimeUnit::Microsecond => {
            let mut seconds = value / 1_000_000;
            let mut nanos = ((value % 1_000_000) * 1_000) as i32;
            if nanos < 0 {
                seconds -= 1;
                nanos += 1_000_000_000;
            }
            (seconds, nanos)
        }
        TimeUnit::Nanosecond => {
            let mut seconds = value / 1_000_000_000;
            let mut nanos = (value % 1_000_000_000) as i32;
            if nanos < 0 {
                seconds -= 1;
                nanos += 1_000_000_000;
            }
            (seconds, nanos)
        }
    }
}

fn time_unit_to_duration_seconds_and_nanos(value: i64, unit: TimeUnit) -> (i64, i32) {
    match unit {
        TimeUnit::Second => (value, 0),
        TimeUnit::Millisecond => {
            let seconds = value / 1_000;
            let nanos = ((value % 1_000) * 1_000_000) as i32;
            (seconds, nanos)
        }
        TimeUnit::Microsecond => {
            let seconds = value / 1_000_000;
            let nanos = ((value % 1_000_000) * 1_000) as i32;
            (seconds, nanos)
        }
        TimeUnit::Nanosecond => {
            let seconds = value / 1_000_000_000;
            let nanos = (value % 1_000_000_000) as i32;
            (seconds, nanos)
        }
    }
}

fn time32_unit_to_nanos(value: i32, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => i64::from(value) * 1_000_000_000,
        TimeUnit::Millisecond => i64::from(value) * 1_000_000,
        _ => panic!("Time32 only supports Second and Millisecond units"),
    }
}

fn time64_unit_to_nanos(value: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Microsecond => value * 1_000,
        TimeUnit::Nanosecond => value,
        _ => panic!("Time64 only supports Microsecond and Nanosecond units"),
    }
}

// ---------------------------------------------------------------------------
// Direct wire-format encoding helpers for well-known types
// ---------------------------------------------------------------------------

/// Encode a Timestamp (seconds=1, nanos=2) directly into buf.
/// Both fields use int64/int32 varint encoding. Skips default values.
fn encode_timestamp_fields(seconds: i64, nanos: i32, buf: &mut Vec<u8>) {
    if seconds != 0 {
        prost::encoding::int64::encode(1, &seconds, buf);
    }
    if nanos != 0 {
        prost::encoding::int32::encode(2, &nanos, buf);
    }
}

/// Encode a Duration (seconds=1, nanos=2) directly into buf.
fn encode_duration_fields(seconds: i64, nanos: i32, buf: &mut Vec<u8>) {
    if seconds != 0 {
        prost::encoding::int64::encode(1, &seconds, buf);
    }
    if nanos != 0 {
        prost::encoding::int32::encode(2, &nanos, buf);
    }
}

/// Encode a Date (year=1, month=2, day=3) directly into buf.
fn encode_date_fields(days: i32, buf: &mut Vec<u8>) {
    if days == 0 {
        // Special case: year=0, month=0, day=0 — all default, empty message
        return;
    }
    let date = NaiveDate::from_num_days_from_ce_opt(days + CE_OFFSET).unwrap();
    let year = date.year();
    let month = date.month() as i32;
    let day = date.day() as i32;
    if year != 0 {
        prost::encoding::int32::encode(1, &year, buf);
    }
    if month != 0 {
        prost::encoding::int32::encode(2, &month, buf);
    }
    if day != 0 {
        prost::encoding::int32::encode(3, &day, buf);
    }
}

/// Encode a TimeOfDay (hours=1, minutes=2, seconds=3, nanos=4) directly into buf.
fn encode_time_of_day_fields(total_nanos: i64, buf: &mut Vec<u8>) {
    let hours = (total_nanos / 3_600_000_000_000) as i32;
    let remaining = total_nanos % 3_600_000_000_000;
    let minutes = (remaining / 60_000_000_000) as i32;
    let remaining = remaining % 60_000_000_000;
    let seconds = (remaining / 1_000_000_000) as i32;
    let nanos = (remaining % 1_000_000_000) as i32;

    if hours != 0 {
        prost::encoding::int32::encode(1, &hours, buf);
    }
    if minutes != 0 {
        prost::encoding::int32::encode(2, &minutes, buf);
    }
    if seconds != 0 {
        prost::encoding::int32::encode(3, &seconds, buf);
    }
    if nanos != 0 {
        prost::encoding::int32::encode(4, &nanos, buf);
    }
}

/// Write a length-delimited submessage: tag + varint(len) + bytes
fn write_submessage(tag: u32, content: &[u8], buf: &mut Vec<u8>) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(content.len() as u64, buf);
    buf.extend_from_slice(content);
}

// ---------------------------------------------------------------------------
// FieldEncoder: one variant per proto kind
// ---------------------------------------------------------------------------

enum FieldEncoder<'a> {
    // Scalar variants
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

    // Enum variants
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

    // Nested message (regular)
    Message {
        tag: u32,
        struct_arr: &'a StructArray,
        sub_encoder: MessageEncoder<'a>,
    },

    // Well-known types (singular)
    Timestamp {
        tag: u32,
        unit: TimeUnit,
        array: WellKnownPrimitiveArray<'a>,
    },
    Duration {
        tag: u32,
        unit: TimeUnit,
        array: WellKnownPrimitiveArray<'a>,
    },
    Date {
        tag: u32,
        arr: &'a PrimitiveArray<Date32Type>,
    },
    TimeOfDay {
        tag: u32,
        array: TimeOfDayArray<'a>,
    },

    // Wrapper types (singular)
    WrapperDouble {
        tag: u32,
        arr: &'a PrimitiveArray<Float64Type>,
    },
    WrapperFloat {
        tag: u32,
        arr: &'a PrimitiveArray<Float32Type>,
    },
    WrapperInt64 {
        tag: u32,
        arr: &'a PrimitiveArray<Int64Type>,
    },
    WrapperUInt64 {
        tag: u32,
        arr: &'a PrimitiveArray<UInt64Type>,
    },
    WrapperInt32 {
        tag: u32,
        arr: &'a PrimitiveArray<Int32Type>,
    },
    WrapperUInt32 {
        tag: u32,
        arr: &'a PrimitiveArray<UInt32Type>,
    },
    WrapperBool {
        tag: u32,
        arr: &'a BooleanArray,
    },
    WrapperString {
        tag: u32,
        col: StringColumnRef<'a>,
    },
    WrapperBytes {
        tag: u32,
        col: BinaryColumnRef<'a>,
    },

    // Repeated fields
    RepeatedPacked {
        tag: u32,
        list: GenericListArray<'a>,
        encoder: PackedEncoder<'a>,
    },
    RepeatedBool {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a BooleanArray,
    },
    RepeatedString {
        tag: u32,
        list: GenericListArray<'a>,
        col: StringColumnRef<'a>,
    },
    RepeatedBytes {
        tag: u32,
        list: GenericListArray<'a>,
        col: BinaryColumnRef<'a>,
    },
    RepeatedEnumInt32 {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<Int32Type>,
    },
    RepeatedEnumString {
        tag: u32,
        list: GenericListArray<'a>,
        col: StringColumnRef<'a>,
        enum_descriptor: EnumDescriptor,
    },
    RepeatedEnumBinary {
        tag: u32,
        list: GenericListArray<'a>,
        col: BinaryColumnRef<'a>,
        enum_descriptor: EnumDescriptor,
    },
    RepeatedMessage {
        tag: u32,
        list: GenericListArray<'a>,
        sub_encoder: MessageEncoder<'a>,
    },
    RepeatedTimestamp {
        tag: u32,
        list: GenericListArray<'a>,
        unit: TimeUnit,
        values: WellKnownPrimitiveArray<'a>,
    },
    RepeatedDuration {
        tag: u32,
        list: GenericListArray<'a>,
        unit: TimeUnit,
        values: WellKnownPrimitiveArray<'a>,
    },
    RepeatedDate {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<Date32Type>,
    },
    RepeatedTimeOfDay {
        tag: u32,
        list: GenericListArray<'a>,
        values: TimeOfDayArray<'a>,
    },
    RepeatedWrapperDouble {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<Float64Type>,
    },
    RepeatedWrapperFloat {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<Float32Type>,
    },
    RepeatedWrapperInt64 {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<Int64Type>,
    },
    RepeatedWrapperUInt64 {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<UInt64Type>,
    },
    RepeatedWrapperInt32 {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<Int32Type>,
    },
    RepeatedWrapperUInt32 {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a PrimitiveArray<UInt32Type>,
    },
    RepeatedWrapperBool {
        tag: u32,
        list: GenericListArray<'a>,
        values: &'a BooleanArray,
    },
    RepeatedWrapperString {
        tag: u32,
        list: GenericListArray<'a>,
        col: StringColumnRef<'a>,
    },
    RepeatedWrapperBytes {
        tag: u32,
        list: GenericListArray<'a>,
        col: BinaryColumnRef<'a>,
    },

    // Map fields
    Map {
        tag: u32,
        map_array: &'a MapArray,
        key_encoder: MapKeyEncoder<'a>,
        value_encoder: MapValueEncoder<'a>,
    },
}

/// Holds a timestamp/duration array of any time unit.
enum WellKnownPrimitiveArray<'a> {
    Second(&'a PrimitiveArray<TimestampSecondType>),
    Millisecond(&'a PrimitiveArray<TimestampMillisecondType>),
    Microsecond(&'a PrimitiveArray<TimestampMicrosecondType>),
    Nanosecond(&'a PrimitiveArray<TimestampNanosecondType>),
    DurSecond(&'a PrimitiveArray<DurationSecondType>),
    DurMillisecond(&'a PrimitiveArray<DurationMillisecondType>),
    DurMicrosecond(&'a PrimitiveArray<DurationMicrosecondType>),
    DurNanosecond(&'a PrimitiveArray<DurationNanosecondType>),
}

impl WellKnownPrimitiveArray<'_> {
    fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Second(a) => a.is_null(idx),
            Self::Millisecond(a) => a.is_null(idx),
            Self::Microsecond(a) => a.is_null(idx),
            Self::Nanosecond(a) => a.is_null(idx),
            Self::DurSecond(a) => a.is_null(idx),
            Self::DurMillisecond(a) => a.is_null(idx),
            Self::DurMicrosecond(a) => a.is_null(idx),
            Self::DurNanosecond(a) => a.is_null(idx),
        }
    }
    fn value_i64(&self, idx: usize) -> i64 {
        match self {
            Self::Second(a) => a.value(idx),
            Self::Millisecond(a) => a.value(idx),
            Self::Microsecond(a) => a.value(idx),
            Self::Nanosecond(a) => a.value(idx),
            Self::DurSecond(a) => a.value(idx),
            Self::DurMillisecond(a) => a.value(idx),
            Self::DurMicrosecond(a) => a.value(idx),
            Self::DurNanosecond(a) => a.value(idx),
        }
    }
}

/// Holds a time-of-day array of any time unit.
enum TimeOfDayArray<'a> {
    Time32Second(&'a PrimitiveArray<Time32SecondType>),
    Time32Millisecond(&'a PrimitiveArray<Time32MillisecondType>),
    Time64Microsecond(&'a PrimitiveArray<Time64MicrosecondType>),
    Time64Nanosecond(&'a PrimitiveArray<Time64NanosecondType>),
}

impl TimeOfDayArray<'_> {
    fn is_null(&self, idx: usize) -> bool {
        match self {
            Self::Time32Second(a) => a.is_null(idx),
            Self::Time32Millisecond(a) => a.is_null(idx),
            Self::Time64Microsecond(a) => a.is_null(idx),
            Self::Time64Nanosecond(a) => a.is_null(idx),
        }
    }
    fn to_nanos(&self, idx: usize) -> i64 {
        match self {
            Self::Time32Second(a) => time32_unit_to_nanos(a.value(idx), TimeUnit::Second),
            Self::Time32Millisecond(a) => time32_unit_to_nanos(a.value(idx), TimeUnit::Millisecond),
            Self::Time64Microsecond(a) => time64_unit_to_nanos(a.value(idx), TimeUnit::Microsecond),
            Self::Time64Nanosecond(a) => time64_unit_to_nanos(a.value(idx), TimeUnit::Nanosecond),
        }
    }
}

/// Encoder for packed repeated numeric fields.
enum PackedEncoder<'a> {
    Int32(&'a PrimitiveArray<Int32Type>),
    Int64(&'a PrimitiveArray<Int64Type>),
    UInt32(&'a PrimitiveArray<UInt32Type>),
    UInt64(&'a PrimitiveArray<UInt64Type>),
    Sint32(&'a PrimitiveArray<Int32Type>),
    Sint64(&'a PrimitiveArray<Int64Type>),
    Sfixed32(&'a PrimitiveArray<Int32Type>),
    Sfixed64(&'a PrimitiveArray<Int64Type>),
    Fixed32(&'a PrimitiveArray<UInt32Type>),
    Fixed64(&'a PrimitiveArray<UInt64Type>),
    Float32(&'a PrimitiveArray<Float32Type>),
    Float64(&'a PrimitiveArray<Float64Type>),
}

impl PackedEncoder<'_> {
    /// Encode elements in [start..end) as packed wire format (no tag, just values).
    fn encode_packed_values(&self, start: usize, end: usize, buf: &mut Vec<u8>) {
        match self {
            Self::Int32(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    encode_varint(v as u64, buf);
                }
            }
            Self::Int64(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    encode_varint(v as u64, buf);
                }
            }
            Self::UInt32(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    encode_varint(u64::from(v), buf);
                }
            }
            Self::UInt64(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    encode_varint(v, buf);
                }
            }
            Self::Sint32(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    // zigzag encoding
                    encode_varint(((v << 1) ^ (v >> 31)) as u32 as u64, buf);
                }
            }
            Self::Sint64(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    // zigzag encoding
                    encode_varint(((v << 1) ^ (v >> 63)) as u64, buf);
                }
            }
            Self::Sfixed32(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Self::Sfixed64(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Self::Fixed32(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Self::Fixed64(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0 } else { arr.value(i) };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Self::Float32(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0.0 } else { arr.value(i) };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
            Self::Float64(arr) => {
                for i in start..end {
                    let v = if arr.is_null(i) { 0.0 } else { arr.value(i) };
                    buf.extend_from_slice(&v.to_le_bytes());
                }
            }
        }
    }
}

/// Encoder for map key fields.
/// Each variant uses the correct wire encoding for its proto type.
enum MapKeyEncoder<'a> {
    String(StringColumnRef<'a>),
    Int32(&'a PrimitiveArray<Int32Type>),
    Sint32(&'a PrimitiveArray<Int32Type>),
    Sfixed32(&'a PrimitiveArray<Int32Type>),
    Int64(&'a PrimitiveArray<Int64Type>),
    Sint64(&'a PrimitiveArray<Int64Type>),
    Sfixed64(&'a PrimitiveArray<Int64Type>),
    UInt32(&'a PrimitiveArray<UInt32Type>),
    Fixed32(&'a PrimitiveArray<UInt32Type>),
    UInt64(&'a PrimitiveArray<UInt64Type>),
    Fixed64(&'a PrimitiveArray<UInt64Type>),
    Bool(&'a BooleanArray),
}

impl MapKeyEncoder<'_> {
    fn encode_at(&self, idx: usize, buf: &mut Vec<u8>) {
        match self {
            Self::String(col) => {
                if !col.is_null(idx) {
                    let v = col.value(idx);
                    encode_key(1, WireType::LengthDelimited, buf);
                    encode_varint(v.len() as u64, buf);
                    buf.extend_from_slice(v.as_bytes());
                }
            }
            Self::Int32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::int32::encode(1, &v, buf);
                    }
                }
            }
            Self::Sint32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sint32::encode(1, &v, buf);
                    }
                }
            }
            Self::Sfixed32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sfixed32::encode(1, &v, buf);
                    }
                }
            }
            Self::Int64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::int64::encode(1, &v, buf);
                    }
                }
            }
            Self::Sint64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sint64::encode(1, &v, buf);
                    }
                }
            }
            Self::Sfixed64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sfixed64::encode(1, &v, buf);
                    }
                }
            }
            Self::UInt32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::uint32::encode(1, &v, buf);
                    }
                }
            }
            Self::Fixed32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::fixed32::encode(1, &v, buf);
                    }
                }
            }
            Self::UInt64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::uint64::encode(1, &v, buf);
                    }
                }
            }
            Self::Fixed64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::fixed64::encode(1, &v, buf);
                    }
                }
            }
            Self::Bool(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v {
                        prost::encoding::bool::encode(1, &v, buf);
                    }
                }
            }
        }
    }
}

/// Encoder for map value fields.
enum MapValueEncoder<'a> {
    Double(&'a PrimitiveArray<Float64Type>),
    Float(&'a PrimitiveArray<Float32Type>),
    Int32(&'a PrimitiveArray<Int32Type>),
    Sint32(&'a PrimitiveArray<Int32Type>),
    Sfixed32(&'a PrimitiveArray<Int32Type>),
    Int64(&'a PrimitiveArray<Int64Type>),
    Sint64(&'a PrimitiveArray<Int64Type>),
    Sfixed64(&'a PrimitiveArray<Int64Type>),
    UInt32(&'a PrimitiveArray<UInt32Type>),
    Fixed32(&'a PrimitiveArray<UInt32Type>),
    UInt64(&'a PrimitiveArray<UInt64Type>),
    Fixed64(&'a PrimitiveArray<UInt64Type>),
    Bool(&'a BooleanArray),
    String(StringColumnRef<'a>),
    Bytes(BinaryColumnRef<'a>),
    EnumInt32(&'a PrimitiveArray<Int32Type>),
    EnumString(StringColumnRef<'a>, EnumDescriptor),
    EnumBinary(BinaryColumnRef<'a>, EnumDescriptor),
    Message(&'a StructArray, MessageEncoder<'a>),
    Timestamp(TimeUnit, WellKnownPrimitiveArray<'a>),
    Duration(TimeUnit, WellKnownPrimitiveArray<'a>),
    Date(&'a PrimitiveArray<Date32Type>),
    TimeOfDay(TimeOfDayArray<'a>),
    WrapperDouble(&'a PrimitiveArray<Float64Type>),
    WrapperFloat(&'a PrimitiveArray<Float32Type>),
    WrapperInt64(&'a PrimitiveArray<Int64Type>),
    WrapperUInt64(&'a PrimitiveArray<UInt64Type>),
    WrapperInt32(&'a PrimitiveArray<Int32Type>),
    WrapperUInt32(&'a PrimitiveArray<UInt32Type>),
    WrapperBool(&'a BooleanArray),
    WrapperString(StringColumnRef<'a>),
    WrapperBytes(BinaryColumnRef<'a>),
}

impl MapValueEncoder<'_> {
    fn encode_at(&self, idx: usize, buf: &mut Vec<u8>) {
        match self {
            Self::Double(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0.0 {
                        prost::encoding::double::encode(2, &v, buf);
                    }
                }
            }
            Self::Float(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0.0 {
                        prost::encoding::float::encode(2, &v, buf);
                    }
                }
            }
            Self::Int32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::int32::encode(2, &v, buf);
                    }
                }
            }
            Self::Sint32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sint32::encode(2, &v, buf);
                    }
                }
            }
            Self::Sfixed32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sfixed32::encode(2, &v, buf);
                    }
                }
            }
            Self::Int64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::int64::encode(2, &v, buf);
                    }
                }
            }
            Self::Sint64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sint64::encode(2, &v, buf);
                    }
                }
            }
            Self::Sfixed64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::sfixed64::encode(2, &v, buf);
                    }
                }
            }
            Self::UInt32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::uint32::encode(2, &v, buf);
                    }
                }
            }
            Self::Fixed32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::fixed32::encode(2, &v, buf);
                    }
                }
            }
            Self::UInt64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::uint64::encode(2, &v, buf);
                    }
                }
            }
            Self::Fixed64(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::fixed64::encode(2, &v, buf);
                    }
                }
            }
            Self::Bool(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v {
                        prost::encoding::bool::encode(2, &v, buf);
                    }
                }
            }
            Self::String(col) => {
                if !col.is_null(idx) {
                    let v = col.value(idx);
                    if !v.is_empty() {
                        encode_key(2, WireType::LengthDelimited, buf);
                        encode_varint(v.len() as u64, buf);
                        buf.extend_from_slice(v.as_bytes());
                    }
                }
            }
            Self::Bytes(col) => {
                if !col.is_null(idx) {
                    let v = col.value(idx);
                    if !v.is_empty() {
                        encode_key(2, WireType::LengthDelimited, buf);
                        encode_varint(v.len() as u64, buf);
                        buf.extend_from_slice(v);
                    }
                }
            }
            Self::EnumInt32(arr) => {
                if !arr.is_null(idx) {
                    let v = arr.value(idx);
                    if v != 0 {
                        prost::encoding::int32::encode(2, &v, buf);
                    }
                }
            }
            Self::EnumString(col, ed) => {
                if !col.is_null(idx) {
                    let v = enum_number_from_name(col.value(idx), ed);
                    if v != 0 {
                        prost::encoding::int32::encode(2, &v, buf);
                    }
                }
            }
            Self::EnumBinary(col, ed) => {
                if !col.is_null(idx) {
                    let name = std::str::from_utf8(col.value(idx)).unwrap();
                    let v = enum_number_from_name(name, ed);
                    if v != 0 {
                        prost::encoding::int32::encode(2, &v, buf);
                    }
                }
            }
            Self::Message(struct_arr, sub_enc) => {
                if struct_arr.is_valid(idx) {
                    let mut tmp = Vec::new();
                    sub_enc.encode_row(idx, &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::Timestamp(unit, arr) => {
                if !arr.is_null(idx) {
                    let (s, n) = time_unit_to_seconds_and_nanos(arr.value_i64(idx), *unit);
                    let mut tmp = Vec::new();
                    encode_timestamp_fields(s, n, &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::Duration(unit, arr) => {
                if !arr.is_null(idx) {
                    let (s, n) = time_unit_to_duration_seconds_and_nanos(arr.value_i64(idx), *unit);
                    let mut tmp = Vec::new();
                    encode_duration_fields(s, n, &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::Date(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    encode_date_fields(arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::TimeOfDay(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    encode_time_of_day_fields(arr.to_nanos(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperDouble(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    prost::encoding::double::encode(1, &arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperFloat(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    prost::encoding::float::encode(1, &arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperInt64(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    prost::encoding::int64::encode(1, &arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperUInt64(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    prost::encoding::uint64::encode(1, &arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperInt32(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    prost::encoding::int32::encode(1, &arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperUInt32(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    prost::encoding::uint32::encode(1, &arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperBool(arr) => {
                if !arr.is_null(idx) {
                    let mut tmp = Vec::new();
                    prost::encoding::bool::encode(1, &arr.value(idx), &mut tmp);
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperString(col) => {
                if !col.is_null(idx) {
                    let v = col.value(idx);
                    let mut tmp = Vec::new();
                    encode_key(1, WireType::LengthDelimited, &mut tmp);
                    encode_varint(v.len() as u64, &mut tmp);
                    tmp.extend_from_slice(v.as_bytes());
                    write_submessage(2, &tmp, buf);
                }
            }
            Self::WrapperBytes(col) => {
                if !col.is_null(idx) {
                    let v = col.value(idx);
                    let mut tmp = Vec::new();
                    encode_key(1, WireType::LengthDelimited, &mut tmp);
                    encode_varint(v.len() as u64, &mut tmp);
                    tmp.extend_from_slice(v);
                    write_submessage(2, &tmp, buf);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FieldEncoder::encode_at — the main per-row encoding dispatch
// ---------------------------------------------------------------------------

impl FieldEncoder<'_> {
    fn encode_at(&self, idx: usize, buf: &mut Vec<u8>) {
        match self {
            // ----- scalars -----
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

            // ----- enums -----
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
                let val = enum_number_from_name(col.value(idx), enum_descriptor);
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
                let name = std::str::from_utf8(col.value(idx)).unwrap();
                let val = enum_number_from_name(name, enum_descriptor);
                if !has_presence && val == 0 {
                    return;
                }
                prost::encoding::int32::encode(*tag, &val, buf);
            }

            // ----- nested message -----
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
                write_submessage(*tag, &tmp, buf);
            }

            // ----- well-known: Timestamp -----
            Self::Timestamp { tag, unit, array } => {
                if array.is_null(idx) {
                    return;
                }
                let (s, n) = time_unit_to_seconds_and_nanos(array.value_i64(idx), *unit);
                let mut tmp = Vec::new();
                encode_timestamp_fields(s, n, &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            // ----- well-known: Duration -----
            Self::Duration { tag, unit, array } => {
                if array.is_null(idx) {
                    return;
                }
                let (s, n) = time_unit_to_duration_seconds_and_nanos(array.value_i64(idx), *unit);
                let mut tmp = Vec::new();
                encode_duration_fields(s, n, &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            // ----- well-known: Date -----
            Self::Date { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                encode_date_fields(arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            // ----- well-known: TimeOfDay -----
            Self::TimeOfDay { tag, array } => {
                if array.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                encode_time_of_day_fields(array.to_nanos(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }

            // ----- wrapper types -----
            Self::WrapperDouble { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                prost::encoding::double::encode(1, &arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperFloat { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                prost::encoding::float::encode(1, &arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperInt64 { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                prost::encoding::int64::encode(1, &arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperUInt64 { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                prost::encoding::uint64::encode(1, &arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperInt32 { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                prost::encoding::int32::encode(1, &arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperUInt32 { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                prost::encoding::uint32::encode(1, &arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperBool { tag, arr } => {
                if arr.is_null(idx) {
                    return;
                }
                let mut tmp = Vec::new();
                prost::encoding::bool::encode(1, &arr.value(idx), &mut tmp);
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperString { tag, col } => {
                if col.is_null(idx) {
                    return;
                }
                let v = col.value(idx);
                let mut tmp = Vec::new();
                encode_key(1, WireType::LengthDelimited, &mut tmp);
                encode_varint(v.len() as u64, &mut tmp);
                tmp.extend_from_slice(v.as_bytes());
                write_submessage(*tag, &tmp, buf);
            }
            Self::WrapperBytes { tag, col } => {
                if col.is_null(idx) {
                    return;
                }
                let v = col.value(idx);
                let mut tmp = Vec::new();
                encode_key(1, WireType::LengthDelimited, &mut tmp);
                encode_varint(v.len() as u64, &mut tmp);
                tmp.extend_from_slice(v);
                write_submessage(*tag, &tmp, buf);
            }

            // ----- repeated packed (int32/int64/uint32/uint64/float/double) -----
            Self::RepeatedPacked { tag, list, encoder } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                if start >= end {
                    return;
                }
                let mut packed = Vec::new();
                encoder.encode_packed_values(start, end, &mut packed);
                encode_key(*tag, WireType::LengthDelimited, buf);
                encode_varint(packed.len() as u64, buf);
                buf.extend_from_slice(&packed);
            }
            Self::RepeatedBool { tag, list, values } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                if start >= end {
                    return;
                }
                let mut packed = Vec::new();
                for i in start..end {
                    encode_varint(u64::from(values.value(i)), &mut packed);
                }
                encode_key(*tag, WireType::LengthDelimited, buf);
                encode_varint(packed.len() as u64, buf);
                buf.extend_from_slice(&packed);
            }
            Self::RepeatedString { tag, list, col } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = col.value(i);
                    encode_key(*tag, WireType::LengthDelimited, buf);
                    encode_varint(v.len() as u64, buf);
                    buf.extend_from_slice(v.as_bytes());
                }
            }
            Self::RepeatedBytes { tag, list, col } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = col.value(i);
                    encode_key(*tag, WireType::LengthDelimited, buf);
                    encode_varint(v.len() as u64, buf);
                    buf.extend_from_slice(v);
                }
            }
            Self::RepeatedEnumInt32 { tag, list, values } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                if start >= end {
                    return;
                }
                let mut packed = Vec::new();
                for i in start..end {
                    let v = if values.is_null(i) {
                        0
                    } else {
                        values.value(i)
                    };
                    encode_varint(v as u64, &mut packed);
                }
                encode_key(*tag, WireType::LengthDelimited, buf);
                encode_varint(packed.len() as u64, buf);
                buf.extend_from_slice(&packed);
            }
            Self::RepeatedEnumString {
                tag,
                list,
                col,
                enum_descriptor,
            } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                if start >= end {
                    return;
                }
                let mut packed = Vec::new();
                for i in start..end {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = enum_number_from_name(col.value(i), enum_descriptor);
                    encode_varint(v as u64, &mut packed);
                }
                if !packed.is_empty() {
                    encode_key(*tag, WireType::LengthDelimited, buf);
                    encode_varint(packed.len() as u64, buf);
                    buf.extend_from_slice(&packed);
                }
            }
            Self::RepeatedEnumBinary {
                tag,
                list,
                col,
                enum_descriptor,
            } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                if start >= end {
                    return;
                }
                let mut packed = Vec::new();
                for i in start..end {
                    if col.is_null(i) {
                        continue;
                    }
                    let name = std::str::from_utf8(col.value(i)).unwrap();
                    let v = enum_number_from_name(name, enum_descriptor);
                    encode_varint(v as u64, &mut packed);
                }
                if !packed.is_empty() {
                    encode_key(*tag, WireType::LengthDelimited, buf);
                    encode_varint(packed.len() as u64, buf);
                    buf.extend_from_slice(&packed);
                }
            }
            Self::RepeatedMessage {
                tag,
                list,
                sub_encoder,
            } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    let mut tmp = Vec::new();
                    sub_encoder.encode_row(i, &mut tmp);
                    write_submessage(*tag, &tmp, buf);
                }
            }
            Self::RepeatedTimestamp {
                tag,
                list,
                unit,
                values,
            } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if values.is_null(i) {
                        continue;
                    }
                    let (s, n) = time_unit_to_seconds_and_nanos(values.value_i64(i), *unit);
                    let mut tmp = Vec::new();
                    encode_timestamp_fields(s, n, &mut tmp);
                    write_submessage(*tag, &tmp, buf);
                }
            }
            Self::RepeatedDuration {
                tag,
                list,
                unit,
                values,
            } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if values.is_null(i) {
                        continue;
                    }
                    let (s, n) =
                        time_unit_to_duration_seconds_and_nanos(values.value_i64(i), *unit);
                    let mut tmp = Vec::new();
                    encode_duration_fields(s, n, &mut tmp);
                    write_submessage(*tag, &tmp, buf);
                }
            }
            Self::RepeatedDate { tag, list, values } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    let mut tmp = Vec::new();
                    encode_date_fields(values.value(i), &mut tmp);
                    write_submessage(*tag, &tmp, buf);
                }
            }
            Self::RepeatedTimeOfDay { tag, list, values } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if values.is_null(i) {
                        continue;
                    }
                    let mut tmp = Vec::new();
                    encode_time_of_day_fields(values.to_nanos(i), &mut tmp);
                    write_submessage(*tag, &tmp, buf);
                }
            }
            Self::RepeatedWrapperDouble { tag, list, values } => encode_repeated_wrapper_primitive(
                idx,
                *tag,
                list,
                values,
                |v, t| prost::encoding::double::encode(1, &v, t),
                buf,
            ),
            Self::RepeatedWrapperFloat { tag, list, values } => encode_repeated_wrapper_primitive(
                idx,
                *tag,
                list,
                values,
                |v, t| prost::encoding::float::encode(1, &v, t),
                buf,
            ),
            Self::RepeatedWrapperInt64 { tag, list, values } => encode_repeated_wrapper_primitive(
                idx,
                *tag,
                list,
                values,
                |v, t| prost::encoding::int64::encode(1, &v, t),
                buf,
            ),
            Self::RepeatedWrapperUInt64 { tag, list, values } => encode_repeated_wrapper_primitive(
                idx,
                *tag,
                list,
                values,
                |v, t| prost::encoding::uint64::encode(1, &v, t),
                buf,
            ),
            Self::RepeatedWrapperInt32 { tag, list, values } => encode_repeated_wrapper_primitive(
                idx,
                *tag,
                list,
                values,
                |v, t| prost::encoding::int32::encode(1, &v, t),
                buf,
            ),
            Self::RepeatedWrapperUInt32 { tag, list, values } => encode_repeated_wrapper_primitive(
                idx,
                *tag,
                list,
                values,
                |v, t| prost::encoding::uint32::encode(1, &v, t),
                buf,
            ),
            Self::RepeatedWrapperBool { tag, list, values } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if values.is_null(i) {
                        continue;
                    }
                    let mut tmp = Vec::new();
                    prost::encoding::bool::encode(1, &values.value(i), &mut tmp);
                    write_submessage(*tag, &tmp, buf);
                }
            }
            Self::RepeatedWrapperString { tag, list, col } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = col.value(i);
                    let mut tmp = Vec::new();
                    encode_key(1, WireType::LengthDelimited, &mut tmp);
                    encode_varint(v.len() as u64, &mut tmp);
                    tmp.extend_from_slice(v.as_bytes());
                    write_submessage(*tag, &tmp, buf);
                }
            }
            Self::RepeatedWrapperBytes { tag, list, col } => {
                if list.is_null(idx) {
                    return;
                }
                let (start, end) = list.value_offsets(idx);
                for i in start..end {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = col.value(i);
                    let mut tmp = Vec::new();
                    encode_key(1, WireType::LengthDelimited, &mut tmp);
                    encode_varint(v.len() as u64, &mut tmp);
                    tmp.extend_from_slice(v);
                    write_submessage(*tag, &tmp, buf);
                }
            }

            // ----- map fields -----
            Self::Map {
                tag,
                map_array,
                key_encoder,
                value_encoder,
            } => {
                if map_array.is_null(idx) {
                    return;
                }
                let start = map_array.value_offsets()[idx] as usize;
                let end = map_array.value_offsets()[idx + 1] as usize;
                for i in start..end {
                    let mut entry = Vec::new();
                    key_encoder.encode_at(i, &mut entry);
                    value_encoder.encode_at(i, &mut entry);
                    write_submessage(*tag, &entry, buf);
                }
            }
        }
    }
}

/// Helper for repeated wrapper primitives.
fn encode_repeated_wrapper_primitive<P: ArrowPrimitiveType>(
    idx: usize,
    tag: u32,
    list: &GenericListArray,
    values: &PrimitiveArray<P>,
    encode_value: impl Fn(P::Native, &mut Vec<u8>),
    buf: &mut Vec<u8>,
) {
    if list.is_null(idx) {
        return;
    }
    let (start, end) = list.value_offsets(idx);
    for i in start..end {
        if values.is_null(i) {
            continue;
        }
        let mut tmp = Vec::new();
        encode_value(values.value(i), &mut tmp);
        write_submessage(tag, &tmp, buf);
    }
}

// ---------------------------------------------------------------------------
// MessageEncoder: holds Vec<FieldEncoder>, built from descriptor + arrays
// ---------------------------------------------------------------------------

/// Encodes rows directly from Arrow arrays to protobuf wire format.
pub struct MessageEncoder<'a> {
    encoders: Vec<FieldEncoder<'a>>,
}

impl<'a> MessageEncoder<'a> {
    /// Build a MessageEncoder from a RecordBatch (top-level).
    pub fn from_record_batch(descriptor: &MessageDescriptor, batch: &'a RecordBatch) -> Self {
        let mut encoders = Vec::new();
        for field in descriptor.fields() {
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

// ---------------------------------------------------------------------------
// build_field_encoder: construct a FieldEncoder from a FieldDescriptor + array
// ---------------------------------------------------------------------------

fn build_field_encoder<'a>(
    field: &FieldDescriptor,
    array: &'a dyn Array,
) -> Option<FieldEncoder<'a>> {
    if field.is_map() {
        return build_map_encoder(field, array);
    }
    if field.is_list() {
        return build_repeated_encoder(field, array);
    }

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
            let col = make_string_col_ref(array)?;
            Some(FieldEncoder::String {
                tag,
                col,
                has_presence,
            })
        }
        Kind::Bytes => {
            let col = make_binary_col_ref(array)?;
            Some(FieldEncoder::Bytes {
                tag,
                col,
                has_presence,
            })
        }
        Kind::Enum(enum_desc) => build_enum_encoder(tag, has_presence, array, &enum_desc),
        Kind::Message(msg_desc) => build_message_encoder(tag, array, &msg_desc),
    }
}

fn make_string_col_ref(array: &dyn Array) -> Option<StringColumnRef<'_>> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(StringColumnRef::Regular)
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(StringColumnRef::Large)
        })
}

fn make_binary_col_ref(array: &dyn Array) -> Option<BinaryColumnRef<'_>> {
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .map(BinaryColumnRef::Regular)
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .map(BinaryColumnRef::Large)
        })
}

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

/// Build encoder for a singular message field (regular or well-known).
fn build_message_encoder<'a>(
    tag: u32,
    array: &'a dyn Array,
    msg_desc: &MessageDescriptor,
) -> Option<FieldEncoder<'a>> {
    match msg_desc.full_name() {
        "google.protobuf.Timestamp" => build_timestamp_encoder(tag, array),
        "google.protobuf.Duration" => build_duration_encoder(tag, array),
        "google.type.Date" => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Date32Type>>()?;
            Some(FieldEncoder::Date { tag, arr })
        }
        "google.type.TimeOfDay" => {
            let ta = build_time_of_day_array(array)?;
            Some(FieldEncoder::TimeOfDay { tag, array: ta })
        }
        "google.protobuf.DoubleValue" => Some(FieldEncoder::WrapperDouble {
            tag,
            arr: array.as_any().downcast_ref()?,
        }),
        "google.protobuf.FloatValue" => Some(FieldEncoder::WrapperFloat {
            tag,
            arr: array.as_any().downcast_ref()?,
        }),
        "google.protobuf.Int64Value" => Some(FieldEncoder::WrapperInt64 {
            tag,
            arr: array.as_any().downcast_ref()?,
        }),
        "google.protobuf.UInt64Value" => Some(FieldEncoder::WrapperUInt64 {
            tag,
            arr: array.as_any().downcast_ref()?,
        }),
        "google.protobuf.Int32Value" => Some(FieldEncoder::WrapperInt32 {
            tag,
            arr: array.as_any().downcast_ref()?,
        }),
        "google.protobuf.UInt32Value" => Some(FieldEncoder::WrapperUInt32 {
            tag,
            arr: array.as_any().downcast_ref()?,
        }),
        "google.protobuf.BoolValue" => Some(FieldEncoder::WrapperBool {
            tag,
            arr: array.as_any().downcast_ref()?,
        }),
        "google.protobuf.StringValue" => {
            let col = make_string_col_ref(array)?;
            Some(FieldEncoder::WrapperString { tag, col })
        }
        "google.protobuf.BytesValue" => {
            let col = make_binary_col_ref(array)?;
            Some(FieldEncoder::WrapperBytes { tag, col })
        }
        _ => {
            let struct_arr = array.as_any().downcast_ref::<StructArray>()?;
            let sub_encoder = MessageEncoder::from_struct_array(msg_desc, struct_arr);
            Some(FieldEncoder::Message {
                tag,
                struct_arr,
                sub_encoder,
            })
        }
    }
}

fn build_timestamp_encoder(tag: u32, array: &dyn Array) -> Option<FieldEncoder<'_>> {
    let unit = match array.data_type() {
        DataType::Timestamp(u, _) => *u,
        _ => return None,
    };
    let wk = match unit {
        TimeUnit::Second => WellKnownPrimitiveArray::Second(array.as_any().downcast_ref()?),
        TimeUnit::Millisecond => {
            WellKnownPrimitiveArray::Millisecond(array.as_any().downcast_ref()?)
        }
        TimeUnit::Microsecond => {
            WellKnownPrimitiveArray::Microsecond(array.as_any().downcast_ref()?)
        }
        TimeUnit::Nanosecond => WellKnownPrimitiveArray::Nanosecond(array.as_any().downcast_ref()?),
    };
    Some(FieldEncoder::Timestamp {
        tag,
        unit,
        array: wk,
    })
}

fn build_duration_encoder(tag: u32, array: &dyn Array) -> Option<FieldEncoder<'_>> {
    let unit = match array.data_type() {
        DataType::Duration(u) => *u,
        _ => return None,
    };
    let wk = match unit {
        TimeUnit::Second => WellKnownPrimitiveArray::DurSecond(array.as_any().downcast_ref()?),
        TimeUnit::Millisecond => {
            WellKnownPrimitiveArray::DurMillisecond(array.as_any().downcast_ref()?)
        }
        TimeUnit::Microsecond => {
            WellKnownPrimitiveArray::DurMicrosecond(array.as_any().downcast_ref()?)
        }
        TimeUnit::Nanosecond => {
            WellKnownPrimitiveArray::DurNanosecond(array.as_any().downcast_ref()?)
        }
    };
    Some(FieldEncoder::Duration {
        tag,
        unit,
        array: wk,
    })
}

fn build_time_of_day_array(array: &dyn Array) -> Option<TimeOfDayArray<'_>> {
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            Some(TimeOfDayArray::Time32Second(array.as_any().downcast_ref()?))
        }
        DataType::Time32(TimeUnit::Millisecond) => Some(TimeOfDayArray::Time32Millisecond(
            array.as_any().downcast_ref()?,
        )),
        DataType::Time64(TimeUnit::Microsecond) => Some(TimeOfDayArray::Time64Microsecond(
            array.as_any().downcast_ref()?,
        )),
        DataType::Time64(TimeUnit::Nanosecond) => Some(TimeOfDayArray::Time64Nanosecond(
            array.as_any().downcast_ref()?,
        )),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Repeated field encoder construction
// ---------------------------------------------------------------------------

fn build_repeated_encoder<'a>(
    field: &FieldDescriptor,
    array: &'a dyn Array,
) -> Option<FieldEncoder<'a>> {
    let tag = field.number();
    let list = GenericListArray::from_array(array)?;
    let values: &'a dyn Array = match &list {
        GenericListArray::Regular(a) => a.values().as_ref(),
        GenericListArray::Large(a) => a.values().as_ref(),
    };

    match field.kind() {
        Kind::Int32 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Int32(arr),
            })
        }
        Kind::Sint32 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Sint32(arr),
            })
        }
        Kind::Sfixed32 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Sfixed32(arr),
            })
        }
        Kind::Int64 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Int64(arr),
            })
        }
        Kind::Sint64 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Sint64(arr),
            })
        }
        Kind::Sfixed64 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Sfixed64(arr),
            })
        }
        Kind::Uint32 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::UInt32(arr),
            })
        }
        Kind::Fixed32 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Fixed32(arr),
            })
        }
        Kind::Uint64 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::UInt64(arr),
            })
        }
        Kind::Fixed64 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Fixed64(arr),
            })
        }
        Kind::Float => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Float32(arr),
            })
        }
        Kind::Double => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()?;
            Some(FieldEncoder::RepeatedPacked {
                tag,
                list,
                encoder: PackedEncoder::Float64(arr),
            })
        }
        Kind::Bool => {
            let arr = values.as_any().downcast_ref::<BooleanArray>()?;
            Some(FieldEncoder::RepeatedBool {
                tag,
                list,
                values: arr,
            })
        }
        Kind::String => {
            let col = make_string_col_ref(values)?;
            Some(FieldEncoder::RepeatedString { tag, list, col })
        }
        Kind::Bytes => {
            let col = make_binary_col_ref(values)?;
            Some(FieldEncoder::RepeatedBytes { tag, list, col })
        }
        Kind::Enum(enum_desc) => build_repeated_enum_encoder(tag, list, values, &enum_desc),
        Kind::Message(msg_desc) => build_repeated_message_encoder(tag, list, values, &msg_desc),
    }
}

fn build_repeated_enum_encoder<'a>(
    tag: u32,
    list: GenericListArray<'a>,
    values: &'a dyn Array,
    enum_desc: &EnumDescriptor,
) -> Option<FieldEncoder<'a>> {
    match values.data_type() {
        DataType::Int32 => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()?;
            Some(FieldEncoder::RepeatedEnumInt32 {
                tag,
                list,
                values: arr,
            })
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let col = make_string_col_ref(values)?;
            Some(FieldEncoder::RepeatedEnumString {
                tag,
                list,
                col,
                enum_descriptor: enum_desc.clone(),
            })
        }
        DataType::Binary | DataType::LargeBinary => {
            let col = make_binary_col_ref(values)?;
            Some(FieldEncoder::RepeatedEnumBinary {
                tag,
                list,
                col,
                enum_descriptor: enum_desc.clone(),
            })
        }
        _ => None,
    }
}

fn build_repeated_message_encoder<'a>(
    tag: u32,
    list: GenericListArray<'a>,
    values: &'a dyn Array,
    msg_desc: &MessageDescriptor,
) -> Option<FieldEncoder<'a>> {
    match msg_desc.full_name() {
        "google.protobuf.Timestamp" => {
            let unit = match values.data_type() {
                DataType::Timestamp(u, _) => *u,
                _ => return None,
            };
            let wk = match unit {
                TimeUnit::Second => {
                    WellKnownPrimitiveArray::Second(values.as_any().downcast_ref()?)
                }
                TimeUnit::Millisecond => {
                    WellKnownPrimitiveArray::Millisecond(values.as_any().downcast_ref()?)
                }
                TimeUnit::Microsecond => {
                    WellKnownPrimitiveArray::Microsecond(values.as_any().downcast_ref()?)
                }
                TimeUnit::Nanosecond => {
                    WellKnownPrimitiveArray::Nanosecond(values.as_any().downcast_ref()?)
                }
            };
            Some(FieldEncoder::RepeatedTimestamp {
                tag,
                list,
                unit,
                values: wk,
            })
        }
        "google.protobuf.Duration" => {
            let unit = match values.data_type() {
                DataType::Duration(u) => *u,
                _ => return None,
            };
            let wk = match unit {
                TimeUnit::Second => {
                    WellKnownPrimitiveArray::DurSecond(values.as_any().downcast_ref()?)
                }
                TimeUnit::Millisecond => {
                    WellKnownPrimitiveArray::DurMillisecond(values.as_any().downcast_ref()?)
                }
                TimeUnit::Microsecond => {
                    WellKnownPrimitiveArray::DurMicrosecond(values.as_any().downcast_ref()?)
                }
                TimeUnit::Nanosecond => {
                    WellKnownPrimitiveArray::DurNanosecond(values.as_any().downcast_ref()?)
                }
            };
            Some(FieldEncoder::RepeatedDuration {
                tag,
                list,
                unit,
                values: wk,
            })
        }
        "google.type.Date" => {
            let arr = values
                .as_any()
                .downcast_ref::<PrimitiveArray<Date32Type>>()?;
            Some(FieldEncoder::RepeatedDate {
                tag,
                list,
                values: arr,
            })
        }
        "google.type.TimeOfDay" => {
            let ta = build_time_of_day_array(values)?;
            Some(FieldEncoder::RepeatedTimeOfDay {
                tag,
                list,
                values: ta,
            })
        }
        "google.protobuf.DoubleValue" => Some(FieldEncoder::RepeatedWrapperDouble {
            tag,
            list,
            values: values.as_any().downcast_ref()?,
        }),
        "google.protobuf.FloatValue" => Some(FieldEncoder::RepeatedWrapperFloat {
            tag,
            list,
            values: values.as_any().downcast_ref()?,
        }),
        "google.protobuf.Int64Value" => Some(FieldEncoder::RepeatedWrapperInt64 {
            tag,
            list,
            values: values.as_any().downcast_ref()?,
        }),
        "google.protobuf.UInt64Value" => Some(FieldEncoder::RepeatedWrapperUInt64 {
            tag,
            list,
            values: values.as_any().downcast_ref()?,
        }),
        "google.protobuf.Int32Value" => Some(FieldEncoder::RepeatedWrapperInt32 {
            tag,
            list,
            values: values.as_any().downcast_ref()?,
        }),
        "google.protobuf.UInt32Value" => Some(FieldEncoder::RepeatedWrapperUInt32 {
            tag,
            list,
            values: values.as_any().downcast_ref()?,
        }),
        "google.protobuf.BoolValue" => Some(FieldEncoder::RepeatedWrapperBool {
            tag,
            list,
            values: values.as_any().downcast_ref()?,
        }),
        "google.protobuf.StringValue" => {
            let col = make_string_col_ref(values)?;
            Some(FieldEncoder::RepeatedWrapperString { tag, list, col })
        }
        "google.protobuf.BytesValue" => {
            let col = make_binary_col_ref(values)?;
            Some(FieldEncoder::RepeatedWrapperBytes { tag, list, col })
        }
        _ => {
            let struct_arr = values.as_any().downcast_ref::<StructArray>()?;
            let sub_encoder = MessageEncoder::from_struct_array(msg_desc, struct_arr);
            Some(FieldEncoder::RepeatedMessage {
                tag,
                list,
                sub_encoder,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Map field encoder construction
// ---------------------------------------------------------------------------

fn build_map_encoder<'a>(
    field: &FieldDescriptor,
    array: &'a dyn Array,
) -> Option<FieldEncoder<'a>> {
    let tag = field.number();
    let map_array = array.as_any().downcast_ref::<MapArray>()?;

    let map_entry_descriptor = match field.kind() {
        Kind::Message(desc) => desc,
        _ => return None,
    };
    let key_field = map_entry_descriptor.get_field_by_name("key")?;
    let value_field = map_entry_descriptor.get_field_by_name("value")?;

    let entries = map_array.entries();
    let key_array = entries.column_by_name("key")?;
    // Try "value" first, fall back to second struct field for custom map_value_name
    let value_array = entries
        .column_by_name("value")
        .or_else(|| entries.columns().get(1))?;

    let key_encoder = build_map_key_encoder(key_array.as_ref(), &key_field)?;
    let value_encoder = build_map_value_encoder(value_array.as_ref(), &value_field)?;

    Some(FieldEncoder::Map {
        tag,
        map_array,
        key_encoder,
        value_encoder,
    })
}

fn build_map_key_encoder<'a>(
    array: &'a dyn Array,
    field: &FieldDescriptor,
) -> Option<MapKeyEncoder<'a>> {
    match field.kind() {
        Kind::String => Some(MapKeyEncoder::String(make_string_col_ref(array)?)),
        Kind::Int32 => Some(MapKeyEncoder::Int32(array.as_any().downcast_ref()?)),
        Kind::Sint32 => Some(MapKeyEncoder::Sint32(array.as_any().downcast_ref()?)),
        Kind::Sfixed32 => Some(MapKeyEncoder::Sfixed32(array.as_any().downcast_ref()?)),
        Kind::Int64 => Some(MapKeyEncoder::Int64(array.as_any().downcast_ref()?)),
        Kind::Sint64 => Some(MapKeyEncoder::Sint64(array.as_any().downcast_ref()?)),
        Kind::Sfixed64 => Some(MapKeyEncoder::Sfixed64(array.as_any().downcast_ref()?)),
        Kind::Uint32 => Some(MapKeyEncoder::UInt32(array.as_any().downcast_ref()?)),
        Kind::Fixed32 => Some(MapKeyEncoder::Fixed32(array.as_any().downcast_ref()?)),
        Kind::Uint64 => Some(MapKeyEncoder::UInt64(array.as_any().downcast_ref()?)),
        Kind::Fixed64 => Some(MapKeyEncoder::Fixed64(array.as_any().downcast_ref()?)),
        Kind::Bool => Some(MapKeyEncoder::Bool(array.as_any().downcast_ref()?)),
        _ => None,
    }
}

fn build_map_value_encoder<'a>(
    array: &'a dyn Array,
    field: &FieldDescriptor,
) -> Option<MapValueEncoder<'a>> {
    match field.kind() {
        Kind::Double => Some(MapValueEncoder::Double(array.as_any().downcast_ref()?)),
        Kind::Float => Some(MapValueEncoder::Float(array.as_any().downcast_ref()?)),
        Kind::Int32 => Some(MapValueEncoder::Int32(array.as_any().downcast_ref()?)),
        Kind::Sint32 => Some(MapValueEncoder::Sint32(array.as_any().downcast_ref()?)),
        Kind::Sfixed32 => Some(MapValueEncoder::Sfixed32(array.as_any().downcast_ref()?)),
        Kind::Int64 => Some(MapValueEncoder::Int64(array.as_any().downcast_ref()?)),
        Kind::Sint64 => Some(MapValueEncoder::Sint64(array.as_any().downcast_ref()?)),
        Kind::Sfixed64 => Some(MapValueEncoder::Sfixed64(array.as_any().downcast_ref()?)),
        Kind::Uint32 => Some(MapValueEncoder::UInt32(array.as_any().downcast_ref()?)),
        Kind::Fixed32 => Some(MapValueEncoder::Fixed32(array.as_any().downcast_ref()?)),
        Kind::Uint64 => Some(MapValueEncoder::UInt64(array.as_any().downcast_ref()?)),
        Kind::Fixed64 => Some(MapValueEncoder::Fixed64(array.as_any().downcast_ref()?)),
        Kind::Bool => Some(MapValueEncoder::Bool(array.as_any().downcast_ref()?)),
        Kind::String => Some(MapValueEncoder::String(make_string_col_ref(array)?)),
        Kind::Bytes => Some(MapValueEncoder::Bytes(make_binary_col_ref(array)?)),
        Kind::Enum(enum_desc) => match array.data_type() {
            DataType::Int32 => Some(MapValueEncoder::EnumInt32(array.as_any().downcast_ref()?)),
            DataType::Utf8 | DataType::LargeUtf8 => Some(MapValueEncoder::EnumString(
                make_string_col_ref(array)?,
                enum_desc.clone(),
            )),
            DataType::Binary | DataType::LargeBinary => Some(MapValueEncoder::EnumBinary(
                make_binary_col_ref(array)?,
                enum_desc.clone(),
            )),
            _ => None,
        },
        Kind::Message(msg_desc) => build_map_value_message_encoder(array, &msg_desc),
    }
}

fn build_map_value_message_encoder<'a>(
    array: &'a dyn Array,
    msg_desc: &MessageDescriptor,
) -> Option<MapValueEncoder<'a>> {
    match msg_desc.full_name() {
        "google.protobuf.Timestamp" => {
            let unit = match array.data_type() {
                DataType::Timestamp(u, _) => *u,
                _ => return None,
            };
            let wk = match unit {
                TimeUnit::Second => WellKnownPrimitiveArray::Second(array.as_any().downcast_ref()?),
                TimeUnit::Millisecond => {
                    WellKnownPrimitiveArray::Millisecond(array.as_any().downcast_ref()?)
                }
                TimeUnit::Microsecond => {
                    WellKnownPrimitiveArray::Microsecond(array.as_any().downcast_ref()?)
                }
                TimeUnit::Nanosecond => {
                    WellKnownPrimitiveArray::Nanosecond(array.as_any().downcast_ref()?)
                }
            };
            Some(MapValueEncoder::Timestamp(unit, wk))
        }
        "google.protobuf.Duration" => {
            let unit = match array.data_type() {
                DataType::Duration(u) => *u,
                _ => return None,
            };
            let wk = match unit {
                TimeUnit::Second => {
                    WellKnownPrimitiveArray::DurSecond(array.as_any().downcast_ref()?)
                }
                TimeUnit::Millisecond => {
                    WellKnownPrimitiveArray::DurMillisecond(array.as_any().downcast_ref()?)
                }
                TimeUnit::Microsecond => {
                    WellKnownPrimitiveArray::DurMicrosecond(array.as_any().downcast_ref()?)
                }
                TimeUnit::Nanosecond => {
                    WellKnownPrimitiveArray::DurNanosecond(array.as_any().downcast_ref()?)
                }
            };
            Some(MapValueEncoder::Duration(unit, wk))
        }
        "google.type.Date" => Some(MapValueEncoder::Date(array.as_any().downcast_ref()?)),
        "google.type.TimeOfDay" => {
            let ta = build_time_of_day_array(array)?;
            Some(MapValueEncoder::TimeOfDay(ta))
        }
        "google.protobuf.DoubleValue" => Some(MapValueEncoder::WrapperDouble(
            array.as_any().downcast_ref()?,
        )),
        "google.protobuf.FloatValue" => Some(MapValueEncoder::WrapperFloat(
            array.as_any().downcast_ref()?,
        )),
        "google.protobuf.Int64Value" => Some(MapValueEncoder::WrapperInt64(
            array.as_any().downcast_ref()?,
        )),
        "google.protobuf.UInt64Value" => Some(MapValueEncoder::WrapperUInt64(
            array.as_any().downcast_ref()?,
        )),
        "google.protobuf.Int32Value" => Some(MapValueEncoder::WrapperInt32(
            array.as_any().downcast_ref()?,
        )),
        "google.protobuf.UInt32Value" => Some(MapValueEncoder::WrapperUInt32(
            array.as_any().downcast_ref()?,
        )),
        "google.protobuf.BoolValue" => {
            Some(MapValueEncoder::WrapperBool(array.as_any().downcast_ref()?))
        }
        "google.protobuf.StringValue" => {
            Some(MapValueEncoder::WrapperString(make_string_col_ref(array)?))
        }
        "google.protobuf.BytesValue" => {
            Some(MapValueEncoder::WrapperBytes(make_binary_col_ref(array)?))
        }
        _ => {
            let struct_arr = array.as_any().downcast_ref::<StructArray>()?;
            let sub_encoder = MessageEncoder::from_struct_array(msg_desc, struct_arr);
            Some(MapValueEncoder::Message(struct_arr, sub_encoder))
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Encode a RecordBatch directly to protobuf wire format bytes.
///
/// Returns a BinaryArray (as ArrayData) where each element is the protobuf
/// encoding of the corresponding row. This skips DynamicMessage allocation
/// and writes wire format directly from Arrow arrays.
pub fn record_batch_to_array(batch: &RecordBatch, descriptor: &MessageDescriptor) -> ArrayData {
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
