use arrow::array::ArrayData;
use arrow_array::builder::BinaryBuilder;
use arrow_array::types::{
    Date32Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Float32Type, Float64Type, Int32Type, Int64Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, FixedSizeListArray,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, PrimitiveArray,
    RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, TimeUnit};
use chrono::{Datelike, NaiveDate};
use prost::Message;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value};
use std::collections::HashMap;

// Days from CE epoch to Unix epoch (1970-01-01)
const CE_OFFSET: i32 = 719163;

/// Helper enum to work with ListArray, LargeListArray, and FixedSizeListArray
enum GenericListArray<'a> {
    Regular(&'a ListArray),
    Large(&'a LargeListArray),
    FixedSize(&'a FixedSizeListArray),
}

impl<'a> GenericListArray<'a> {
    fn from_array(array: &'a ArrayRef) -> Option<Self> {
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
            .or_else(|| {
                array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .map(GenericListArray::FixedSize)
            })
    }

    fn values(&self) -> ArrayRef {
        match self {
            GenericListArray::Regular(a) => a.values().clone(),
            GenericListArray::Large(a) => a.values().clone(),
            GenericListArray::FixedSize(a) => a.values().clone(),
        }
    }

    fn is_null(&self, i: usize) -> bool {
        match self {
            GenericListArray::Regular(a) => a.is_null(i),
            GenericListArray::Large(a) => a.is_null(i),
            GenericListArray::FixedSize(a) => a.is_null(i),
        }
    }

    fn value_offsets(&self, i: usize) -> (usize, usize) {
        match self {
            GenericListArray::Regular(a) => {
                let offsets = a.value_offsets();
                (offsets[i] as usize, offsets[i + 1] as usize)
            }
            GenericListArray::Large(a) => {
                let offsets = a.value_offsets();
                (offsets[i] as usize, offsets[i + 1] as usize)
            }
            GenericListArray::FixedSize(a) => {
                let size = a.value_length() as usize;
                (i * size, (i + 1) * size)
            }
        }
    }
}

/// Convert total nanoseconds to (seconds, nanos) tuple for Timestamp.
/// Handles negative values correctly by ensuring nanos is always in [0, 999999999].
/// This is correct for google.protobuf.Timestamp where nanos must be non-negative.
fn nanos_to_seconds_and_nanos(nanos_total: i64) -> (i64, i32) {
    let mut seconds = nanos_total / 1_000_000_000;
    let mut nanos = (nanos_total % 1_000_000_000) as i32;
    // Ensure nanos is non-negative (protobuf Timestamp requirement)
    if nanos < 0 {
        seconds -= 1;
        nanos += 1_000_000_000;
    }
    (seconds, nanos)
}

/// Convert total nanoseconds to (seconds, nanos) tuple for Duration.
/// For google.protobuf.Duration, seconds and nanos must have the same sign (or one is zero).
/// This differs from Timestamp where nanos is always non-negative.
fn nanos_to_duration_seconds_and_nanos(nanos_total: i64) -> (i64, i32) {
    let seconds = nanos_total / 1_000_000_000;
    let nanos = (nanos_total % 1_000_000_000) as i32;
    // For Duration, nanos sign must match seconds sign (Rust's % preserves sign)
    (seconds, nanos)
}

/// Convert a value from the given time unit directly to (seconds, nanos) tuple for Timestamp.
/// This avoids overflow that would occur when converting large values to nanoseconds.
/// Handles negative values correctly by ensuring nanos is always in [0, 999999999].
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
        TimeUnit::Nanosecond => nanos_to_seconds_and_nanos(value),
    }
}

/// Convert a value from the given time unit directly to (seconds, nanos) tuple for Duration.
/// For google.protobuf.Duration, seconds and nanos must have the same sign (or one is zero).
/// This differs from time_unit_to_seconds_and_nanos which normalizes for Timestamp.
fn time_unit_to_duration_seconds_and_nanos(value: i64, unit: TimeUnit) -> (i64, i32) {
    match unit {
        TimeUnit::Second => (value, 0),
        TimeUnit::Millisecond => {
            let seconds = value / 1_000;
            let nanos = ((value % 1_000) * 1_000_000) as i32;
            // Rust's % preserves sign, so nanos will have same sign as value
            (seconds, nanos)
        }
        TimeUnit::Microsecond => {
            let seconds = value / 1_000_000;
            let nanos = ((value % 1_000_000) * 1_000) as i32;
            (seconds, nanos)
        }
        TimeUnit::Nanosecond => nanos_to_duration_seconds_and_nanos(value),
    }
}

/// Convert a 32-bit time value from the given time unit to nanoseconds.
/// Safe from overflow since i32 * 1e9 fits in i64.
fn time32_unit_to_nanos(value: i32, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => i64::from(value) * 1_000_000_000,
        TimeUnit::Millisecond => i64::from(value) * 1_000_000,
        _ => panic!("Time32 only supports Second and Millisecond units"),
    }
}

/// Convert a 64-bit time value from the given time unit to nanoseconds.
/// Only safe for Time64 which uses Microsecond or Nanosecond units.
fn time64_unit_to_nanos(value: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Microsecond => value * 1_000,
        TimeUnit::Nanosecond => value,
        _ => panic!("Time64 only supports Microsecond and Nanosecond units"),
    }
}

/// Cached field descriptors for google.protobuf.Timestamp
struct TimestampFields {
    seconds: FieldDescriptor,
    nanos: FieldDescriptor,
}

impl TimestampFields {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            seconds: message_descriptor.get_field_by_name("seconds").unwrap(),
            nanos: message_descriptor.get_field_by_name("nanos").unwrap(),
        }
    }
}

/// Create a google.protobuf.Timestamp DynamicMessage from seconds and nanoseconds.
fn create_timestamp_message(
    seconds: i64,
    nanos: i32,
    message_descriptor: &MessageDescriptor,
    fields: &TimestampFields,
) -> DynamicMessage {
    let mut msg = DynamicMessage::new(message_descriptor.clone());
    msg.set_field(&fields.seconds, Value::I64(seconds));
    msg.set_field(&fields.nanos, Value::I32(nanos));
    msg
}

/// Cached field descriptors for google.protobuf.Duration
struct DurationFields {
    seconds: FieldDescriptor,
    nanos: FieldDescriptor,
}

impl DurationFields {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            seconds: message_descriptor.get_field_by_name("seconds").unwrap(),
            nanos: message_descriptor.get_field_by_name("nanos").unwrap(),
        }
    }
}

/// Create a google.protobuf.Duration DynamicMessage from seconds and nanoseconds.
fn create_duration_message(
    seconds: i64,
    nanos: i32,
    message_descriptor: &MessageDescriptor,
    fields: &DurationFields,
) -> DynamicMessage {
    let mut msg = DynamicMessage::new(message_descriptor.clone());
    msg.set_field(&fields.seconds, Value::I64(seconds));
    msg.set_field(&fields.nanos, Value::I32(nanos));
    msg
}

/// Cached field descriptors for google.type.Date
struct DateFields {
    year: FieldDescriptor,
    month: FieldDescriptor,
    day: FieldDescriptor,
}

impl DateFields {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            year: message_descriptor.get_field_by_name("year").unwrap(),
            month: message_descriptor.get_field_by_name("month").unwrap(),
            day: message_descriptor.get_field_by_name("day").unwrap(),
        }
    }
}

/// Create a google.type.Date DynamicMessage from days since Unix epoch.
/// Special case: days == 0 represents an empty/unset date (year=0, month=0, day=0).
fn create_date_message(
    days: i32,
    message_descriptor: &MessageDescriptor,
    fields: &DateFields,
) -> DynamicMessage {
    let mut msg = DynamicMessage::new(message_descriptor.clone());
    if days == 0 {
        msg.set_field(&fields.year, Value::I32(0));
        msg.set_field(&fields.month, Value::I32(0));
        msg.set_field(&fields.day, Value::I32(0));
    } else {
        let date = NaiveDate::from_num_days_from_ce_opt(days + CE_OFFSET).unwrap();
        msg.set_field(&fields.year, Value::I32(date.year()));
        msg.set_field(&fields.month, Value::I32(date.month() as i32));
        msg.set_field(&fields.day, Value::I32(date.day() as i32));
    }
    msg
}

/// Cached field descriptors for google.type.TimeOfDay
struct TimeOfDayFields {
    hours: FieldDescriptor,
    minutes: FieldDescriptor,
    seconds: FieldDescriptor,
    nanos: FieldDescriptor,
}

impl TimeOfDayFields {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            hours: message_descriptor.get_field_by_name("hours").unwrap(),
            minutes: message_descriptor.get_field_by_name("minutes").unwrap(),
            seconds: message_descriptor.get_field_by_name("seconds").unwrap(),
            nanos: message_descriptor.get_field_by_name("nanos").unwrap(),
        }
    }
}

/// Create a google.type.TimeOfDay DynamicMessage from total nanoseconds since midnight.
fn create_time_of_day_message(
    total_nanos: i64,
    message_descriptor: &MessageDescriptor,
    fields: &TimeOfDayFields,
) -> DynamicMessage {
    let mut msg = DynamicMessage::new(message_descriptor.clone());

    let hours = (total_nanos / 3_600_000_000_000) as i32;
    let remaining = total_nanos % 3_600_000_000_000;
    let minutes = (remaining / 60_000_000_000) as i32;
    let remaining = remaining % 60_000_000_000;
    let seconds = (remaining / 1_000_000_000) as i32;
    let nanos = (remaining % 1_000_000_000) as i32;

    msg.set_field(&fields.hours, Value::I32(hours));
    msg.set_field(&fields.minutes, Value::I32(minutes));
    msg.set_field(&fields.seconds, Value::I32(seconds));
    msg.set_field(&fields.nanos, Value::I32(nanos));
    msg
}

pub fn extract_single_primitive<P: ArrowPrimitiveType>(
    array: &ArrayRef,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    value_creator: &dyn Fn(P::Native) -> Value,
) {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<P>>()
        .unwrap()
        .iter()
        .enumerate()
        .for_each(|(index, value)| match value {
            None => {}
            Some(x) => {
                let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                element.set_field(field_descriptor, value_creator(x));
            }
        })
}

fn extract_repeated_primitive_type<P>(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    value_creator: &dyn Fn(P::Native) -> Value,
) where
    P: ArrowPrimitiveType,
{
    let values_array = list_array.values();
    let values: &PrimitiveArray<P> = values_array
        .as_any()
        .downcast_ref::<PrimitiveArray<P>>()
        .unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let (start, end) = list_array.value_offsets(i);
            if start < end {
                let slice = values.slice(start, end - start);
                let values = slice
                    .iter()
                    .map(|value| match value {
                        None => value_creator(P::default_value()),
                        Some(x) => value_creator(x),
                    })
                    .collect();
                message.set_field(field_descriptor, Value::List(values));
            }
        }
    }
}

fn extract_repeated_boolean(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
) {
    let values_array = list_array.values();
    let values: &BooleanArray = values_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let (start, end) = list_array.value_offsets(i);
            if start < end {
                let each_values = (start..end)
                    .map(|x| values.value(x))
                    .map(Value::Bool)
                    .collect();

                message.set_field(field_descriptor, Value::List(each_values));
            }
        }
    }
}

fn extract_repeated_message(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: MessageDescriptor,
) {
    // Handle special message types
    match message_descriptor.full_name() {
        "google.protobuf.Timestamp" => {
            extract_repeated_timestamp(list_array, messages, field_descriptor, &message_descriptor);
            return;
        }
        "google.protobuf.Duration" => {
            extract_repeated_duration(list_array, messages, field_descriptor, &message_descriptor);
            return;
        }
        "google.type.Date" => {
            extract_repeated_date(list_array, messages, field_descriptor, &message_descriptor);
            return;
        }
        "google.type.TimeOfDay" => {
            extract_repeated_time_of_day(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
            );
            return;
        }
        "google.protobuf.DoubleValue" => {
            extract_repeated_wrapper_primitive::<Float64Type>(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
                &Value::F64,
            );
            return;
        }
        "google.protobuf.FloatValue" => {
            extract_repeated_wrapper_primitive::<Float32Type>(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
                &Value::F32,
            );
            return;
        }
        "google.protobuf.Int64Value" => {
            extract_repeated_wrapper_primitive::<Int64Type>(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
                &Value::I64,
            );
            return;
        }
        "google.protobuf.UInt64Value" => {
            extract_repeated_wrapper_primitive::<UInt64Type>(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
                &Value::U64,
            );
            return;
        }
        "google.protobuf.Int32Value" => {
            extract_repeated_wrapper_primitive::<Int32Type>(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
                &Value::I32,
            );
            return;
        }
        "google.protobuf.UInt32Value" => {
            extract_repeated_wrapper_primitive::<UInt32Type>(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
                &Value::U32,
            );
            return;
        }
        "google.protobuf.BoolValue" => {
            extract_repeated_wrapper_bool(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
            );
            return;
        }
        "google.protobuf.StringValue" => {
            extract_repeated_wrapper_string(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
            );
            return;
        }
        "google.protobuf.BytesValue" => {
            extract_repeated_wrapper_bytes(
                list_array,
                messages,
                field_descriptor,
                &message_descriptor,
            );
            return;
        }
        _ => {}
    }

    let values_array = list_array.values();
    let struct_array = values_array.as_any().downcast_ref::<StructArray>().unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let (start, end) = list_array.value_offsets(i);

            if start < end {
                // Create sub-messages for each list element
                let mut sub_messages: Vec<DynamicMessage> = (start..end)
                    .map(|_| DynamicMessage::new(message_descriptor.clone()))
                    .collect();

                // Extract fields into sub-messages
                let mut sub_refs: Vec<&mut DynamicMessage> = sub_messages.iter_mut().collect();

                for sub_field in message_descriptor.fields() {
                    if let Some(column) = struct_array.column_by_name(sub_field.name()) {
                        // Slice the column to match the range for this list
                        let sliced = column.slice(start, end - start);
                        extract_array(&sliced, &sub_field, &mut sub_refs);
                    }
                }

                // Set the repeated field as Value::List of Value::Message
                let values: Vec<Value> = sub_messages.into_iter().map(Value::Message).collect();
                message.set_field(field_descriptor, Value::List(values));
            }
        }
    }
}

fn extract_repeated_timestamp(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let fields = TimestampFields::new(message_descriptor);
    let values_array = list_array.values();

    // Determine the time unit from the values array's data type
    let time_unit = match values_array.data_type() {
        DataType::Timestamp(unit, _) => *unit,
        _ => panic!(
            "Expected Timestamp array, got {:?}",
            values_array.data_type()
        ),
    };

    // Helper macro to avoid code duplication
    macro_rules! extract_timestamps {
        ($array_type:ty) => {{
            let values = values_array
                .as_any()
                .downcast_ref::<PrimitiveArray<$array_type>>()
                .expect(concat!("Failed to downcast to ", stringify!($array_type)));

            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let (start, end) = list_array.value_offsets(i);

                    if start < end {
                        // Filter out null child values - protobuf repeated fields cannot contain nulls
                        let sub_messages: Vec<Value> = (start..end)
                            .filter(|&idx| !values.is_null(idx))
                            .map(|idx| {
                                let (seconds, nanos) =
                                    time_unit_to_seconds_and_nanos(values.value(idx), time_unit);
                                Value::Message(create_timestamp_message(
                                    seconds,
                                    nanos,
                                    message_descriptor,
                                    &fields,
                                ))
                            })
                            .collect();

                        if !sub_messages.is_empty() {
                            message.set_field(field_descriptor, Value::List(sub_messages));
                        }
                    }
                }
            }
        }};
    }

    match time_unit {
        TimeUnit::Second => extract_timestamps!(TimestampSecondType),
        TimeUnit::Millisecond => extract_timestamps!(TimestampMillisecondType),
        TimeUnit::Microsecond => extract_timestamps!(TimestampMicrosecondType),
        TimeUnit::Nanosecond => extract_timestamps!(TimestampNanosecondType),
    }
}

fn extract_repeated_duration(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let fields = DurationFields::new(message_descriptor);
    let values_array = list_array.values();

    // Determine the time unit from the values array's data type
    let time_unit = match values_array.data_type() {
        DataType::Duration(unit) => *unit,
        _ => panic!(
            "Expected Duration array, got {:?}",
            values_array.data_type()
        ),
    };

    // Helper macro to avoid code duplication
    macro_rules! extract_durations {
        ($array_type:ty) => {{
            let values = values_array
                .as_any()
                .downcast_ref::<PrimitiveArray<$array_type>>()
                .expect(concat!("Failed to downcast to ", stringify!($array_type)));

            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let (start, end) = list_array.value_offsets(i);

                    if start < end {
                        // Filter out null child values - protobuf repeated fields cannot contain nulls
                        let sub_messages: Vec<Value> = (start..end)
                            .filter(|&idx| !values.is_null(idx))
                            .map(|idx| {
                                let (seconds, nanos) = time_unit_to_duration_seconds_and_nanos(
                                    values.value(idx),
                                    time_unit,
                                );
                                Value::Message(create_duration_message(
                                    seconds,
                                    nanos,
                                    message_descriptor,
                                    &fields,
                                ))
                            })
                            .collect();

                        if !sub_messages.is_empty() {
                            message.set_field(field_descriptor, Value::List(sub_messages));
                        }
                    }
                }
            }
        }};
    }

    match time_unit {
        TimeUnit::Second => extract_durations!(DurationSecondType),
        TimeUnit::Millisecond => extract_durations!(DurationMillisecondType),
        TimeUnit::Microsecond => extract_durations!(DurationMicrosecondType),
        TimeUnit::Nanosecond => extract_durations!(DurationNanosecondType),
    }
}

fn extract_repeated_date(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values_array = list_array.values();
    let values: &PrimitiveArray<Date32Type> = values_array
        .as_any()
        .downcast_ref::<PrimitiveArray<Date32Type>>()
        .unwrap();
    let fields = DateFields::new(message_descriptor);

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let (start, end) = list_array.value_offsets(i);

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        Value::Message(create_date_message(
                            values.value(idx),
                            message_descriptor,
                            &fields,
                        ))
                    })
                    .collect();

                message.set_field(field_descriptor, Value::List(sub_messages));
            }
        }
    }
}

fn extract_repeated_time_of_day(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let fields = TimeOfDayFields::new(message_descriptor);
    let values_array = list_array.values();

    // Helper macro for Time64 types
    macro_rules! extract_time64 {
        ($array_type:ty, $time_unit:expr) => {{
            let values = values_array
                .as_any()
                .downcast_ref::<PrimitiveArray<$array_type>>()
                .expect(concat!("Failed to downcast to ", stringify!($array_type)));

            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let (start, end) = list_array.value_offsets(i);

                    if start < end {
                        // Filter out null child values - protobuf repeated fields cannot contain nulls
                        let sub_messages: Vec<Value> = (start..end)
                            .filter(|&idx| !values.is_null(idx))
                            .map(|idx| {
                                let nanos = time64_unit_to_nanos(values.value(idx), $time_unit);
                                Value::Message(create_time_of_day_message(
                                    nanos,
                                    message_descriptor,
                                    &fields,
                                ))
                            })
                            .collect();

                        if !sub_messages.is_empty() {
                            message.set_field(field_descriptor, Value::List(sub_messages));
                        }
                    }
                }
            }
        }};
    }

    // Helper macro for Time32 types
    macro_rules! extract_time32 {
        ($array_type:ty, $time_unit:expr) => {{
            let values = values_array
                .as_any()
                .downcast_ref::<PrimitiveArray<$array_type>>()
                .expect(concat!("Failed to downcast to ", stringify!($array_type)));

            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let (start, end) = list_array.value_offsets(i);

                    if start < end {
                        // Filter out null child values - protobuf repeated fields cannot contain nulls
                        let sub_messages: Vec<Value> = (start..end)
                            .filter(|&idx| !values.is_null(idx))
                            .map(|idx| {
                                let nanos = time32_unit_to_nanos(values.value(idx), $time_unit);
                                Value::Message(create_time_of_day_message(
                                    nanos,
                                    message_descriptor,
                                    &fields,
                                ))
                            })
                            .collect();

                        if !sub_messages.is_empty() {
                            message.set_field(field_descriptor, Value::List(sub_messages));
                        }
                    }
                }
            }
        }};
    }

    match values_array.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            extract_time32!(Time32SecondType, TimeUnit::Second)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            extract_time32!(Time32MillisecondType, TimeUnit::Millisecond)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            extract_time64!(Time64MicrosecondType, TimeUnit::Microsecond)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            extract_time64!(Time64NanosecondType, TimeUnit::Nanosecond)
        }
        _ => panic!(
            "Expected Time32 or Time64 array, got {:?}",
            values_array.data_type()
        ),
    }
}

// Generic repeated wrapper type extraction function for primitive types
fn extract_repeated_wrapper_primitive<P: ArrowPrimitiveType>(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    value_creator: &dyn Fn(P::Native) -> Value,
) {
    let values_array = list_array.values();
    let values = values_array
        .as_any()
        .downcast_ref::<PrimitiveArray<P>>()
        .unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let (start, end) = list_array.value_offsets(i);

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        let mut sub_message = DynamicMessage::new(message_descriptor.clone());
                        sub_message.set_field(&value_descriptor, value_creator(values.value(idx)));
                        Value::Message(sub_message)
                    })
                    .collect();
                message.set_field(field_descriptor, Value::List(sub_messages));
            }
        }
    }
}

fn extract_repeated_wrapper_bool(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values_array = list_array.values();
    let values = values_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let (start, end) = list_array.value_offsets(i);

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        let mut sub_message = DynamicMessage::new(message_descriptor.clone());
                        sub_message.set_field(&value_descriptor, Value::Bool(values.value(idx)));
                        Value::Message(sub_message)
                    })
                    .collect();
                message.set_field(field_descriptor, Value::List(sub_messages));
            }
        }
    }
}

fn extract_repeated_wrapper_string(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values_array = list_array.values();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    // Helper macro to avoid code duplication across string types
    macro_rules! process_string_values {
        ($arr:expr) => {{
            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let (start, end) = list_array.value_offsets(i);

                    if start < end {
                        let sub_messages: Vec<Value> = (start..end)
                            .map(|idx| {
                                let mut sub_message =
                                    DynamicMessage::new(message_descriptor.clone());
                                sub_message.set_field(
                                    &value_descriptor,
                                    Value::String($arr.value(idx).to_string()),
                                );
                                Value::Message(sub_message)
                            })
                            .collect();
                        message.set_field(field_descriptor, Value::List(sub_messages));
                    }
                }
            }
        }};
    }

    if let Some(arr) = values_array.as_any().downcast_ref::<StringArray>() {
        process_string_values!(arr);
    } else if let Some(arr) = values_array.as_any().downcast_ref::<LargeStringArray>() {
        process_string_values!(arr);
    } else {
        panic!(
            "Expected StringArray or LargeStringArray, got {:?}",
            values_array.data_type()
        );
    }
}

fn extract_repeated_wrapper_bytes(
    list_array: &GenericListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values_array = list_array.values();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    // Helper macro to avoid code duplication across binary types
    macro_rules! process_bytes_values {
        ($arr:expr) => {{
            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let (start, end) = list_array.value_offsets(i);

                    if start < end {
                        let sub_messages: Vec<Value> = (start..end)
                            .map(|idx| {
                                let mut sub_message =
                                    DynamicMessage::new(message_descriptor.clone());
                                sub_message.set_field(
                                    &value_descriptor,
                                    Value::Bytes(prost::bytes::Bytes::from(
                                        $arr.value(idx).to_vec(),
                                    )),
                                );
                                Value::Message(sub_message)
                            })
                            .collect();
                        message.set_field(field_descriptor, Value::List(sub_messages));
                    }
                }
            }
        }};
    }

    if let Some(arr) = values_array.as_any().downcast_ref::<BinaryArray>() {
        process_bytes_values!(arr);
    } else if let Some(arr) = values_array.as_any().downcast_ref::<LargeBinaryArray>() {
        process_bytes_values!(arr);
    } else {
        panic!(
            "Expected BinaryArray or LargeBinaryArray, got {:?}",
            values_array.data_type()
        );
    }
}

pub fn extract_repeated_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let list_array = GenericListArray::from_array(array)
        .expect("Expected ListArray, LargeListArray, or FixedSizeListArray");
    let values = list_array.values();

    match field_descriptor.kind() {
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => {
            extract_repeated_primitive_type::<Int32Type>(
                &list_array,
                messages,
                field_descriptor,
                &Value::I32,
            )
        }
        Kind::Fixed32 | Kind::Uint32 => extract_repeated_primitive_type::<UInt32Type>(
            &list_array,
            messages,
            field_descriptor,
            &Value::U32,
        ),
        Kind::Sint64 | Kind::Sfixed64 | Kind::Int64 => {
            extract_repeated_primitive_type::<Int64Type>(
                &list_array,
                messages,
                field_descriptor,
                &Value::I64,
            )
        }
        Kind::Fixed64 | Kind::Uint64 => extract_repeated_primitive_type::<UInt64Type>(
            &list_array,
            messages,
            field_descriptor,
            &Value::U64,
        ),
        Kind::Float => extract_repeated_primitive_type::<Float32Type>(
            &list_array,
            messages,
            field_descriptor,
            &Value::F32,
        ),
        Kind::Double => extract_repeated_primitive_type::<Float64Type>(
            &list_array,
            messages,
            field_descriptor,
            &Value::F64,
        ),
        Kind::Bool => extract_repeated_boolean(&list_array, messages, field_descriptor),

        Kind::String => {
            // Helper macro to extract repeated strings
            macro_rules! extract_repeated_strings {
                ($array_type:ty) => {{
                    let values_arr = values.as_any().downcast_ref::<$array_type>().unwrap();
                    for (i, message) in messages.iter_mut().enumerate() {
                        if !list_array.is_null(i) {
                            let (start, end) = list_array.value_offsets(i);
                            let values_vec: Vec<Value> = (start..end)
                                .filter(|&idx| !values_arr.is_null(idx))
                                .map(|idx| Value::String(values_arr.value(idx).to_string()))
                                .collect();
                            if !values_vec.is_empty() {
                                message.set_field(field_descriptor, Value::List(values_vec));
                            }
                        }
                    }
                }};
            }
            match values.data_type() {
                DataType::Utf8 => extract_repeated_strings!(StringArray),
                DataType::LargeUtf8 => extract_repeated_strings!(LargeStringArray),
                _ => panic!("Expected Utf8 or LargeUtf8, got {:?}", values.data_type()),
            }
        }
        Kind::Bytes => {
            // Helper macro to extract repeated bytes
            macro_rules! extract_repeated_bytes {
                ($array_type:ty) => {{
                    let values_arr = values.as_any().downcast_ref::<$array_type>().unwrap();
                    for (i, message) in messages.iter_mut().enumerate() {
                        if !list_array.is_null(i) {
                            let (start, end) = list_array.value_offsets(i);
                            let values_vec: Vec<Value> = (start..end)
                                .filter(|&idx| !values_arr.is_null(idx))
                                .map(|idx| {
                                    Value::Bytes(prost::bytes::Bytes::from(
                                        values_arr.value(idx).to_vec(),
                                    ))
                                })
                                .collect();
                            if !values_vec.is_empty() {
                                message.set_field(field_descriptor, Value::List(values_vec));
                            }
                        }
                    }
                }};
            }
            match values.data_type() {
                DataType::Binary => extract_repeated_bytes!(BinaryArray),
                DataType::LargeBinary => extract_repeated_bytes!(LargeBinaryArray),
                _ => panic!(
                    "Expected Binary or LargeBinary, got {:?}",
                    values.data_type()
                ),
            }
        }
        Kind::Message(message_descriptor) => {
            extract_repeated_message(&list_array, messages, field_descriptor, message_descriptor)
        }
        Kind::Enum(_) => extract_repeated_primitive_type::<Int32Type>(
            &list_array,
            messages,
            field_descriptor,
            &Value::EnumNumber,
        ),
    }
}

pub fn extract_singular_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    match field_descriptor.kind() {
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => {
            extract_single_primitive::<arrow_array::types::Int32Type>(
                array,
                messages,
                field_descriptor,
                &Value::I32,
            )
        }
        Kind::Fixed32 | Kind::Uint32 => extract_single_primitive::<arrow_array::types::UInt32Type>(
            array,
            messages,
            field_descriptor,
            &Value::U32,
        ),
        Kind::Sfixed64 | Kind::Sint64 | Kind::Int64 => {
            extract_single_primitive::<arrow_array::types::Int64Type>(
                array,
                messages,
                field_descriptor,
                &Value::I64,
            )
        }
        Kind::Fixed64 | Kind::Uint64 => extract_single_primitive::<arrow_array::types::UInt64Type>(
            array,
            messages,
            field_descriptor,
            &Value::U64,
        ),
        Kind::Float => extract_single_primitive::<arrow_array::types::Float32Type>(
            array,
            messages,
            field_descriptor,
            &Value::F32,
        ),
        Kind::Double => extract_single_primitive::<arrow_array::types::Float64Type>(
            array,
            messages,
            field_descriptor,
            &Value::F64,
        ),
        Kind::Bool => {
            // BooleanType doesn't implement primitive type
            extract_single_bool(array, field_descriptor, messages);
        }
        Kind::String => extract_single_string(array, field_descriptor, messages),
        Kind::Bytes => extract_single_bytes(array, field_descriptor, messages),

        Kind::Message(message_descriptor) => {
            extract_single_message(array, field_descriptor, message_descriptor, messages)
        }
        Kind::Enum(_) => extract_single_primitive::<Int32Type>(
            array,
            messages,
            field_descriptor,
            &Value::EnumNumber,
        ),
    }
}

pub fn extract_single_string(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    // Helper to process string values from an iterator
    fn process_strings<'a>(
        iter: impl Iterator<Item = Option<&'a str>>,
        field_descriptor: &FieldDescriptor,
        messages: &mut [&mut DynamicMessage],
    ) {
        iter.enumerate().for_each(|(index, value)| {
            if let Some(x) = value {
                let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                element.set_field(field_descriptor, Value::String(x.to_string()));
            }
        });
    }

    // Try StringArray first, then LargeStringArray
    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        process_strings(arr.iter(), field_descriptor, messages);
    } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        process_strings(arr.iter(), field_descriptor, messages);
    } else {
        panic!(
            "Expected StringArray or LargeStringArray, got {:?}",
            array.data_type()
        );
    }
}

pub fn extract_single_bytes(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    // Helper to process binary values from an iterator
    fn process_bytes<'a>(
        iter: impl Iterator<Item = Option<&'a [u8]>>,
        field_descriptor: &FieldDescriptor,
        messages: &mut [&mut DynamicMessage],
    ) {
        iter.enumerate().for_each(|(index, value)| {
            if let Some(x) = value {
                let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                element.set_field(
                    field_descriptor,
                    Value::Bytes(prost::bytes::Bytes::from(x.to_vec())),
                );
            }
        });
    }

    // Try BinaryArray first, then LargeBinaryArray
    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        process_bytes(arr.iter(), field_descriptor, messages);
    } else if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        process_bytes(arr.iter(), field_descriptor, messages);
    } else {
        panic!(
            "Expected BinaryArray or LargeBinaryArray, got {:?}",
            array.data_type()
        );
    }
}

pub fn extract_single_message(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    match message_descriptor.full_name() {
        "google.protobuf.Timestamp" => {
            extract_single_timestamp(array, field_descriptor, &message_descriptor, messages);
            return;
        }
        "google.protobuf.Duration" => {
            extract_single_duration(array, field_descriptor, &message_descriptor, messages);
            return;
        }
        "google.type.Date" => {
            extract_single_date(array, field_descriptor, &message_descriptor, messages);
            return;
        }
        "google.type.TimeOfDay" => {
            extract_single_time_of_day(array, field_descriptor, &message_descriptor, messages);
            return;
        }
        "google.protobuf.DoubleValue" => {
            extract_single_wrapper_primitive::<Float64Type>(
                array,
                field_descriptor,
                &message_descriptor,
                messages,
                &Value::F64,
            );
            return;
        }
        "google.protobuf.FloatValue" => {
            extract_single_wrapper_primitive::<Float32Type>(
                array,
                field_descriptor,
                &message_descriptor,
                messages,
                &Value::F32,
            );
            return;
        }
        "google.protobuf.Int64Value" => {
            extract_single_wrapper_primitive::<Int64Type>(
                array,
                field_descriptor,
                &message_descriptor,
                messages,
                &Value::I64,
            );
            return;
        }
        "google.protobuf.UInt64Value" => {
            extract_single_wrapper_primitive::<UInt64Type>(
                array,
                field_descriptor,
                &message_descriptor,
                messages,
                &Value::U64,
            );
            return;
        }
        "google.protobuf.Int32Value" => {
            extract_single_wrapper_primitive::<Int32Type>(
                array,
                field_descriptor,
                &message_descriptor,
                messages,
                &Value::I32,
            );
            return;
        }
        "google.protobuf.UInt32Value" => {
            extract_single_wrapper_primitive::<UInt32Type>(
                array,
                field_descriptor,
                &message_descriptor,
                messages,
                &Value::U32,
            );
            return;
        }
        "google.protobuf.BoolValue" => {
            extract_single_wrapper_bool(array, field_descriptor, &message_descriptor, messages);
            return;
        }
        "google.protobuf.StringValue" => {
            extract_single_wrapper_string(array, field_descriptor, &message_descriptor, messages);
            return;
        }
        "google.protobuf.BytesValue" => {
            extract_single_wrapper_bytes(array, field_descriptor, &message_descriptor, messages);
            return;
        }
        _ => {}
    }

    let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
    let mut sub_messages: Vec<&mut DynamicMessage> = messages
        .iter_mut()
        .map(|message| {
            message
                .get_field_mut(field_descriptor)
                .as_message_mut()
                .unwrap()
        })
        .collect();

    message_descriptor
        .fields()
        .for_each(|field_descriptor: FieldDescriptor| {
            let column: Option<&ArrayRef> = struct_array.column_by_name(field_descriptor.name());
            match column {
                None => {}
                Some(column) => extract_array(column, &field_descriptor, &mut sub_messages),
            }
        });
    messages.iter_mut().enumerate().for_each(|(i, x)| {
        if !struct_array.is_valid(i) {
            x.clear_field(field_descriptor)
        }
    });
}

fn extract_single_timestamp(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let fields = TimestampFields::new(message_descriptor);

    // Determine the time unit from the array's data type
    let time_unit = match array.data_type() {
        DataType::Timestamp(unit, _) => *unit,
        _ => panic!("Expected Timestamp array, got {:?}", array.data_type()),
    };

    // Helper macro to avoid code duplication
    macro_rules! extract_timestamps {
        ($array_type:ty) => {{
            let timestamp_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<$array_type>>()
                .expect(concat!("Failed to downcast to ", stringify!($array_type)));
            for (i, message) in messages.iter_mut().enumerate() {
                if !timestamp_array.is_null(i) {
                    let (seconds, nanos) =
                        time_unit_to_seconds_and_nanos(timestamp_array.value(i), time_unit);
                    let ts_msg =
                        create_timestamp_message(seconds, nanos, message_descriptor, &fields);
                    message.set_field(field_descriptor, Value::Message(ts_msg));
                }
            }
        }};
    }

    match time_unit {
        TimeUnit::Second => extract_timestamps!(TimestampSecondType),
        TimeUnit::Millisecond => extract_timestamps!(TimestampMillisecondType),
        TimeUnit::Microsecond => extract_timestamps!(TimestampMicrosecondType),
        TimeUnit::Nanosecond => extract_timestamps!(TimestampNanosecondType),
    }
}

fn extract_single_duration(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let fields = DurationFields::new(message_descriptor);

    // Determine the time unit from the array's data type
    let time_unit = match array.data_type() {
        DataType::Duration(unit) => *unit,
        _ => panic!("Expected Duration array, got {:?}", array.data_type()),
    };

    // Helper macro to avoid code duplication
    macro_rules! extract_durations {
        ($array_type:ty) => {{
            let duration_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<$array_type>>()
                .expect(concat!("Failed to downcast to ", stringify!($array_type)));
            for (i, message) in messages.iter_mut().enumerate() {
                if !duration_array.is_null(i) {
                    let (seconds, nanos) =
                        time_unit_to_duration_seconds_and_nanos(duration_array.value(i), time_unit);
                    let dur_msg =
                        create_duration_message(seconds, nanos, message_descriptor, &fields);
                    message.set_field(field_descriptor, Value::Message(dur_msg));
                }
            }
        }};
    }

    match time_unit {
        TimeUnit::Second => extract_durations!(DurationSecondType),
        TimeUnit::Millisecond => extract_durations!(DurationMillisecondType),
        TimeUnit::Microsecond => extract_durations!(DurationMicrosecondType),
        TimeUnit::Nanosecond => extract_durations!(DurationNanosecondType),
    }
}

fn extract_single_date(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let date_array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<Date32Type>>()
        .unwrap();
    let fields = DateFields::new(message_descriptor);

    for (i, message) in messages.iter_mut().enumerate() {
        if !date_array.is_null(i) {
            let date_msg = create_date_message(date_array.value(i), message_descriptor, &fields);
            message.set_field(field_descriptor, Value::Message(date_msg));
        }
    }
}

fn extract_single_time_of_day(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let fields = TimeOfDayFields::new(message_descriptor);

    // Handle Time32 (Second, Millisecond) and Time64 (Microsecond, Nanosecond)
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Time32SecondType>>()
                .expect("Failed to downcast to Time32SecondType");
            for (i, message) in messages.iter_mut().enumerate() {
                if !time_array.is_null(i) {
                    let nanos = time32_unit_to_nanos(time_array.value(i), TimeUnit::Second);
                    let time_msg = create_time_of_day_message(nanos, message_descriptor, &fields);
                    message.set_field(field_descriptor, Value::Message(time_msg));
                }
            }
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Time32MillisecondType>>()
                .expect("Failed to downcast to Time32MillisecondType");
            for (i, message) in messages.iter_mut().enumerate() {
                if !time_array.is_null(i) {
                    let nanos = time32_unit_to_nanos(time_array.value(i), TimeUnit::Millisecond);
                    let time_msg = create_time_of_day_message(nanos, message_descriptor, &fields);
                    message.set_field(field_descriptor, Value::Message(time_msg));
                }
            }
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Time64MicrosecondType>>()
                .expect("Failed to downcast to Time64MicrosecondType");
            for (i, message) in messages.iter_mut().enumerate() {
                if !time_array.is_null(i) {
                    let nanos = time64_unit_to_nanos(time_array.value(i), TimeUnit::Microsecond);
                    let time_msg = create_time_of_day_message(nanos, message_descriptor, &fields);
                    message.set_field(field_descriptor, Value::Message(time_msg));
                }
            }
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let time_array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Time64NanosecondType>>()
                .expect("Failed to downcast to Time64NanosecondType");
            for (i, message) in messages.iter_mut().enumerate() {
                if !time_array.is_null(i) {
                    let time_msg = create_time_of_day_message(
                        time_array.value(i),
                        message_descriptor,
                        &fields,
                    );
                    message.set_field(field_descriptor, Value::Message(time_msg));
                }
            }
        }
        _ => panic!(
            "Expected Time32 or Time64 array, got {:?}",
            array.data_type()
        ),
    }
}

// Generic wrapper type extraction function for primitive types
fn extract_single_wrapper_primitive<P: ArrowPrimitiveType>(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
    value_creator: &dyn Fn(P::Native) -> Value,
) {
    let arr = array.as_any().downcast_ref::<PrimitiveArray<P>>().unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !arr.is_null(i) {
            let sub_message = message
                .get_field_mut(field_descriptor)
                .as_message_mut()
                .unwrap();
            sub_message.set_field(&value_descriptor, value_creator(arr.value(i)));
        }
    }
}

fn extract_single_wrapper_bool(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !arr.is_null(i) {
            let sub_message = message
                .get_field_mut(field_descriptor)
                .as_message_mut()
                .unwrap();
            sub_message.set_field(&value_descriptor, Value::Bool(arr.value(i)));
        }
    }
}

fn extract_single_wrapper_string(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    // Helper to process string values
    fn process<'a>(
        iter: impl Iterator<Item = Option<&'a str>>,
        field_descriptor: &FieldDescriptor,
        value_descriptor: &FieldDescriptor,
        messages: &mut [&mut DynamicMessage],
    ) {
        for (i, value) in iter.enumerate() {
            if let Some(v) = value {
                let sub_message = messages[i]
                    .get_field_mut(field_descriptor)
                    .as_message_mut()
                    .unwrap();
                sub_message.set_field(value_descriptor, Value::String(v.to_string()));
            }
        }
    }

    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        process(arr.iter(), field_descriptor, &value_descriptor, messages);
    } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        process(arr.iter(), field_descriptor, &value_descriptor, messages);
    } else {
        panic!(
            "Expected StringArray or LargeStringArray, got {:?}",
            array.data_type()
        );
    }
}

fn extract_single_wrapper_bytes(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    // Helper to process binary values
    fn process<'a>(
        iter: impl Iterator<Item = Option<&'a [u8]>>,
        field_descriptor: &FieldDescriptor,
        value_descriptor: &FieldDescriptor,
        messages: &mut [&mut DynamicMessage],
    ) {
        for (i, value) in iter.enumerate() {
            if let Some(v) = value {
                let sub_message = messages[i]
                    .get_field_mut(field_descriptor)
                    .as_message_mut()
                    .unwrap();
                sub_message.set_field(
                    value_descriptor,
                    Value::Bytes(prost::bytes::Bytes::from(v.to_vec())),
                );
            }
        }
    }

    if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
        process(arr.iter(), field_descriptor, &value_descriptor, messages);
    } else if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        process(arr.iter(), field_descriptor, &value_descriptor, messages);
    } else {
        panic!(
            "Expected BinaryArray or LargeBinaryArray, got {:?}",
            array.data_type()
        );
    }
}

pub fn extract_single_bool(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .enumerate()
        .for_each(|(index, value)| match value {
            None => {}
            Some(x) => {
                let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                element.set_field(field_descriptor, Value::Bool(x));
            }
        })
}

/// Extract a single map value at a given index from a MapArray.
fn extract_single_map(
    map_array: &MapArray,
    idx: usize,
    key_field: &FieldDescriptor,
    value_field: &FieldDescriptor,
) -> Option<Value> {
    if map_array.is_null(idx) {
        return None;
    }
    let start = map_array.value_offsets()[idx] as usize;
    let end = map_array.value_offsets()[idx + 1] as usize;
    if start >= end {
        return Some(Value::Map(HashMap::new()));
    }

    let entries = map_array.entries();
    let key_array = entries.column_by_name("key")?;
    // Try "value" first (default), fall back to second struct field for custom map_value_name
    let value_array = entries
        .column_by_name("value")
        .or_else(|| entries.columns().get(1))?;

    let mut map: HashMap<MapKey, Value> = HashMap::new();
    for i in start..end {
        let key = extract_map_key(key_array, i, key_field);
        let value = extract_map_value(value_array, i, value_field);
        if let (Some(k), Some(v)) = (key, value) {
            map.insert(k, v);
        }
    }
    Some(Value::Map(map))
}

pub fn extract_map_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();

    // Get the key and value field descriptors from the map entry message type
    let map_entry_descriptor = match field_descriptor.kind() {
        Kind::Message(desc) => desc,
        _ => return,
    };
    let key_field = map_entry_descriptor.get_field_by_name("key").unwrap();
    let value_field = map_entry_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if let Some(map_value) = extract_single_map(map_array, i, &key_field, &value_field) {
            message.set_field(field_descriptor, map_value);
        }
    }
}

fn extract_map_key(array: &ArrayRef, idx: usize, field: &FieldDescriptor) -> Option<MapKey> {
    match field.kind() {
        Kind::String => {
            // Try StringArray first, then LargeStringArray
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(MapKey::String(arr.value(idx).to_string()))
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(MapKey::String(arr.value(idx).to_string()))
                }
            } else {
                None
            }
        }
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(MapKey::I32(arr.value(idx)))
            }
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(MapKey::I64(arr.value(idx)))
            }
        }
        Kind::Uint32 | Kind::Fixed32 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(MapKey::U32(arr.value(idx)))
            }
        }
        Kind::Uint64 | Kind::Fixed64 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(MapKey::U64(arr.value(idx)))
            }
        }
        Kind::Bool => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(MapKey::Bool(arr.value(idx)))
            }
        }
        _ => None,
    }
}

fn extract_map_value(array: &ArrayRef, idx: usize, field: &FieldDescriptor) -> Option<Value> {
    match field.kind() {
        Kind::Double => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::F64(arr.value(idx)))
            }
        }
        Kind::Float => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::F32(arr.value(idx)))
            }
        }
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::I32(arr.value(idx)))
            }
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::I64(arr.value(idx)))
            }
        }
        Kind::Uint32 | Kind::Fixed32 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::U32(arr.value(idx)))
            }
        }
        Kind::Uint64 | Kind::Fixed64 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::U64(arr.value(idx)))
            }
        }
        Kind::Bool => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::Bool(arr.value(idx)))
            }
        }
        Kind::String => {
            // Try StringArray first, then LargeStringArray
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::String(arr.value(idx).to_string()))
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::String(arr.value(idx).to_string()))
                }
            } else {
                None
            }
        }
        Kind::Bytes => {
            // Try BinaryArray first, then LargeBinaryArray
            if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::Bytes(prost::bytes::Bytes::from(
                        arr.value(idx).to_vec(),
                    )))
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::Bytes(prost::bytes::Bytes::from(
                        arr.value(idx).to_vec(),
                    )))
                }
            } else {
                None
            }
        }
        Kind::Enum(_) => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::EnumNumber(arr.value(idx)))
            }
        }
        Kind::Message(message_descriptor) => {
            extract_map_message_value(array, idx, &message_descriptor)
        }
    }
}

fn extract_map_message_value(
    array: &ArrayRef,
    idx: usize,
    message_descriptor: &MessageDescriptor,
) -> Option<Value> {
    let full_name = message_descriptor.full_name();

    // Handle google.protobuf.Timestamp - detect time unit from array's DataType
    if full_name == "google.protobuf.Timestamp" {
        let fields = TimestampFields::new(message_descriptor);

        // Helper macro to extract timestamp with proper time unit handling
        macro_rules! extract_ts {
            ($array_type:ty, $time_unit:expr) => {{
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$array_type>>()?;
                if arr.is_null(idx) {
                    return None;
                }
                let (seconds, nanos) = time_unit_to_seconds_and_nanos(arr.value(idx), $time_unit);
                return Some(Value::Message(create_timestamp_message(
                    seconds,
                    nanos,
                    message_descriptor,
                    &fields,
                )));
            }};
        }

        match array.data_type() {
            DataType::Timestamp(TimeUnit::Second, _) => {
                extract_ts!(TimestampSecondType, TimeUnit::Second)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                extract_ts!(TimestampMillisecondType, TimeUnit::Millisecond)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                extract_ts!(TimestampMicrosecondType, TimeUnit::Microsecond)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                extract_ts!(TimestampNanosecondType, TimeUnit::Nanosecond)
            }
            _ => return None,
        }
    }

    // Handle google.protobuf.Duration - detect time unit from array's DataType
    if full_name == "google.protobuf.Duration" {
        let fields = DurationFields::new(message_descriptor);

        // Helper macro to extract duration with proper time unit handling
        macro_rules! extract_dur {
            ($array_type:ty, $time_unit:expr) => {{
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$array_type>>()?;
                if arr.is_null(idx) {
                    return None;
                }
                let (seconds, nanos) =
                    time_unit_to_duration_seconds_and_nanos(arr.value(idx), $time_unit);
                return Some(Value::Message(create_duration_message(
                    seconds,
                    nanos,
                    message_descriptor,
                    &fields,
                )));
            }};
        }

        match array.data_type() {
            DataType::Duration(TimeUnit::Second) => {
                extract_dur!(DurationSecondType, TimeUnit::Second)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                extract_dur!(DurationMillisecondType, TimeUnit::Millisecond)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                extract_dur!(DurationMicrosecondType, TimeUnit::Microsecond)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                extract_dur!(DurationNanosecondType, TimeUnit::Nanosecond)
            }
            _ => return None,
        }
    }

    // Handle google.type.Date
    if full_name == "google.type.Date" {
        let arr = array
            .as_any()
            .downcast_ref::<PrimitiveArray<Date32Type>>()?;
        if arr.is_null(idx) {
            return None;
        }
        let fields = DateFields::new(message_descriptor);
        return Some(Value::Message(create_date_message(
            arr.value(idx),
            message_descriptor,
            &fields,
        )));
    }

    // Handle google.type.TimeOfDay - detect time unit from array's DataType
    if full_name == "google.type.TimeOfDay" {
        let fields = TimeOfDayFields::new(message_descriptor);

        match array.data_type() {
            DataType::Time32(TimeUnit::Second) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Time32SecondType>>()?;
                if arr.is_null(idx) {
                    return None;
                }
                let nanos = time32_unit_to_nanos(arr.value(idx), TimeUnit::Second);
                return Some(Value::Message(create_time_of_day_message(
                    nanos,
                    message_descriptor,
                    &fields,
                )));
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Time32MillisecondType>>()?;
                if arr.is_null(idx) {
                    return None;
                }
                let nanos = time32_unit_to_nanos(arr.value(idx), TimeUnit::Millisecond);
                return Some(Value::Message(create_time_of_day_message(
                    nanos,
                    message_descriptor,
                    &fields,
                )));
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Time64MicrosecondType>>()?;
                if arr.is_null(idx) {
                    return None;
                }
                let nanos = time64_unit_to_nanos(arr.value(idx), TimeUnit::Microsecond);
                return Some(Value::Message(create_time_of_day_message(
                    nanos,
                    message_descriptor,
                    &fields,
                )));
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Time64NanosecondType>>()?;
                if arr.is_null(idx) {
                    return None;
                }
                return Some(Value::Message(create_time_of_day_message(
                    arr.value(idx),
                    message_descriptor,
                    &fields,
                )));
            }
            _ => return None,
        }
    }

    // Handle wrapper types
    match full_name {
        "google.protobuf.DoubleValue" => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::F64(arr.value(idx)),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.FloatValue" => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::F32(arr.value(idx)),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.Int64Value" => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::I64(arr.value(idx)),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.UInt64Value" => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::U64(arr.value(idx)),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.Int32Value" => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::I32(arr.value(idx)),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.UInt32Value" => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::U32(arr.value(idx)),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.BoolValue" => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::Bool(arr.value(idx)),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.StringValue" => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::String(arr.value(idx).to_string()),
            );
            return Some(Value::Message(msg));
        }
        "google.protobuf.BytesValue" => {
            let arr = array.as_any().downcast_ref::<BinaryArray>()?;
            if arr.is_null(idx) {
                return None;
            }
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field(
                &message_descriptor.get_field_by_name("value").unwrap(),
                Value::Bytes(prost::bytes::Bytes::from(arr.value(idx).to_vec())),
            );
            return Some(Value::Message(msg));
        }
        _ => {}
    }

    // Handle regular messages (StructArray)
    let struct_array = array.as_any().downcast_ref::<StructArray>()?;
    if !struct_array.is_valid(idx) {
        return None;
    }

    let mut msg = DynamicMessage::new(message_descriptor.clone());
    for field_desc in message_descriptor.fields() {
        if let Some(column) = struct_array.column_by_name(field_desc.name()) {
            if let Some(value) = extract_struct_field_value(column, idx, &field_desc) {
                msg.set_field(&field_desc, value);
            }
        }
    }
    Some(Value::Message(msg))
}

fn extract_struct_field_value(
    array: &ArrayRef,
    idx: usize,
    field: &FieldDescriptor,
) -> Option<Value> {
    if field.is_map() {
        let map_array = array.as_any().downcast_ref::<MapArray>()?;
        let map_entry_descriptor = match field.kind() {
            Kind::Message(desc) => desc,
            _ => return None,
        };
        let key_field = map_entry_descriptor.get_field_by_name("key")?;
        let value_field = map_entry_descriptor.get_field_by_name("value")?;
        return extract_single_map(map_array, idx, &key_field, &value_field);
    }

    if field.is_list() {
        // Handle repeated fields - support both ListArray and LargeListArray
        let list_array = GenericListArray::from_array(array)?;
        if list_array.is_null(idx) {
            return None;
        }
        let (start, end) = list_array.value_offsets(idx);
        if start >= end {
            return Some(Value::List(vec![]));
        }
        let values_array = list_array.values();
        let mut values = Vec::with_capacity(end - start);
        for i in start..end {
            if let Some(v) = extract_single_field_value(&values_array, i, field) {
                values.push(v);
            }
        }
        return Some(Value::List(values));
    }

    extract_single_field_value(array, idx, field)
}

fn extract_single_field_value(
    array: &ArrayRef,
    idx: usize,
    field: &FieldDescriptor,
) -> Option<Value> {
    match field.kind() {
        Kind::Double => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::F64(arr.value(idx)))
            }
        }
        Kind::Float => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<Float32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::F32(arr.value(idx)))
            }
        }
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::I32(arr.value(idx)))
            }
        }
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::I64(arr.value(idx)))
            }
        }
        Kind::Uint32 | Kind::Fixed32 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::U32(arr.value(idx)))
            }
        }
        Kind::Uint64 | Kind::Fixed64 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::U64(arr.value(idx)))
            }
        }
        Kind::Bool => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::Bool(arr.value(idx)))
            }
        }
        Kind::String => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::String(arr.value(idx).to_string()))
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::String(arr.value(idx).to_string()))
                }
            } else {
                None
            }
        }
        Kind::Bytes => {
            if let Some(arr) = array.as_any().downcast_ref::<BinaryArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::Bytes(prost::bytes::Bytes::from(
                        arr.value(idx).to_vec(),
                    )))
                }
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeBinaryArray>() {
                if arr.is_null(idx) {
                    None
                } else {
                    Some(Value::Bytes(prost::bytes::Bytes::from(
                        arr.value(idx).to_vec(),
                    )))
                }
            } else {
                None
            }
        }
        Kind::Enum(_) => {
            let arr = array.as_any().downcast_ref::<PrimitiveArray<Int32Type>>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::EnumNumber(arr.value(idx)))
            }
        }
        Kind::Message(msg_desc) => extract_map_message_value(array, idx, &msg_desc),
    }
}

pub fn extract_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    if field_descriptor.is_map() {
        extract_map_array(array, field_descriptor, messages)
    } else if field_descriptor.is_list() {
        extract_repeated_array(array, field_descriptor, messages)
    } else {
        extract_singular_array(array, field_descriptor, messages)
    }
}

pub fn record_batch_to_array(
    record_batch: &RecordBatch,
    message_descriptor: &MessageDescriptor,
) -> ArrayData {
    let mut messages: Vec<DynamicMessage> = (0..record_batch.num_rows())
        .map(|_| DynamicMessage::new(message_descriptor.clone()))
        .collect::<Vec<DynamicMessage>>();
    let mut references: Vec<&mut DynamicMessage> = messages.iter_mut().collect();

    message_descriptor
        .fields()
        .for_each(|field_descriptor: FieldDescriptor| {
            let column: Option<&ArrayRef> = record_batch.column_by_name(field_descriptor.name());
            match column {
                None => {}
                Some(column) => extract_array(column, &field_descriptor, &mut references),
            }
        });
    let mut results = BinaryBuilder::new();

    messages
        .iter()
        .for_each(|x| results.append_value(x.encode_to_vec()));
    results.finish().to_data()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanos_to_seconds_and_nanos_positive() {
        let (seconds, nanos) = nanos_to_seconds_and_nanos(1_500_000_000);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_nanos_to_seconds_and_nanos_negative() {
        // -0.5 seconds = -1 second + 500_000_000 nanos (for Timestamp)
        let (seconds, nanos) = nanos_to_seconds_and_nanos(-500_000_000);
        assert_eq!(seconds, -1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_nanos_to_seconds_and_nanos_exact_second() {
        let (seconds, nanos) = nanos_to_seconds_and_nanos(2_000_000_000);
        assert_eq!(seconds, 2);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_nanos_to_duration_seconds_and_nanos_positive() {
        let (seconds, nanos) = nanos_to_duration_seconds_and_nanos(1_500_000_000);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_nanos_to_duration_seconds_and_nanos_negative() {
        // For Duration, nanos keeps the sign
        let (seconds, nanos) = nanos_to_duration_seconds_and_nanos(-1_500_000_000);
        assert_eq!(seconds, -1);
        assert_eq!(nanos, -500_000_000);
    }

    #[test]
    fn test_time_unit_to_seconds_and_nanos_second() {
        let (seconds, nanos) = time_unit_to_seconds_and_nanos(42, TimeUnit::Second);
        assert_eq!(seconds, 42);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_time_unit_to_seconds_and_nanos_millisecond_positive() {
        let (seconds, nanos) = time_unit_to_seconds_and_nanos(1500, TimeUnit::Millisecond);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time_unit_to_seconds_and_nanos_millisecond_negative() {
        let (seconds, nanos) = time_unit_to_seconds_and_nanos(-500, TimeUnit::Millisecond);
        assert_eq!(seconds, -1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time_unit_to_seconds_and_nanos_microsecond_positive() {
        let (seconds, nanos) = time_unit_to_seconds_and_nanos(1_500_000, TimeUnit::Microsecond);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time_unit_to_seconds_and_nanos_microsecond_negative() {
        let (seconds, nanos) = time_unit_to_seconds_and_nanos(-500_000, TimeUnit::Microsecond);
        assert_eq!(seconds, -1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time_unit_to_seconds_and_nanos_nanosecond() {
        let (seconds, nanos) = time_unit_to_seconds_and_nanos(1_500_000_000, TimeUnit::Nanosecond);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time_unit_to_duration_seconds_and_nanos_second() {
        let (seconds, nanos) = time_unit_to_duration_seconds_and_nanos(42, TimeUnit::Second);
        assert_eq!(seconds, 42);
        assert_eq!(nanos, 0);
    }

    #[test]
    fn test_time_unit_to_duration_seconds_and_nanos_millisecond() {
        let (seconds, nanos) = time_unit_to_duration_seconds_and_nanos(1500, TimeUnit::Millisecond);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time_unit_to_duration_seconds_and_nanos_microsecond() {
        let (seconds, nanos) =
            time_unit_to_duration_seconds_and_nanos(1_500_000, TimeUnit::Microsecond);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time_unit_to_duration_seconds_and_nanos_nanosecond() {
        let (seconds, nanos) =
            time_unit_to_duration_seconds_and_nanos(1_500_000_000, TimeUnit::Nanosecond);
        assert_eq!(seconds, 1);
        assert_eq!(nanos, 500_000_000);
    }

    #[test]
    fn test_time32_unit_to_nanos_second() {
        let nanos = time32_unit_to_nanos(5, TimeUnit::Second);
        assert_eq!(nanos, 5_000_000_000);
    }

    #[test]
    fn test_time32_unit_to_nanos_millisecond() {
        let nanos = time32_unit_to_nanos(1500, TimeUnit::Millisecond);
        assert_eq!(nanos, 1_500_000_000);
    }

    #[test]
    #[should_panic(expected = "Time32 only supports Second and Millisecond units")]
    fn test_time32_unit_to_nanos_invalid_unit() {
        time32_unit_to_nanos(1000, TimeUnit::Microsecond);
    }

    #[test]
    fn test_time64_unit_to_nanos_microsecond() {
        let nanos = time64_unit_to_nanos(1_500_000, TimeUnit::Microsecond);
        assert_eq!(nanos, 1_500_000_000);
    }

    #[test]
    fn test_time64_unit_to_nanos_nanosecond() {
        let nanos = time64_unit_to_nanos(1_500_000_000, TimeUnit::Nanosecond);
        assert_eq!(nanos, 1_500_000_000);
    }

    #[test]
    #[should_panic(expected = "Time64 only supports Microsecond and Nanosecond units")]
    fn test_time64_unit_to_nanos_invalid_unit() {
        time64_unit_to_nanos(1000, TimeUnit::Second);
    }

    #[test]
    fn test_date32_conversion() {
        // Test the date conversion logic used inline
        // 2024-01-15 = 19737 days since Unix epoch
        let days: i32 = 19737;
        let date = NaiveDate::from_num_days_from_ce_opt(days + CE_OFFSET).unwrap();
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn test_date32_conversion_unix_epoch() {
        let days: i32 = 0;
        let date = NaiveDate::from_num_days_from_ce_opt(days + CE_OFFSET).unwrap();
        assert_eq!(date.year(), 1970);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 1);
    }

    #[test]
    fn test_date32_conversion_negative() {
        // 1969-12-31 = -1 days since Unix epoch
        let days: i32 = -1;
        let date = NaiveDate::from_num_days_from_ce_opt(days + CE_OFFSET).unwrap();
        assert_eq!(date.year(), 1969);
        assert_eq!(date.month(), 12);
        assert_eq!(date.day(), 31);
    }
}
