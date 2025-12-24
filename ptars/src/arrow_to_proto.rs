use arrow::array::ArrayData;
use arrow_array::builder::BinaryBuilder;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, Time64NanosecondType,
    TimestampNanosecondType, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, ListArray, MapArray,
    PrimitiveArray, RecordBatch, StringArray, StructArray,
};
use chrono::{Datelike, NaiveDate};
use prost::Message;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value};
use std::collections::HashMap;

// Days from CE epoch to Unix epoch (1970-01-01)
const CE_OFFSET: i32 = 719163;

/// Convert total nanoseconds to (seconds, nanos) tuple.
/// Handles negative timestamps correctly by ensuring nanos is always in [0, 999999999].
fn nanos_to_seconds_and_nanos(nanos_total: i64) -> (i64, i32) {
    let mut seconds = nanos_total / 1_000_000_000;
    let mut nanos = (nanos_total % 1_000_000_000) as i32;
    // Ensure nanos is non-negative (protobuf requirement)
    if nanos < 0 {
        seconds -= 1;
        nanos += 1_000_000_000;
    }
    (seconds, nanos)
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

/// Create a google.protobuf.Timestamp DynamicMessage from total nanoseconds.
fn create_timestamp_message(
    nanos_total: i64,
    message_descriptor: &MessageDescriptor,
    fields: &TimestampFields,
) -> DynamicMessage {
    let (seconds, nanos) = nanos_to_seconds_and_nanos(nanos_total);
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

pub fn extract_repeated_primitive_type<P>(
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    value_creator: &dyn Fn(P::Native) -> Value,
) where
    P: ArrowPrimitiveType,
{
    let values: &PrimitiveArray<P> = list_array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<P>>()
        .unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;
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

pub fn extract_repeated_boolean(
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
) {
    let values: &BooleanArray = list_array
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;
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

pub fn extract_repeated_message(
    list_array: &ListArray,
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

    let struct_array = list_array
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

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
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values: &PrimitiveArray<TimestampNanosecondType> = list_array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        .unwrap();
    let fields = TimestampFields::new(message_descriptor);

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        Value::Message(create_timestamp_message(
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

fn extract_repeated_date(
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values: &PrimitiveArray<Date32Type> = list_array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<Date32Type>>()
        .unwrap();
    let fields = DateFields::new(message_descriptor);

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

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
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values: &PrimitiveArray<Time64NanosecondType> = list_array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<Time64NanosecondType>>()
        .unwrap();
    let fields = TimeOfDayFields::new(message_descriptor);

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        Value::Message(create_time_of_day_message(
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

// Generic repeated wrapper type extraction function for primitive types
fn extract_repeated_wrapper_primitive<P: ArrowPrimitiveType>(
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    value_creator: &dyn Fn(P::Native) -> Value,
) {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<PrimitiveArray<P>>()
        .unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

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
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

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
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        let mut sub_message = DynamicMessage::new(message_descriptor.clone());
                        sub_message.set_field(
                            &value_descriptor,
                            Value::String(values.value(idx).to_string()),
                        );
                        Value::Message(sub_message)
                    })
                    .collect();
                message.set_field(field_descriptor, Value::List(sub_messages));
            }
        }
    }
}

fn extract_repeated_wrapper_bytes(
    list_array: &ListArray,
    messages: &mut [&mut DynamicMessage],
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
) {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        let mut sub_message = DynamicMessage::new(message_descriptor.clone());
                        sub_message.set_field(
                            &value_descriptor,
                            Value::Bytes(prost::bytes::Bytes::from(values.value(idx).to_vec())),
                        );
                        Value::Message(sub_message)
                    })
                    .collect();
                message.set_field(field_descriptor, Value::List(sub_messages));
            }
        }
    }
}

pub fn extract_repeated_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let list_array: &ListArray = array.as_any().downcast_ref::<ListArray>().unwrap();
    let values = list_array.values();

    match field_descriptor.kind() {
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => {
            extract_repeated_primitive_type::<Int32Type>(
                list_array,
                messages,
                field_descriptor,
                &Value::I32,
            )
        }
        Kind::Fixed32 | Kind::Uint32 => extract_repeated_primitive_type::<UInt32Type>(
            list_array,
            messages,
            field_descriptor,
            &Value::U32,
        ),
        Kind::Sint64 | Kind::Sfixed64 | Kind::Int64 => {
            extract_repeated_primitive_type::<Int64Type>(
                list_array,
                messages,
                field_descriptor,
                &Value::I64,
            )
        }
        Kind::Fixed64 | Kind::Uint64 => extract_repeated_primitive_type::<UInt64Type>(
            list_array,
            messages,
            field_descriptor,
            &Value::U64,
        ),
        Kind::Float => extract_repeated_primitive_type::<Float32Type>(
            list_array,
            messages,
            field_descriptor,
            &Value::F32,
        ),
        Kind::Double => extract_repeated_primitive_type::<Float64Type>(
            list_array,
            messages,
            field_descriptor,
            &Value::F64,
        ),
        Kind::Bool => extract_repeated_boolean(list_array, messages, field_descriptor),

        Kind::String => {
            let values = values.as_any().downcast_ref::<StringArray>().unwrap();
            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let start = list_array.value_offsets()[i] as usize;
                    let end = list_array.value_offsets()[i + 1] as usize;
                    let values_vec: Vec<Value> = (start..end)
                        .map(|idx| Value::String(values.value(idx).to_string()))
                        .collect();
                    message.set_field(field_descriptor, Value::List(values_vec));
                }
            }
        }
        Kind::Bytes => {
            let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
            for (i, message) in messages.iter_mut().enumerate() {
                if !list_array.is_null(i) {
                    let start = list_array.value_offsets()[i] as usize;
                    let end = list_array.value_offsets()[i + 1] as usize;
                    let values_vec: Vec<Value> = (start..end)
                        .map(|idx| {
                            Value::Bytes(prost::bytes::Bytes::from(values.value(idx).to_vec()))
                        })
                        .collect();
                    message.set_field(field_descriptor, Value::List(values_vec));
                }
            }
        }
        Kind::Message(message_descriptor) => {
            extract_repeated_message(list_array, messages, field_descriptor, message_descriptor)
        }
        Kind::Enum(_) => extract_repeated_primitive_type::<Int32Type>(
            list_array,
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
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .enumerate()
        .for_each(|(index, value)| match value {
            None => {}
            Some(x) => {
                let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                element.set_field(field_descriptor, Value::String(x.to_string()));
            }
        })
}

pub fn extract_single_bytes(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .enumerate()
        .for_each(|(index, value)| match value {
            None => {}
            Some(x) => {
                let element: &mut DynamicMessage = messages.get_mut(index).unwrap();

                element.set_field(
                    field_descriptor,
                    Value::Bytes(prost::bytes::Bytes::from(x.to_vec())),
                );
            }
        })
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
    let timestamp_array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()
        .unwrap();
    let fields = TimestampFields::new(message_descriptor);

    for (i, message) in messages.iter_mut().enumerate() {
        if !timestamp_array.is_null(i) {
            let ts_msg =
                create_timestamp_message(timestamp_array.value(i), message_descriptor, &fields);
            message.set_field(field_descriptor, Value::Message(ts_msg));
        }
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
    let time_array = array
        .as_any()
        .downcast_ref::<PrimitiveArray<Time64NanosecondType>>()
        .unwrap();
    let fields = TimeOfDayFields::new(message_descriptor);

    for (i, message) in messages.iter_mut().enumerate() {
        if !time_array.is_null(i) {
            let time_msg =
                create_time_of_day_message(time_array.value(i), message_descriptor, &fields);
            message.set_field(field_descriptor, Value::Message(time_msg));
        }
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
    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !arr.is_null(i) {
            let sub_message = message
                .get_field_mut(field_descriptor)
                .as_message_mut()
                .unwrap();
            sub_message.set_field(&value_descriptor, Value::String(arr.value(i).to_string()));
        }
    }
}

fn extract_single_wrapper_bytes(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    let value_descriptor = message_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !arr.is_null(i) {
            let sub_message = message
                .get_field_mut(field_descriptor)
                .as_message_mut()
                .unwrap();
            sub_message.set_field(
                &value_descriptor,
                Value::Bytes(prost::bytes::Bytes::from(arr.value(i).to_vec())),
            );
        }
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

pub fn extract_map_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
    let entries = map_array.entries();
    let key_array = entries.column_by_name("key").unwrap();
    let value_array = entries.column_by_name("value").unwrap();

    // Get the key and value field descriptors from the map entry message type
    let map_entry_descriptor = match field_descriptor.kind() {
        Kind::Message(desc) => desc,
        _ => return,
    };
    let key_field = map_entry_descriptor.get_field_by_name("key").unwrap();
    let value_field = map_entry_descriptor.get_field_by_name("value").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !map_array.is_null(i) {
            let start = map_array.value_offsets()[i] as usize;
            let end = map_array.value_offsets()[i + 1] as usize;

            if start < end {
                let mut map: HashMap<MapKey, Value> = HashMap::new();

                for idx in start..end {
                    let key = extract_map_key(key_array, idx, &key_field);
                    let value = extract_map_value(value_array, idx, &value_field);
                    if let (Some(k), Some(v)) = (key, value) {
                        map.insert(k, v);
                    }
                }

                message.set_field(field_descriptor, Value::Map(map));
            }
        }
    }
}

fn extract_map_key(array: &ArrayRef, idx: usize, field: &FieldDescriptor) -> Option<MapKey> {
    match field.kind() {
        Kind::String => {
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(MapKey::String(arr.value(idx).to_string()))
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
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::String(arr.value(idx).to_string()))
            }
        }
        Kind::Bytes => {
            let arr = array.as_any().downcast_ref::<BinaryArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::Bytes(prost::bytes::Bytes::from(
                    arr.value(idx).to_vec(),
                )))
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

    // Handle google.protobuf.Timestamp
    if full_name == "google.protobuf.Timestamp" {
        let arr = array
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampNanosecondType>>()?;
        if arr.is_null(idx) {
            return None;
        }
        let fields = TimestampFields::new(message_descriptor);
        return Some(Value::Message(create_timestamp_message(
            arr.value(idx),
            message_descriptor,
            &fields,
        )));
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

    // Handle google.type.TimeOfDay
    if full_name == "google.type.TimeOfDay" {
        let arr = array
            .as_any()
            .downcast_ref::<PrimitiveArray<Time64NanosecondType>>()?;
        if arr.is_null(idx) {
            return None;
        }
        let fields = TimeOfDayFields::new(message_descriptor);
        return Some(Value::Message(create_time_of_day_message(
            arr.value(idx),
            message_descriptor,
            &fields,
        )));
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
    if field.is_list() {
        // Handle repeated fields
        let list_array = array.as_any().downcast_ref::<ListArray>()?;
        if list_array.is_null(idx) {
            return None;
        }
        let start = list_array.value_offsets()[idx] as usize;
        let end = list_array.value_offsets()[idx + 1] as usize;
        if start >= end {
            return Some(Value::List(vec![]));
        }
        let values_array = list_array.values();
        let mut values = Vec::with_capacity(end - start);
        for i in start..end {
            if let Some(v) = extract_single_field_value(values_array, i, field) {
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
            let arr = array.as_any().downcast_ref::<StringArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::String(arr.value(idx).to_string()))
            }
        }
        Kind::Bytes => {
            let arr = array.as_any().downcast_ref::<BinaryArray>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(Value::Bytes(prost::bytes::Bytes::from(
                    arr.value(idx).to_vec(),
                )))
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
