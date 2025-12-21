use arrow::array::ArrayData;
use arrow_array::builder::BinaryBuilder;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampNanosecondType,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, ListArray, PrimitiveArray,
    RecordBatch, StringArray, StructArray,
};
use chrono::{Datelike, NaiveDate};
use prost::Message;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};

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
                let slice = values.slice(start, end);
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
    if message_descriptor.full_name() == "google.protobuf.Timestamp" {
        extract_repeated_timestamp(list_array, messages, field_descriptor, &message_descriptor);
        return;
    }
    if message_descriptor.full_name() == "google.type.Date" {
        extract_repeated_date(list_array, messages, field_descriptor, &message_descriptor);
        return;
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

    let seconds_descriptor = message_descriptor.get_field_by_name("seconds").unwrap();
    let nanos_descriptor = message_descriptor.get_field_by_name("nanos").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        let nanos_total = values.value(idx);
                        let (seconds, nanos) = nanos_to_seconds_and_nanos(nanos_total);

                        let mut sub_message = DynamicMessage::new(message_descriptor.clone());
                        sub_message.set_field(&seconds_descriptor, Value::I64(seconds));
                        sub_message.set_field(&nanos_descriptor, Value::I32(nanos));
                        Value::Message(sub_message)
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

    let year_descriptor = message_descriptor.get_field_by_name("year").unwrap();
    let month_descriptor = message_descriptor.get_field_by_name("month").unwrap();
    let day_descriptor = message_descriptor.get_field_by_name("day").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !list_array.is_null(i) {
            let start = list_array.value_offsets()[i] as usize;
            let end = list_array.value_offsets()[i + 1] as usize;

            if start < end {
                let sub_messages: Vec<Value> = (start..end)
                    .map(|idx| {
                        let days = values.value(idx);
                        let date = NaiveDate::from_num_days_from_ce_opt(days + CE_OFFSET).unwrap();

                        let mut sub_message = DynamicMessage::new(message_descriptor.clone());
                        sub_message.set_field(&year_descriptor, Value::I32(date.year()));
                        sub_message.set_field(&month_descriptor, Value::I32(date.month() as i32));
                        sub_message.set_field(&day_descriptor, Value::I32(date.day() as i32));
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
    if message_descriptor.full_name() == "google.protobuf.Timestamp" {
        extract_single_timestamp(array, field_descriptor, &message_descriptor, messages);
        return;
    }
    if message_descriptor.full_name() == "google.type.Date" {
        extract_single_date(array, field_descriptor, &message_descriptor, messages);
        return;
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

    let seconds_descriptor = message_descriptor.get_field_by_name("seconds").unwrap();
    let nanos_descriptor = message_descriptor.get_field_by_name("nanos").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !timestamp_array.is_null(i) {
            let nanos_total = timestamp_array.value(i);
            let (seconds, nanos) = nanos_to_seconds_and_nanos(nanos_total);

            let sub_message = message
                .get_field_mut(field_descriptor)
                .as_message_mut()
                .unwrap();
            sub_message.set_field(&seconds_descriptor, Value::I64(seconds));
            sub_message.set_field(&nanos_descriptor, Value::I32(nanos));
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

    let year_descriptor = message_descriptor.get_field_by_name("year").unwrap();
    let month_descriptor = message_descriptor.get_field_by_name("month").unwrap();
    let day_descriptor = message_descriptor.get_field_by_name("day").unwrap();

    for (i, message) in messages.iter_mut().enumerate() {
        if !date_array.is_null(i) {
            let days = date_array.value(i);
            let date = NaiveDate::from_num_days_from_ce_opt(days + CE_OFFSET).unwrap();

            let sub_message = message
                .get_field_mut(field_descriptor)
                .as_message_mut()
                .unwrap();
            sub_message.set_field(&year_descriptor, Value::I32(date.year()));
            sub_message.set_field(&month_descriptor, Value::I32(date.month() as i32));
            sub_message.set_field(&day_descriptor, Value::I32(date.day() as i32));
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

pub fn extract_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [&mut DynamicMessage],
) {
    if field_descriptor.is_map() {
        // TODO:
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
