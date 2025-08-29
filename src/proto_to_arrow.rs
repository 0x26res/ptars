use arrow::array::ArrayData;
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::ArrowNativeType;
use arrow_array::builder::{ArrayBuilder, BinaryBuilder, Int32Builder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, ListArray, PrimitiveArray, RecordBatch,
    Scalar, StructArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field};
use chrono::Datelike;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};
use std::iter::zip;
use std::sync::Arc;

pub trait ProtoArrayBuilder {
    fn append(&mut self, value: &Value);
    fn append_null(&mut self);
    fn finish(&mut self) -> Arc<dyn Array>;
}

pub static CE_OFFSET: i32 = 719163;

use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type};

pub fn get_singular_array_builder(
    field_descriptor: &FieldDescriptor,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    match field_descriptor.kind() {
        Kind::Double => Ok(Box::new(PrimitiveBuilderWrapper::<Float64Type>::new(
            Value::as_f64,
        ))),
        Kind::Float => Ok(Box::new(PrimitiveBuilderWrapper::<Float32Type>::new(
            Value::as_f32,
        ))),
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => Ok(Box::new(PrimitiveBuilderWrapper::<
            Int32Type,
        >::new(Value::as_i32))),
        Kind::Sfixed64 | Kind::Sint64 | Kind::Int64 => Ok(Box::new(PrimitiveBuilderWrapper::<
            Int64Type,
        >::new(Value::as_i64))),
        Kind::Fixed32 | Kind::Uint32 => Ok(Box::new(PrimitiveBuilderWrapper::<UInt32Type>::new(
            Value::as_u32,
        ))),
        Kind::Fixed64 | Kind::Uint64 => Ok(Box::new(PrimitiveBuilderWrapper::<UInt64Type>::new(
            Value::as_u64,
        ))),
        Kind::Bool => Ok(Box::new(BooleanBuilderWrapper::new())),
        Kind::Enum(_) => Ok(Box::new(PrimitiveBuilderWrapper::<Int32Type>::new(
            Value::as_enum_number,
        ))),
        Kind::String => Ok(Box::new(StringBuilderWrapper::new())),
        Kind::Bytes => Ok(Box::new(BinaryBuilderWrapper::new())),
        _ => Err("Unsupported type for singular array builder"),
    }
}

pub fn get_repeated_array_builder(
    field_descriptor: &FieldDescriptor,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    match field_descriptor.kind() {
        Kind::Double => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Float64Type>::new(Value::as_f64),
        )),
        Kind::Float => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Float32Type>::new(Value::as_f32),
        )),
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Int32Type>::new(Value::as_i32),
        )),
        Kind::Sfixed64 | Kind::Sint64 | Kind::Int64 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Int64Type>::new(Value::as_i64),
        )),
        Kind::Fixed32 | Kind::Uint32 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<UInt32Type>::new(Value::as_u32),
        )),
        Kind::Fixed64 | Kind::Uint64 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<UInt64Type>::new(Value::as_u64),
        )),
        Kind::Bool => Ok(Box::new(RepeatedBooleanBuilderWrapper::new())),
        Kind::Enum(_) => Ok(Box::new(RepeatedPrimitiveBuilderWrapper::<Int32Type>::new(
            Value::as_enum_number,
        ))),
        Kind::String => Ok(Box::new(RepeatedStringBuilderWrapper::new())),
        Kind::Bytes => Ok(Box::new(RepeatedBinaryBuilderWrapper::new())),
        _ => Err("Unsupported type for repeated array builder"),
    }
}

pub fn singular_field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &[DynamicMessage],
) -> Result<Arc<dyn Array>, &'static str> {
    if let Ok(builder) = get_singular_array_builder(field_descriptor) {
        return Ok(read_primitive(messages, field_descriptor, builder));
    }
    match field_descriptor.kind() {
        Kind::Message(message_descriptor) => Ok(nested_messages_to_array(
            field_descriptor,
            &message_descriptor,
            messages,
        )),
        _ => Err("Unsupported field type"),
    }
}

pub fn read_i32(message: &DynamicMessage, field_descriptor: &FieldDescriptor) -> i32 {
    message.get_field(field_descriptor).as_i32().unwrap()
}

pub fn convert_date(
    messages: &[DynamicMessage],
    is_valid: &Vec<bool>,
    message_descriptor: &MessageDescriptor,
) -> Arc<Date32Array> {
    let year_descriptor = message_descriptor.get_field_by_name("year").unwrap();
    let month_descriptor = message_descriptor.get_field_by_name("month").unwrap();
    let day_descriptor = message_descriptor.get_field_by_name("day").unwrap();

    let mut builder = Int32Builder::new();
    for (message, message_valid) in zip(messages, is_valid) {
        if *message_valid {
            let year: i32 = read_i32(message, &year_descriptor);
            let month: i32 = read_i32(message, &month_descriptor);
            let day: i32 = read_i32(message, &day_descriptor);

            if (year == 0) && (month == 0) && (day == 0) {
                builder.append_value(0)
            } else {
                builder.append_value(
                    chrono::NaiveDate::from_ymd_opt(
                        year,
                        u32::try_from(month).unwrap(),
                        u32::try_from(day).unwrap(),
                    )
                    .unwrap()
                    .num_days_from_ce()
                        - CE_OFFSET,
                )
            }
        } else {
            builder.append_null()
        }
    }
    Arc::new(builder.finish().reinterpret_cast())
}

pub fn convert_timestamps(
    arrays: &[(Arc<Field>, Arc<dyn Array>)],
    is_valid: &[bool],
) -> Arc<TimestampNanosecondArray> {
    let scalar: Scalar<PrimitiveArray<Int64Type>> = Int64Array::new_scalar(1_000_000_000);
    let seconds: Arc<dyn Array> = arrays[0].clone().1;
    let nanos: Arc<dyn Array> = arrays[1].clone().1;
    let casted = arrow::compute::kernels::cast(&nanos, &DataType::Int64).unwrap();
    let multiplied = arrow::compute::kernels::numeric::mul(&seconds, &scalar).unwrap();
    let total: ArrayRef = arrow::compute::kernels::numeric::add(&multiplied, &casted).unwrap();

    let is_valid_array = BooleanArray::from(is_valid.to_owned());
    let is_null = arrow::compute::not(&is_valid_array).unwrap();
    let total_nullable = arrow::compute::nullif(&total, &is_null).unwrap();
    Arc::new(Int64Array::from(total_nullable.to_data()).reinterpret_cast())
}

pub fn nested_messages_to_array(
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &[DynamicMessage],
) -> Arc<dyn Array> {
    let mut nested_messages: Vec<DynamicMessage> = Vec::new();
    let mut is_valid: Vec<bool> = Vec::new();
    for message in messages {
        is_valid.push(message.has_field(field_descriptor));
        let ee = message.get_field(field_descriptor);
        let each_value = ee.as_message().unwrap();
        nested_messages.push(each_value.clone());
    }
    if message_descriptor.full_name() == "google.type.Date" {
        convert_date(&nested_messages, &is_valid, message_descriptor)
    } else {
        let arrays: Vec<(Arc<Field>, Arc<dyn Array>)> =
            fields_to_arrays(&nested_messages, message_descriptor);
        if arrays.is_empty() {
            Arc::new(StructArray::new_empty_fields(
                nested_messages.len(),
                Some(NullBuffer::from_iter(is_valid)),
            ))
        } else if message_descriptor.full_name() == "google.protobuf.Timestamp" {
            convert_timestamps(&arrays, &is_valid)
        } else {
            Arc::new(StructArray::from((arrays, Buffer::from_iter(is_valid))))
        }
    }
}

pub fn read_primitive(
    messages: &[DynamicMessage],
    field_descriptor: &FieldDescriptor,
    mut builder: Box<dyn ProtoArrayBuilder>,
) -> Arc<dyn Array> {
    for message in messages {
        if field_descriptor.supports_presence() && !message.has_field(field_descriptor) {
            builder.append_null();
        } else {
            builder.append(&message.get_field(field_descriptor));
        }
    }
    builder.finish()
}

pub fn repeated_field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &[DynamicMessage],
) -> Result<Arc<dyn Array>, &'static str> {
    if let Ok(mut builder) = get_repeated_array_builder(field_descriptor) {
        for message in messages {
            builder.append(&message.get_field(field_descriptor));
        }
        return Ok(builder.finish());
    }
    match field_descriptor.kind() {
        Kind::Message(message_descriptor) => {
            read_repeated_messages(field_descriptor, &message_descriptor, messages)
        }
        _ => Err("Unsupported field type"),
    }
}

pub fn read_repeated_messages(
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &[DynamicMessage],
) -> Result<Arc<dyn Array>, &'static str> {
    let mut repeated_messages: Vec<DynamicMessage> = Vec::new();
    let mut offsets: Vec<i32> = Vec::new();
    offsets.push(0);
    for message in messages {
        for each_message in message.get_field(field_descriptor).as_list().unwrap() {
            repeated_messages.push(each_message.as_message().unwrap().clone());
        }
        offsets.push(i32::from_usize(repeated_messages.len()).unwrap());
    }
    let arrays = fields_to_arrays(&repeated_messages, message_descriptor);
    let struct_array: Arc<StructArray> = if arrays.is_empty() {
        Arc::new(StructArray::new_empty_fields(repeated_messages.len(), None))
    } else {
        Arc::new(StructArray::from(arrays))
    };
    let list_data_type = DataType::List(Arc::new(Field::new(
        "item",
        struct_array.data_type().clone(),
        false,
    )));
    let list_data: ArrayData = ArrayData::builder(list_data_type)
        .len(messages.len())
        .add_buffer(Buffer::from_iter(offsets))
        .add_child_data(struct_array.to_data())
        .build()
        .unwrap();
    Ok(Arc::new(ListArray::from(list_data)))
}

pub fn field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &[DynamicMessage],
) -> Result<Arc<dyn Array>, &'static str> {
    if field_descriptor.is_list() {
        repeated_field_to_array(field_descriptor, messages)
    } else if field_descriptor.is_map() {
        Err("map not supported")
    } else {
        singular_field_to_array(field_descriptor, messages)
    }
}

pub fn is_nullable(field: &FieldDescriptor) -> bool {
    field.supports_presence()
}

pub fn field_to_tuple(
    field: &FieldDescriptor,
    messages: &[DynamicMessage],
) -> Result<(Arc<Field>, Arc<dyn Array>), &'static str> {
    let results = field_to_array(field, messages);
    match results {
        Ok(array) => Ok((
            Arc::new(Field::new(
                field.name(),
                array.data_type().clone(),
                is_nullable(field),
            )),
            array,
        )),
        Err(x) => Err(x),
    }
}

pub fn fields_to_arrays(
    messages: &[DynamicMessage],
    message_descriptor: &MessageDescriptor,
) -> Vec<(Arc<Field>, Arc<dyn Array>)> {
    message_descriptor
        .fields()
        .map(|x| field_to_tuple(&x, messages).unwrap())
        .collect()
}

pub fn messages_to_record_batch(
    messages: &[DynamicMessage],
    message_descriptor: &MessageDescriptor,
) -> RecordBatch {
    let arrays: Vec<(Arc<Field>, Arc<dyn Array>)> = fields_to_arrays(messages, message_descriptor);
    let struct_array = if arrays.is_empty() {
        StructArray::new_empty_fields(messages.len(), None)
    } else {
        StructArray::from(arrays)
    };
    RecordBatch::from(struct_array)
}

use arrow_array::builder::BooleanBuilder;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::ArrowPrimitiveType;

struct PrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    builder: PrimitiveBuilder<T>,
    extractor: fn(&Value) -> Option<T::Native>,
}

impl<T> PrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn new(extractor: fn(&Value) -> Option<T::Native>) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::new(),
            extractor,
        }
    }
}

impl<T> ProtoArrayBuilder for PrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn append(&mut self, value: &Value) {
        let v = (self.extractor)(value).unwrap();
        self.builder.append_value(v);
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(std::mem::take(&mut self.builder).finish())
    }
}

struct RepeatedPrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    builder: PrimitiveBuilder<T>,
    offsets: Vec<i32>,
    extractor: fn(&Value) -> Option<T::Native>,
}

impl<T> RepeatedPrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn new(extractor: fn(&Value) -> Option<T::Native>) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: PrimitiveBuilder::<T>::new(),
            offsets,
            extractor,
        }
    }
}

impl<T> ProtoArrayBuilder for RepeatedPrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                self.builder
                    .append_value((self.extractor)(each_value).unwrap());
            }
        }
        self.offsets.push(self.builder.len() as i32);
    }

    fn append_null(&mut self) {
        // For repeated fields, a null value is an empty list.
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            false, // list items are not nullable
        )));

        let list_data = ArrayData::builder(list_data_type)
            .len(offsets_buffer.len() / 4 - 1)
            .add_buffer(offsets_buffer)
            .add_child_data(values.to_data())
            .build()
            .unwrap();

        Arc::new(ListArray::from(list_data))
    }
}

struct RepeatedBooleanBuilderWrapper {
    builder: BooleanBuilder,
    offsets: Vec<i32>,
}

impl RepeatedBooleanBuilderWrapper {
    fn new() -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: BooleanBuilder::new(),
            offsets,
        }
    }
}

impl ProtoArrayBuilder for RepeatedBooleanBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                self.builder.append_value(each_value.as_bool().unwrap());
            }
        }
        self.offsets.push(self.builder.len() as i32);
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Boolean, false)));

        let list_data = ArrayData::builder(list_data_type)
            .len(offsets_buffer.len() / 4 - 1)
            .add_buffer(offsets_buffer)
            .add_child_data(values.to_data())
            .build()
            .unwrap();

        Arc::new(ListArray::from(list_data))
    }
}

struct RepeatedBinaryBuilderWrapper {
    builder: BinaryBuilder,
    offsets: Vec<i32>,
}

impl RepeatedBinaryBuilderWrapper {
    fn new() -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: BinaryBuilder::new(),
            offsets,
        }
    }
}

impl ProtoArrayBuilder for RepeatedBinaryBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                self.builder.append_value(each_value.as_bytes().unwrap());
            }
        }
        self.offsets.push(self.builder.len() as i32);
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Binary, false)));

        let list_data = ArrayData::builder(list_data_type)
            .len(offsets_buffer.len() / 4 - 1)
            .add_buffer(offsets_buffer)
            .add_child_data(values.to_data())
            .build()
            .unwrap();

        Arc::new(ListArray::from(list_data))
    }
}

struct RepeatedStringBuilderWrapper {
    builder: StringBuilder,
    offsets: Vec<i32>,
}

impl RepeatedStringBuilderWrapper {
    fn new() -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: StringBuilder::new(),
            offsets,
        }
    }
}

impl ProtoArrayBuilder for RepeatedStringBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                self.builder.append_value(each_value.as_str().unwrap());
            }
        }
        self.offsets.push(self.builder.len() as i32);
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));

        let list_data = ArrayData::builder(list_data_type)
            .len(offsets_buffer.len() / 4 - 1)
            .add_buffer(offsets_buffer)
            .add_child_data(values.to_data())
            .build()
            .unwrap();

        Arc::new(ListArray::from(list_data))
    }
}

struct BooleanBuilderWrapper {
    builder: BooleanBuilder,
}

impl BooleanBuilderWrapper {
    fn new() -> Self {
        Self {
            builder: BooleanBuilder::new(),
        }
    }
}

impl ProtoArrayBuilder for BooleanBuilderWrapper {
    fn append(&mut self, value: &Value) {
        self.builder.append_value(value.as_bool().unwrap());
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(std::mem::take(&mut self.builder).finish())
    }
}

struct StringBuilderWrapper {
    builder: StringBuilder,
}

impl StringBuilderWrapper {
    fn new() -> Self {
        Self {
            builder: StringBuilder::new(),
        }
    }
}

impl ProtoArrayBuilder for StringBuilderWrapper {
    fn append(&mut self, value: &Value) {
        self.builder.append_value(value.as_str().unwrap());
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(std::mem::take(&mut self.builder).finish())
    }
}

struct BinaryBuilderWrapper {
    builder: BinaryBuilder,
}

impl BinaryBuilderWrapper {
    fn new() -> Self {
        Self {
            builder: BinaryBuilder::new(),
        }
    }
}

impl ProtoArrayBuilder for BinaryBuilderWrapper {
    fn append(&mut self, value: &Value) {
        self.builder.append_value(value.as_bytes().unwrap());
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(std::mem::take(&mut self.builder).finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_timestamps() {
        let seconds_field = Arc::new(Field::new("seconds", DataType::Int64, true));
        let nanos_field = Arc::new(Field::new("nanos", DataType::Int32, true));

        //let seconds = vec![1710330693i64, 1710330702i64];
        let seconds_array: Arc<dyn Array> = Arc::new(arrow::array::Int64Array::from(vec![
            1710330693i64,
            1710330702i64,
            0i64,
        ]));
        let nanos_array: Arc<dyn Array> =
            Arc::new(arrow::array::Int32Array::from(vec![1_000, 123_456_789, 0]));

        let arrays = vec![(seconds_field, seconds_array), (nanos_field, nanos_array)];

        let valid = vec![true, true, false];
        let results = convert_timestamps(&arrays, &valid);
        assert_eq!(results.len(), 3);

        let expected: TimestampNanosecondArray = arrow::array::Int64Array::from(vec![
            1710330693i64 * 1_000_000_000i64 + 1_000i64,
            1710330702i64 * 1_000_000_000i64 + 123_456_789i64,
            0,
        ])
        .reinterpret_cast();

        let mask = BooleanArray::from(vec![false, false, true]);
        let expected_with_null = arrow::compute::nullif(&expected, &mask).unwrap();

        assert_eq!(
            results.as_ref().to_data(),
            expected_with_null.as_ref().to_data()
        )
    }

    #[test]
    fn test_convert_timestamps_empty() {
        let seconds_field = Arc::new(Field::new("seconds", DataType::Int64, true));
        let nanos_field = Arc::new(Field::new("nanos", DataType::Int32, true));

        let seconds_array: Arc<dyn Array> =
            Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new()));
        let nanos_array: Arc<dyn Array> =
            Arc::new(arrow::array::Int32Array::from(Vec::<i32>::new()));

        let arrays = vec![(seconds_field, seconds_array), (nanos_field, nanos_array)];
        let valid: Vec<bool> = vec![];
        let results = convert_timestamps(&arrays, &valid);
        assert_eq!(results.len(), 0);

        let expected: TimestampNanosecondArray =
            arrow::array::Int64Array::from(Vec::<i64>::new()).reinterpret_cast();
        assert_eq!(results.as_ref(), &expected)
    }
}
