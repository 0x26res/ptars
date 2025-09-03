use arrow::array::ArrayData;
use arrow::buffer::Buffer;
use arrow_array::builder::{ArrayBuilder, BinaryBuilder, StringBuilder};
use arrow_array::types::{Date32Type, TimestampNanosecondType};
use arrow_array::{Array, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field};
use chrono::Datelike;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};
use std::collections::BTreeMap;
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
        Kind::Message(message_descriptor) => {
            if message_descriptor.full_name() == "google.protobuf.Timestamp" {
                Ok(Box::new(TimestampArrayBuilder::new(&message_descriptor)))
            } else if message_descriptor.full_name() == "google.type.Date" {
                Ok(Box::new(DateArrayBuilder::new(&message_descriptor)))
            } else {
                Ok(Box::new(MessageArrayBuilder::new(&message_descriptor)))
            }
        }
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
        Kind::Message(message_descriptor) => Ok(Box::new(RepeatedMessageBuilderWrapper::new(
            &message_descriptor,
        ))),
    }
}

pub fn get_array_builder(
    field_descriptor: &FieldDescriptor,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    if field_descriptor.is_list() {
        get_repeated_array_builder(field_descriptor)
    } else if field_descriptor.is_map() {
        Err("map not supported")
    } else {
        get_singular_array_builder(field_descriptor)
    }
}

pub fn singular_field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &[DynamicMessage],
) -> Result<Arc<dyn Array>, &'static str> {
    let builder = get_singular_array_builder(field_descriptor)?;
    Ok(read_primitive(messages, field_descriptor, builder))
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
    let mut builder = get_repeated_array_builder(field_descriptor)?;
    for message in messages {
        builder.append(&message.get_field(field_descriptor));
    }
    Ok(builder.finish())
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

struct MessageArrayBuilder {
    builders: BTreeMap<u32, Box<dyn ProtoArrayBuilder>>,
    message_descriptor: MessageDescriptor,
    is_valid: BooleanBuilder,
}

impl MessageArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        let mut builders = BTreeMap::new();

        for field_descriptor in message_descriptor.fields() {
            let builder = get_array_builder(&field_descriptor).unwrap();
            builders.insert(field_descriptor.number(), builder);
        }

        Self {
            builders,
            message_descriptor: message_descriptor.clone(),
            is_valid: BooleanBuilder::new(),
        }
    }
}

impl ProtoArrayBuilder for MessageArrayBuilder {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            self.is_valid.append_value(true);
            for field_descriptor in self.message_descriptor.fields() {
                let builder = self.builders.get_mut(&field_descriptor.number()).unwrap();
                if field_descriptor.supports_presence() && !message.has_field(&field_descriptor) {
                    builder.append_null();
                } else {
                    builder.append(&message.get_field(&field_descriptor));
                }
            }
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.is_valid.append_value(false);
        for field_descriptor in self.message_descriptor.fields() {
            let builder = self.builders.get_mut(&field_descriptor.number()).unwrap();
            builder.append_null();
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let is_valid = std::mem::take(&mut self.is_valid).finish();
        if self.message_descriptor.fields().next().is_none() {
            return Arc::new(StructArray::new_empty_fields(
                is_valid.len(),
                Some(arrow::buffer::NullBuffer::new(is_valid.values().clone())),
            ));
        }

        let (fields, columns): (Vec<_>, Vec<_>) = self
            .message_descriptor
            .fields()
            .map(|field_descriptor| {
                let builder = self.builders.get_mut(&field_descriptor.number()).unwrap();
                let array = builder.finish();
                let field = Field::new(
                    field_descriptor.name(),
                    array.data_type().clone(),
                    field_descriptor.supports_presence(),
                );
                (field, array)
            })
            .unzip();

        Arc::new(StructArray::new(
            arrow_schema::Fields::from(fields),
            columns,
            Some(arrow::buffer::NullBuffer::new(is_valid.values().clone())),
        ))
    }
}

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

struct RepeatedMessageBuilderWrapper {
    builder: MessageArrayBuilder,
    offsets: Vec<i32>,
}

impl RepeatedMessageBuilderWrapper {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: MessageArrayBuilder::new(message_descriptor),
            offsets,
        }
    }
}

impl ProtoArrayBuilder for RepeatedMessageBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                self.builder.append(each_value);
            }
        }
        self.offsets.push(self.builder.is_valid.len() as i32);
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.is_valid.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = self.builder.finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new(
            "item",
            values.data_type().clone(),
            false,
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

struct TimestampArrayBuilder {
    builder: PrimitiveBuilder<TimestampNanosecondType>,
    seconds_descriptor: FieldDescriptor,
    nanos_descriptor: FieldDescriptor,
}

impl TimestampArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            builder: PrimitiveBuilder::<TimestampNanosecondType>::new(),
            seconds_descriptor: message_descriptor.get_field_by_name("seconds").unwrap(),
            nanos_descriptor: message_descriptor.get_field_by_name("nanos").unwrap(),
        }
    }
}

impl ProtoArrayBuilder for TimestampArrayBuilder {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let seconds = message
                .get_field(&self.seconds_descriptor)
                .as_i64()
                .unwrap();
            let nanos = message.get_field(&self.nanos_descriptor).as_i32().unwrap();
            self.builder
                .append_value(seconds * 1_000_000_000 + i64::from(nanos));
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        Arc::new(std::mem::take(&mut self.builder).finish())
    }
}

struct DateArrayBuilder {
    builder: PrimitiveBuilder<Date32Type>,
    year_descriptor: FieldDescriptor,
    month_descriptor: FieldDescriptor,
    day_descriptor: FieldDescriptor,
}

impl DateArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            builder: PrimitiveBuilder::<Date32Type>::new(),
            year_descriptor: message_descriptor.get_field_by_name("year").unwrap(),
            month_descriptor: message_descriptor.get_field_by_name("month").unwrap(),
            day_descriptor: message_descriptor.get_field_by_name("day").unwrap(),
        }
    }
}

impl ProtoArrayBuilder for DateArrayBuilder {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let year = message.get_field(&self.year_descriptor).as_i32().unwrap();
            let month = message.get_field(&self.month_descriptor).as_i32().unwrap();
            let day = message.get_field(&self.day_descriptor).as_i32().unwrap();

            if year == 0 && month == 0 && day == 0 {
                self.builder.append_value(0);
            } else {
                self.builder.append_value(
                    chrono::NaiveDate::from_ymd_opt(year, month as u32, day as u32)
                        .unwrap()
                        .num_days_from_ce()
                        - CE_OFFSET,
                );
            }
        } else {
            self.append_null();
        }
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
