use arrow::array::ArrayData;
use arrow::array::MapArray;
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
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
}

pub static CE_OFFSET: i32 = 719163;

use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type};

pub fn get_message_array_builder(
    message_descriptor: &MessageDescriptor,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    if message_descriptor.full_name() == "google.protobuf.Timestamp" {
        Ok(Box::new(TimestampArrayBuilder::new(message_descriptor)))
    } else if message_descriptor.full_name() == "google.type.Date" {
        Ok(Box::new(DateArrayBuilder::new(message_descriptor)))
    } else {
        Ok(Box::new(MessageArrayBuilder::new(message_descriptor)))
    }
}

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
        Kind::Message(message_descriptor) => get_message_array_builder(&message_descriptor),
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
        Kind::Message(message_descriptor) => {
            let message_builder = get_message_array_builder(&message_descriptor)?;
            Ok(Box::new(RepeatedMessageBuilderWrapper::new(
                message_builder,
            )))
        }
    }
}

pub fn get_array_builder(
    field_descriptor: &FieldDescriptor,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    if field_descriptor.is_map() {
        if let Kind::Message(message_descriptor) = field_descriptor.kind() {
            Ok(Box::new(MapArrayBuilder::new(&message_descriptor)))
        } else {
            Err("map field is not a message")
        }
    } else if field_descriptor.is_list() {
        get_repeated_array_builder(field_descriptor)
    } else {
        get_singular_array_builder(field_descriptor)
    }
}

pub fn field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &[DynamicMessage],
) -> Result<Arc<dyn Array>, &'static str> {
    let mut builder = get_array_builder(field_descriptor)?;
    for message in messages {
        if field_descriptor.supports_presence() && !message.has_field(field_descriptor) {
            builder.append_null();
        } else {
            builder.append(&message.get_field(field_descriptor));
        }
    }
    Ok(builder.finish())
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

use arrow_array::builder::BooleanBuilder;

use arrow_array::builder::PrimitiveBuilder;
use arrow_array::ArrowPrimitiveType;

struct MapArrayBuilder {
    key_builder: Box<dyn ProtoArrayBuilder>,
    value_builder: Box<dyn ProtoArrayBuilder>,
    key_field_descriptor: FieldDescriptor,
    value_field_descriptor: FieldDescriptor,
    offsets: Vec<i32>,
}

impl MapArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        let key_field_descriptor = message_descriptor.get_field_by_name("key").unwrap();
        let value_field_descriptor = message_descriptor.get_field_by_name("value").unwrap();
        let key_builder = get_singular_array_builder(&key_field_descriptor).unwrap();
        let value_builder = get_singular_array_builder(&value_field_descriptor).unwrap();
        Self {
            key_builder,
            value_builder,
            key_field_descriptor,
            value_field_descriptor,
            offsets: vec![0],
        }
    }
}

impl ProtoArrayBuilder for MapArrayBuilder {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                let message = each_value.as_message().unwrap();
                self.key_builder
                    .append(&message.get_field(&self.key_field_descriptor));
                self.value_builder
                    .append(&message.get_field(&self.value_field_descriptor));
            }
        }
        let last_offset = *self.offsets.last().unwrap();
        self.offsets
            .push(last_offset + value.as_list().map_or(0, |v| v.len() as i32));
    }

    fn append_null(&mut self) {
        let last_offset = *self.offsets.last().unwrap();
        self.offsets.push(last_offset);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let key_array = self.key_builder.finish();
        let value_array = self.value_builder.finish();

        let key_field = Arc::new(Field::new(
            "key",
            key_array.data_type().clone(),
            is_nullable(&self.key_field_descriptor),
        ));
        let value_field = Arc::new(Field::new(
            "value",
            value_array.data_type().clone(),
            is_nullable(&self.value_field_descriptor),
        ));

        let entry_struct =
            StructArray::from(vec![(key_field, key_array), (value_field, value_array)]);

        let map_data_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                entry_struct.data_type().clone(),
                false,
            )),
            false,
        );

        let len = self.offsets.len() - 1;
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let map_data = ArrayData::builder(map_data_type)
            .len(len)
            .add_buffer(offsets_buffer)
            .add_child_data(entry_struct.into_data())
            .build()
            .unwrap();

        Arc::new(MapArray::from(map_data))
    }

    fn len(&self) -> usize {
        self.key_builder.len()
    }

    fn is_empty(&self) -> bool {
        self.key_builder.is_empty()
    }
}

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

    fn append(&mut self, message: &DynamicMessage) {
        self.is_valid.append_value(true);
        for field_descriptor in self.message_descriptor.fields() {
            let builder = self.builders.get_mut(&field_descriptor.number()).unwrap();
            if field_descriptor.supports_presence() && !message.has_field(&field_descriptor) {
                builder.append_null();
            } else {
                builder.append(&message.get_field(&field_descriptor));
            }
        }
    }

    fn build_struct_array(&mut self) -> StructArray {
        let is_valid = std::mem::take(&mut self.is_valid).finish();
        if self.message_descriptor.fields().next().is_none() {
            return StructArray::new_empty_fields(
                is_valid.len(),
                Some(arrow::buffer::NullBuffer::new(is_valid.values().clone())),
            );
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

        StructArray::new(
            arrow_schema::Fields::from(fields),
            columns,
            Some(arrow::buffer::NullBuffer::new(is_valid.values().clone())),
        )
    }
}

impl ProtoArrayBuilder for MessageArrayBuilder {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            self.append(message);
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
        Arc::new(self.build_struct_array())
    }

    fn len(&self) -> usize {
        self.is_valid.len()
    }

    fn is_empty(&self) -> bool {
        self.is_valid.is_empty()
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

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
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

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.iter().len() == 1
    }
}

struct RepeatedMessageBuilderWrapper {
    builder: Box<dyn ProtoArrayBuilder>,
    offsets: Vec<i32>,
}

impl RepeatedMessageBuilderWrapper {
    fn new(builder: Box<dyn ProtoArrayBuilder>) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self { builder, offsets }
    }
}

impl ProtoArrayBuilder for RepeatedMessageBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                self.builder.append(each_value);
            }
        }
        self.offsets.push(self.builder.len() as i32);
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.len() as i32);
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

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() == 1
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

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() == 1
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

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() == 1
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

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() == 1
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

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
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

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
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

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
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

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
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

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }
}

pub fn messages_to_record_batch(
    messages: &[DynamicMessage],
    message_descriptor: &MessageDescriptor,
) -> RecordBatch {
    let mut builder = MessageArrayBuilder::new(message_descriptor);
    messages.iter().for_each(|message| {
        builder.append(message);
    });
    RecordBatch::from(builder.build_struct_array())
}
