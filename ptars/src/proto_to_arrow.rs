use arrow::array::ArrayData;
use arrow::array::MapArray;
use arrow::buffer::Buffer;
use arrow_array::builder::{ArrayBuilder, BinaryBuilder, StringBuilder};
use arrow_array::types::Date32Type;
use arrow_array::{Array, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::Datelike;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};
use std::sync::Arc;

use crate::config::PtarsConfig;

pub trait ProtoArrayBuilder {
    fn append(&mut self, value: &Value);
    fn append_null(&mut self);
    /// Append the protobuf default value for this field type.
    /// For primitives: 0, false, "", b""
    /// For messages: null (messages don't have a non-null default)
    /// For repeated: empty list
    fn append_default(&mut self);
    fn finish(&mut self) -> Arc<dyn Array>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
}

pub static CE_OFFSET: i32 = 719163;

use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type, UInt32Type, UInt64Type};

pub fn get_message_array_builder(
    message_descriptor: &MessageDescriptor,
    config: &PtarsConfig,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    match message_descriptor.full_name() {
        "google.protobuf.Timestamp" => Ok(Box::new(TimestampArrayBuilder::new(
            message_descriptor,
            config.timestamp_tz.clone(),
            config.timestamp_unit,
        ))),
        "google.type.Date" => Ok(Box::new(DateArrayBuilder::new(message_descriptor))),
        "google.type.TimeOfDay" => Ok(Box::new(TimeOfDayArrayBuilder::new(
            message_descriptor,
            config.time_unit,
        ))),
        "google.protobuf.Duration" => Ok(Box::new(DurationArrayBuilder::new(
            message_descriptor,
            config.duration_unit,
        ))),
        // Wrapper types - stored as nullable primitives
        "google.protobuf.DoubleValue" => Ok(Box::new(WrapperBuilderWrapper::<Float64Type>::new(
            message_descriptor,
            Value::as_f64,
        ))),
        "google.protobuf.FloatValue" => Ok(Box::new(WrapperBuilderWrapper::<Float32Type>::new(
            message_descriptor,
            Value::as_f32,
        ))),
        "google.protobuf.Int64Value" => Ok(Box::new(WrapperBuilderWrapper::<Int64Type>::new(
            message_descriptor,
            Value::as_i64,
        ))),
        "google.protobuf.UInt64Value" => Ok(Box::new(WrapperBuilderWrapper::<UInt64Type>::new(
            message_descriptor,
            Value::as_u64,
        ))),
        "google.protobuf.Int32Value" => Ok(Box::new(WrapperBuilderWrapper::<Int32Type>::new(
            message_descriptor,
            Value::as_i32,
        ))),
        "google.protobuf.UInt32Value" => Ok(Box::new(WrapperBuilderWrapper::<UInt32Type>::new(
            message_descriptor,
            Value::as_u32,
        ))),
        "google.protobuf.BoolValue" => {
            Ok(Box::new(BoolWrapperBuilderWrapper::new(message_descriptor)))
        }
        "google.protobuf.StringValue" => Ok(Box::new(StringWrapperBuilderWrapper::new(
            message_descriptor,
        ))),
        "google.protobuf.BytesValue" => Ok(Box::new(BytesWrapperBuilderWrapper::new(
            message_descriptor,
        ))),
        _ => Ok(Box::new(MessageArrayBuilder::new(
            message_descriptor,
            config,
        ))),
    }
}

pub fn get_singular_array_builder(
    field_descriptor: &FieldDescriptor,
    config: &PtarsConfig,
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
        Kind::Message(message_descriptor) => get_message_array_builder(&message_descriptor, config),
    }
}

pub fn get_repeated_array_builder(
    field_descriptor: &FieldDescriptor,
    config: &PtarsConfig,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    match field_descriptor.kind() {
        Kind::Double => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Float64Type>::new(Value::as_f64, config),
        )),
        Kind::Float => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Float32Type>::new(Value::as_f32, config),
        )),
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Int32Type>::new(Value::as_i32, config),
        )),
        Kind::Sfixed64 | Kind::Sint64 | Kind::Int64 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<Int64Type>::new(Value::as_i64, config),
        )),
        Kind::Fixed32 | Kind::Uint32 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<UInt32Type>::new(Value::as_u32, config),
        )),
        Kind::Fixed64 | Kind::Uint64 => Ok(Box::new(
            RepeatedPrimitiveBuilderWrapper::<UInt64Type>::new(Value::as_u64, config),
        )),
        Kind::Bool => Ok(Box::new(RepeatedBooleanBuilderWrapper::new(config))),
        Kind::Enum(_) => Ok(Box::new(RepeatedPrimitiveBuilderWrapper::<Int32Type>::new(
            Value::as_enum_number,
            config,
        ))),
        Kind::String => Ok(Box::new(RepeatedStringBuilderWrapper::new(config))),
        Kind::Bytes => Ok(Box::new(RepeatedBinaryBuilderWrapper::new(config))),
        Kind::Message(message_descriptor) => {
            let message_builder = get_message_array_builder(&message_descriptor, config)?;
            Ok(Box::new(RepeatedMessageBuilderWrapper::new(
                message_builder,
                config,
            )))
        }
    }
}

pub fn get_array_builder(
    field_descriptor: &FieldDescriptor,
    config: &PtarsConfig,
) -> Result<Box<dyn ProtoArrayBuilder>, &'static str> {
    if field_descriptor.is_map() {
        if let Kind::Message(message_descriptor) = field_descriptor.kind() {
            Ok(Box::new(MapArrayBuilder::new(&message_descriptor, config)))
        } else {
            Err("map field is not a message")
        }
    } else if field_descriptor.is_list() {
        get_repeated_array_builder(field_descriptor, config)
    } else {
        get_singular_array_builder(field_descriptor, config)
    }
}

pub fn field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &[DynamicMessage],
    config: &PtarsConfig,
) -> Result<Arc<dyn Array>, &'static str> {
    let mut builder = get_array_builder(field_descriptor, config)?;
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
    config: &PtarsConfig,
) -> Result<(Arc<Field>, Arc<dyn Array>), &'static str> {
    let results = field_to_array(field, messages, config);
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
    config: &PtarsConfig,
) -> Vec<(Arc<Field>, Arc<dyn Array>)> {
    message_descriptor
        .fields()
        .map(|x| field_to_tuple(&x, messages, config).unwrap())
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
    map_value_name: Arc<str>,
    map_value_nullable: bool,
}

impl MapArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor, config: &PtarsConfig) -> Self {
        let key_field_descriptor = message_descriptor.get_field_by_name("key").unwrap();
        let value_field_descriptor = message_descriptor.get_field_by_name("value").unwrap();
        let key_builder = get_singular_array_builder(&key_field_descriptor, config).unwrap();
        let value_builder = get_singular_array_builder(&value_field_descriptor, config).unwrap();
        Self {
            key_builder,
            value_builder,
            key_field_descriptor,
            value_field_descriptor,
            offsets: vec![0],
            map_value_name: config.map_value_name.clone(),
            map_value_nullable: config.map_value_nullable,
        }
    }
}

impl ProtoArrayBuilder for MapArrayBuilder {
    fn append(&mut self, value: &Value) {
        let entry_count = if let Some(map) = value.as_map() {
            // Handle Value::Map (when map is set directly)
            for (key, val) in map {
                let key_value = match key {
                    prost_reflect::MapKey::Bool(b) => Value::Bool(*b),
                    prost_reflect::MapKey::I32(i) => Value::I32(*i),
                    prost_reflect::MapKey::I64(i) => Value::I64(*i),
                    prost_reflect::MapKey::U32(u) => Value::U32(*u),
                    prost_reflect::MapKey::U64(u) => Value::U64(*u),
                    prost_reflect::MapKey::String(s) => Value::String(s.clone()),
                };
                self.key_builder.append(&key_value);
                self.value_builder.append(val);
            }
            map.len() as i32
        } else if let Some(values) = value.as_list() {
            // Handle Value::List of entry messages (when map comes as repeated message entries)
            for each_value in values {
                let message = each_value.as_message().unwrap();
                self.key_builder
                    .append(&message.get_field(&self.key_field_descriptor));
                self.value_builder
                    .append(&message.get_field(&self.value_field_descriptor));
            }
            values.len() as i32
        } else {
            0
        };
        let last_offset = *self.offsets.last().unwrap();
        self.offsets.push(last_offset + entry_count);
    }

    fn append_null(&mut self) {
        let last_offset = *self.offsets.last().unwrap();
        self.offsets.push(last_offset);
    }

    fn append_default(&mut self) {
        // Default for map is empty map
        let last_offset = *self.offsets.last().unwrap();
        self.offsets.push(last_offset);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let key_array = self.key_builder.finish();
        let value_array = self.value_builder.finish();

        let key_field = Arc::new(Field::new(
            "key",
            key_array.data_type().clone(),
            false, // map keys are not nullable in protobuf
        ));
        let value_field = Arc::new(Field::new(
            &*self.map_value_name,
            value_array.data_type().clone(),
            self.map_value_nullable,
        ));

        // Build the struct type explicitly to preserve field names
        let entries_struct_type =
            DataType::Struct(vec![key_field.as_ref().clone(), value_field.as_ref().clone()].into());

        let entry_struct =
            StructArray::from(vec![(key_field, key_array), (value_field, value_array)]);

        let map_data_type = DataType::Map(
            Arc::new(Field::new("entries", entries_struct_type, false)),
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
    /// Cached field descriptors and their builders, avoiding repeated iteration
    fields: Vec<(FieldDescriptor, Box<dyn ProtoArrayBuilder>)>,
    is_valid: BooleanBuilder,
    list_nullable: bool,
    map_nullable: bool,
}

impl MessageArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor, config: &PtarsConfig) -> Self {
        let fields: Vec<_> = message_descriptor
            .fields()
            .map(|field_descriptor| {
                let builder = get_array_builder(&field_descriptor, config).unwrap();
                (field_descriptor, builder)
            })
            .collect();

        Self {
            fields,
            is_valid: BooleanBuilder::new(),
            list_nullable: config.list_nullable,
            map_nullable: config.map_nullable,
        }
    }

    fn append(&mut self, message: &DynamicMessage) {
        self.is_valid.append_value(true);
        for (field_descriptor, builder) in &mut self.fields {
            if field_descriptor.supports_presence() && !message.has_field(field_descriptor) {
                builder.append_null();
            } else {
                builder.append(&message.get_field(field_descriptor));
            }
        }
    }

    fn build_struct_array(&mut self) -> StructArray {
        let is_valid = std::mem::take(&mut self.is_valid).finish();
        if self.fields.is_empty() {
            return StructArray::new_empty_fields(
                is_valid.len(),
                Some(arrow::buffer::NullBuffer::new(is_valid.values().clone())),
            );
        }

        let (fields, columns): (Vec<_>, Vec<_>) = self
            .fields
            .iter_mut()
            .map(|(field_descriptor, builder)| {
                let array = builder.finish();
                let nullable = if field_descriptor.is_list() {
                    self.list_nullable
                } else if field_descriptor.is_map() {
                    self.map_nullable
                } else {
                    field_descriptor.supports_presence()
                };
                let field =
                    Field::new(field_descriptor.name(), array.data_type().clone(), nullable);
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
        // Use append_default for child fields to be consistent with protobuf semantics:
        // when a message is absent, its fields have their default values
        for (_, builder) in &mut self.fields {
            builder.append_default();
        }
    }

    fn append_default(&mut self) {
        // Default for message is null (absent)
        self.append_null();
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
    T::Native: Default,
{
    fn append(&mut self, value: &Value) {
        let v = (self.extractor)(value).unwrap();
        self.builder.append_value(v);
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        self.builder.append_value(T::Native::default());
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
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl<T> RepeatedPrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn new(extractor: fn(&Value) -> Option<T::Native>, config: &PtarsConfig) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: PrimitiveBuilder::<T>::new(),
            offsets,
            extractor,
            list_value_name: config.list_value_name.clone(),
            list_value_nullable: config.list_value_nullable,
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

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new(
            &*self.list_value_name,
            values.data_type().clone(),
            self.list_value_nullable,
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
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedMessageBuilderWrapper {
    fn new(builder: Box<dyn ProtoArrayBuilder>, config: &PtarsConfig) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder,
            offsets,
            list_value_name: config.list_value_name.clone(),
            list_value_nullable: config.list_value_nullable,
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
        self.offsets.push(self.builder.len() as i32);
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.len() as i32);
    }

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = self.builder.finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new(
            &*self.list_value_name,
            values.data_type().clone(),
            self.list_value_nullable,
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
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedBooleanBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: BooleanBuilder::new(),
            offsets,
            list_value_name: config.list_value_name.clone(),
            list_value_nullable: config.list_value_nullable,
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

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new(
            &*self.list_value_name,
            DataType::Boolean,
            self.list_value_nullable,
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

struct RepeatedBinaryBuilderWrapper {
    builder: BinaryBuilder,
    offsets: Vec<i32>,
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedBinaryBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: BinaryBuilder::new(),
            offsets,
            list_value_name: config.list_value_name.clone(),
            list_value_nullable: config.list_value_nullable,
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

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new(
            &*self.list_value_name,
            DataType::Binary,
            self.list_value_nullable,
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

struct RepeatedStringBuilderWrapper {
    builder: StringBuilder,
    offsets: Vec<i32>,
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedStringBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        let offsets: Vec<i32> = vec![0];
        Self {
            builder: StringBuilder::new(),
            offsets,
            list_value_name: config.list_value_name.clone(),
            list_value_nullable: config.list_value_nullable,
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

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len() as i32);
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let offsets_buffer = Buffer::from_vec(std::mem::take(&mut self.offsets));

        let list_data_type = DataType::List(Arc::new(Field::new(
            &*self.list_value_name,
            DataType::Utf8,
            self.list_value_nullable,
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

    fn append_default(&mut self) {
        self.builder.append_value(false);
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
    builder: PrimitiveBuilder<Int64Type>,
    seconds_descriptor: FieldDescriptor,
    nanos_descriptor: FieldDescriptor,
    timezone: Option<Arc<str>>,
    time_unit: TimeUnit,
}

impl TimestampArrayBuilder {
    fn new(
        message_descriptor: &MessageDescriptor,
        timezone: Option<Arc<str>>,
        time_unit: TimeUnit,
    ) -> Self {
        Self {
            builder: PrimitiveBuilder::<Int64Type>::new(),
            seconds_descriptor: message_descriptor.get_field_by_name("seconds").unwrap(),
            nanos_descriptor: message_descriptor.get_field_by_name("nanos").unwrap(),
            timezone,
            time_unit,
        }
    }

    fn convert_to_unit(&self, seconds: i64, nanos: i32) -> i64 {
        match self.time_unit {
            TimeUnit::Second => seconds,
            TimeUnit::Millisecond => seconds * 1_000 + i64::from(nanos) / 1_000_000,
            TimeUnit::Microsecond => seconds * 1_000_000 + i64::from(nanos) / 1_000,
            TimeUnit::Nanosecond => seconds * 1_000_000_000 + i64::from(nanos),
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
                .append_value(self.convert_to_unit(seconds, nanos));
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        // Timestamp is a message type, default is null
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let data_type = DataType::Timestamp(self.time_unit, self.timezone.clone());

        let array_data = ArrayData::builder(data_type)
            .len(values.len())
            .add_buffer(values.values().inner().clone())
            .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
            .build()
            .unwrap();

        arrow_array::make_array(array_data)
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

    fn append_default(&mut self) {
        // Date is a message type, default is null
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

struct TimeOfDayArrayBuilder {
    builder: PrimitiveBuilder<Int64Type>,
    hours_descriptor: FieldDescriptor,
    minutes_descriptor: FieldDescriptor,
    seconds_descriptor: FieldDescriptor,
    nanos_descriptor: FieldDescriptor,
    time_unit: TimeUnit,
}

impl TimeOfDayArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor, time_unit: TimeUnit) -> Self {
        Self {
            builder: PrimitiveBuilder::<Int64Type>::new(),
            hours_descriptor: message_descriptor.get_field_by_name("hours").unwrap(),
            minutes_descriptor: message_descriptor.get_field_by_name("minutes").unwrap(),
            seconds_descriptor: message_descriptor.get_field_by_name("seconds").unwrap(),
            nanos_descriptor: message_descriptor.get_field_by_name("nanos").unwrap(),
            time_unit,
        }
    }

    fn convert_to_unit(&self, hours: i64, minutes: i64, seconds: i64, nanos: i64) -> i64 {
        let total_seconds = hours * 3600 + minutes * 60 + seconds;
        match self.time_unit {
            TimeUnit::Second => total_seconds,
            TimeUnit::Millisecond => total_seconds * 1_000 + nanos / 1_000_000,
            TimeUnit::Microsecond => total_seconds * 1_000_000 + nanos / 1_000,
            TimeUnit::Nanosecond => total_seconds * 1_000_000_000 + nanos,
        }
    }
}

impl ProtoArrayBuilder for TimeOfDayArrayBuilder {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let hours = message.get_field(&self.hours_descriptor).as_i32().unwrap() as i64;
            let minutes = message
                .get_field(&self.minutes_descriptor)
                .as_i32()
                .unwrap() as i64;
            let seconds = message
                .get_field(&self.seconds_descriptor)
                .as_i32()
                .unwrap() as i64;
            let nanos = message.get_field(&self.nanos_descriptor).as_i32().unwrap() as i64;

            self.builder
                .append_value(self.convert_to_unit(hours, minutes, seconds, nanos));
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        // TimeOfDay is a message type, default is null
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();

        // Time32 for Second/Millisecond, Time64 for Microsecond/Nanosecond
        let data_type = match self.time_unit {
            TimeUnit::Second => DataType::Time32(TimeUnit::Second),
            TimeUnit::Millisecond => DataType::Time32(TimeUnit::Millisecond),
            TimeUnit::Microsecond => DataType::Time64(TimeUnit::Microsecond),
            TimeUnit::Nanosecond => DataType::Time64(TimeUnit::Nanosecond),
        };

        // For Time32, we need to cast from i64 to i32
        // Time of day values should always fit in i32:
        // - Seconds: max 86400 (24*60*60)
        // - Milliseconds: max 86400000 (24*60*60*1000)
        // Both fit well within i32::MAX (2147483647)
        if matches!(self.time_unit, TimeUnit::Second | TimeUnit::Millisecond) {
            let i32_values: Vec<Option<i32>> = (0..values.len())
                .map(|i| {
                    if values.is_null(i) {
                        None
                    } else {
                        let val = values.value(i);
                        // Use try_from to safely convert, clamping out-of-range values
                        // Valid time-of-day values will always fit, but malformed proto
                        // inputs might not
                        Some(i32::try_from(val).unwrap_or(if val > 0 {
                            i32::MAX
                        } else {
                            i32::MIN
                        }))
                    }
                })
                .collect();
            let i32_array = arrow_array::Int32Array::from(i32_values);

            let array_data = ArrayData::builder(data_type)
                .len(i32_array.len())
                .add_buffer(i32_array.values().inner().clone())
                .null_bit_buffer(i32_array.nulls().map(|n| n.buffer().clone()))
                .build()
                .unwrap();

            arrow_array::make_array(array_data)
        } else {
            let array_data = ArrayData::builder(data_type)
                .len(values.len())
                .add_buffer(values.values().inner().clone())
                .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
                .build()
                .unwrap();

            arrow_array::make_array(array_data)
        }
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }
}

struct DurationArrayBuilder {
    builder: PrimitiveBuilder<Int64Type>,
    seconds_descriptor: FieldDescriptor,
    nanos_descriptor: FieldDescriptor,
    time_unit: TimeUnit,
}

impl DurationArrayBuilder {
    fn new(message_descriptor: &MessageDescriptor, time_unit: TimeUnit) -> Self {
        Self {
            builder: PrimitiveBuilder::<Int64Type>::new(),
            seconds_descriptor: message_descriptor.get_field_by_name("seconds").unwrap(),
            nanos_descriptor: message_descriptor.get_field_by_name("nanos").unwrap(),
            time_unit,
        }
    }

    fn convert_to_unit(&self, seconds: i64, nanos: i32) -> i64 {
        match self.time_unit {
            TimeUnit::Second => seconds,
            TimeUnit::Millisecond => seconds * 1_000 + i64::from(nanos) / 1_000_000,
            TimeUnit::Microsecond => seconds * 1_000_000 + i64::from(nanos) / 1_000,
            TimeUnit::Nanosecond => seconds * 1_000_000_000 + i64::from(nanos),
        }
    }
}

impl ProtoArrayBuilder for DurationArrayBuilder {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let seconds = message
                .get_field(&self.seconds_descriptor)
                .as_i64()
                .unwrap();
            let nanos = message.get_field(&self.nanos_descriptor).as_i32().unwrap();
            self.builder
                .append_value(self.convert_to_unit(seconds, nanos));
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        // Duration is a message type, default is null
        self.builder.append_null();
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = std::mem::take(&mut self.builder).finish();
        let data_type = DataType::Duration(self.time_unit);

        let array_data = ArrayData::builder(data_type)
            .len(values.len())
            .add_buffer(values.values().inner().clone())
            .null_bit_buffer(values.nulls().map(|n| n.buffer().clone()))
            .build()
            .unwrap();

        arrow_array::make_array(array_data)
    }

    fn len(&self) -> usize {
        self.builder.len()
    }

    fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }
}

// Wrapper type builders for google.protobuf wrapper types (DoubleValue, Int32Value, etc.)

struct WrapperBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    builder: PrimitiveBuilder<T>,
    value_descriptor: FieldDescriptor,
    extractor: fn(&Value) -> Option<T::Native>,
}

impl<T> WrapperBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn new(
        message_descriptor: &MessageDescriptor,
        extractor: fn(&Value) -> Option<T::Native>,
    ) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::new(),
            value_descriptor: message_descriptor.get_field_by_name("value").unwrap(),
            extractor,
        }
    }
}

impl<T> ProtoArrayBuilder for WrapperBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let v = message.get_field(&self.value_descriptor);
            self.builder.append_value((self.extractor)(&v).unwrap());
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        // Wrapper types are message types, default is null
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

struct BoolWrapperBuilderWrapper {
    builder: BooleanBuilder,
    value_descriptor: FieldDescriptor,
}

impl BoolWrapperBuilderWrapper {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            builder: BooleanBuilder::new(),
            value_descriptor: message_descriptor.get_field_by_name("value").unwrap(),
        }
    }
}

impl ProtoArrayBuilder for BoolWrapperBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let v = message.get_field(&self.value_descriptor);
            self.builder.append_value(v.as_bool().unwrap());
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        // Wrapper types are message types, default is null
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

struct StringWrapperBuilderWrapper {
    builder: StringBuilder,
    value_descriptor: FieldDescriptor,
}

impl StringWrapperBuilderWrapper {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            builder: StringBuilder::new(),
            value_descriptor: message_descriptor.get_field_by_name("value").unwrap(),
        }
    }
}

impl ProtoArrayBuilder for StringWrapperBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let v = message.get_field(&self.value_descriptor);
            self.builder.append_value(v.as_str().unwrap());
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        // Wrapper types are message types, default is null
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

struct BytesWrapperBuilderWrapper {
    builder: BinaryBuilder,
    value_descriptor: FieldDescriptor,
}

impl BytesWrapperBuilderWrapper {
    fn new(message_descriptor: &MessageDescriptor) -> Self {
        Self {
            builder: BinaryBuilder::new(),
            value_descriptor: message_descriptor.get_field_by_name("value").unwrap(),
        }
    }
}

impl ProtoArrayBuilder for BytesWrapperBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let v = message.get_field(&self.value_descriptor);
            self.builder.append_value(v.as_bytes().unwrap());
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        self.builder.append_null();
    }

    fn append_default(&mut self) {
        // Wrapper types are message types, default is null
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

    fn append_default(&mut self) {
        self.builder.append_value("");
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

    fn append_default(&mut self) {
        self.builder.append_value(b"");
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

/// Convert messages to a RecordBatch using the default configuration.
pub fn messages_to_record_batch(
    messages: &[DynamicMessage],
    message_descriptor: &MessageDescriptor,
) -> RecordBatch {
    messages_to_record_batch_with_config(messages, message_descriptor, &PtarsConfig::default())
}

/// Convert messages to a RecordBatch using the specified configuration.
pub fn messages_to_record_batch_with_config(
    messages: &[DynamicMessage],
    message_descriptor: &MessageDescriptor,
    config: &PtarsConfig,
) -> RecordBatch {
    let mut builder = MessageArrayBuilder::new(message_descriptor, config);
    messages.iter().for_each(|message| {
        builder.append(message);
    });
    RecordBatch::from(builder.build_struct_array())
}

use arrow_array::BinaryArray;

/// Convert a binary array to a vector of DynamicMessage.
///
/// Each element in the binary array is expected to be a serialized protobuf message.
/// Null values in the array will result in default (empty) messages.
pub fn binary_array_to_messages(
    array: &BinaryArray,
    message_descriptor: &MessageDescriptor,
) -> Result<Vec<DynamicMessage>, prost::DecodeError> {
    let mut messages = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        let message = if array.is_null(i) {
            DynamicMessage::new(message_descriptor.clone())
        } else {
            let bytes = array.value(i);
            DynamicMessage::decode(message_descriptor.clone(), bytes)?
        };
        messages.push(message);
    }
    Ok(messages)
}

/// Convert a binary array to a record batch using the default configuration.
///
/// Each element in the binary array is expected to be a serialized protobuf message.
/// The resulting record batch will have one column per field in the message descriptor.
pub fn binary_array_to_record_batch(
    array: &BinaryArray,
    message_descriptor: &MessageDescriptor,
) -> Result<RecordBatch, prost::DecodeError> {
    binary_array_to_record_batch_with_config(array, message_descriptor, &PtarsConfig::default())
}

/// Convert a binary array to a record batch using the specified configuration.
///
/// Each element in the binary array is expected to be a serialized protobuf message.
/// The resulting record batch will have one column per field in the message descriptor.
pub fn binary_array_to_record_batch_with_config(
    array: &BinaryArray,
    message_descriptor: &MessageDescriptor,
    config: &PtarsConfig,
) -> Result<RecordBatch, prost::DecodeError> {
    let mut builder = MessageArrayBuilder::new(message_descriptor, config);
    for i in 0..array.len() {
        let message = if array.is_null(i) {
            DynamicMessage::new(message_descriptor.clone())
        } else {
            let bytes = array.value(i);
            DynamicMessage::decode(message_descriptor.clone(), bytes)?
        };
        builder.append(&message);
    }
    Ok(RecordBatch::from(builder.build_struct_array()))
}
