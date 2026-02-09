use arrow::array::ArrayData;
use arrow::array::MapArray;
use arrow::buffer::Buffer;
use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder,
};
use arrow_array::types::Date32Type;
use arrow_array::{Array, LargeListArray, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, TimeUnit};
use chrono::Datelike;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};
use std::sync::Arc;

use crate::config::PtarsConfig;

enum StringBuilderInner {
    Regular(StringBuilder),
    Large(LargeStringBuilder),
}

enum BinaryBuilderInner {
    Regular(BinaryBuilder),
    Large(LargeBinaryBuilder),
}

/// Helper enum to handle both regular (i32) and large (i64) list offsets
enum ListOffsets {
    Regular(Vec<i32>),
    Large(Vec<i64>),
}

impl ListOffsets {
    fn new(use_large_list: bool) -> Self {
        if use_large_list {
            ListOffsets::Large(vec![0])
        } else {
            ListOffsets::Regular(vec![0])
        }
    }

    fn push(&mut self, value: usize) {
        match self {
            ListOffsets::Regular(v) => v.push(value as i32),
            ListOffsets::Large(v) => v.push(value as i64),
        }
    }

    fn len(&self) -> usize {
        match self {
            ListOffsets::Regular(v) => v.len(),
            ListOffsets::Large(v) => v.len(),
        }
    }

    fn finish(
        self,
        values: Arc<dyn Array>,
        list_value_name: &str,
        list_value_nullable: bool,
    ) -> Arc<dyn Array> {
        let field = Arc::new(Field::new(
            list_value_name,
            values.data_type().clone(),
            list_value_nullable,
        ));

        match self {
            ListOffsets::Regular(offsets) => {
                let offsets_buffer = Buffer::from_vec(offsets);
                let list_data_type = DataType::List(field);
                let list_data = ArrayData::builder(list_data_type)
                    .len(offsets_buffer.len() / 4 - 1)
                    .add_buffer(offsets_buffer)
                    .add_child_data(values.to_data())
                    .build()
                    .unwrap();
                Arc::new(ListArray::from(list_data))
            }
            ListOffsets::Large(offsets) => {
                let offsets_buffer = Buffer::from_vec(offsets);
                let list_data_type = DataType::LargeList(field);
                let list_data = ArrayData::builder(list_data_type)
                    .len(offsets_buffer.len() / 8 - 1)
                    .add_buffer(offsets_buffer)
                    .add_child_data(values.to_data())
                    .build()
                    .unwrap();
                Arc::new(LargeListArray::from(list_data))
            }
        }
    }
}

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
            config,
        ))),
        "google.protobuf.BytesValue" => Ok(Box::new(BytesWrapperBuilderWrapper::new(
            message_descriptor,
            config,
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
        Kind::String => Ok(Box::new(StringBuilderWrapper::new(config))),
        Kind::Bytes => Ok(Box::new(BinaryBuilderWrapper::new(config))),
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

/// Determine nullability for a field, taking config into account for lists and maps.
fn is_nullable_with_config(field: &FieldDescriptor, config: &PtarsConfig) -> bool {
    if field.is_list() {
        config.list_nullable
    } else if field.is_map() {
        config.map_nullable
    } else {
        field.supports_presence()
    }
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
                is_nullable_with_config(field, config),
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
    offsets: ListOffsets,
    extractor: fn(&Value) -> Option<T::Native>,
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl<T> RepeatedPrimitiveBuilderWrapper<T>
where
    T: ArrowPrimitiveType,
{
    fn new(extractor: fn(&Value) -> Option<T::Native>, config: &PtarsConfig) -> Self {
        Self {
            builder: PrimitiveBuilder::<T>::new(),
            offsets: ListOffsets::new(config.use_large_list),
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
        self.offsets.push(self.builder.len());
    }

    fn append_null(&mut self) {
        // For repeated fields, a null value is an empty list.
        self.offsets.push(self.builder.len());
    }

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len());
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = Arc::new(std::mem::take(&mut self.builder).finish());
        let offsets = std::mem::replace(&mut self.offsets, ListOffsets::new(false));
        offsets.finish(values, &self.list_value_name, self.list_value_nullable)
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() == 1
    }
}

struct RepeatedMessageBuilderWrapper {
    builder: Box<dyn ProtoArrayBuilder>,
    offsets: ListOffsets,
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedMessageBuilderWrapper {
    fn new(builder: Box<dyn ProtoArrayBuilder>, config: &PtarsConfig) -> Self {
        Self {
            builder,
            offsets: ListOffsets::new(config.use_large_list),
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
        self.offsets.push(self.builder.len());
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.len());
    }

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len());
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = self.builder.finish();
        let offsets = std::mem::replace(&mut self.offsets, ListOffsets::new(false));
        offsets.finish(values, &self.list_value_name, self.list_value_nullable)
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
    offsets: ListOffsets,
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedBooleanBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        Self {
            builder: BooleanBuilder::new(),
            offsets: ListOffsets::new(config.use_large_list),
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
        self.offsets.push(self.builder.len());
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder.len());
    }

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder.len());
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values = Arc::new(std::mem::take(&mut self.builder).finish());
        let offsets = std::mem::replace(&mut self.offsets, ListOffsets::new(false));
        offsets.finish(values, &self.list_value_name, self.list_value_nullable)
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() == 1
    }
}

struct RepeatedBinaryBuilderWrapper {
    builder: BinaryBuilderInner,
    offsets: ListOffsets,
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedBinaryBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        let builder = if config.use_large_binary {
            BinaryBuilderInner::Large(LargeBinaryBuilder::new())
        } else {
            BinaryBuilderInner::Regular(BinaryBuilder::new())
        };
        Self {
            builder,
            offsets: ListOffsets::new(config.use_large_list),
            list_value_name: config.list_value_name.clone(),
            list_value_nullable: config.list_value_nullable,
        }
    }

    fn builder_len(&self) -> usize {
        match &self.builder {
            BinaryBuilderInner::Regular(b) => b.len(),
            BinaryBuilderInner::Large(b) => b.len(),
        }
    }
}

impl ProtoArrayBuilder for RepeatedBinaryBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                match &mut self.builder {
                    BinaryBuilderInner::Regular(b) => {
                        b.append_value(each_value.as_bytes().unwrap())
                    }
                    BinaryBuilderInner::Large(b) => b.append_value(each_value.as_bytes().unwrap()),
                }
            }
        }
        self.offsets.push(self.builder_len());
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder_len());
    }

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder_len());
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values: Arc<dyn Array> = match &mut self.builder {
            BinaryBuilderInner::Regular(b) => Arc::new(std::mem::take(b).finish()),
            BinaryBuilderInner::Large(b) => Arc::new(std::mem::take(b).finish()),
        };
        let offsets = std::mem::replace(&mut self.offsets, ListOffsets::new(false));
        offsets.finish(values, &self.list_value_name, self.list_value_nullable)
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.offsets.len() == 1
    }
}

struct RepeatedStringBuilderWrapper {
    builder: StringBuilderInner,
    offsets: ListOffsets,
    list_value_name: Arc<str>,
    list_value_nullable: bool,
}

impl RepeatedStringBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        let builder = if config.use_large_string {
            StringBuilderInner::Large(LargeStringBuilder::new())
        } else {
            StringBuilderInner::Regular(StringBuilder::new())
        };
        Self {
            builder,
            offsets: ListOffsets::new(config.use_large_list),
            list_value_name: config.list_value_name.clone(),
            list_value_nullable: config.list_value_nullable,
        }
    }

    fn builder_len(&self) -> usize {
        match &self.builder {
            StringBuilderInner::Regular(b) => b.len(),
            StringBuilderInner::Large(b) => b.len(),
        }
    }
}

impl ProtoArrayBuilder for RepeatedStringBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(values) = value.as_list() {
            for each_value in values {
                match &mut self.builder {
                    StringBuilderInner::Regular(b) => b.append_value(each_value.as_str().unwrap()),
                    StringBuilderInner::Large(b) => b.append_value(each_value.as_str().unwrap()),
                }
            }
        }
        self.offsets.push(self.builder_len());
    }

    fn append_null(&mut self) {
        self.offsets.push(self.builder_len());
    }

    fn append_default(&mut self) {
        // Default for repeated field is empty list
        self.offsets.push(self.builder_len());
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        let values: Arc<dyn Array> = match &mut self.builder {
            StringBuilderInner::Regular(b) => Arc::new(std::mem::take(b).finish()),
            StringBuilderInner::Large(b) => Arc::new(std::mem::take(b).finish()),
        };
        let offsets = std::mem::replace(&mut self.offsets, ListOffsets::new(false));
        offsets.finish(values, &self.list_value_name, self.list_value_nullable)
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
        convert_seconds_nanos_to_unit(seconds, nanos, self.time_unit, "Timestamp")
    }
}

/// Convert seconds and nanos to a value in the specified time unit with overflow checking.
/// Panics if the conversion would overflow i64.
fn convert_seconds_nanos_to_unit(seconds: i64, nanos: i32, unit: TimeUnit, type_name: &str) -> i64 {
    match unit {
        TimeUnit::Second => seconds,
        TimeUnit::Millisecond => {
            let scaled = seconds.checked_mul(1_000).unwrap_or_else(|| {
                panic!(
                    "{type_name} overflow: {seconds} seconds cannot be represented in milliseconds"
                )
            });
            scaled
                .checked_add(i64::from(nanos) / 1_000_000)
                .unwrap_or_else(|| {
                    panic!("{type_name} overflow: value cannot be represented in milliseconds")
                })
        }
        TimeUnit::Microsecond => {
            let scaled = seconds.checked_mul(1_000_000).unwrap_or_else(|| {
                panic!(
                    "{type_name} overflow: {seconds} seconds cannot be represented in microseconds"
                )
            });
            scaled
                .checked_add(i64::from(nanos) / 1_000)
                .unwrap_or_else(|| {
                    panic!("{type_name} overflow: value cannot be represented in microseconds")
                })
        }
        TimeUnit::Nanosecond => {
            let scaled = seconds.checked_mul(1_000_000_000).unwrap_or_else(|| {
                panic!(
                    "{type_name} overflow: {seconds} seconds cannot be represented in nanoseconds"
                )
            });
            scaled.checked_add(i64::from(nanos)).unwrap_or_else(|| {
                panic!("{type_name} overflow: value cannot be represented in nanoseconds")
            })
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
        // TimeOfDay is bounded (max 23:59:59.999999999), so total_seconds <= 86399
        // which fits easily in i64 for all time units. Still use checked arithmetic
        // for consistency and to catch any malformed input.
        let total_seconds = hours * 3600 + minutes * 60 + seconds;
        convert_seconds_nanos_to_unit(total_seconds, nanos as i32, self.time_unit, "TimeOfDay")
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
        convert_seconds_nanos_to_unit(seconds, nanos, self.time_unit, "Duration")
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
    builder: StringBuilderInner,
    value_descriptor: FieldDescriptor,
}

impl StringWrapperBuilderWrapper {
    fn new(message_descriptor: &MessageDescriptor, config: &PtarsConfig) -> Self {
        let builder = if config.use_large_string {
            StringBuilderInner::Large(LargeStringBuilder::new())
        } else {
            StringBuilderInner::Regular(StringBuilder::new())
        };
        Self {
            builder,
            value_descriptor: message_descriptor.get_field_by_name("value").unwrap(),
        }
    }
}

impl ProtoArrayBuilder for StringWrapperBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let v = message.get_field(&self.value_descriptor);
            match &mut self.builder {
                StringBuilderInner::Regular(b) => b.append_value(v.as_str().unwrap()),
                StringBuilderInner::Large(b) => b.append_value(v.as_str().unwrap()),
            }
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        match &mut self.builder {
            StringBuilderInner::Regular(b) => b.append_null(),
            StringBuilderInner::Large(b) => b.append_null(),
        }
    }

    fn append_default(&mut self) {
        // Wrapper types are message types, default is null
        match &mut self.builder {
            StringBuilderInner::Regular(b) => b.append_null(),
            StringBuilderInner::Large(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match &mut self.builder {
            StringBuilderInner::Regular(b) => Arc::new(std::mem::take(b).finish()),
            StringBuilderInner::Large(b) => Arc::new(std::mem::take(b).finish()),
        }
    }

    fn len(&self) -> usize {
        match &self.builder {
            StringBuilderInner::Regular(b) => b.len(),
            StringBuilderInner::Large(b) => b.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.builder {
            StringBuilderInner::Regular(b) => b.is_empty(),
            StringBuilderInner::Large(b) => b.is_empty(),
        }
    }
}

struct BytesWrapperBuilderWrapper {
    builder: BinaryBuilderInner,
    value_descriptor: FieldDescriptor,
}

impl BytesWrapperBuilderWrapper {
    fn new(message_descriptor: &MessageDescriptor, config: &PtarsConfig) -> Self {
        let builder = if config.use_large_binary {
            BinaryBuilderInner::Large(LargeBinaryBuilder::new())
        } else {
            BinaryBuilderInner::Regular(BinaryBuilder::new())
        };
        Self {
            builder,
            value_descriptor: message_descriptor.get_field_by_name("value").unwrap(),
        }
    }
}

impl ProtoArrayBuilder for BytesWrapperBuilderWrapper {
    fn append(&mut self, value: &Value) {
        if let Some(message) = value.as_message() {
            let v = message.get_field(&self.value_descriptor);
            match &mut self.builder {
                BinaryBuilderInner::Regular(b) => b.append_value(v.as_bytes().unwrap()),
                BinaryBuilderInner::Large(b) => b.append_value(v.as_bytes().unwrap()),
            }
        } else {
            self.append_null();
        }
    }

    fn append_null(&mut self) {
        match &mut self.builder {
            BinaryBuilderInner::Regular(b) => b.append_null(),
            BinaryBuilderInner::Large(b) => b.append_null(),
        }
    }

    fn append_default(&mut self) {
        // Wrapper types are message types, default is null
        match &mut self.builder {
            BinaryBuilderInner::Regular(b) => b.append_null(),
            BinaryBuilderInner::Large(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match &mut self.builder {
            BinaryBuilderInner::Regular(b) => Arc::new(std::mem::take(b).finish()),
            BinaryBuilderInner::Large(b) => Arc::new(std::mem::take(b).finish()),
        }
    }

    fn len(&self) -> usize {
        match &self.builder {
            BinaryBuilderInner::Regular(b) => b.len(),
            BinaryBuilderInner::Large(b) => b.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.builder {
            BinaryBuilderInner::Regular(b) => b.is_empty(),
            BinaryBuilderInner::Large(b) => b.is_empty(),
        }
    }
}

struct StringBuilderWrapper {
    builder: StringBuilderInner,
}

impl StringBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        let builder = if config.use_large_string {
            StringBuilderInner::Large(LargeStringBuilder::new())
        } else {
            StringBuilderInner::Regular(StringBuilder::new())
        };
        Self { builder }
    }
}

impl ProtoArrayBuilder for StringBuilderWrapper {
    fn append(&mut self, value: &Value) {
        match &mut self.builder {
            StringBuilderInner::Regular(b) => b.append_value(value.as_str().unwrap()),
            StringBuilderInner::Large(b) => b.append_value(value.as_str().unwrap()),
        }
    }

    fn append_null(&mut self) {
        match &mut self.builder {
            StringBuilderInner::Regular(b) => b.append_null(),
            StringBuilderInner::Large(b) => b.append_null(),
        }
    }

    fn append_default(&mut self) {
        match &mut self.builder {
            StringBuilderInner::Regular(b) => b.append_value(""),
            StringBuilderInner::Large(b) => b.append_value(""),
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match &mut self.builder {
            StringBuilderInner::Regular(b) => Arc::new(std::mem::take(b).finish()),
            StringBuilderInner::Large(b) => Arc::new(std::mem::take(b).finish()),
        }
    }

    fn len(&self) -> usize {
        match &self.builder {
            StringBuilderInner::Regular(b) => b.len(),
            StringBuilderInner::Large(b) => b.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.builder {
            StringBuilderInner::Regular(b) => b.is_empty(),
            StringBuilderInner::Large(b) => b.is_empty(),
        }
    }
}

struct BinaryBuilderWrapper {
    builder: BinaryBuilderInner,
}

impl BinaryBuilderWrapper {
    fn new(config: &PtarsConfig) -> Self {
        let builder = if config.use_large_binary {
            BinaryBuilderInner::Large(LargeBinaryBuilder::new())
        } else {
            BinaryBuilderInner::Regular(BinaryBuilder::new())
        };
        Self { builder }
    }
}

impl ProtoArrayBuilder for BinaryBuilderWrapper {
    fn append(&mut self, value: &Value) {
        match &mut self.builder {
            BinaryBuilderInner::Regular(b) => b.append_value(value.as_bytes().unwrap()),
            BinaryBuilderInner::Large(b) => b.append_value(value.as_bytes().unwrap()),
        }
    }

    fn append_null(&mut self) {
        match &mut self.builder {
            BinaryBuilderInner::Regular(b) => b.append_null(),
            BinaryBuilderInner::Large(b) => b.append_null(),
        }
    }

    fn append_default(&mut self) {
        match &mut self.builder {
            BinaryBuilderInner::Regular(b) => b.append_value(b""),
            BinaryBuilderInner::Large(b) => b.append_value(b""),
        }
    }

    fn finish(&mut self) -> Arc<dyn Array> {
        match &mut self.builder {
            BinaryBuilderInner::Regular(b) => Arc::new(std::mem::take(b).finish()),
            BinaryBuilderInner::Large(b) => Arc::new(std::mem::take(b).finish()),
        }
    }

    fn len(&self) -> usize {
        match &self.builder {
            BinaryBuilderInner::Regular(b) => b.len(),
            BinaryBuilderInner::Large(b) => b.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match &self.builder {
            BinaryBuilderInner::Regular(b) => b.is_empty(),
            BinaryBuilderInner::Large(b) => b.is_empty(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_seconds_nanos_to_unit_second() {
        // Second unit just returns seconds, ignoring nanos
        assert_eq!(
            convert_seconds_nanos_to_unit(100, 500_000_000, TimeUnit::Second, "Test"),
            100
        );
        assert_eq!(
            convert_seconds_nanos_to_unit(-100, 500_000_000, TimeUnit::Second, "Test"),
            -100
        );
        assert_eq!(
            convert_seconds_nanos_to_unit(0, 999_999_999, TimeUnit::Second, "Test"),
            0
        );
    }

    #[test]
    fn test_convert_seconds_nanos_to_unit_millisecond() {
        // 1 second + 500ms = 1500ms
        assert_eq!(
            convert_seconds_nanos_to_unit(1, 500_000_000, TimeUnit::Millisecond, "Test"),
            1500
        );
        // 0 seconds + 123ms
        assert_eq!(
            convert_seconds_nanos_to_unit(0, 123_000_000, TimeUnit::Millisecond, "Test"),
            123
        );
        // Negative seconds
        assert_eq!(
            convert_seconds_nanos_to_unit(-1, 0, TimeUnit::Millisecond, "Test"),
            -1000
        );
    }

    #[test]
    fn test_convert_seconds_nanos_to_unit_microsecond() {
        // 1 second + 500us = 1_000_500us
        assert_eq!(
            convert_seconds_nanos_to_unit(1, 500_000, TimeUnit::Microsecond, "Test"),
            1_000_500
        );
        // 0 seconds + 123us
        assert_eq!(
            convert_seconds_nanos_to_unit(0, 123_000, TimeUnit::Microsecond, "Test"),
            123
        );
    }

    #[test]
    fn test_convert_seconds_nanos_to_unit_nanosecond() {
        // 1 second + 500ns = 1_000_000_500ns
        assert_eq!(
            convert_seconds_nanos_to_unit(1, 500, TimeUnit::Nanosecond, "Test"),
            1_000_000_500
        );
        // 0 seconds + 123ns
        assert_eq!(
            convert_seconds_nanos_to_unit(0, 123, TimeUnit::Nanosecond, "Test"),
            123
        );
    }

    #[test]
    #[should_panic(expected = "Timestamp overflow")]
    fn test_convert_seconds_nanos_to_unit_overflow_millisecond() {
        // i64::MAX seconds would overflow when converted to milliseconds
        convert_seconds_nanos_to_unit(i64::MAX, 0, TimeUnit::Millisecond, "Timestamp");
    }

    #[test]
    #[should_panic(expected = "Duration overflow")]
    fn test_convert_seconds_nanos_to_unit_overflow_microsecond() {
        // i64::MAX seconds would overflow when converted to microseconds
        convert_seconds_nanos_to_unit(i64::MAX, 0, TimeUnit::Microsecond, "Duration");
    }

    #[test]
    #[should_panic(expected = "Test overflow")]
    fn test_convert_seconds_nanos_to_unit_overflow_nanosecond() {
        // i64::MAX seconds would overflow when converted to nanoseconds
        convert_seconds_nanos_to_unit(i64::MAX, 0, TimeUnit::Nanosecond, "Test");
    }

    #[test]
    #[should_panic(expected = "overflow: value cannot be represented in milliseconds")]
    fn test_convert_seconds_nanos_to_unit_overflow_addition_millisecond() {
        // i64::MAX / 1000 is the max seconds that can be scaled to milliseconds without overflow
        // Add nanos that would push it over after successful multiplication
        let max_safe_seconds = i64::MAX / 1000;
        // This value plus (nanos / 1_000_000) should overflow
        convert_seconds_nanos_to_unit(max_safe_seconds, 808_000_000, TimeUnit::Millisecond, "Test");
    }

    #[test]
    #[should_panic(expected = "overflow: value cannot be represented in microseconds")]
    fn test_convert_seconds_nanos_to_unit_overflow_addition_microsecond() {
        // i64::MAX / 1_000_000 = 9223372036854
        // 9223372036854 * 1_000_000 = 9223372036854000000
        // i64::MAX - 9223372036854000000 = 775807
        // We need (nanos / 1_000) > 775807, so nanos > 775_807_000
        let max_safe_seconds = i64::MAX / 1_000_000;
        convert_seconds_nanos_to_unit(max_safe_seconds, 776_000_000, TimeUnit::Microsecond, "Test");
    }

    #[test]
    #[should_panic(expected = "overflow: value cannot be represented in nanoseconds")]
    fn test_convert_seconds_nanos_to_unit_overflow_addition_nanosecond() {
        // i64::MAX / 1_000_000_000 = 9223372036
        // 9223372036 * 1_000_000_000 = 9223372036000000000
        // i64::MAX - 9223372036000000000 = 854775807
        // We need nanos > 854775807
        let max_safe_seconds = i64::MAX / 1_000_000_000;
        convert_seconds_nanos_to_unit(max_safe_seconds, 855_000_000, TimeUnit::Nanosecond, "Test");
    }

    #[test]
    fn test_string_builder_wrapper_len_and_is_empty() {
        let config = PtarsConfig::default();
        let mut builder = StringBuilderWrapper::new(&config);

        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        builder.append(&Value::String("hello".to_string()));
        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_string_builder_wrapper_large_len_and_is_empty() {
        let config = PtarsConfig::new().with_use_large_string(true);
        let mut builder = StringBuilderWrapper::new(&config);

        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        builder.append(&Value::String("hello".to_string()));
        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_binary_builder_wrapper_len_and_is_empty() {
        let config = PtarsConfig::default();
        let mut builder = BinaryBuilderWrapper::new(&config);

        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        builder.append(&Value::Bytes(prost::bytes::Bytes::from_static(&[1, 2, 3])));
        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);
    }

    #[test]
    fn test_binary_builder_wrapper_large_len_and_is_empty() {
        let config = PtarsConfig::new().with_use_large_binary(true);
        let mut builder = BinaryBuilderWrapper::new(&config);

        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        builder.append(&Value::Bytes(prost::bytes::Bytes::from_static(&[1, 2, 3])));
        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);
    }
}
