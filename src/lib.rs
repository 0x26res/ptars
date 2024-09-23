use std::collections::HashMap;
use std::iter::zip;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{ArrowNativeType, ToByteSlice};
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use arrow_array::builder::Int32Builder;
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, Date32Array, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, PrimitiveArray, Scalar, StringArray,
    StructArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field};
use chrono::Datelike;
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{
    FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectRepeatedRef, ReflectValueBox,
    ReflectValueRef, RuntimeFieldType, RuntimeType,
};
use protobuf::{Message, MessageDyn};
use pyo3::prelude::{pyfunction, pymodule, PyModule, PyObject, PyResult, Python};
use pyo3::{pyclass, pymethods, wrap_pyfunction, Bound, PyAny};

static CE_OFFSET: i32 = 719163;

#[pyclass]
struct MessageHandler {
    message_descriptor: MessageDescriptor,
}

#[pyclass]
struct ProtoCache {
    cache: HashMap<String, FileDescriptor>,
}

struct StringBuilder {
    values: String,
    offsets: Vec<i32>,
}

impl StringBuilder {
    pub fn new() -> Self {
        Self {
            values: String::new(),
            offsets: Vec::new(),
        }
    }

    fn append(&mut self, message: &dyn MessageDyn, field: &FieldDescriptor) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        match field.get_singular(message) {
            None => {}
            Some(x) => self.values.push_str(x.to_str().unwrap()),
        }
    }

    fn append_ref(&mut self, reflect_value_ref: ReflectValueRef) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        self.values.push_str(reflect_value_ref.to_str().unwrap())
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn build(&mut self) -> Arc<dyn Array> {
        let size = self.offsets.len();
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());

        let array_data = ArrayData::builder(DataType::Utf8)
            .len(size)
            .add_buffer(Buffer::from(self.offsets.to_byte_slice()))
            .add_buffer(Buffer::from(self.values.as_str()))
            .build()
            .unwrap();
        Arc::new(StringArray::from(array_data))
    }
}

struct BinaryBuilder {
    values: Vec<u8>,
    offsets: Vec<i32>,
}

impl BinaryBuilder {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: Vec::new(),
        }
    }

    fn append(&mut self, message: &dyn MessageDyn, field: &FieldDescriptor) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        match field.get_singular(message) {
            None => {}
            Some(x) => {
                for c in x.to_bytes().unwrap() {
                    self.values.push(*c)
                }
            }
        }
    }

    fn append_ref(&mut self, reflect_value_ref: ReflectValueRef) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        for c in reflect_value_ref.to_bytes().unwrap() {
            self.values.push(*c)
        }
    }

    fn append_message(&mut self, message: &Box<dyn MessageDyn>) {
        let bytes = message.write_to_bytes_dyn().unwrap();
        let offset = i32::from_usize(self.values.len()).unwrap();
        self.offsets.push(offset);
        self.values.extend(bytes);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn build(&mut self) -> Arc<dyn Array> {
        let size = self.offsets.len();
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());

        // TODO: look into avoiding copy here
        let array_data = ArrayData::builder(DataType::Binary)
            .len(size)
            .add_buffer(Buffer::from(self.offsets.to_byte_slice()))
            .add_buffer(Buffer::from_iter(self.values.clone()))
            .build()
            .unwrap();
        Arc::new(BinaryArray::from(array_data))
    }
}

fn singular_field_to_array(
    field: &FieldDescriptor,
    runtime_type: &RuntimeType,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn Array>, &'static str> {
    match runtime_type {
        RuntimeType::I32 => Ok(read_primitive::<i32, Int32Array>(
            messages,
            field,
            &ReflectValueRef::to_i32,
            0,
        )),
        RuntimeType::U32 => Ok(read_primitive::<u32, UInt32Array>(
            messages,
            field,
            &ReflectValueRef::to_u32,
            0,
        )),
        RuntimeType::I64 => Ok(read_primitive::<i64, Int64Array>(
            messages,
            field,
            &ReflectValueRef::to_i64,
            0,
        )),
        RuntimeType::U64 => Ok(read_primitive::<u64, UInt64Array>(
            messages,
            field,
            &ReflectValueRef::to_u64,
            0,
        )),
        RuntimeType::F32 => Ok(read_primitive::<f32, Float32Array>(
            messages,
            field,
            &ReflectValueRef::to_f32,
            0.0,
        )),
        RuntimeType::F64 => Ok(read_primitive::<f64, Float64Array>(
            messages,
            field,
            &ReflectValueRef::to_f64,
            0.0,
        )),
        RuntimeType::Bool => Ok(read_primitive::<bool, BooleanArray>(
            messages,
            field,
            &ReflectValueRef::to_bool,
            false,
        )),
        RuntimeType::String => {
            let mut builder = StringBuilder::new();
            for message in messages {
                builder.append(message.as_ref(), field)
            }
            Ok(builder.build())
        }
        RuntimeType::VecU8 => {
            let mut builder = BinaryBuilder::new();
            for message in messages {
                builder.append(message.as_ref(), field);
            }
            Ok(builder.build())
        }
        RuntimeType::Enum(_) => Ok(read_primitive::<i32, Int32Array>(
            messages,
            field,
            &ReflectValueRef::to_enum_value,
            0,
        )),
        RuntimeType::Message(x) => Ok(nested_messages_to_array(field, x, messages)),
    }
}

fn read_i32(message: &dyn MessageDyn, field_descriptor: &FieldDescriptor) -> i32 {
    return match field_descriptor.get_singular(message) {
        None => 0,
        Some(x) => x.to_i32().unwrap(),
    };
}

fn convert_date(
    messages: &Vec<Box<dyn MessageDyn>>,
    is_valid: &Vec<bool>,
    message_descriptor: &MessageDescriptor,
) -> Arc<Date32Array> {
    let year_descriptor = message_descriptor.field_by_name("year").unwrap();
    let month_descriptor = message_descriptor.field_by_name("month").unwrap();
    let day_descriptor = message_descriptor.field_by_name("day").unwrap();

    let mut builder = Int32Builder::new();
    for (message, message_valid) in zip(messages, is_valid) {
        if *message_valid {
            let year: i32 = read_i32(message.deref(), &year_descriptor);
            let month: i32 = read_i32(message.deref(), &month_descriptor);
            let day: i32 = read_i32(message.deref(), &day_descriptor);

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

fn convert_timestamps(
    arrays: &[(Arc<Field>, Arc<dyn Array>)],
    is_valid: &[bool],
) -> Arc<TimestampNanosecondArray> {
    let scalar: Scalar<PrimitiveArray<arrow_array::types::Int64Type>> =
        Int64Array::new_scalar(1_000_000_000);
    let seconds: Arc<dyn Array> = arrays[0].clone().1;
    let nanos: Arc<dyn Array> = arrays[1].clone().1;
    let casted = arrow::compute::kernels::cast(&nanos, &DataType::Int64).unwrap();
    let multiplied = arrow::compute::kernels::numeric::mul(&seconds, &scalar).unwrap();
    let total: ArrayRef = arrow::compute::kernels::numeric::add(&multiplied, &casted).unwrap();

    let is_valid_array = BooleanArray::from(is_valid.to_owned());
    let is_null = arrow::compute::not(&is_valid_array).unwrap();
    let total_nullable = arrow::compute::nullif(&total, &is_null).unwrap();
    Arc::new(Int64Array::from(total_nullable.deref().to_data()).reinterpret_cast())
}

fn nested_messages_to_array(
    field: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Arc<dyn Array> {
    let mut nested_messages: Vec<Box<dyn MessageDyn>> = Vec::new();
    let mut is_valid: Vec<bool> = Vec::new();
    for message in messages {
        nested_messages.push(
            field
                .get_singular_field_or_default(message.as_ref())
                .to_message()
                .unwrap()
                .clone_box(),
        );
        is_valid.push(field.has_field(message.as_ref()));
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

fn read_primitive<'b, T: Clone, A: From<Vec<T>> + Array + 'static>(
    messages: &'b Vec<Box<dyn MessageDyn>>,
    field: &FieldDescriptor,
    extractor: &dyn Fn(&ReflectValueRef<'b>) -> Option<T>,
    default: T,
) -> Arc<dyn Array> {
    let mut values: Vec<T> = Vec::new();
    for message in messages {
        let value = field.get_singular(message.as_ref());
        match value {
            None => values.push(default.clone()),
            Some(x) => values.push(extractor(&x).unwrap()),
        }
    }
    Arc::new(A::from(values))
}

fn read_repeated_primitive<'b, T, A: From<Vec<T>> + Array>(
    field: &FieldDescriptor,
    messages: &'b Vec<Box<dyn MessageDyn>>,
    data_type: DataType,
    extractor: &dyn Fn(&ReflectValueRef<'b>) -> Option<T>,
) -> Result<Arc<dyn Array>, &'static str> {
    let mut values: Vec<T> = Vec::new();
    let mut offsets: Vec<i32> = Vec::new();
    offsets.push(0);
    for message in messages {
        if field.has_field(message.as_ref()) {
            let repeated_ref: ReflectRepeatedRef = field.get_repeated(message.as_ref());
            for index in 0..repeated_ref.len() {
                let value_ref = repeated_ref.get(index);
                values.push(extractor(&value_ref).unwrap());
            }
        }
        offsets.push(i32::from_usize(values.len()).unwrap());
    }
    let list_data_type = DataType::List(Arc::new(Field::new("item", data_type, false)));
    let list_data = ArrayData::builder(list_data_type)
        .len(messages.len())
        .add_buffer(Buffer::from_iter(offsets))
        .add_child_data(A::from(values).to_data())
        .build()
        .unwrap();
    Ok(Arc::new(ListArray::from(list_data)))
}

fn repeated_field_to_array(
    field: &FieldDescriptor,
    runtime_type: &RuntimeType,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn Array>, &'static str> {
    return match runtime_type {
        RuntimeType::I32 => read_repeated_primitive::<i32, Int32Array>(
            field,
            messages,
            DataType::Int32,
            &ReflectValueRef::to_i32,
        ),
        RuntimeType::I64 => read_repeated_primitive::<i64, Int64Array>(
            field,
            messages,
            DataType::Int64,
            &ReflectValueRef::to_i64,
        ),

        RuntimeType::U32 => read_repeated_primitive::<u32, UInt32Array>(
            field,
            messages,
            DataType::UInt32,
            &ReflectValueRef::to_u32,
        ),
        RuntimeType::U64 => read_repeated_primitive::<u64, UInt64Array>(
            field,
            messages,
            DataType::UInt64,
            &ReflectValueRef::to_u64,
        ),
        RuntimeType::F32 => read_repeated_primitive::<f32, Float32Array>(
            field,
            messages,
            DataType::Float32,
            &ReflectValueRef::to_f32,
        ),
        RuntimeType::F64 => read_repeated_primitive::<f64, Float64Array>(
            field,
            messages,
            DataType::Float64,
            &ReflectValueRef::to_f64,
        ),
        RuntimeType::Bool => read_repeated_primitive::<bool, BooleanArray>(
            field,
            messages,
            DataType::Boolean,
            &ReflectValueRef::to_bool,
        ),
        RuntimeType::String => {
            let mut string_builder = StringBuilder::new();
            let mut offsets: Vec<i32> = Vec::new();
            offsets.push(0);
            for message in messages {
                if field.has_field(message.as_ref()) {
                    let repeated_ref: ReflectRepeatedRef = field.get_repeated(message.as_ref());
                    for index in 0..repeated_ref.len() {
                        let value_ref = repeated_ref.get(index);
                        string_builder.append_ref(value_ref)
                    }
                }
                offsets.push(i32::from_usize(string_builder.len()).unwrap());
            }
            let list_data_type =
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
            let list_data = ArrayData::builder(list_data_type)
                .len(messages.len())
                .add_buffer(Buffer::from_iter(offsets))
                .add_child_data(string_builder.build().to_data())
                .build()
                .unwrap();
            return Ok(Arc::new(ListArray::from(list_data)));
        }
        RuntimeType::VecU8 => {
            let mut builder = BinaryBuilder::new();
            let mut offsets: Vec<i32> = Vec::new();
            offsets.push(0);
            for message in messages {
                if field.has_field(message.as_ref()) {
                    let repeated_ref: ReflectRepeatedRef = field.get_repeated(message.as_ref());
                    for index in 0..repeated_ref.len() {
                        let value_ref = repeated_ref.get(index);
                        builder.append_ref(value_ref)
                    }
                }
                offsets.push(i32::from_usize(builder.len()).unwrap());
            }
            let list_data_type =
                DataType::List(Arc::new(Field::new("item", DataType::Binary, false)));
            let list_data = ArrayData::builder(list_data_type)
                .len(messages.len())
                .add_buffer(Buffer::from_iter(offsets))
                .add_child_data(builder.build().to_data())
                .build()
                .unwrap();
            return Ok(Arc::new(ListArray::from(list_data)));
        }
        RuntimeType::Enum(_) => read_repeated_primitive::<i32, Int32Array>(
            field,
            messages,
            DataType::Int32,
            &ReflectValueRef::to_enum_value,
        ),

        RuntimeType::Message(message_descriptor) => {
            let mut repeated_messages: Vec<Box<dyn MessageDyn>> = Vec::new();
            let mut offsets: Vec<i32> = Vec::new();
            offsets.push(0);
            for message in messages {
                if field.has_field(message.as_ref()) {
                    let repeated_ref: ReflectRepeatedRef = field.get_repeated(message.as_ref());
                    for index in 0..repeated_ref.len() {
                        let value_ref = repeated_ref.get(index);
                        repeated_messages.push(value_ref.to_message().unwrap().clone_box())
                    }
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
            return Ok(Arc::new(ListArray::from(list_data)));
        }
    };
}

fn field_to_array(
    field: &FieldDescriptor,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn Array>, &'static str> {
    match field.runtime_field_type() {
        RuntimeFieldType::Singular(x) => singular_field_to_array(field, &x, messages),
        RuntimeFieldType::Repeated(x) => repeated_field_to_array(field, &x, messages),
        RuntimeFieldType::Map(_, _) => Err("map not supported"),
    }
}

fn is_nullable(field: &FieldDescriptor) -> bool {
    match field.runtime_field_type() {
        RuntimeFieldType::Singular(runtime_type) => matches!(runtime_type, RuntimeType::Message(_)),
        RuntimeFieldType::Repeated(_) => false,
        RuntimeFieldType::Map(_, _) => false,
    }
}

fn field_to_tuple(
    field: &FieldDescriptor,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<(Arc<Field>, Arc<dyn Array>), &'static str> {
    let results = field_to_array(field, messages);
    return match results {
        Ok(array) => {
            return Ok((
                Arc::new(Field::new(
                    field.name(),
                    array.data_type().clone(),
                    is_nullable(field),
                )),
                array,
            ));
        }
        Err(x) => Err(x),
    };
}

fn fields_to_arrays(
    messages: &Vec<Box<dyn MessageDyn>>,
    message_descriptor: &MessageDescriptor,
) -> Vec<(Arc<Field>, Arc<dyn Array>)> {
    return message_descriptor
        .fields()
        .map(|x| field_to_tuple(&x, messages).unwrap())
        .collect();
}

fn set_primitive<P: ArrowPrimitiveType>(
    array: &ArrayRef,
    messages: &mut Vec<Box<dyn MessageDyn>>,
    field_descriptor: &FieldDescriptor,
    rvb_creator: &dyn Fn(P::Native) -> ReflectValueBox,
) {
    let specific_array: &PrimitiveArray<P> = array.as_any().downcast_ref().unwrap();
    specific_array
        .iter()
        .enumerate()
        .for_each(|(index, value)| match value {
            None => {}
            Some(x) => {
                let element: &mut dyn MessageDyn = messages.get_mut(index).unwrap().as_mut();
                field_descriptor.set_singular_field(&mut *element, rvb_creator(x));
            }
        })
}

fn i64_rvb(value: i64) -> ReflectValueBox {
    return ReflectValueBox::I64(value);
}

fn extract_singular_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut Vec<Box<dyn MessageDyn>>,
    runtime_type: &RuntimeType,
) {
    match runtime_type {
        RuntimeType::I32 => {
            set_primitive::<arrow_array::types::Int32Type>(
                array,
                messages,
                field_descriptor,
                &ReflectValueBox::I32,
            );
        }
        RuntimeType::U32 => {
            set_primitive::<arrow_array::types::UInt32Type>(
                array,
                messages,
                field_descriptor,
                &ReflectValueBox::U32,
            );
        }
        RuntimeType::I64 => set_primitive::<arrow_array::types::Int64Type>(
            array,
            messages,
            field_descriptor,
            &ReflectValueBox::I64,
        ),
        RuntimeType::U64 => set_primitive::<arrow_array::types::UInt64Type>(
            array,
            messages,
            field_descriptor,
            &ReflectValueBox::U64,
        ),
        RuntimeType::F32 => set_primitive::<arrow_array::types::Float32Type>(
            array,
            messages,
            field_descriptor,
            &ReflectValueBox::F32,
        ),
        RuntimeType::F64 => set_primitive::<arrow_array::types::Float64Type>(
            array,
            messages,
            field_descriptor,
            &ReflectValueBox::F64,
        ),
        RuntimeType::Bool => {} // BooleanType doesn't implement primitive type
        RuntimeType::String => {}
        RuntimeType::VecU8 => {}
        RuntimeType::Enum(_) => {}
        RuntimeType::Message(_) => {}
    }
}

fn extract_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut Vec<Box<dyn MessageDyn>>,
) {
    println!("!!!  {}", field_descriptor.name());
    match field_descriptor.runtime_field_type() {
        RuntimeFieldType::Singular(x) => {
            extract_singular_array(&array, &field_descriptor, messages, &x)
        }
        RuntimeFieldType::Repeated(_) => {}
        RuntimeFieldType::Map(_, _) => {}
    }
}

#[pymethods]
impl MessageHandler {
    fn list_to_record_batch(&self, values: Vec<Vec<u8>>, py: Python<'_>) -> PyResult<PyObject> {
        let messages: Vec<Box<dyn MessageDyn>> = values
            .iter()
            .map(|x| {
                self.message_descriptor
                    .parse_from_bytes(x.as_slice())
                    .unwrap()
            })
            .collect();
        let arrays: Vec<(Arc<Field>, Arc<dyn Array>)> =
            fields_to_arrays(&messages, &self.message_descriptor);
        let struct_array = if arrays.is_empty() {
            StructArray::new_empty_fields(messages.len(), None)
        } else {
            StructArray::from(arrays)
        };
        let batch = RecordBatch::from(struct_array);
        batch.to_pyarrow(py)
    }

    fn just_convert(&self, values: Vec<Vec<u8>>, _py: Python<'_>) {
        let _unused: Vec<Box<dyn MessageDyn>> = values
            .iter()
            .map(|x| {
                self.message_descriptor
                    .parse_from_bytes(x.as_slice())
                    .unwrap()
            })
            .collect();
    }

    fn record_batch_to_array(
        &self,
        record_batch: &Bound<PyAny>,
        py: Python<'_>,
    ) -> PyResult<PyObject> {
        let arrow_record_batch: RecordBatch =
            RecordBatch::from_pyarrow_bound(record_batch).unwrap();
        let mut messages: Vec<Box<dyn MessageDyn>> = (0..arrow_record_batch.num_rows())
            .map(|_| self.message_descriptor.new_instance())
            .collect::<Vec<Box<dyn MessageDyn>>>();

        self.message_descriptor
            .fields()
            .for_each(|field_descriptor: FieldDescriptor| {
                let column: Option<&ArrayRef> =
                    arrow_record_batch.column_by_name(field_descriptor.name());
                match column {
                    None => {}
                    Some(column) => extract_array(column, &field_descriptor, &mut messages),
                }
            });
        let mut results = BinaryBuilder::new();
        messages.iter().for_each(|x| results.append_message(&x));
        results.build().to_data().to_pyarrow(py)
    }
}

impl ProtoCache {
    fn get_or_create(&mut self, file_descriptor_proto: &FileDescriptorProto) -> FileDescriptor {
        let tmp = file_descriptor_proto.name.as_ref().unwrap();
        let name: &str = tmp.as_ref();
        let available = self.cache.get(name);
        return match available {
            Some(x) => x.clone(),
            None => {
                let dependencies: Vec<&FileDescriptor> = file_descriptor_proto
                    .dependency
                    .iter()
                    .map(|x| self.cache.get(x.as_str()).unwrap())
                    .collect();
                let copy: Vec<FileDescriptor> = dependencies.into_iter().cloned().collect();
                let descriptor =
                    FileDescriptor::new_dynamic(file_descriptor_proto.clone(), copy.as_slice())
                        .unwrap();
                self.cache.insert(name.to_string(), descriptor);
                self.cache.get(name).unwrap().clone()
            }
        };
    }
}

#[pymethods]
impl ProtoCache {
    #[new]
    fn new() -> Self {
        ProtoCache {
            cache: HashMap::new(),
        }
    }

    fn create_for_message(
        &mut self,
        message_name: String,
        file_descriptors_bytes: Vec<Vec<u8>>,
    ) -> PyResult<MessageHandler> {
        let file_descriptors_protos: Vec<FileDescriptorProto> = file_descriptors_bytes
            .iter()
            .map(|x| FileDescriptorProto::parse_from_bytes(x.as_slice()).unwrap())
            .collect();

        let file_descriptors: Vec<FileDescriptor> = file_descriptors_protos
            .iter()
            .rev()
            .map(|x| self.get_or_create(x))
            .collect();

        let message_descriptor: MessageDescriptor = file_descriptors
            .last()
            .unwrap()
            .message_by_full_name(message_name.as_str())
            .unwrap();

        Ok(MessageHandler { message_descriptor })
    }
}

#[pyfunction]
fn get_a_table(py: Python<'_>) -> PyResult<PyObject> {
    let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
    let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;

    let batch = RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap();
    batch.to_pyarrow(py)
}

#[pymodule]
fn _lib(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(get_a_table))?;
    m.add_class::<ProtoCache>()?;
    m.add_class::<MessageHandler>()?;
    PyResult::Ok(())
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
