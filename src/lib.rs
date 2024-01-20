use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{ArrowNativeType, ToByteSlice};
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray, PrimitiveArray, Scalar, StringArray, StructArray, TimestampNanosecondArray, UInt32Array, UInt64Array};
use arrow_array::types::Int64Type;
use arrow_schema::{DataType, Field};
use protobuf::{Message, MessageDyn};
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{
    FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectRepeatedRef,
    ReflectValueRef, RuntimeFieldType, RuntimeType,
};
use pyo3::{pyclass, pymethods, wrap_pyfunction};
use pyo3::prelude::{pyfunction, pymodule, PyModule, PyObject, PyResult, Python};

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
        return self.offsets.len();
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
        return Arc::new(StringArray::from(array_data));
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
                    self.values.push(c.clone())
                }
            }
        }
    }

    fn append_ref(&mut self, reflect_value_ref: ReflectValueRef) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        for c in reflect_value_ref.to_bytes().unwrap() {
            self.values.push(c.clone())
        }
    }

    fn len(&self) -> usize {
        return self.offsets.len();
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
        return Arc::new(BinaryArray::from(array_data));
    }
}

fn singular_field_to_array(
    field: &FieldDescriptor,
    runtime_type: &RuntimeType,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn Array>, &'static str> {
    return match runtime_type {
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
            return Ok(builder.build());
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
    };
}

fn convert_timestamps(
    arrays: &Vec<(Arc<Field>, Arc<dyn Array>)>
) -> Arc<TimestampNanosecondArray> {
    let scalar: Scalar<PrimitiveArray<Int64Type>> = Int64Array::new_scalar(1_000_000_000);
    let seconds:  Arc<dyn Array> = arrays[0].clone().1;
    let nanos:  Arc<dyn Array> = arrays[1].clone().1;
    let casted = arrow::compute::kernels::cast(
        &nanos,
        &DataType::Int64,
    ).unwrap();
    let multiplied = arrow::compute::kernels::numeric::mul(
        &casted,
        &scalar,
    ).unwrap();
    let total: ArrayRef = arrow::compute::kernels::numeric::add(
        &multiplied,
        &seconds,
    ).unwrap();

    Arc::new(
        Int64Array::from(total.deref().to_data()).reinterpret_cast()
    )
}

fn nested_messages_to_array(
    field: & FieldDescriptor,
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
    let arrays: Vec<(Arc<Field>, Arc<dyn Array>)> = fields_to_arrays(&nested_messages, message_descriptor);
    return if arrays.is_empty() {
        Arc::new(StructArray::new_empty_fields(nested_messages.len(), Some(NullBuffer::from_iter(is_valid))))
    } else if message_descriptor.full_name() == "google.protobuf.Timestamp" {
        convert_timestamps(&arrays)
    } else {
        Arc::new(StructArray::from((arrays, Buffer::from_iter(is_valid))))
    };
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
    return Arc::new(A::from(values));
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
    return Ok(Arc::new(ListArray::from(list_data)));
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
            let list_data_type =
                DataType::List(Arc::new(Field::new("item", struct_array.data_type().clone(), false)));
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
    return match field.runtime_field_type() {
        RuntimeFieldType::Singular(x) => singular_field_to_array(field, &x, messages),
        RuntimeFieldType::Repeated(x) => repeated_field_to_array(field, &x, messages),
        RuntimeFieldType::Map(_, _) => Err("map not supported"),
    };
}

fn is_nullable(field: &FieldDescriptor) -> bool {
    match field.runtime_field_type() {
        RuntimeFieldType::Singular(runtime_type) => match runtime_type {
            RuntimeType::Message(_) => true,
            _ => false,
        },

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
        .map(|x| field_to_tuple(&x, &messages).unwrap())
        .collect();
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
        let batch = RecordBatch::from(StructArray::from(struct_array));
        return batch.to_pyarrow(py);
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
            .map(|x| self.get_or_create(&x))
            .collect();

        let message_descriptor: MessageDescriptor = file_descriptors
            .last()
            .unwrap()
            .message_by_full_name(message_name.as_str())
            .unwrap();

        return PyResult::Ok(MessageHandler { message_descriptor });
    }
}

#[pyfunction]
fn get_a_table(py: Python<'_>) -> PyResult<PyObject> {
    let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
    let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;

    let batch = RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap();
    return batch.to_pyarrow(py);
}

#[pymodule]
fn _lib(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(get_a_table))?;
    m.add_class::<ProtoCache>()?;
    m.add_class::<MessageHandler>()?;
    PyResult::Ok(())
}
