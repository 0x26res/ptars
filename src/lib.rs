use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arrow::array::{ArrayData, ArrayDataBuilder};
use arrow::buffer::Buffer;
use arrow::datatypes::{ArrowNativeType, ToByteSlice};
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_array::{
    Array, ArrowPrimitiveType, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
    Int64Array, ListArray, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, FieldRef};
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{
    FieldDescriptor, FileDescriptor, MessageDescriptor, MessageRef, ReflectRepeatedRef,
    ReflectValueRef, RuntimeFieldType, RuntimeType,
};
use protobuf::{Message, MessageDyn};
use pyo3::prelude::{pyfunction, pymodule, PyModule, PyObject, PyResult, Python};
use pyo3::{pyclass, pymethods, wrap_pyfunction};

#[pyclass]
struct MessageHandler {
    message_descriptor: MessageDescriptor,
}

#[pyclass]
struct ProtoCache {
    cache: HashMap<String, FileDescriptor>,
}

fn read_i32(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> i32 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::I32(x) = value {
            x
        } else {
            0
        }
    } else {
        0
    };
}

fn read_i32_repeated(
    message: &Box<dyn MessageDyn>,
    field: &FieldDescriptor,
    values: &mut Vec<i32>,
    offsets: &mut Vec<i32>,
) {
    if field.has_field(message.as_ref()) {
        let value: ReflectRepeatedRef = field.get_repeated(message.as_ref());
        for index in 0..value.len() {
            values.push(value.get(index).to_i32().unwrap())
        }
    }
    offsets.push(i32::from_usize(values.len()).unwrap())
}

fn read_i64(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> i64 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::I64(x) = value {
            x
        } else {
            0
        }
    } else {
        0
    };
}

fn read_u32(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> u32 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::U32(x) = value {
            x
        } else {
            0
        }
    } else {
        0
    };
}

fn read_u64(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> u64 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::U64(x) = value {
            x
        } else {
            0
        }
    } else {
        0
    };
}

fn read_f32(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> f32 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::F32(x) = value {
            x
        } else {
            0.0
        }
    } else {
        0.0
    };
}

fn read_f64(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> f64 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::F64(x) = value {
            x
        } else {
            0.0
        }
    } else {
        0.0
    };
}

fn read_bool(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> bool {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::Bool(x) = value {
            x
        } else {
            false
        }
    } else {
        false
    };
}

fn read_enum(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> i32 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::Enum(_x, n) = value {
            n
        } else {
            0
        }
    } else {
        0
    };
}

fn pass_builder(array_data: ArrayDataBuilder) -> ArrayDataBuilder {
    return array_data.add_buffer(Buffer::from("123"));
}

fn read_string(
    message: &Box<dyn MessageDyn>,
    field: &FieldDescriptor,
    values: &mut String,
    offsets: &mut Vec<i32>,
) -> () {
    let value = field.get_singular(message.as_ref()).unwrap();
    if let ReflectValueRef::String(x) = value {
        values.push_str(x);
        offsets.push(i32::from_usize(x.len()).unwrap())
    } else {
        panic!("Wrong type {value}");
    }
}

fn read_binary(
    message: &Box<dyn MessageDyn>,
    field: &FieldDescriptor,
    values: &mut Vec<u8>,
    offsets: &mut Vec<i32>,
) -> () {
    let value = field.get_singular(message.as_ref()).unwrap();
    if let ReflectValueRef::Bytes(x) = value {
        for b in x {
            values.push(b.clone());
        }
        offsets.push(i32::from_usize(x.len()).unwrap())
    } else {
        panic!("Wrong type {value}");
    }
}

fn singular_field_to_array(
    field: &FieldDescriptor,
    runtime_type: &RuntimeType,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn Array>, &'static str> {
    return match runtime_type {
        RuntimeType::I32 => {
            let values: Vec<i32> = messages.iter().map(|x| read_i32(x, field)).collect();
            Ok(Arc::new(Int32Array::from_iter(values)))
        }
        RuntimeType::U32 => {
            let values: Vec<u32> = messages.iter().map(|x| read_u32(x, field)).collect();
            Ok(Arc::new(UInt32Array::from_iter(values)))
        }
        RuntimeType::I64 => {
            let values: Vec<i64> = messages.iter().map(|x| read_i64(x, field)).collect();
            Ok(Arc::new(Int64Array::from_iter(values)))
        }
        RuntimeType::U64 => {
            let values: Vec<u64> = messages.iter().map(|x| read_u64(x, field)).collect();
            Ok(Arc::new(UInt64Array::from_iter(values)))
        }
        RuntimeType::F32 => {
            let values: Vec<f32> = messages.iter().map(|x| read_f32(x, field)).collect();
            Ok(Arc::new(Float32Array::from_iter(values)))
        }
        RuntimeType::F64 => {
            let values: Vec<f64> = messages.iter().map(|x| read_f64(x, field)).collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        RuntimeType::Bool => {
            let values: Vec<bool> = messages.iter().map(|x| read_bool(x, field)).collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        RuntimeType::String => {
            // TODO: specify capacity
            let mut values = String::new();
            let mut offsets: Vec<i32> = Vec::new();
            let mut nulls: Vec<bool> = Vec::new();
            offsets.push(0);
            for message in messages {
                if field.has_field(message.as_ref()) {
                    read_string(message, field, &mut values, &mut offsets);
                    nulls.push(true);
                } else {
                    nulls.push(true);
                    offsets.push(0);
                }
            }

            let builder: ArrayDataBuilder = ArrayData::builder(DataType::Utf8)
                .len(messages.len())
                .add_buffer(Buffer::from(offsets.to_byte_slice()))
                .add_buffer(Buffer::from(values.as_str()))
                .null_bit_buffer(Option::Some(Buffer::from_iter(nulls)));
            let array_data = builder.build().unwrap();
            Ok(Arc::new(StringArray::from(array_data)))
        }
        RuntimeType::VecU8 => {
            // TODO: specify capacity
            let mut values: Vec<u8> = Vec::new();
            let mut offsets: Vec<i32> = Vec::new();
            let mut nulls: Vec<bool> = Vec::new();
            offsets.push(0);
            for message in messages {
                if field.has_field(message.as_ref()) {
                    read_binary(message, field, &mut values, &mut offsets);
                    nulls.push(true);
                } else {
                    nulls.push(true);
                    offsets.push(0);
                }
            }

            let builder: ArrayDataBuilder = ArrayData::builder(DataType::Binary)
                .len(messages.len())
                .add_buffer(Buffer::from(offsets.to_byte_slice()))
                .add_buffer(Buffer::from_iter(values))
                .null_bit_buffer(Option::Some(Buffer::from_iter(nulls)));
            let array_data = builder.build().unwrap();
            Ok(Arc::new(BinaryArray::from(array_data)))
        }
        RuntimeType::Enum(_) => {
            let values: Vec<i32> = messages.iter().map(|x| read_enum(x, field)).collect();
            Ok(Arc::new(Int32Array::from(values)))
        }
        RuntimeType::Message(x) => Ok(nested_messages_to_array(field, x, messages)),
    };
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
    let arrays = fields_to_arrays(&nested_messages, message_descriptor);
    // TODO: deal with nullable

    let arrays_with_types: Vec<(Arc<Field>, Arc<dyn Array>)> = arrays
        .iter()
        .map(|x| {
            (
                Arc::new(Field::new(x.0.clone(), x.1.data_type().clone(), false)),
                x.1.clone(),
            )
        })
        .collect();
    return Arc::new(StructArray::from((
        arrays_with_types,
        Buffer::from_iter(is_valid),
    )));
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
        .len(3)
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
        //RuntimeType::Bool => Err("Bool not supported"),
        RuntimeType::String => Err("String not supported"),
        RuntimeType::VecU8 => Err("VecU8 not supported"),
        RuntimeType::Enum(_) => Err("Enum not supported"),
        RuntimeType::Message(_) => Err("Message not supported"),
    };
}

fn field_to_array(
    field: &FieldDescriptor,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn Array>, &'static str> {
    return match field.runtime_field_type() {
        RuntimeFieldType::Singular(x) => singular_field_to_array(field, &x, messages),
        RuntimeFieldType::Repeated(x) => repeated_field_to_array(field, &x, messages),
        RuntimeFieldType::Map(_, _) => Err("repeated not supported"),
    };
}

fn fields_to_arrays(
    messages: &Vec<Box<dyn MessageDyn>>,
    message_descriptor: &MessageDescriptor,
) -> Vec<(String, Arc<dyn Array>)> {
    return message_descriptor
        .fields()
        .map(|x| (x.name().to_string(), field_to_array(&x, &messages).unwrap()))
        .collect();
}

#[pymethods]
impl MessageHandler {
    fn list_to_table(&self, values: Vec<Vec<u8>>, py: Python<'_>) -> PyResult<PyObject> {
        let messages: Vec<Box<dyn MessageDyn>> = values
            .iter()
            .map(|x| {
                self.message_descriptor
                    .parse_from_bytes(x.as_slice())
                    .unwrap()
            })
            .collect();
        let arrays: Vec<(String, Arc<dyn Array>)> =
            fields_to_arrays(&messages, &self.message_descriptor);
        let batch = RecordBatch::try_from_iter(arrays).unwrap();
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
