use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_array::{
    Array, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array, UInt64Array,
};
use protobuf::descriptor::FileDescriptorProto;
use protobuf::reflect::{
    FieldDescriptor, FileDescriptor, MessageDescriptor, ReflectValueRef, RuntimeFieldType,
    RuntimeType,
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
            0 // this should not happen
        }
    } else {
        0
    };
}

fn read_i64(message: &Box<dyn MessageDyn>, field: &FieldDescriptor) -> i64 {
    return if field.has_field(message.as_ref()) {
        let value = field.get_singular(message.as_ref()).unwrap();
        if let ReflectValueRef::I64(x) = value {
            x
        } else {
            0 // this should not happen
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
            0 // this should not happen
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
            0 // this should not happen
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
            0.0 // this should not happen
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
            0.0 // this should not happen
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

fn singular_field_to_array(
    field: &FieldDescriptor,
    runtime_type: &RuntimeType,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn arrow::array::Array>, &'static str> {
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
            Ok(Arc::new(Float64Array::from_iter(values)))
        }
        //  RuntimeType::Bool => {
        //     let values: Vec<bool> = messages.iter().map(
        //         |x| read_bool(x, field)
        //     ).collect();
        //     Ok(Arc::new(BooleanArray::from_iter(values)))
        // }
        RuntimeType::Bool => Err("Bool message not supported"),
        RuntimeType::String => Err("String message not supported"),
        RuntimeType::VecU8 => Err("Binary message not supported"),
        RuntimeType::Enum(_) => Err("Enum message not supported"),
        RuntimeType::Message(_) => Err("nested message not supported"),
    };
}

fn field_to_array(
    field: &FieldDescriptor,
    messages: &Vec<Box<dyn MessageDyn>>,
) -> Result<Arc<dyn arrow::array::Array>, &'static str> {
    return match field.runtime_field_type() {
        RuntimeFieldType::Singular(x) => singular_field_to_array(field, &x, messages),
        RuntimeFieldType::Repeated(_) => Err("repeated not supported"),
        RuntimeFieldType::Map(_, _) => Err("repeated not supported"),
    };
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
        let arrays: Vec<(String, Arc<dyn Array>)> = self
            .message_descriptor
            .fields()
            .map(|x| (x.name().to_string(), field_to_array(&x, &messages).unwrap()))
            .collect();
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
        py: Python<'_>,
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
