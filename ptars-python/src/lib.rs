use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use arrow_array::{BinaryArray, Float32Array, Int32Array};
use prost::Message;
use prost_reflect::prost_types::FileDescriptorProto;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use pyo3::prelude::{pyfunction, pymodule, PyModule, PyResult, Python};
use pyo3::types::{PyAnyMethods, PyList, PyListMethods, PyModuleMethods};
use pyo3::Py;
use pyo3::{pyclass, pymethods, wrap_pyfunction, Bound, PyAny};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use pyo3_stub_gen::{Result as StubResult, StubInfo};
use std::io::Cursor;
use std::sync::Arc;

/// Handler for converting protobuf messages to/from Arrow format.
#[gen_stub_pyclass]
#[pyclass(module = "ptars._lib")]
struct MessageHandler {
    message_descriptor: MessageDescriptor,
}

#[gen_stub_pymethods]
#[pymethods]
impl MessageHandler {
    fn list_to_record_batch(
        &self,
        values: &Bound<'_, PyList>,
        py: Python<'_>,
    ) -> PyResult<Py<PyAny>> {
        let mut messages: Vec<DynamicMessage> = Vec::with_capacity(values.len());
        for value in values.iter() {
            let bytes: &[u8] = value.extract()?;
            let message = DynamicMessage::decode(self.message_descriptor.clone(), bytes)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            messages.push(message);
        }
        Ok(
            ptars::messages_to_record_batch(&messages, &self.message_descriptor)
                .to_pyarrow(py)?
                .unbind(),
        )
    }

    fn just_convert(&self, values: &Bound<'_, PyList>, _py: Python<'_>) {
        for value in values.iter() {
            let bytes: &[u8] = value.extract().unwrap();
            let _message = DynamicMessage::decode(self.message_descriptor.clone(), bytes).unwrap();
        }
    }

    fn record_batch_to_array(
        &self,
        record_batch: &Bound<PyAny>,
        py: Python<'_>,
    ) -> PyResult<Py<PyAny>> {
        let arrow_record_batch: RecordBatch =
            RecordBatch::from_pyarrow_bound(record_batch).unwrap();
        Ok(
            ptars::record_batch_to_array(&arrow_record_batch, &self.message_descriptor)
                .to_pyarrow(py)?
                .unbind(),
        )
    }

    /// Convert a binary array of serialized protobuf messages to a record batch.
    ///
    /// Each element in the binary array is expected to be a serialized protobuf message.
    /// The resulting record batch will have one column per field in the message descriptor.
    fn array_to_record_batch(&self, array: &Bound<PyAny>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let array_data = arrow::array::ArrayData::from_pyarrow_bound(array).map_err(|e| {
            pyo3::exceptions::PyTypeError::new_err(format!("Failed to convert array: {}", e))
        })?;
        let arrow_array = BinaryArray::from(array_data);
        let record_batch =
            ptars::binary_array_to_record_batch(&arrow_array, &self.message_descriptor)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(record_batch.to_pyarrow(py)?.unbind())
    }
}

/// Cache for protobuf descriptors, used to create MessageHandler instances.
#[gen_stub_pyclass]
#[pyclass(module = "ptars._lib")]
#[derive(Clone)]
struct ProtoCache {
    pool: Arc<DescriptorPool>,
}

#[gen_stub_pymethods]
#[pymethods]
impl ProtoCache {
    #[new]
    fn new(file_descriptors_bytes: Vec<Vec<u8>>) -> Self {
        let mut pool = DescriptorPool::new();
        for file_descriptor_bytes in &file_descriptors_bytes {
            let cursor = Cursor::new(file_descriptor_bytes);
            let proto = FileDescriptorProto::decode(cursor).unwrap();
            pool.add_file_descriptor_proto(proto).unwrap();
        }

        ProtoCache {
            pool: Arc::new(pool),
        }
    }

    fn get_names(&self) -> Vec<String> {
        self.pool
            .all_messages()
            .map(|x| String::from(x.full_name()))
            .collect()
    }

    fn create_for_message(&mut self, message_name: String) -> PyResult<MessageHandler> {
        let message_descriptor = self
            .pool
            .get_message_by_name(message_name.as_str())
            .unwrap();

        Ok(MessageHandler { message_descriptor })
    }
}

/// Get a sample Arrow table for testing.
#[gen_stub_pyfunction(module = "ptars._lib")]
#[pyfunction]
fn get_a_table(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
    let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;

    let batch = RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap();
    Ok(batch.to_pyarrow(py)?.unbind())
}

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_a_table, m)?)?;
    m.add_class::<ProtoCache>()?;
    m.add_class::<MessageHandler>()?;
    PyResult::Ok(())
}

/// Generate stub info from gathered macros and pyproject.toml in the project root
pub fn stub_info() -> StubResult<StubInfo> {
    // pyproject.toml is in the parent directory (project root)
    let manifest_dir: &std::path::Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let pyproject_path = manifest_dir.parent().unwrap().join("pyproject.toml");
    StubInfo::from_pyproject_toml(pyproject_path)
}
