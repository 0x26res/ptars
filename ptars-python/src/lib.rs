use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow::record_batch::RecordBatch;
use arrow_array::{BinaryArray, Float32Array, Int32Array};
use arrow_schema::TimeUnit;
use prost::Message;
use prost_reflect::prost_types::FileDescriptorProto;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use pyo3::prelude::{pyfunction, pymodule, PyModule, PyResult, Python};
use pyo3::types::{PyAnyMethods, PyList, PyListMethods, PyModuleMethods};
use pyo3::Py;
use pyo3::{pyclass, pymethods, wrap_pyfunction, Bound, PyAny};
use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::PathBuf;
use std::sync::Arc;

/// Convert a Python time unit string to Arrow TimeUnit.
fn parse_time_unit(s: &str) -> PyResult<TimeUnit> {
    match s {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid time unit '{}', expected one of: s, ms, us, ns",
            s
        ))),
    }
}

/// Extract a PtarsConfig from a Python object (dataclass).
fn extract_config(config: &Bound<'_, PyAny>) -> PyResult<ptars::PtarsConfig> {
    let timestamp_tz: Option<String> = config.getattr("timestamp_tz")?.extract()?;
    let timestamp_unit: String = config.getattr("timestamp_unit")?.extract()?;
    let time_unit: String = config.getattr("time_unit")?.extract()?;
    let duration_unit: String = config.getattr("duration_unit")?.extract()?;
    let list_value_name: String = config.getattr("list_value_name")?.extract()?;
    let list_nullable: bool = config.getattr("list_nullable")?.extract()?;
    let map_nullable: bool = config.getattr("map_nullable")?.extract()?;
    let list_value_nullable: bool = config.getattr("list_value_nullable")?.extract()?;
    let map_value_nullable: bool = config.getattr("map_value_nullable")?.extract()?;
    let use_large_string: bool = config.getattr("use_large_string")?.extract()?;
    let use_large_binary: bool = config.getattr("use_large_binary")?.extract()?;
    let use_large_list: bool = config.getattr("use_large_list")?.extract()?;

    Ok(ptars::PtarsConfig::default()
        .with_timestamp_tz(timestamp_tz.as_deref())
        .with_timestamp_unit(parse_time_unit(&timestamp_unit)?)
        .with_time_unit(parse_time_unit(&time_unit)?)
        .with_duration_unit(parse_time_unit(&duration_unit)?)
        .with_list_value_name(&list_value_name)
        .with_list_nullable(list_nullable)
        .with_map_nullable(map_nullable)
        .with_list_value_nullable(list_value_nullable)
        .with_map_value_nullable(map_value_nullable)
        .with_use_large_string(use_large_string)
        .with_use_large_binary(use_large_binary)
        .with_use_large_list(use_large_list))
}

/// Read a varint from a reader. Returns None if EOF is reached at the start.
fn read_varint<R: Read>(reader: &mut R) -> std::io::Result<Option<u64>> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut buf = [0u8; 1];

    loop {
        match reader.read_exact(&mut buf) {
            Ok(()) => {
                let byte = buf[0];
                result |= ((byte & 0x7F) as u64) << shift;
                if (byte & 0x80) == 0 {
                    return Ok(Some(result));
                }
                shift += 7;
                if shift >= 64 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "varint too large",
                    ));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if shift == 0 {
                    return Ok(None); // EOF at start of varint
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected EOF while reading varint",
                ));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Maximum allowed message size (64 MB) to prevent DoS via massive allocations.
const MAX_MESSAGE_SIZE: u64 = 64 * 1024 * 1024;

/// Read size-delimited messages from a reader.
fn read_size_delimited_messages<R: Read>(reader: &mut R) -> std::io::Result<Vec<Vec<u8>>> {
    let mut messages = Vec::new();
    loop {
        match read_varint(reader)? {
            None => break, // EOF
            Some(size) => {
                // Validate message size to prevent DoS
                if size > MAX_MESSAGE_SIZE {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "message size {} exceeds maximum allowed size {}",
                            size, MAX_MESSAGE_SIZE
                        ),
                    ));
                }
                // Safe cast: we've validated size <= MAX_MESSAGE_SIZE which fits in usize
                let size_usize = size as usize;
                let mut buf = vec![0u8; size_usize];
                reader.read_exact(&mut buf)?;
                messages.push(buf);
            }
        }
    }
    Ok(messages)
}

#[pyclass]
struct MessageHandler {
    message_descriptor: MessageDescriptor,
    config: ptars::PtarsConfig,
}

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
        Ok(ptars::messages_to_record_batch_with_config(
            &messages,
            &self.message_descriptor,
            &self.config,
        )
        .to_pyarrow(py)?
        .unbind())
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
        let record_batch = ptars::binary_array_to_record_batch_with_config(
            &arrow_array,
            &self.message_descriptor,
            &self.config,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        Ok(record_batch.to_pyarrow(py)?.unbind())
    }

    /// Read size-delimited protobuf messages from a file and convert to a record batch.
    ///
    /// Each message in the file should be preceded by its size encoded as a varint.
    fn read_size_delimited_file(&self, path: PathBuf, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let file = File::open(&path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to open file: {}", e))
        })?;
        let mut reader = BufReader::new(file);
        let message_bytes = read_size_delimited_messages(&mut reader).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to read file: {}", e))
        })?;

        let mut messages: Vec<DynamicMessage> = Vec::with_capacity(message_bytes.len());
        for bytes in &message_bytes {
            let message = DynamicMessage::decode(self.message_descriptor.clone(), bytes.as_slice())
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            messages.push(message);
        }

        Ok(ptars::messages_to_record_batch_with_config(
            &messages,
            &self.message_descriptor,
            &self.config,
        )
        .to_pyarrow(py)?
        .unbind())
    }
}

/// Registry of protobuf descriptors used to create message handlers.
#[pyclass]
#[derive(Clone)]
struct ProtoRegistry {
    pool: Arc<DescriptorPool>,
}

#[pymethods]
impl ProtoRegistry {
    #[new]
    fn new(file_descriptors_bytes: Vec<Vec<u8>>) -> Self {
        let mut pool = DescriptorPool::new();
        for file_descriptor_bytes in &file_descriptors_bytes {
            let cursor = Cursor::new(file_descriptor_bytes);
            let proto = FileDescriptorProto::decode(cursor).unwrap();
            pool.add_file_descriptor_proto(proto).unwrap();
        }

        ProtoRegistry {
            pool: Arc::new(pool),
        }
    }

    fn get_names(&self) -> Vec<String> {
        self.pool
            .all_messages()
            .map(|x| String::from(x.full_name()))
            .collect()
    }

    #[pyo3(signature = (message_name, config=None))]
    fn create_for_message(
        &mut self,
        message_name: String,
        config: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<MessageHandler> {
        let message_descriptor = self
            .pool
            .get_message_by_name(message_name.as_str())
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Message '{}' not found in descriptor pool",
                    message_name
                ))
            })?;

        let config = match config {
            Some(c) => extract_config(c)?,
            None => ptars::PtarsConfig::default(),
        };

        Ok(MessageHandler {
            message_descriptor,
            config,
        })
    }
}

#[pyfunction]
fn get_a_table(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
    let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;

    let batch = RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap();
    Ok(batch.to_pyarrow(py)?.unbind())
}

/// Version of the ptars package, sourced from Cargo.toml at compile time.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pymodule]
fn _lib(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_function(wrap_pyfunction!(get_a_table, m)?)?;
    m.add_class::<ProtoRegistry>()?;
    m.add_class::<MessageHandler>()?;
    PyResult::Ok(())
}
