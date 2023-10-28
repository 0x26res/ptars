use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use arrow_array::{Float32Array, Int32Array};
use protobuf::descriptor::FileDescriptorProto;
use protobuf::Message;
use pyo3::prelude::{pyfunction, pymodule, PyModule, PyObject, PyResult, Python};
use pyo3::wrap_pyfunction;

#[pyfunction]
fn get_a_table(py: Python<'_>) -> PyResult<PyObject> {
    let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
    let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;

    let batch = RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap();
    return batch.to_pyarrow(py);
}

#[pyfunction]
fn py_create(descriptors_bytes: Vec<Vec<u8>>, py: Python<'_>) -> PyResult<()> {
    for bytes in descriptors_bytes {
        let im_msg = FileDescriptorProto::parse_from_bytes(bytes.as_slice()).unwrap();

    }
    return PyResult::Ok(())
}

#[pymodule]
fn _lib(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(get_a_table))?;
    m.add_wrapped(wrap_pyfunction!(py_create))?;

    PyResult::Ok(())
}
