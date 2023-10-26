use std::sync::Arc;



use pyo3::prelude::*;
use pyo3::{wrap_pyfunction};
use arrow::record_batch::RecordBatch;
use arrow_array::{Float32Array, Int32Array};
use arrow::pyarrow::ToPyArrow;

/// Returns the maps for the given process.
#[pyfunction]
fn get_a_table(py: Python<'_>) -> PyResult<PyObject> {
    let col_1 = Arc::new(Int32Array::from_iter([1, 2, 3])) as _;
    let col_2 = Arc::new(Float32Array::from_iter([1., 6.3, 4.])) as _;

    let batch = RecordBatch::try_from_iter([("col1", col_1), ("col_2", col_2)]).unwrap();
    return batch.to_pyarrow(py);
}



#[pymodule]
fn protarrowrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(get_a_table))?;

    Ok(())
}