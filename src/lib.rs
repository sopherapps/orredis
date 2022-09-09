pub mod store;

use crate::store::Redis;
use pyo3::prelude::*;

/// The package is to be as follows:
/// - Create a Store
///     - with name, redis instance, lifespan
/// - Store can register models
/// - Declare Model classes
/// - Model classes can
///     - insert bulk
///     - insert single
///     - insert nested
///     - update bulk
///     - update bulk nested
///     - select all columns
///     - select some columns
///     - select nested
///     - delete bulk

/// Formats the sum of two numbers as string.
#[pyfunction]
fn get_type(obj: &PyAny) -> PyResult<String> {
    let v = obj.get_type().to_string();
    Ok(v)
}

/// A Python module implemented in Rust.
#[pymodule]
fn orredis(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_type, m)?)?;
    m.add_class::<Redis>()?;
    Ok(())
}
