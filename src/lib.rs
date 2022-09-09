pub mod store;

use crate::store::Redis;
use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn orredis(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Redis>()?;
    Ok(())
}
