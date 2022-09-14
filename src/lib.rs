pub mod model;
mod redis_utils;
pub mod store;

use crate::model::Model;
use pyo3::prelude::*;
use store::Store;

/// A Python module implemented in Rust.
#[pymodule]
fn orredis(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Store>()?;
    m.add_class::<Model>()?;
    Ok(())
}
