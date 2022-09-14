use pyo3::prelude::*;

use store::Store;

use crate::model::Model;

pub mod model;
mod parsers;
mod pyparsers;
mod redis_utils;
pub mod store;

/// A Python module implemented in Rust.
#[pymodule]
fn orredis(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Store>()?;
    m.add_class::<Model>()?;
    Ok(())
}
