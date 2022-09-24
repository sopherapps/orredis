use pyo3::prelude::*;

use store::{Collection, Store};

mod parsers;
mod records;
mod schema;
pub mod store;
mod utils;

/// A Python module implemented in Rust.
#[pymodule]
fn orredis(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Store>()?;
    m.add_class::<Collection>()?;
    Ok(())
}
