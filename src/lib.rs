use pyo3::prelude::*;

use async_store::{AsyncCollection, AsyncStore};
use store::{Collection, Store};

mod async_store;
mod async_utils;
mod asyncio;
mod field_types;
mod mobc_redis;
mod parsers;
mod schema;
mod store;
mod utils;

/// A Python module implemented in Rust.
#[pymodule]
fn orredis(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Store>()?;
    m.add_class::<Collection>()?;
    m.add_class::<AsyncStore>()?;
    m.add_class::<AsyncCollection>()?;
    Ok(())
}
