use pyo3::prelude::*;

use asyncio::store::{AsyncCollection, AsyncStore};
use syncio::store::{Collection, Store};

mod asyncio;
mod external;
mod shared;
mod syncio;

/// A Python module implemented in Rust.
#[pymodule]
fn orredis(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Store>()?;
    m.add_class::<Collection>()?;
    m.add_class::<AsyncStore>()?;
    m.add_class::<AsyncCollection>()?;
    Ok(())
}
