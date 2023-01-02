//! This is a subset of pyo3-asyncio.
//! I had to get some of the methods because it was conflicting with the pyo3 version of this project
//! But all the code is copied as is from [pyo3-asyncio version 1.16.0](https://docs.rs/pyo3-asyncio/)

use futures::channel::oneshot;
use once_cell::sync::OnceCell;
use pyo3::{
    prelude::*,
    types::{PyDict, PyTuple},
};

pub mod async_std;

/// Errors and exceptions related to PyO3 Asyncio
pub mod err;

pub mod generic;

static ASYNCIO: OnceCell<PyObject> = OnceCell::new();
static CONTEXTVARS: OnceCell<PyObject> = OnceCell::new();
static ENSURE_FUTURE: OnceCell<PyObject> = OnceCell::new();
static GET_RUNNING_LOOP: OnceCell<PyObject> = OnceCell::new();

fn ensure_future<'p>(py: Python<'p>, awaitable: &'p PyAny) -> PyResult<&'p PyAny> {
    ENSURE_FUTURE
        .get_or_try_init(|| -> PyResult<PyObject> {
            Ok(asyncio(py)?.getattr("ensure_future")?.into())
        })?
        .as_ref(py)
        .call1((awaitable,))
}

fn create_future(event_loop: &PyAny) -> PyResult<&PyAny> {
    event_loop.call_method0("create_future")
}

fn asyncio(py: Python) -> PyResult<&PyAny> {
    ASYNCIO
        .get_or_try_init(|| Ok(py.import("asyncio")?.into()))
        .map(|asyncio| asyncio.as_ref(py))
}

/// Get a reference to the Python Event Loop from Rust
///
/// Equivalent to `asyncio.get_running_loop()` in Python 3.7+.
pub fn get_running_loop(py: Python) -> PyResult<&PyAny> {
    // Ideally should call get_running_loop, but calls get_event_loop for compatibility when
    // get_running_loop is not available.
    GET_RUNNING_LOOP
        .get_or_try_init(|| -> PyResult<PyObject> {
            let asyncio = asyncio(py)?;

            Ok(asyncio.getattr("get_running_loop")?.into())
        })?
        .as_ref(py)
        .call0()
}

fn contextvars(py: Python) -> PyResult<&PyAny> {
    Ok(CONTEXTVARS
        .get_or_try_init(|| py.import("contextvars").map(|m| m.into()))?
        .as_ref(py))
}

fn copy_context(py: Python) -> PyResult<&PyAny> {
    contextvars(py)?.call_method0("copy_context")
}

/// Task-local data to store for Python conversions.
#[derive(Debug, Clone)]
pub struct TaskLocals {
    /// Track the event loop of the Python task
    event_loop: PyObject,
}

impl TaskLocals {
    /// At a minimum, TaskLocals must store the event loop.
    pub fn new(event_loop: &PyAny) -> Self {
        Self {
            event_loop: event_loop.into(),
        }
    }

    /// Construct TaskLocals with the event loop returned by `get_running_loop`
    pub fn with_running_loop(py: Python) -> PyResult<Self> {
        Ok(Self::new(get_running_loop(py)?))
    }

    /// Manually provide the contextvars for the current task.
    pub fn with_context(self, _context: &PyAny) -> Self {
        Self { ..self }
    }

    /// Capture the current task's contextvars
    pub fn copy_context(self, py: Python) -> PyResult<Self> {
        Ok(self.with_context(copy_context(py)?))
    }

    /// Get a reference to the event loop
    pub fn event_loop<'p>(&self, py: Python<'p>) -> &'p PyAny {
        self.event_loop.clone().into_ref(py)
    }
}

#[pyclass]
struct PyTaskCompleter {
    tx: Option<oneshot::Sender<PyResult<PyObject>>>,
}

#[pymethods]
impl PyTaskCompleter {
    #[args(task)]
    pub fn __call__(&mut self, task: &PyAny) -> PyResult<()> {
        debug_assert!(task.call_method0("done")?.extract()?);

        let result = match task.call_method0("result") {
            Ok(val) => Ok(val.into()),
            Err(e) => Err(e),
        };

        // unclear to me whether or not this should be a panic or silent error.
        //
        // calling PyTaskCompleter twice should not be possible, but I don't think it really hurts
        // anything if it happens.
        if let Some(tx) = self.tx.take() {
            if tx.send(result).is_err() {
                // cancellation is not an error
            }
        }

        Ok(())
    }
}

#[pyclass]
struct PyEnsureFuture {
    awaitable: PyObject,
    tx: Option<oneshot::Sender<PyResult<PyObject>>>,
}

#[pymethods]
impl PyEnsureFuture {
    pub fn __call__(&mut self) -> PyResult<()> {
        Python::with_gil(|py| {
            let task = ensure_future(py, self.awaitable.as_ref(py))?;
            let on_complete = PyTaskCompleter { tx: self.tx.take() };
            task.call_method1("add_done_callback", (on_complete,))?;

            Ok(())
        })
    }
}

fn call_soon_threadsafe(
    event_loop: &PyAny,
    context: &PyAny,
    args: impl IntoPy<Py<PyTuple>>,
) -> PyResult<()> {
    let py = event_loop.py();

    let kwargs = PyDict::new(py);
    kwargs.set_item("context", context)?;

    event_loop.call_method("call_soon_threadsafe", args, Some(kwargs))?;
    Ok(())
}

fn dump_err(py: Python<'_>) -> impl FnOnce(PyErr) + '_ {
    move |e| {
        // We can't display Python exceptions via std::fmt::Display,
        // so print the error here manually.
        e.print_and_set_sys_last_vars(py);
    }
}
