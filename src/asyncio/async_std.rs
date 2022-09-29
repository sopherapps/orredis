use std::{any::Any, cell::RefCell, future::Future, panic::AssertUnwindSafe, pin::Pin};

use crate::asyncio::{
    generic::{self, ContextExt, JoinError, LocalContextExt, Runtime, SpawnLocalExt},
    TaskLocals,
};
use async_std::task;
use futures::FutureExt;
use pyo3::prelude::*;

struct AsyncStdJoinErr(Box<dyn Any + Send + 'static>);

impl JoinError for AsyncStdJoinErr {
    fn is_panic(&self) -> bool {
        true
    }
}

async_std::task_local! {
    static TASK_LOCALS: RefCell<Option<TaskLocals>> = RefCell::new(None);
}

struct AsyncStdRuntime;

impl Runtime for AsyncStdRuntime {
    type JoinError = AsyncStdJoinErr;
    type JoinHandle = task::JoinHandle<Result<(), AsyncStdJoinErr>>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        task::spawn(async move {
            AssertUnwindSafe(fut)
                .catch_unwind()
                .await
                .map_err(|e| AsyncStdJoinErr(e))
        })
    }
}

impl ContextExt for AsyncStdRuntime {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        let old = TASK_LOCALS.with(|c| c.replace(Some(locals)));
        Box::pin(async move {
            let result = fut.await;
            TASK_LOCALS.with(|c| c.replace(old));
            result
        })
    }

    fn get_task_locals() -> Option<TaskLocals> {
        match TASK_LOCALS.try_with(|c| c.borrow().clone()) {
            Ok(locals) => locals,
            Err(_) => None,
        }
    }
}

impl SpawnLocalExt for AsyncStdRuntime {
    fn spawn_local<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + 'static,
    {
        task::spawn_local(async move {
            fut.await;
            Ok(())
        })
    }
}

impl LocalContextExt for AsyncStdRuntime {
    fn scope_local<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R>>>
    where
        F: Future<Output = R> + 'static,
    {
        let old = TASK_LOCALS.with(|c| c.replace(Some(locals)));
        Box::pin(async move {
            let result = fut.await;
            TASK_LOCALS.with(|c| c.replace(old));
            result
        })
    }
}

/// Set the task local event loop for the given future
pub async fn scope<F, R>(locals: TaskLocals, fut: F) -> R
where
    F: Future<Output = R> + Send + 'static,
{
    AsyncStdRuntime::scope(locals, fut).await
}

/// Either copy the task locals from the current task OR get the current running loop and
/// contextvars from Python.
pub fn get_current_locals(py: Python) -> PyResult<TaskLocals> {
    generic::get_current_locals::<AsyncStdRuntime>(py)
}

/// Convert a Rust Future into a Python awaitable
///
/// If the `asyncio.Future` returned by this conversion is cancelled via `asyncio.Future.cancel`,
/// the Rust future will be cancelled as well (new behaviour in `v0.15`).
///
/// Python `contextvars` are preserved when calling async Python functions within the Rust future
/// via [`into_future`] (new behaviour in `v0.15`).
///
/// > Although `contextvars` are preserved for async Python functions, synchronous functions will
/// unfortunately fail to resolve them when called within the Rust future. This is because the
/// function is being called from a Rust thread, not inside an actual Python coroutine context.
/// >
/// > As a workaround, you can get the `contextvars` from the current task locals using
/// [`get_current_locals`] and [`TaskLocals::context`](`crate::TaskLocals::context`), then wrap your
/// synchronous function in a call to `contextvars.Context.run`. This will set the context, call the
/// synchronous function, and restore the previous context when it returns or raises an exception.
///
/// # Arguments
/// * `py` - PyO3 GIL guard
/// * `locals` - The task locals for the given future
/// * `fut` - The Rust future to be converted
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use pyo3::prelude::*;
///
/// /// Awaitable sleep function
/// #[pyfunction]
/// fn sleep_for<'p>(py: Python<'p>, secs: &'p PyAny) -> PyResult<&'p PyAny> {
///     let secs = secs.extract()?;
///     pyo3_asyncio::async_std::future_into_py_with_locals(
///         py,
///         pyo3_asyncio::async_std::get_current_locals(py)?,
///         async move {
///             async_std::task::sleep(Duration::from_secs(secs)).await;
///             Python::with_gil(|py| Ok(py.None()))
///         }
///     )
/// }
/// ```
pub fn future_into_py_with_locals<F, T>(py: Python, locals: TaskLocals, fut: F) -> PyResult<&PyAny>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: IntoPy<PyObject>,
{
    generic::future_into_py_with_locals::<AsyncStdRuntime, F, T>(py, locals, fut)
}
