use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::asyncio::err::RustPanic;
use crate::asyncio::{call_soon_threadsafe, create_future, dump_err, TaskLocals};
use futures::{
    channel::{mpsc, oneshot},
    SinkExt,
};
use once_cell::sync::OnceCell;
use pin_project_lite::pin_project;
use pyo3::prelude::*;

/// Generic utilities for a JoinError
pub trait JoinError {
    /// Check if the spawned task exited because of a panic
    fn is_panic(&self) -> bool;
}

/// Generic Rust async/await runtime
pub trait Runtime: Send + 'static {
    /// The error returned by a JoinHandle after being awaited
    type JoinError: JoinError + Send;
    /// A future that completes with the result of the spawned task
    type JoinHandle: Future<Output = Result<(), Self::JoinError>> + Send;

    /// Spawn a future onto this runtime's event loop
    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static;
}

/// Extension trait for async/await runtimes that support spawning local tasks
pub trait SpawnLocalExt: Runtime {
    /// Spawn a !Send future onto this runtime's event loop
    fn spawn_local<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + 'static;
}

/// Exposes the utilities necessary for using task-local data in the Runtime
pub trait ContextExt: Runtime {
    /// Set the task locals for the given future
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static;

    /// Get the task locals for the current task
    fn get_task_locals() -> Option<TaskLocals>;
}

/// Adds the ability to scope task-local data for !Send futures
pub trait LocalContextExt: Runtime {
    /// Set the task locals for the given !Send future
    fn scope_local<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R>>>
    where
        F: Future<Output = R> + 'static;
}

/// Either copy the task locals from the current task OR get the current running loop and
/// contextvars from Python.
pub fn get_current_locals<R>(py: Python) -> PyResult<TaskLocals>
where
    R: ContextExt,
{
    if let Some(locals) = R::get_task_locals() {
        Ok(locals)
    } else {
        Ok(TaskLocals::with_running_loop(py)?.copy_context(py)?)
    }
}

fn cancelled(future: &PyAny) -> PyResult<bool> {
    future.getattr("cancelled")?.call0()?.is_true()
}

fn set_result(event_loop: &PyAny, future: &PyAny, result: PyResult<PyObject>) -> PyResult<()> {
    let py = event_loop.py();
    let none = py.None().into_ref(py);

    match result {
        Ok(val) => {
            let set_result = future.getattr("set_result")?;
            call_soon_threadsafe(event_loop, none, (set_result, val))?;
        }
        Err(err) => {
            let set_exception = future.getattr("set_exception")?;
            call_soon_threadsafe(event_loop, none, (set_exception, err))?;
        }
    }

    Ok(())
}

/// Convert a Rust Future into a Python awaitable with a generic runtime
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
/// * `locals` - The task-local data for Python
/// * `fut` - The Rust future to be converted
pub fn future_into_py_with_locals<R, F, T>(
    py: Python,
    locals: TaskLocals,
    fut: F,
) -> PyResult<&PyAny>
where
    R: Runtime + ContextExt,
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: IntoPy<PyObject>,
{
    let (cancel_tx, cancel_rx) = oneshot::channel();

    let py_fut = create_future(locals.event_loop.clone().into_ref(py))?;
    py_fut.call_method1(
        "add_done_callback",
        (PyDoneCallback {
            cancel_tx: Some(cancel_tx),
        },),
    )?;

    let future_tx1 = PyObject::from(py_fut);
    let future_tx2 = future_tx1.clone();

    R::spawn(async move {
        let locals2 = locals.clone();

        if let Err(e) = R::spawn(async move {
            let result = R::scope(
                locals2.clone(),
                Cancellable::new_with_cancel_rx(fut, cancel_rx),
            )
            .await;

            Python::with_gil(move |py| {
                if cancelled(future_tx1.as_ref(py))
                    .map_err(dump_err(py))
                    .unwrap_or(false)
                {
                    return;
                }

                let _ = set_result(
                    locals2.event_loop(py),
                    future_tx1.as_ref(py),
                    result.map(|val| val.into_py(py)),
                )
                .map_err(dump_err(py));
            });
        })
        .await
        {
            if e.is_panic() {
                Python::with_gil(move |py| {
                    if cancelled(future_tx2.as_ref(py))
                        .map_err(dump_err(py))
                        .unwrap_or(false)
                    {
                        return;
                    }

                    let _ = set_result(
                        locals.event_loop.as_ref(py),
                        future_tx2.as_ref(py),
                        Err(RustPanic::new_err("rust future panicked")),
                    )
                    .map_err(dump_err(py));
                });
            }
        }
    });

    Ok(py_fut)
}

pin_project! {
    /// Future returned by [`timeout`](timeout) and [`timeout_at`](timeout_at).
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    struct Cancellable<T> {
        #[pin]
        future: T,
        #[pin]
        cancel_rx: oneshot::Receiver<()>,

        poll_cancel_rx: bool
    }
}

impl<T> Cancellable<T> {
    fn new_with_cancel_rx(future: T, cancel_rx: oneshot::Receiver<()>) -> Self {
        Self {
            future,
            cancel_rx,

            poll_cancel_rx: true,
        }
    }
}

impl<F, T> Future for Cancellable<F>
where
    F: Future<Output = PyResult<T>>,
    T: IntoPy<PyObject>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // First, try polling the future
        if let Poll::Ready(v) = this.future.poll(cx) {
            return Poll::Ready(v);
        }

        // Now check for cancellation
        if *this.poll_cancel_rx {
            match this.cancel_rx.poll(cx) {
                Poll::Ready(Ok(())) => {
                    *this.poll_cancel_rx = false;
                    // The python future has already been cancelled, so this return value will never
                    // be used.
                    Poll::Ready(Err(pyo3::exceptions::PyBaseException::new_err(
                        "unreachable",
                    )))
                }
                Poll::Ready(Err(_)) => {
                    *this.poll_cancel_rx = false;
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
}

#[pyclass]
struct PyDoneCallback {
    cancel_tx: Option<oneshot::Sender<()>>,
}

#[pymethods]
impl PyDoneCallback {
    pub fn __call__(&mut self, fut: &PyAny) -> PyResult<()> {
        let py = fut.py();

        if cancelled(fut).map_err(dump_err(py)).unwrap_or(false) {
            let _ = self.cancel_tx.take().unwrap().send(());
        }

        Ok(())
    }
}

fn py_true() -> PyObject {
    static TRUE: OnceCell<PyObject> = OnceCell::new();
    TRUE.get_or_init(|| Python::with_gil(|py| true.into_py(py)))
        .clone()
}
fn py_false() -> PyObject {
    static FALSE: OnceCell<PyObject> = OnceCell::new();
    FALSE
        .get_or_init(|| Python::with_gil(|py| false.into_py(py)))
        .clone()
}

trait Sender: Send + 'static {
    fn send(&mut self, locals: TaskLocals, item: PyObject) -> PyResult<PyObject>;
    fn close(&mut self) -> PyResult<()>;
}

struct GenericSender<R>
where
    R: Runtime,
{
    runtime: PhantomData<R>,
    tx: mpsc::Sender<PyObject>,
}

impl<R> Sender for GenericSender<R>
where
    R: Runtime + ContextExt,
{
    fn send(&mut self, locals: TaskLocals, item: PyObject) -> PyResult<PyObject> {
        match self.tx.try_send(item.clone()) {
            Ok(_) => Ok(py_true()),
            Err(e) => {
                if e.is_full() {
                    let mut tx = self.tx.clone();
                    Python::with_gil(move |py| {
                        Ok(
                            future_into_py_with_locals::<R, _, PyObject>(py, locals, async move {
                                if tx.flush().await.is_err() {
                                    // receiving side disconnected
                                    return Ok(py_false());
                                }
                                if tx.send(item).await.is_err() {
                                    // receiving side disconnected
                                    return Ok(py_false());
                                }
                                Ok(py_true())
                            })?
                            .into(),
                        )
                    })
                } else {
                    Ok(py_false())
                }
            }
        }
    }
    fn close(&mut self) -> PyResult<()> {
        self.tx.close_channel();
        Ok(())
    }
}

#[pyclass]
struct SenderGlue {
    locals: TaskLocals,
    tx: Box<dyn Sender>,
}
#[pymethods]
impl SenderGlue {
    pub fn send(&mut self, item: PyObject) -> PyResult<PyObject> {
        self.tx.send(self.locals.clone(), item)
    }
    pub fn close(&mut self) -> PyResult<()> {
        self.tx.close()
    }
}
