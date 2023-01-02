macro_rules! py_key_error {
    ($v:expr, $det:expr) => {
        pyo3::exceptions::PyKeyError::new_err(format!("{:?} (key was {:?})", $det, $v))
    };
}

macro_rules! py_value_error {
    ($v:expr, $det:expr) => {
        pyo3::exceptions::PyValueError::new_err(format!("{:?} (value was {:?})", $det, $v))
    };
}

macro_rules! to_py {
    ($v:expr) => {
        Ok(pyo3::prelude::Python::with_gil(|py| $v.into_py(py)))
    };
}

pub(crate) use py_key_error;
pub(crate) use py_value_error;
pub(crate) use to_py;
