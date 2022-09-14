use std::collections::HashMap;
use std::hash::Hash;
use std::str::FromStr;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyModule;
use pyo3::types::{PyDate, PyType};
use pyo3::{IntoPy, Py, PyAny, PyDowncastError, PyResult, Python};

use crate::model::ModelMeta;
use crate::store::find_one_by_raw_id;
use crate::{parsers, Store};

pub fn str_to_py_obj(store: &mut Store, value: &str, field_type: &PyAny) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        let field_name: Result<&PyType, PyDowncastError> = field_type.downcast::<PyType>();
        let field_name = match field_name {
            Ok(v) => {
                let v = v.name()?;
                v.to_string()
            }
            Err(_) => field_type.to_string(),
        };
        let name = field_name.as_str();

        if name == "int" {
            str_to_py_int(py, value)
        } else if name == "float" {
            str_to_py_float(py, value)
        } else if name == "str" {
            str_to_py_str(py, value)
        } else if name == "bool" {
            str_to_py_bool(py, value)
        } else if name == "date" {
            str_to_py_date(py, value)
        } else if name == "datetime" {
            str_to_py_datetime(py, value)
        } else if name.starts_with("typing.Tuple[int") {
            str_to_py_tuple::<i64>(py, value)
        } else if name.starts_with("typing.Tuple[float") {
            str_to_py_tuple::<f64>(py, value)
        } else if name.starts_with("typing.Tuple[str") {
            str_to_py_tuple::<String>(py, value)
        } else if name.starts_with("typing.Tuple[bool") {
            str_to_py_tuple::<bool>(py, value)
        } else if name.starts_with("typing.Tuple[") {
            Err(PyTypeError::new_err(format!(
                "tuples of type {} are not supported yet",
                name
            )))
        } else if name.starts_with("typing.List[int") {
            str_to_py_list::<i64>(py, value)
        } else if name.starts_with("typing.List[float") {
            str_to_py_list::<f64>(py, value)
        } else if name.starts_with("typing.List[str") {
            str_to_py_list::<String>(py, value)
        } else if name.starts_with("typing.List[bool") {
            str_to_py_list::<bool>(py, value)
        } else if name.starts_with("typing.List[") {
            Err(PyTypeError::new_err(format!(
                "lists of type {} are not supported yet",
                name
            )))
        } else if name == "typing.Dict[int, int]" {
            str_to_py_dict::<i64, i64>(py, value)
        } else if name == "typing.Dict[int, float]" {
            str_to_py_dict::<i64, f64>(py, value)
        } else if name == "typing.Dict[int, str]" {
            str_to_py_dict::<i64, String>(py, value)
        } else if name == "typing.Dict[int, bool]" {
            str_to_py_dict::<i64, bool>(py, value)
        } else if name == "typing.Dict[str, int]" {
            str_to_py_dict::<String, i64>(py, value)
        } else if name == "typing.Dict[str, float]" {
            str_to_py_dict::<String, f64>(py, value)
        } else if name == "typing.Dict[str, str]" {
            str_to_py_dict::<String, String>(py, value)
        } else if name == "typing.Dict[str, bool]" {
            str_to_py_dict::<String, bool>(py, value)
        } else if name == "typing.Dict[bool, str]" {
            str_to_py_dict::<bool, String>(py, value)
        } else if name == "typing.Dict[bool, int]" {
            str_to_py_dict::<bool, i64>(py, value)
        } else if name == "typing.Dict[bool, float]" {
            str_to_py_dict::<bool, f64>(py, value)
        } else if name == "typing.Dict[bool, bool]" {
            str_to_py_dict::<bool, bool>(py, value)
        } else if name.starts_with("typing.Dict[") {
            Err(PyTypeError::new_err(format!(
                "dictionaries of type {} are not supported yet",
                name
            )))
        } else {
            let model_name = name.to_lowercase();
            if let Some(model_meta) = store.models.clone().get(&model_name) {
                str_to_nested_model(py, store, model_meta, value)
            } else {
                Err(PyTypeError::new_err(format!(
                    "type annotation {} is not supported",
                    name
                )))
            }
        }
    })
}

fn str_to_nested_model(
    py: Python,
    store: &mut Store,
    model_meta: &ModelMeta,
    value: &str,
) -> PyResult<Py<PyAny>> {
    let model_type = model_meta.model_type.clone_ref(py);
    let nested_model = find_one_by_raw_id(store, model_meta.fields.clone(), value)?;
    match nested_model {
        None => Ok(py.None()),
        Some(nested_model) => nested_model.to_subclass_instance(&model_type),
    }
}

fn str_to_py_dict<T, U>(py: Python, value: &str) -> PyResult<Py<PyAny>>
where
    T: FromStr + Hash + std::cmp::Eq + IntoPy<Py<PyAny>>,
    U: FromStr + IntoPy<Py<PyAny>>,
{
    let v: HashMap<T, U> = parsers::parse_dict(value)?;
    Ok(v.into_py(py))
}

fn str_to_py_list<T>(py: Python, value: &str) -> PyResult<Py<PyAny>>
where
    T: FromStr + IntoPy<Py<PyAny>>,
{
    let v: Vec<T> = parsers::parse_list(value)?;
    Ok(v.into_py(py))
}

fn str_to_py_tuple<T>(py: Python, value: &str) -> PyResult<Py<PyAny>>
where
    T: FromStr + IntoPy<Py<PyAny>>,
{
    let v: Vec<T> = parsers::parse_tuple(value)?;
    let v = v.into_py(py);
    let builtins = PyModule::import(py, "builtins")?;
    builtins
        .getattr("tuple")?
        .call1((&v,))?
        .extract::<Py<PyAny>>()
}

fn str_to_py_datetime(py: Python, value: &str) -> PyResult<Py<PyAny>> {
    let datetime = PyDate::from_timestamp(py, parsers::parse_datetime_to_timestamp(value)?)?;
    Ok(Py::from(datetime))
}

fn str_to_py_date(py: Python, value: &str) -> PyResult<Py<PyAny>> {
    let date = PyDate::from_timestamp(py, parsers::parse_date_to_timestamp(value)?)?;
    Ok(Py::from(date))
}

fn str_to_py_bool(py: Python, value: &str) -> PyResult<Py<PyAny>> {
    let v = value.to_lowercase().parse::<bool>()?;
    Ok(v.into_py(py))
}

fn str_to_py_str(py: Python, value: &str) -> PyResult<Py<PyAny>> {
    let v = value.parse::<String>()?;
    Ok(v.into_py(py))
}

fn str_to_py_float(py: Python, value: &str) -> PyResult<Py<PyAny>> {
    let v = value.parse::<f64>()?;
    Ok(v.into_py(py))
}

fn str_to_py_int(py: Python, value: &str) -> PyResult<Py<PyAny>> {
    let v = value.parse::<i64>()?;
    Ok(v.into_py(py))
}
