use std::collections::HashMap;
use std::hash::Hash;
use std::str::FromStr;

use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::PyModule;
use pyo3::types::{IntoPyDict, PyDate, PyType};
use pyo3::{IntoPy, Py, PyAny, PyResult, Python};
use redis::FromRedisValue;

use crate::model::ModelMeta;
use crate::store::find_one_by_raw_id;
use crate::{parsers, Store};

macro_rules! py_key_error {
    ($v:expr, $det:expr) => {
        PyValueError::new_err(format!("{:?} (value was {:?})", $det, $v))
    };
}

macro_rules! py_value_error {
    ($v:expr, $det:expr) => {
        PyKeyError::new_err(format!("{:?} (key was {:?})", $det, $v))
    };
}

macro_rules! to_py {
    ($v:expr) => {
        Ok(Python::with_gil(|py| $v.into_py(py)))
    };
}

pub(crate) fn parse_redis_single_raw_value(
    store: &mut Store,
    fields: &HashMap<String, Py<PyAny>>,
    value: &redis::Value,
) -> PyResult<HashMap<String, Py<PyAny>>> {
    let value_as_map = value.as_map_iter();
    match value_as_map {
        None => {
            let v = redis_to_py::<String>(value)?;
            let v = Python::with_gil(|py| v.into_py(py));
            Ok(HashMap::from([("key".to_string(), v)]))
        }
        Some(value) => value
            .map(|(k, v)| {
                let k = redis::from_redis_value::<String>(k)
                    .or_else(|e| Err(py_value_error!(&k, e.to_string())))?;
                let field_type = fields
                    .get(&k)
                    .ok_or_else(|| py_key_error!(&k, "Unexpected field name"))?;
                Ok((k, redis_to_py_value(store, v, field_type)?))
            })
            .collect(),
    }
}

pub(crate) fn redis_to_py_value(
    store: &mut Store,
    value: &redis::Value,
    field_type: &Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let field_type_name = get_name_of_py_type(field_type)?;

    if field_type_name == "int" {
        let v = redis_to_py::<i64>(value)?;
        to_py!(v)
    } else if field_type_name == "float" {
        let v = redis_to_py::<f64>(value)?;
        to_py!(v)
    } else if field_type_name == "str" {
        let v = redis_to_py::<String>(value)?;
        to_py!(v)
    } else if field_type_name == "bool" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_bool(&v)
    } else if field_type_name == "date" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_date(&v)
    } else if field_type_name == "datetime" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_datetime(&v)
    } else if field_type_name.starts_with("typing.Tuple[int") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_tuple::<i64>(&v)
    } else if field_type_name.starts_with("typing.Tuple[float") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_tuple::<f64>(&v)
    } else if field_type_name.starts_with("typing.Tuple[str") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_tuple::<String>(&v)
    } else if field_type_name.starts_with("typing.Tuple[bool") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_tuple::<bool>(&v)
    } else if field_type_name.starts_with("typing.Tuple[") {
        Err(PyTypeError::new_err(format!(
            "tuples of type {} are not supported yet",
            field_type_name
        )))
    } else if field_type_name.starts_with("typing.List[int") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_list::<i64>(&v)
    } else if field_type_name.starts_with("typing.List[float") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_list::<f64>(&v)
    } else if field_type_name.starts_with("typing.List[str") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_list::<String>(&v)
    } else if field_type_name.starts_with("typing.List[bool") {
        let v = redis_to_py::<String>(value)?;
        str_to_py_list::<bool>(&v)
    } else if field_type_name.starts_with("typing.Tuple[") {
        Err(PyTypeError::new_err(format!(
            "lists of type {} are not supported yet",
            field_type_name
        )))
    } else if field_type_name == "typing.Dict[int, int]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<i64, i64>(&v)
    } else if field_type_name == "typing.Dict[int, float]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<i64, f64>(&v)
    } else if field_type_name == "typing.Dict[int, str]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<i64, String>(&v)
    } else if field_type_name == "typing.Dict[int, bool]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<i64, bool>(&v)
    } else if field_type_name == "typing.Dict[str, int]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<String, i64>(&v)
    } else if field_type_name == "typing.Dict[str, float]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<String, f64>(&v)
    } else if field_type_name == "typing.Dict[str, str]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<String, String>(&v)
    } else if field_type_name == "typing.Dict[str, bool]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<String, bool>(&v)
    } else if field_type_name == "typing.Dict[bool, str]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<bool, String>(&v)
    } else if field_type_name == "typing.Dict[bool, int]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<bool, i64>(&v)
    } else if field_type_name == "typing.Dict[bool, float]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<bool, f64>(&v)
    } else if field_type_name == "typing.Dict[bool, bool]" {
        let v = redis_to_py::<String>(value)?;
        str_to_py_dict::<bool, bool>(&v)
    } else if field_type_name.starts_with("typing.Dict[") {
        Err(PyTypeError::new_err(format!(
            "dictionaries of type {} are not supported yet",
            field_type_name
        )))
    } else {
        let model_name = field_type_name.to_lowercase();

        if let Some(model_meta) = store.models.get(&model_name) {
            let model_meta = model_meta.clone();
            let v = parse_redis_single_raw_value(store, &model_meta.fields, value)?;
            Python::with_gil(|py| {
                let v = v.into_py_dict(py);
                field_type.call(py, (), Some(v))
            })
        } else {
            Err(PyTypeError::new_err(format!(
                "type annotation {} is not supported",
                field_type_name
            )))
        }
    }
}

pub fn str_to_py_obj(
    store: &mut Store,
    value: &str,
    field_type: &Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    let name = get_name_of_py_type(field_type)?;
    let name = name.as_str();

    if name == "int" {
        str_to_py_int(value)
    } else if name == "float" {
        str_to_py_float(value)
    } else if name == "str" {
        str_to_py_str(value)
    } else if name == "bool" {
        str_to_py_bool(value)
    } else if name == "date" {
        str_to_py_date(value)
    } else if name == "datetime" {
        str_to_py_datetime(value)
    } else if name.starts_with("typing.Tuple[int") {
        str_to_py_tuple::<i64>(value)
    } else if name.starts_with("typing.Tuple[float") {
        str_to_py_tuple::<f64>(value)
    } else if name.starts_with("typing.Tuple[str") {
        str_to_py_tuple::<String>(value)
    } else if name.starts_with("typing.Tuple[bool") {
        str_to_py_tuple::<bool>(value)
    } else if name.starts_with("typing.Tuple[") {
        Err(PyTypeError::new_err(format!(
            "tuples of type {} are not supported yet",
            name
        )))
    } else if name.starts_with("typing.List[int") {
        str_to_py_list::<i64>(value)
    } else if name.starts_with("typing.List[float") {
        str_to_py_list::<f64>(value)
    } else if name.starts_with("typing.List[str") {
        str_to_py_list::<String>(value)
    } else if name.starts_with("typing.List[bool") {
        str_to_py_list::<bool>(value)
    } else if name.starts_with("typing.List[") {
        Err(PyTypeError::new_err(format!(
            "lists of type {} are not supported yet",
            name
        )))
    } else if name == "typing.Dict[int, int]" {
        str_to_py_dict::<i64, i64>(value)
    } else if name == "typing.Dict[int, float]" {
        str_to_py_dict::<i64, f64>(value)
    } else if name == "typing.Dict[int, str]" {
        str_to_py_dict::<i64, String>(value)
    } else if name == "typing.Dict[int, bool]" {
        str_to_py_dict::<i64, bool>(value)
    } else if name == "typing.Dict[str, int]" {
        str_to_py_dict::<String, i64>(value)
    } else if name == "typing.Dict[str, float]" {
        str_to_py_dict::<String, f64>(value)
    } else if name == "typing.Dict[str, str]" {
        str_to_py_dict::<String, String>(value)
    } else if name == "typing.Dict[str, bool]" {
        str_to_py_dict::<String, bool>(value)
    } else if name == "typing.Dict[bool, str]" {
        str_to_py_dict::<bool, String>(value)
    } else if name == "typing.Dict[bool, int]" {
        str_to_py_dict::<bool, i64>(value)
    } else if name == "typing.Dict[bool, float]" {
        str_to_py_dict::<bool, f64>(value)
    } else if name == "typing.Dict[bool, bool]" {
        str_to_py_dict::<bool, bool>(value)
    } else if name.starts_with("typing.Dict[") {
        Err(PyTypeError::new_err(format!(
            "dictionaries of type {} are not supported yet",
            name
        )))
    } else {
        let model_name = name.to_lowercase();
        if let Some(model_meta) = store.models.get(&model_name) {
            let model_meta = model_meta.clone();
            str_to_nested_model(store, &model_meta, value)
        } else {
            Err(PyTypeError::new_err(format!(
                "type annotation {} is not supported",
                name
            )))
        }
    }
}

fn str_to_nested_model(
    store: &mut Store,
    model_meta: &ModelMeta,
    value: &str,
) -> PyResult<Py<PyAny>> {
    let model_type = Python::with_gil(|py| model_meta.model_type.clone_ref(py));
    let nested_model = find_one_by_raw_id(store, model_meta.fields.clone(), value)?;
    match nested_model {
        None => {
            let none = Python::with_gil(|py| py.None());
            Ok(none)
        }
        Some(nested_model) => nested_model.to_subclass_instance(&model_type),
    }
}

fn str_to_py_dict<T, U>(value: &str) -> PyResult<Py<PyAny>>
where
    T: FromStr + Hash + std::cmp::Eq + IntoPy<Py<PyAny>>,
    U: FromStr + IntoPy<Py<PyAny>>,
{
    let v: HashMap<T, U> = parsers::parse_dict(value)?;
    let v = Python::with_gil(|py| v.into_py(py));
    Ok(v)
}

fn str_to_py_list<T>(value: &str) -> PyResult<Py<PyAny>>
where
    T: FromStr + IntoPy<Py<PyAny>>,
{
    let v: Vec<T> = parsers::parse_list(value)?;
    let v = Python::with_gil(|py| v.into_py(py));
    Ok(v)
}

fn str_to_py_tuple<T>(value: &str) -> PyResult<Py<PyAny>>
where
    T: FromStr + IntoPy<Py<PyAny>>,
{
    let v: Vec<T> = parsers::parse_tuple(value)?;
    Python::with_gil(|py| {
        let v = v.into_py(py);
        let builtins = PyModule::import(py, "builtins")?;
        builtins
            .getattr("tuple")?
            .call1((&v,))?
            .extract::<Py<PyAny>>()
    })
}

fn str_to_py_datetime(value: &str) -> PyResult<Py<PyAny>> {
    let timestamp = parsers::parse_datetime_to_timestamp(value)?;
    let datetime = timestamp_to_py_date(timestamp)?;
    Ok(datetime)
}

fn str_to_py_date(value: &str) -> PyResult<Py<PyAny>> {
    let timestamp = parsers::parse_date_to_timestamp(value)?;
    let date = timestamp_to_py_date(timestamp)?;
    Ok(date)
}

fn timestamp_to_py_date(timestamp: i64) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        let v = PyDate::from_timestamp(py, timestamp)?;
        Ok(Py::from(v))
    })
}

fn str_to_py_bool(value: &str) -> PyResult<Py<PyAny>> {
    let v = value.to_lowercase().parse::<bool>()?;
    let v = Python::with_gil(|py| v.into_py(py));
    Ok(v)
}

fn str_to_py_str(value: &str) -> PyResult<Py<PyAny>> {
    let v = value.parse::<String>()?;
    let v = Python::with_gil(|py| v.into_py(py));
    Ok(v)
}

fn str_to_py_float(value: &str) -> PyResult<Py<PyAny>> {
    let v = value.parse::<f64>()?;
    let v = Python::with_gil(|py| v.into_py(py));
    Ok(v)
}

fn str_to_py_int(value: &str) -> PyResult<Py<PyAny>> {
    let v = value.parse::<i64>()?;
    let v = Python::with_gil(|py| v.into_py(py));
    Ok(v)
}

/// Returns the name of the pytype as a string.
/// Since this is got from type hints, some of the actual types might be GeneralAliases and not classes
#[inline]
fn get_name_of_py_type(field_type: &Py<PyAny>) -> PyResult<String> {
    Python::with_gil(|py| -> PyResult<String> {
        let field_name = field_type.as_ref(py).downcast::<PyType>();
        match field_name {
            Ok(v) => {
                let v = v.name()?;
                Ok(v.to_string())
            }
            Err(_) => Ok(field_type.to_string()),
        }
    })
}

#[inline]
fn redis_to_py<T>(v: &redis::Value) -> PyResult<T>
where
    T: FromRedisValue,
{
    redis::from_redis_value::<T>(v).or_else(|e| Err(PyValueError::new_err(e.to_string())))
}
