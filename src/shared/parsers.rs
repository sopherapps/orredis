use std::collections::HashMap;
use std::str::FromStr;

use chrono::{NaiveDate, TimeZone, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{timezone_utc, PyDate, PyDateTime};
use redis::{FromRedisValue, Value};

use crate::shared::collections::CollectionMeta;
use crate::shared::macros::{py_key_error, py_value_error};

/// Parses datetime strings into timestamps using the "%Y-%m-%d %H:%M:%S.6%f%:z" format which was the default format
/// on my PC :-) for UTC times
pub fn parse_datetime_to_timestamp(value: &str) -> PyResult<i64> {
    let datetime = Utc
        .datetime_from_str(value, "%Y-%m-%d %H:%M:%S%.6f%:z")
        .or(Err(PyValueError::new_err(format!(
            "error parsing {} as '%Y-%m-%d %H:%M:%S%.6f%:z'",
            value
        ))))?;
    Ok(datetime.timestamp())
}

/// Parses date strings into timestamps using the %Y-%m-%d format
pub fn parse_date_to_timestamp(value: &str) -> PyResult<i64> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").or(Err(PyValueError::new_err(
        format!("error parsing {} as Year-Month-Date", value),
    )))?;
    let datetime = date.and_hms(0, 0, 0);
    Ok(datetime.timestamp())
}

/// Extracts the portions of string from a string representation of a given value
pub(crate) fn extract_str_portions<'a>(
    value: &'a str,
    start_char: &'a str,
    end_char: &'a str,
    separator: &'a str,
) -> Vec<&'a str> {
    value
        .trim_start_matches(start_char)
        .trim_end_matches(end_char)
        .split(separator)
        .into_iter()
        .map(|v| v.trim().trim_end_matches("'").trim_start_matches("'"))
        .collect()
}

/// Redis value to pyresult type
#[inline]
pub(crate) fn redis_to_py<T>(v: &redis::Value) -> PyResult<T>
where
    T: FromRedisValue,
{
    redis::from_redis_value::<T>(v).or_else(|e| Err(PyValueError::new_err(e.to_string())))
}

/// Parses a string into the given type, returning a PyValue error if it fails
///
/// # Errors
///
/// [PyValueError](PyValueError) is returned if parsing fails
///
#[inline]
pub(crate) fn parse_str<T>(data: &str) -> PyResult<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    data.parse::<T>()
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Converts a timestamp into a python date/datetime
pub(crate) fn timestamp_to_py_date(timestamp: i64) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        let v = PyDate::from_timestamp(py, timestamp)?;
        Ok(Py::from(v))
    })
}

/// Converts a timestamp into a python date/datetime
pub(crate) fn timestamp_to_py_datetime(timestamp: i64) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        let v = PyDateTime::from_timestamp(py, timestamp as f64, Some(timezone_utc(py)))?;
        Ok(Py::from(v))
    })
}

/// Parses the response got from redis lua scripts i.e. a list of lists,
/// Converting it into a list of py values
pub(crate) fn parse_lua_script_response<F>(
    meta: &CollectionMeta,
    item_parser: F,
    result: Value,
) -> PyResult<Vec<Py<PyAny>>>
where
    F: FnOnce(HashMap<String, Py<PyAny>>) -> PyResult<Py<PyAny>> + Copy,
{
    let results = result
        .as_sequence()
        .ok_or_else(|| py_value_error!(result, "Response from redis is of unexpected shape"))?
        .get(0)
        .ok_or_else(|| py_value_error!(result, "Response from redis is of unexpected shape"))?
        .as_sequence()
        .ok_or_else(|| py_value_error!(result, "Response from redis is of unexpected shape"))?;

    let empty_value = redis::Value::Bulk(vec![]);
    let mut list_of_results: Vec<Py<PyAny>> = Vec::with_capacity(results.len());

    for item in results {
        if *item != empty_value {
            match item.as_map_iter() {
                None => return Err(py_value_error!(item, "redis value is not a map")),
                Some(item) => {
                    let data = item
                        .map(|(k, v)| {
                            let key = redis_to_py::<String>(k)?;
                            let value = match meta.schema.get_type(&key) {
                                Some(field_type) => field_type.redis_to_py(v),
                                None => {
                                    Err(py_key_error!(&key, "key found in data but not in schema"))
                                }
                            }?;
                            Ok((key, value))
                        })
                        .collect::<PyResult<HashMap<String, Py<PyAny>>>>()?;
                    let data = item_parser(data)?;
                    list_of_results.push(data);
                }
            }
        }
    }

    Ok(list_of_results)
}
