use std::str::FromStr;

use chrono::{NaiveDate, TimeZone, Utc};
use pyo3::exceptions::PyValueError;
use pyo3::PyResult;
use redis::FromRedisValue;

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
