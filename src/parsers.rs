use std::any::type_name;
use std::collections::HashMap;
use std::hash::Hash;
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::PyResult;

/// Parses string into vectors that can be converted to py tuples
pub fn parse_tuple<T>(value: &str) -> PyResult<Vec<T>>
where
    T: FromStr,
{
    let mut v: Vec<T> = Default::default();
    let items = extract_str_portions(value, "(", ")", ",");

    for item in items {
        let parsed_item = item.parse::<T>().or(Err(PyTypeError::new_err(
            "typing.Tuple fields can only have one type.",
        )))?;
        v.push(parsed_item);
    }

    Ok(v)
}

/// Parses strings into vectors that can be converted into python lists
pub fn parse_list<T>(value: &str) -> PyResult<Vec<T>>
where
    T: FromStr,
{
    let mut v: Vec<T> = Default::default();
    let items = extract_str_portions(value, "[", "]", ",");
    for item in items {
        let parsed_item = item.parse::<T>().or(Err(PyTypeError::new_err(format!(
            "failed to parse {} to List of {}",
            value,
            type_name::<T>()
        ))))?;
        v.push(parsed_item);
    }

    Ok(v)
}

/// Parses a string representation of a dictionary into a hashmap
pub fn parse_dict<T, U>(value: &str) -> PyResult<HashMap<T, U>>
where
    T: FromStr + Hash + std::cmp::Eq,
    U: FromStr,
{
    let mut v: HashMap<T, U> = Default::default();
    let items = extract_str_portions(value, "{", "}", ",");

    for item in items {
        let kv_items = extract_str_portions(item, "", "", ":");

        if kv_items.len() == 2 {
            let (key, value) = (kv_items[0], kv_items[1]);

            let key = key.parse::<T>().or(Err(PyTypeError::new_err(format!(
                "failed to parse key {} to type of {}",
                value,
                type_name::<T>()
            ))))?;

            let value = value.parse::<U>().or(Err(PyTypeError::new_err(format!(
                "failed to parse value {} to type of {}",
                value,
                type_name::<U>()
            ))))?;

            v.insert(key, value);
        }
    }

    Ok(v)
}

/// Parses datetime strings into timestamps using the "YYYY-MM-DD HH:MM:SS.mmmmmm" format which was the default format
/// on my PC :-)
/// for python i.e. "YYYY-MM-DD HH:MM:SS.mmmmmm" (python) or "%Y-%m-%d %H:%M:%S.%f" (rust)
pub fn parse_datetime_to_timestamp(value: &str) -> PyResult<i64> {
    let datetime = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.6f").or(Err(
        PyValueError::new_err(format!(
            "error parsing {} as 'YYYY-MM-DD HH:MM:SS.mmmmmm'",
            value
        )),
    ))?;
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
fn extract_str_portions<'a>(
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
