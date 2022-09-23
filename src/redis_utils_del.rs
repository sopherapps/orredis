use pyo3::exceptions::{PyConnectionError, PyValueError};
use pyo3::{Py, PyAny, PyResult, Python};
use std::collections::HashMap;
use std::str;

use crate::store_del::Record;
use crate::{pyparsers_del, Model, Store};

macro_rules! py_value_error {
    ($v:expr, $det:expr) => {
        PyValueError::new_err(format!("{:?} (value was {:?})", $det, $v))
    };
}

/// Opens a connection to redis given the url
pub(crate) fn connect_to_redis(url: &str) -> redis::RedisResult<redis::Connection> {
    let client = redis::Client::open(url)?;
    client.get_connection()
}

/// Runs a given routine in a redis transaction for atomicity.
pub(crate) fn run_in_transaction<T, F>(store: &mut Store, f: F) -> PyResult<T>
where
    F: FnOnce(&Store, &mut redis::Pipeline) -> PyResult<T>,
    T: redis::FromRedisValue,
{
    let mut pipe = redis::pipe();
    // attempt to open a transaction in a pipeline manually
    pipe.cmd("MULTI");
    f(store, &mut pipe)?;
    // attempt to close a transaction manually
    pipe.cmd("EXEC");

    let conn = store.conn.as_mut();
    match conn {
        None => Err(PyConnectionError::new_err("redis server disconnected")),
        Some(conn) => {
            let result = pipe
                .query::<T>(conn)
                .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;

            Ok(result)
        }
    }
}

/// Runs an EVAL
pub(crate) fn run_script<F>(
    store: &mut Store,
    fields: &HashMap<String, Py<PyAny>>,
    f: F,
) -> PyResult<Vec<HashMap<String, Py<PyAny>>>>
where
    F: FnOnce(&Store, &mut redis::Pipeline) -> PyResult<Vec<HashMap<String, Py<PyAny>>>>,
{
    let mut pipe = redis::pipe();
    f(store, &mut pipe)?;

    let conn = store.conn.as_mut();
    match conn {
        None => Err(PyConnectionError::new_err("redis server disconnected")),
        Some(conn) => {
            let result: redis::Value = pipe
                .query(conn)
                .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;

            // Adds about 800us
            let results = result
                .as_sequence()
                .ok_or_else(|| {
                    py_value_error!(result, "Response from redis is of unexpected shape")
                })?
                .get(0)
                .ok_or_else(|| {
                    py_value_error!(result, "Response from redis is of unexpected shape")
                })?
                .as_sequence()
                .ok_or_else(|| {
                    py_value_error!(result, "Response from redis is of unexpected shape")
                })?;

            let list_of_results: PyResult<Vec<HashMap<String, Py<PyAny>>>> = results
                .into_iter()
                .map(|item| pyparsers_del::parse_redis_single_raw_value(store, fields, item))
                .collect();

            Ok(list_of_results?)
        }
    }
}

/// Inserts a given model instance on a pipeline without executing the pipeline
/// The caller is expected to execute the pipeline
pub fn insert_on_pipeline(
    store: &Store,
    pipe: &mut redis::Pipeline,
    model_name: &str,
    life_span: Option<usize>,
    key: &str,
    raw_data: &Record,
) -> PyResult<String> {
    let name = get_primary_key(model_name, key);
    let data = serialize_to_key_value_pairs(store, pipe, raw_data, life_span)?;

    pipe.hset_multiple(&name, &data);

    if let Some(life_span) = life_span {
        pipe.expire(&name, life_span);
    }

    Ok(name)
}

/// Gets the primary key for the given table-key combination
#[inline]
pub(crate) fn get_primary_key(model_name: &str, key: &str) -> String {
    format!("{}_%&_{}", model_name, key)
}

fn serialize_to_key_value_pairs(
    store: &Store,
    pipe: &mut redis::Pipeline,
    raw_data: &Record,
    life_span: Option<usize>,
) -> PyResult<Vec<(String, String)>> {
    let mut data: Vec<(String, String)> = Default::default();
    let raw_data = match raw_data {
        Record::Full { data } => data.dict()?,
        Record::Partial { data } => data.clone(),
    };

    for (k, v) in raw_data {
        let model_result: PyResult<Model> = Python::with_gil(|py| {
            let v_ptr = v.as_ref(py);
            v_ptr.extract()
        });

        match model_result {
            Ok(model) => {
                let model_name = Model::get_instance_model_name(v)?;
                let model_meta =
                    store
                        .models
                        .get(&model_name)
                        .ok_or(PyValueError::new_err(format!(
                            "{} does not exist on this store",
                            model_name
                        )))?;

                let key = model.get(&model_meta.primary_key_field)?;
                let key = format!("{}", key);
                let record = Record::Full { data: model };
                let foreign_key =
                    insert_on_pipeline(store, pipe, &model_name, life_span, &key, &record)?;
                data.push((k, foreign_key));
            }
            Err(_) => {
                data.push((k, format!("{}", v)));
            }
        }
    }
    Ok(data)
}
