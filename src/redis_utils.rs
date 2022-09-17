use std::collections::HashMap;
use std::fmt::Debug;
use std::str;

use pyo3::exceptions::{PyConnectionError, PyValueError};
use pyo3::{Py, PyAny, PyResult, Python};

use crate::store::Record;
use crate::{pyparsers, Model, Store};

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

/// Runs in pipeline but without transaction
pub(crate) fn run_without_transaction<T, F>(store: &mut Store, f: F) -> PyResult<T>
where
    F: FnOnce(&Store, &mut redis::Pipeline) -> PyResult<T>,
    T: redis::FromRedisValue + Debug,
{
    let mut pipe = redis::pipe();
    f(store, &mut pipe)?;

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

            let mut list_of_results: Vec<HashMap<String, Py<PyAny>>> = Default::default();

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

            for item in results {
                let record = pyparsers::parse_redis_single_raw_value(store, fields, item)?;
                list_of_results.push(record);
            }

            Ok(list_of_results)
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
    let model_index = get_model_index(model_name);

    pipe.hset_multiple(&name, &data);
    pipe.sadd(&model_index, &name);

    if let Some(life_span) = life_span {
        pipe.expire(&name, life_span);
        pipe.expire(&model_index, life_span);
    }

    Ok(name)
}

/// Gets the index key of the virtual table.
#[inline]
pub(crate) fn get_model_index(model_name: &str) -> String {
    format!("{}__index", model_name)
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

/// Converts a hashmap into a Model instance
pub(crate) fn parse_model(
    fields: &HashMap<String, Py<PyAny>>,
    store: &mut Store,
    data: &HashMap<String, String>,
) -> PyResult<Model> {
    let mut _data: HashMap<String, Py<PyAny>> = HashMap::with_capacity(data.len());
    for (k, v) in data {
        let field_type = fields.get(k);
        match field_type {
            None => {}
            Some(field_type) => {
                let value = pyparsers::str_to_py_obj(store, &v, field_type)?;
                _data.insert(k.clone(), value);
            }
        }
    }

    Ok(Model { _data })
}
