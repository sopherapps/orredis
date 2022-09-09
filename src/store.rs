extern crate redis;

use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use redis::{Connection, ConnectionLike, RedisError, RedisResult};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[pyclass]
pub struct Redis {
    conn: Option<redis::Connection>,
    url: String,
}

#[pymethods]
impl Redis {
    // __init__
    // [x] reopen
    // [x] close
    // [x] insert_dict
    // [x] insert_dict_list
    // select
    // update
    // delete
    // [x] is_open

    #[new]
    pub fn new(url: Py<PyAny>) -> PyResult<Self> {
        Python::with_gil(|py| -> PyResult<Redis> {
            let url: &str = url.extract(py)?;
            let conn = connect_to_redis(url)
                .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;

            Ok(Redis {
                conn: Some(conn),
                url: url.to_string(),
            })
        })
    }

    #[args(table, key, data, life_span = "None")]
    #[pyo3(text_signature = "($self, table, key, data, lifespan)")]
    pub(crate) fn insert_dict(
        &mut self,
        table: &str,
        key: &str,
        data: Py<PyDict>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        let conn = self.conn.as_mut();
        match conn {
            None => Err(PyConnectionError::new_err("redis server disconnected")),
            Some(conn) => run_in_transaction(conn, |pipe| {
                Python::with_gil(|py| {
                    let data_cast_result: PyResult<HashMap<String, Py<PyAny>>> = data.extract(py);
                    data_cast_result.and_then(|raw_data| {
                        insert_on_pipeline(pipe, table, life_span, key, &raw_data)?;
                        Ok(())
                    })
                })
            }),
        }
    }

    #[args(table, key, data, life_span = "None")]
    #[pyo3(text_signature = "($self, table, key, data, lifespan)")]
    pub(crate) fn insert_dict_list(
        &mut self,
        table: &str,
        key_field: &str,
        data: Py<PyList>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        let conn = self.conn.as_mut();
        match conn {
            None => Err(PyConnectionError::new_err("redis server disconnected")),
            Some(conn) => run_in_transaction(conn, |pipe| {
                Python::with_gil(|py| {
                    let data_cast_result: PyResult<Vec<HashMap<String, Py<PyAny>>>> =
                        data.extract(py);
                    data_cast_result.and_then(|data_list| {
                        for raw_data in &data_list {
                            let key: &str = raw_data
                                .get(key_field)
                                .ok_or(PyKeyError::new_err(key_field.to_string()))
                                .and_then(|raw_key| raw_key.extract(py))?;

                            insert_on_pipeline(pipe, table, life_span, key, raw_data)?;
                        }
                        Ok(())
                    })
                })
            }),
        }
    }

    #[args(table, ids = "None", columns = "None")]
    #[pyo3(text_signature = "($self, table, ids, columns)")]
    pub(crate) fn select(
        &mut self,
        table: &str,
        ids: Option<Vec<&str>>,
        columns: Option<Vec<&str>>,
    ) -> PyResult<Vec<HashMap<String, String>>> {
        let conn = self.conn.as_mut();
        match conn {
            None => Err(PyConnectionError::new_err("redis server disconnected")),
            Some(conn) => run_in_transaction(conn, |pipe| {
                let result: Vec<HashMap<String, String>> = vec![];

                Ok(result)
            }),
        }
    }

    pub fn is_open(&self) -> PyResult<bool> {
        match &self.conn {
            None => Ok(false),
            Some(conn) => Ok(conn.is_open()),
        }
    }

    pub fn close(&mut self) -> PyResult<()> {
        if let Some(conn) = self.conn.take() {
            drop(conn);
        }

        Ok(())
    }

    pub fn reopen(&mut self) -> PyResult<()> {
        match self.conn {
            None => {
                let new_conn = connect_to_redis(&self.url)
                    .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;
                self.conn = Some(new_conn);
                Ok(())
            }
            Some(_) => Ok(()),
        }
    }

    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl Debug for Redis {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Redis").field("url", &self.url).finish()
    }
}

pub(crate) fn run_in_transaction<T, F>(conn: &mut redis::Connection, f: F) -> PyResult<T>
where
    F: FnOnce(&mut redis::Pipeline) -> PyResult<T>,
    T: redis::FromRedisValue,
{
    let mut pipe = redis::pipe();
    // attempt to open a transaction in a pipeline manually
    pipe.cmd("MULTI");
    f(&mut pipe)?;
    // attempt to close a transaction manually
    pipe.cmd("EXEC");
    pipe.query::<T>(conn)
        .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))
}

pub(crate) fn connect_to_redis(url: &str) -> redis::RedisResult<redis::Connection> {
    let client = redis::Client::open(url)?;
    client.get_connection()
}

pub(crate) fn insert_on_pipeline(
    pipe: &mut redis::Pipeline,
    table: &str,
    life_span: Option<usize>,
    key: &str,
    raw_data: &HashMap<String, Py<PyAny>>,
) -> PyResult<String> {
    let name = format!("{}_%&_{}", table, key);
    let data = serialize_to_key_value_pairs(pipe, raw_data)?;

    pipe.hset_multiple(&name, &data);
    pipe.sadd(table, &name);

    if let Some(life_span) = life_span {
        pipe.expire(&name, life_span);
        pipe.expire(table, life_span);
    }

    Ok(name)
}

pub(crate) fn serialize_to_key_value_pairs(
    pipe: &mut redis::Pipeline,
    raw_data: &HashMap<String, Py<PyAny>>,
) -> PyResult<Vec<(String, String)>> {
    Python::with_gil(|py| -> PyResult<Vec<(String, String)>> {
        let mut data: Vec<(String, String)> = Vec::with_capacity(raw_data.len());

        for (field, value) in raw_data {
            let value = value.as_ref(py);
            let field_type = value.get_type().to_string();
            if field_type.contains(".") {
                let kv_pair = serialize_nested_model(pipe, field, value, &field_type);
                match kv_pair {
                    None => data.push((field.to_owned(), value.to_string())),
                    Some((k, v)) => data.push((k, v)),
                }
            } else {
                data.push((field.to_owned(), value.to_string()));
            }
        }

        Ok(data)
    })
}

/// serialize_nested_model will return the key with a '__' suffix and the foreign key
/// if the nested data is actually a nested model that has a dict() method
fn serialize_nested_model(
    pipe: &mut redis::Pipeline,
    key: &str,
    nested_model: &PyAny,
    field_type: &str,
) -> Option<(String, String)> {
    let name_portions: Vec<&str> = field_type.rsplit(".").collect();

    match name_portions.last() {
        Some(table) => {
            let result = nested_model.call_method("dict", (), None);
            match result {
                Ok(dict) => dict
                    .extract::<HashMap<String, Py<PyAny>>>()
                    .and_then(|data| {
                        let suffixed_key = format!("__{}", key);
                        let foreign_key =
                            insert_on_pipeline(pipe, &table, None, &suffixed_key, &data)?;
                        Ok((suffixed_key, foreign_key))
                    })
                    .ok(),
                Err(_) => None,
            }
        }
        None => None,
    }
}
