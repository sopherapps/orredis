extern crate redis;

use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use redis::{Commands, ConnectionLike};
use serde_json::json;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[pyclass]
pub struct Redis {
    conn: Option<redis::Connection>,
    url: String,
}

#[pymethods]
impl Redis {
    /// Initializes Redis
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

    /// Inserts a python dict object into redis in the given virtual table, with the given life span
    #[args(table, key_field, data, life_span = "None")]
    #[pyo3(text_signature = "($self, table, key_field, data, lifespan)")]
    pub fn insert_dict(
        &mut self,
        table: &str,
        key_field: &str,
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
                        let key: String = raw_data
                            .get(key_field)
                            .ok_or(PyKeyError::new_err(format!("{}", key_field)))
                            .and_then(|raw_key| Ok(format!("{}", raw_key)))?;

                        insert_on_pipeline(pipe, table, life_span, &key, &raw_data)?;
                        Ok(())
                    })
                })
            }),
        }
    }

    /// Inserts multiple dicts in redis in the given virtual table
    #[args(table, key, data, life_span = "None")]
    #[pyo3(text_signature = "($self, table, key, data, lifespan)")]
    pub fn insert_dict_list(
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
                            let key: String = raw_data
                                .get(key_field)
                                .ok_or(PyKeyError::new_err(key_field.to_string()))
                                .and_then(|raw_key| Ok(format!("{}", raw_key)))?;

                            insert_on_pipeline(pipe, table, life_span, &key, raw_data)?;
                        }
                        Ok(())
                    })
                })
            }),
        }
    }

    /// Selects the given ids or if none is given, all ids are selected for the given virtual table
    #[args(table, ids = "None", columns = "None", nested_columns = "None")]
    #[pyo3(text_signature = "($self, table, ids, columns, nested_columns)")]
    pub fn select(
        &mut self,
        table: &str,
        ids: Option<Vec<Py<PyAny>>>,
        columns: Option<Vec<&str>>,
        nested_columns: Option<HashMap<&str, String>>,
    ) -> PyResult<Vec<HashMap<String, String>>> {
        let conn = self.conn.as_mut();

        match conn {
            None => Err(PyConnectionError::new_err("redis server disconnected")),
            Some(conn) => {
                let table_index = get_table_index(table);
                let ids: Vec<String> = match ids {
                    None => conn
                        .sscan(&table_index)
                        .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?
                        .collect(),
                    Some(list) => list
                        .into_iter()
                        .map(|k| format!("{}", k))
                        .map(|k| get_primary_key(table, &k))
                        .collect(),
                };

                let mut response = match columns {
                    None => {
                        let raw_data = run_without_transaction(
                            conn,
                            |pipe| -> PyResult<Vec<HashMap<String, String>>> {
                                for k in ids {
                                    pipe.hgetall(k);
                                }
                                Ok(vec![])
                            },
                        )?;
                        raw_data
                    }
                    Some(cols) => {
                        let raw =
                            run_without_transaction(conn, |pipe| -> PyResult<Vec<Vec<String>>> {
                                for k in ids {
                                    pipe.cmd("HMGET").arg(k).arg(&cols);
                                }
                                Ok(vec![])
                            })?;

                        raw.into_iter()
                            .map(|item| {
                                item.into_iter()
                                    .zip(&cols)
                                    .map(|(v, k)| (k.to_string(), v))
                                    .collect::<HashMap<String, String>>()
                            })
                            .collect()
                    }
                };

                // do some eager loading, but only handling single-level nesting
                match nested_columns {
                    None => {}
                    Some(nested_col_map) => {
                        for (field, table) in nested_col_map {
                            let eager_response = run_without_transaction(
                                conn,
                                |pipe| -> PyResult<Vec<HashMap<String, String>>> {
                                    for i in 0..response.len() {
                                        let item = &response[i];
                                        let f_key =
                                            item.get(field).unwrap_or(&"".to_string()).to_owned();
                                        pipe.hgetall(&f_key);
                                    }
                                    Ok(vec![])
                                },
                            )?;

                            for i in 0..response.len() {
                                let _ = &response[i].insert(
                                    field.to_string(),
                                    format!("{}", json!(eager_response[i])),
                                );
                            }
                        }
                    }
                }

                Ok(response)
            }
        }
    }

    /// Updates the record of the given key in the given virtual table
    #[args(table, key, data, life_span = "None")]
    #[pyo3(text_signature = "($self, table, key, data, life_span)")]
    pub fn update(
        &mut self,
        table: &str,
        key: Py<PyAny>,
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
                        let key = format!("{}", key);
                        insert_on_pipeline(pipe, table, life_span, &key, &raw_data)?;
                        Ok(())
                    })
                })
            }),
        }
    }

    /// Deletes the records of the given ids from the given virtual table in redis
    #[args(table, ids)]
    #[pyo3(text_signature = "($self, table, ids)")]
    pub fn delete(&mut self, table: &str, ids: Vec<Py<PyAny>>) -> PyResult<()> {
        let conn = self.conn.as_mut();
        match conn {
            None => Err(PyConnectionError::new_err("redis server disconnected")),
            Some(conn) => run_in_transaction(conn, |pipe| {
                let table_index = get_table_index(table);
                let keys: Vec<String> = ids
                    .into_iter()
                    .map(|k| format!("{}", k))
                    .map(|k| get_primary_key(table, &k))
                    .collect();

                pipe.del(&keys);
                pipe.srem(table_index, &keys);

                Ok(())
            }),
        }
    }

    /// Clears all keys on this redis instance
    #[args(asynchronous = "false")]
    #[pyo3(text_signature = "($self, asynchronous)")]
    pub fn flushall(&mut self, asynchronous: bool) -> PyResult<()> {
        let conn = self.conn.as_mut();
        match conn {
            None => Err(PyConnectionError::new_err("redis server disconnected")),
            Some(conn) => {
                let arg = if asynchronous { "ASYNC" } else { "SYNC" };
                redis::cmd("FLUSHALL")
                    .arg(arg)
                    .query(conn)
                    .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))
            }
        }
    }

    /// Checks to see if redis is still connected
    #[pyo3(text_signature = "($self)")]
    pub fn is_open(&self) -> PyResult<bool> {
        match &self.conn {
            None => Ok(false),
            Some(conn) => Ok(conn.is_open()),
        }
    }

    /// Closes the connection to redis
    #[pyo3(text_signature = "($self)")]
    pub fn close(&mut self) -> PyResult<()> {
        if let Some(conn) = self.conn.take() {
            drop(conn);
        }

        Ok(())
    }

    /// Reopens the connection to redis
    #[pyo3(text_signature = "($self)")]
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

    /// The string representation of this class in python
    pub fn __str__(&self) -> PyResult<String> {
        Ok(format!("{:?}", self))
    }
}

impl Debug for Redis {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Redis").field("url", &self.url).finish()
    }
}

/// Runs a given routine in a redis transaction for atomicity.
fn run_in_transaction<T, F>(conn: &mut redis::Connection, f: F) -> PyResult<T>
where
    F: FnOnce(&mut redis::Pipeline) -> PyResult<T>,
    T: redis::FromRedisValue + serde::ser::Serialize,
{
    let mut pipe = redis::pipe();
    // attempt to open a transaction in a pipeline manually
    pipe.cmd("MULTI");
    f(&mut pipe)?;
    // attempt to close a transaction manually
    pipe.cmd("EXEC");
    let result = pipe
        .query::<T>(conn)
        .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;

    Ok(result)
}

/// Runs in pipeline but without transaction
fn run_without_transaction<T, F>(conn: &mut redis::Connection, f: F) -> PyResult<T>
where
    F: FnOnce(&mut redis::Pipeline) -> PyResult<T>,
    T: redis::FromRedisValue + serde::ser::Serialize,
{
    let mut pipe = redis::pipe();
    f(&mut pipe)?;
    let result = pipe
        .query::<T>(conn)
        .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;

    Ok(result)
}

/// Opens a connection to redis given the url
fn connect_to_redis(url: &str) -> redis::RedisResult<redis::Connection> {
    let client = redis::Client::open(url)?;
    client.get_connection()
}

/// Inserts a given item on a pipeline without executing the pipeline
fn insert_on_pipeline(
    pipe: &mut redis::Pipeline,
    table: &str,
    life_span: Option<usize>,
    key: &str,
    raw_data: &HashMap<String, Py<PyAny>>,
) -> PyResult<String> {
    let name = get_primary_key(table, key);
    let data = serialize_to_key_value_pairs(pipe, raw_data)?;
    let table_index = get_table_index(table);

    pipe.hset_multiple(&name, &data);
    pipe.sadd(&table_index, &name);

    if let Some(life_span) = life_span {
        pipe.expire(&name, life_span);
        pipe.expire(&table_index, life_span);
    }

    Ok(name)
}

/// Converts a hashmap whose values are any oython object, into a vector of (key, value) tuples
/// with the value converted to a string representation
fn serialize_to_key_value_pairs<'a>(
    pipe: &mut redis::Pipeline,
    raw_data: &'a HashMap<String, Py<PyAny>>,
) -> PyResult<Vec<(&'a str, String)>> {
    Python::with_gil(|py| -> PyResult<Vec<(&'a str, String)>> {
        let mut data: Vec<(&'a str, String)> = Vec::with_capacity(raw_data.len());

        for (field, value) in raw_data {
            let value = value.as_ref(py);
            let field_type = value.get_type().to_string();
            if field_type.contains(".") {
                let foreign_key = serialize_py_value(pipe, value, &field_type)?;
                data.push((field, foreign_key));
            } else {
                data.push((field, format!("{}", value)));
            }
        }

        Ok(data)
    })
}

/// serialize_py_value will return a foreign key of a value if the value is a nested model
/// or it will return the value as a string
/// if the nested data is actually a nested model that has a dict() method and a get_primary_key_field
/// method
fn serialize_py_value(
    pipe: &mut redis::Pipeline,
    nested_model: &PyAny,
    field_type: &str,
) -> PyResult<String> {
    let name_portions: Vec<&str> = field_type.rsplit(".").collect();

    match name_portions.first() {
        Some(table) => {
            let table = table.trim_end_matches("'>").to_lowercase();
            let result = nested_model.call_method("dict", (), None);
            let key_field_result = nested_model
                .call_method("get_primary_key_field", (), None)
                .and_then(|v| Ok(format!("{}", v)));
            match result {
                Ok(dict) => dict
                    .extract::<HashMap<String, Py<PyAny>>>()
                    .and_then(|data| match key_field_result {
                        Ok(key_field) => {
                            let key = data
                                .get(&key_field)
                                .and_then(|v| Some(format!("{}", v)))
                                .ok_or(PyKeyError::new_err(format!(
                                    "{} not found on {}",
                                    &key_field, field_type
                                )))?;
                            insert_on_pipeline(pipe, &table, None, &key, &data)
                        }
                        Err(e) => Err(e),
                    }),
                Err(_) => Err(PyValueError::new_err(format!(
                    "{} is not a Model",
                    field_type
                ))),
            }
        }
        None => Ok(format!("{}", nested_model)),
    }
}

/// Gets the index key of the virtual table.
fn get_table_index(table: &str) -> String {
    format!("{}__index", table)
}

/// Gets the primary key for the given table-key combination
fn get_primary_key(table: &str, key: &str) -> String {
    format!("{}_%&_{}", table, key)
}
