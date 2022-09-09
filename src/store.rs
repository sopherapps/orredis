extern crate redis;

use pyo3::exceptions::{PyConnectionError, PyKeyError};
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
    // __init__
    // [x] reopen
    // [x] close
    // [x] insert_dict
    // [x] insert_dict_list
    // [x] select
    // [x] update
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
        ids: Option<Vec<String>>,
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
                        .map(|k| get_primary_key(table, &k))
                        .collect(),
                };

                let mut response = match columns {
                    None => {
                        let raw_data = run_in_transaction(
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
                        let raw = run_in_transaction(conn, |pipe| -> PyResult<Vec<Vec<String>>> {
                            for k in ids {
                                pipe.cmd("HMGET").arg(k).arg(&cols);
                            }
                            Ok(vec![])
                        })?;

                        raw.into_iter()
                            .map(|item| {
                                item.into_iter()
                                    .zip(&cols)
                                    .map(|(k, v)| (k, v.to_string()))
                                    .collect::<HashMap<String, String>>()
                            })
                            .collect()
                    }
                };

                // do some eager loading
                match nested_columns {
                    None => {}
                    Some(nested_col_map) => {
                        let response_length = response.len();

                        for (field, table) in nested_col_map {
                            let table = table.to_lowercase();
                            let mut foreign_keys: Vec<String> = Vec::with_capacity(response_length);

                            for i in 0..response.len() {
                                let item = &response[i];
                                let f_key = item.get(field).unwrap_or(&"".to_string()).to_owned();
                                foreign_keys.push(f_key);
                            }

                            let eager_response =
                                self.select(&table, Some(foreign_keys), None, None)?;

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

    #[args(table, key, data, life_span = "None")]
    #[pyo3(text_signature = "($self, table, key, data, lifespan)")]
    pub(crate) fn update(
        &mut self,
        table: &str,
        key: &str,
        data: Py<PyDict>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        self.insert_dict(table, key, data, life_span)
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

pub(crate) fn serialize_to_key_value_pairs<'a>(
    pipe: &mut redis::Pipeline,
    raw_data: &'a HashMap<String, Py<PyAny>>,
) -> PyResult<Vec<(&'a str, String)>> {
    Python::with_gil(|py| -> PyResult<Vec<(&'a str, String)>> {
        let mut data: Vec<(&'a str, String)> = Vec::with_capacity(raw_data.len());

        for (field, value) in raw_data {
            let value = value.as_ref(py);
            let field_type = value.get_type().to_string();
            if field_type.contains(".") {
                let foreign_key = serialize_nested_model(pipe, field, value, &field_type)
                    .unwrap_or(value.to_string());
                data.push((field, foreign_key));
            } else {
                data.push((field, value.to_string()));
            }
        }

        Ok(data)
    })
}

/// serialize_nested_model will return the foreign key
/// if the nested data is actually a nested model that has a dict() method
fn serialize_nested_model(
    pipe: &mut redis::Pipeline,
    key: &str,
    nested_model: &PyAny,
    field_type: &str,
) -> Option<String> {
    let name_portions: Vec<&str> = field_type.rsplit(".").collect();

    match name_portions.last() {
        Some(table) => {
            let table = table.to_lowercase();
            let result = nested_model.call_method("dict", (), None);
            match result {
                Ok(dict) => dict
                    .extract::<HashMap<String, Py<PyAny>>>()
                    .and_then(|data| insert_on_pipeline(pipe, &table, None, key, &data))
                    .ok(),
                Err(_) => None,
            }
        }
        None => None,
    }
}

fn get_table_index(table: &str) -> String {
    format!("{}__index", table)
}

fn get_primary_key(table: &str, key: &str) -> String {
    format!("{}_%&_{}", table, key)
}
