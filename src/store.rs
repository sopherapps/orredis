use std::collections::HashMap;

use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyType};
use pyo3::{Py, PyAny, PyResult, Python};
use redis::{Commands, ConnectionLike};

use crate::model::ModelMeta;
use crate::{redis_utils, Model};

pub enum Record {
    Full { data: Model },
    Partial { data: HashMap<String, Py<PyAny>> },
}

#[pyclass(subclass)]
pub struct Store {
    pub(crate) models: HashMap<String, ModelMeta>,
    pub(crate) conn: Option<redis::Connection>,
    url: String,
}

#[pymethods]
impl Store {
    /// Initializes the Store
    #[new]
    pub fn new(url: Py<PyAny>) -> PyResult<Self> {
        Python::with_gil(|py| -> PyResult<Store> {
            let url: &str = url.extract(py)?;
            let conn = redis_utils::connect_to_redis(url)
                .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;

            Ok(Store {
                models: Default::default(),
                conn: Some(conn),
                url: url.to_string(),
            })
        })
    }

    /// Clears all keys on this redis instance
    #[args(asynchronous = "false")]
    #[pyo3(text_signature = "($self, asynchronous)")]
    pub fn clear(&mut self, asynchronous: bool) -> PyResult<()> {
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
                let new_conn = redis_utils::connect_to_redis(&self.url)
                    .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;
                self.conn = Some(new_conn);
                Ok(())
            }
            Some(_) => Ok(()),
        }
    }

    /// Adds the given model to the hashmap of models,
    /// Also sets the class attribute _store of the model to itself
    /// to be retrieved later when running any query
    pub(crate) fn register_model(
        mut slf: PyRefMut<'_, Self>,
        model_type: Py<PyType>,
    ) -> PyResult<()> {
        Python::with_gil(|py| -> PyResult<()> {
            let model_type = model_type.as_ref(py);
            let model_name: String = model_type.call_method0("get_name")?.extract()?;
            match slf.models.get(&model_name) {
                None => {
                    let new_model_meta = ModelMeta::new(model_type)?;
                    slf.models.insert(model_name, new_model_meta);
                    model_type.setattr("_store", Py::clone_ref(&Py::from(slf), py))?;
                    Ok(())
                }
                Some(_) => Err(PyKeyError::new_err(format!(
                    "{} model has already been registered",
                    model_name
                ))),
            }
        })
    }

    pub fn insert_one(
        &mut self,
        model_name: &str,
        data: Py<Model>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        redis_utils::run_in_transaction(self, |store, pipe| {
            let model_meta = store.models.get(model_name);
            match model_meta {
                None => Err(PyValueError::new_err(format!(
                    "{} is not a model in this store",
                    model_name
                ))),
                Some(model_meta) => Python::with_gil(|py| {
                    let data: Model = data.extract(py)?;
                    let key = data.get(&model_meta.primary_key_field)?;
                    let key = format!("{}", key);
                    let record = Record::Full { data };
                    redis_utils::insert_on_pipeline(
                        store, pipe, model_name, life_span, &key, &record,
                    )?;
                    Ok(())
                }),
            }
        })
    }

    pub fn insert_many(
        &mut self,
        model_name: &str,
        data: Vec<Py<Model>>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        redis_utils::run_in_transaction(self, |store, pipe| {
            let model_meta = store.models.get(model_name);
            match model_meta {
                None => Err(PyValueError::new_err(format!(
                    "{} is not a model in this store",
                    model_name
                ))),
                Some(model_meta) => Python::with_gil(|py| {
                    for item in data {
                        let item: Model = item.extract(py)?;
                        let key = item.get(&model_meta.primary_key_field)?;
                        let key = format!("{}", key);
                        let record = Record::Full { data: item };
                        redis_utils::insert_on_pipeline(
                            store, pipe, model_name, life_span, &key, &record,
                        )?;
                    }

                    Ok(())
                }),
            }
        })
    }

    pub fn update_one(
        &mut self,
        model_name: &str,
        id: Py<PyAny>,
        data: HashMap<String, Py<PyAny>>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        redis_utils::run_in_transaction(self, |store, pipe| {
            let model_meta = store.models.get(model_name);
            match model_meta {
                None => Err(PyValueError::new_err(format!(
                    "{} is not a model in this store",
                    model_name
                ))),
                Some(_) => {
                    let key = format!("{}", id);
                    let record = Record::Partial { data };
                    redis_utils::insert_on_pipeline(
                        store, pipe, model_name, life_span, &key, &record,
                    )?;
                    Ok(())
                }
            }
        })
    }

    pub fn delete_one(&mut self, model_name: &str, id: Py<PyAny>) -> PyResult<()> {
        redis_utils::run_in_transaction(self, |_store, pipe| {
            let model_index = redis_utils::get_model_index(model_name);
            let key = format!("{}", id);
            let primary_key = redis_utils::get_primary_key(model_name, &key);

            pipe.del(&primary_key);
            pipe.srem(model_index, &primary_key);

            Ok(())
        })
    }

    pub fn delete_many(&mut self, model_name: &str, ids: Vec<Py<PyAny>>) -> PyResult<()> {
        redis_utils::run_in_transaction(self, |_store, pipe| {
            let model_index = redis_utils::get_model_index(model_name);
            let keys: Vec<String> = ids
                .into_iter()
                .map(|k| format!("{}", k))
                .map(|k| redis_utils::get_primary_key(model_name, &k))
                .collect();

            pipe.del(&keys);
            pipe.srem(model_index, &keys);

            Ok(())
        })
    }

    pub fn find_one(&mut self, model_name: &str, id: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let model_meta = self.models.get(model_name);
        match model_meta {
            None => Err(PyValueError::new_err(format!(
                "{} is not a model in this store",
                model_name
            ))),
            Some(model_meta) => {
                let fields = model_meta.fields.clone();
                let model_type = &model_meta.model_type.clone();
                let data = redis_utils::run_without_transaction(
                    self,
                    |_store, pipe| -> PyResult<HashMap<String, String>> {
                        let key = format!("{}", id);
                        let primary_key = redis_utils::get_primary_key(model_name, &key);

                        pipe.hgetall(primary_key);

                        Ok(HashMap::new())
                    },
                )?;

                let model = redis_utils::parse_model(&fields, self, data)?;
                model.to_subclass_instance(model_type)
            }
        }
    }

    pub fn find_many(&mut self, model_name: &str, ids: Vec<Py<PyAny>>) -> PyResult<Vec<Py<PyAny>>> {
        let model_meta = self.models.get(model_name);
        match model_meta {
            None => Err(PyValueError::new_err(format!(
                "{} is not a model in this store",
                model_name
            ))),
            Some(model_meta) => {
                let fields = model_meta.fields.clone();
                let model_type = &model_meta.model_type.clone();

                let data = redis_utils::run_without_transaction(
                    self,
                    |_store, pipe| -> PyResult<Vec<HashMap<String, String>>> {
                        for id in ids {
                            let key = format!("{}", id);
                            let primary_key = redis_utils::get_primary_key(model_name, &key);

                            pipe.hgetall(primary_key);
                        }

                        Ok(vec![])
                    },
                )?;

                let mut records: Vec<Py<PyAny>> = Vec::with_capacity(data.len());
                for item in data {
                    let model = redis_utils::parse_model(&fields, self, item)?;
                    let item = model.to_subclass_instance(model_type)?;
                    records.push(item);
                }

                Ok(records)
            }
        }
    }

    pub fn find_all(&mut self, model_name: &str) -> PyResult<Vec<Py<PyAny>>> {
        let model_meta = self.models.get(model_name);
        match model_meta {
            None => Err(PyValueError::new_err(format!(
                "{} is not a model in this store",
                model_name
            ))),
            Some(model_meta) => {
                let fields = model_meta.fields.clone();
                let model_type = &model_meta.model_type.clone();

                let conn = self.conn.as_mut();
                let ids: Vec<String> = match conn {
                    None => Err(PyConnectionError::new_err("redis server disconnected")),
                    Some(conn) => {
                        let model_index = redis_utils::get_model_index(model_name);
                        let keys = conn
                            .sscan(&model_index)
                            .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?
                            .collect();
                        Ok(keys)
                    }
                }?;

                let data = redis_utils::run_without_transaction(
                    self,
                    |_store, pipe| -> PyResult<Vec<HashMap<String, String>>> {
                        for id in ids {
                            let key = format!("{}", id);
                            let primary_key = redis_utils::get_primary_key(model_name, &key);

                            pipe.hgetall(primary_key);
                        }

                        Ok(vec![])
                    },
                )?;

                let mut records: Vec<Py<PyAny>> = Vec::with_capacity(data.len());
                for item in data {
                    let model = redis_utils::parse_model(&fields, self, item)?;
                    let item = model.to_subclass_instance(model_type)?;
                    records.push(item);
                }

                Ok(records)
            }
        }
    }

    pub fn find_partial_one(
        &mut self,
        model_name: &str,
        id: Py<PyAny>,
        columns: Vec<&str>,
    ) -> PyResult<HashMap<String, Py<PyAny>>> {
        let model_meta = self.models.get(model_name);
        match model_meta {
            None => Err(PyValueError::new_err(format!(
                "{} is not a model in this store",
                model_name
            ))),
            Some(model_meta) => {
                let fields = model_meta.fields.clone();
                let raw = redis_utils::run_without_transaction(
                    self,
                    |_store, pipe| -> PyResult<Vec<String>> {
                        let key = format!("{}", id);
                        let primary_key = redis_utils::get_primary_key(model_name, &key);
                        pipe.cmd("HMGET").arg(primary_key).arg(&columns);
                        Ok(vec![])
                    },
                )?;

                let data = raw
                    .into_iter()
                    .zip(&columns)
                    .map(|(v, k)| (k.to_string(), v))
                    .collect::<HashMap<String, String>>();

                let model = redis_utils::parse_model(&fields, self, data)?;
                model.dict()
            }
        }
    }

    pub fn find_partial_many(
        &mut self,
        model_name: &str,
        ids: Vec<Py<PyAny>>,
        columns: Vec<&str>,
    ) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
        let model_meta = self.models.get(model_name);
        match model_meta {
            None => Err(PyValueError::new_err(format!(
                "{} is not a model in this store",
                model_name
            ))),
            Some(model_meta) => {
                let fields = model_meta.fields.clone();
                let raw = redis_utils::run_without_transaction(
                    self,
                    |_store, pipe| -> PyResult<Vec<Vec<String>>> {
                        for id in ids {
                            let key = format!("{}", id);
                            let primary_key = redis_utils::get_primary_key(model_name, &key);
                            pipe.cmd("HMGET").arg(primary_key).arg(&columns);
                        }

                        Ok(vec![])
                    },
                )?;

                let data: Vec<HashMap<String, String>> = raw
                    .into_iter()
                    .map(|item| {
                        item.into_iter()
                            .zip(&columns)
                            .map(|(v, k)| (k.to_string(), v))
                            .collect::<HashMap<String, String>>()
                    })
                    .collect();

                let mut parsed_data: Vec<HashMap<String, Py<PyAny>>> =
                    Vec::with_capacity(data.len());
                for item in data {
                    let model = redis_utils::parse_model(&fields, self, item)?;
                    let dict = model.dict()?;
                    parsed_data.push(dict);
                }

                Ok(parsed_data)
            }
        }
    }

    pub fn find_partial_all(
        &mut self,
        model_name: &str,
        columns: Vec<&str>,
    ) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
        let model_meta = self.models.get(model_name);
        match model_meta {
            None => Err(PyValueError::new_err(format!(
                "{} is not a model in this store",
                model_name
            ))),
            Some(model_meta) => {
                let fields = model_meta.fields.clone();
                let conn = self.conn.as_mut();
                let ids: Vec<String> = match conn {
                    None => Err(PyConnectionError::new_err("redis server disconnected")),
                    Some(conn) => {
                        let model_index = redis_utils::get_model_index(model_name);
                        let keys = conn
                            .sscan(&model_index)
                            .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?
                            .collect();
                        Ok(keys)
                    }
                }?;

                let raw = redis_utils::run_without_transaction(
                    self,
                    |_store, pipe| -> PyResult<Vec<Vec<String>>> {
                        for id in ids {
                            let key = format!("{}", id);
                            let primary_key = redis_utils::get_primary_key(model_name, &key);
                            pipe.cmd("HMGET").arg(primary_key).arg(&columns);
                        }

                        Ok(vec![])
                    },
                )?;

                let data: Vec<HashMap<String, String>> = raw
                    .into_iter()
                    .map(|item| {
                        item.into_iter()
                            .zip(&columns)
                            .map(|(v, k)| (k.to_string(), v))
                            .collect::<HashMap<String, String>>()
                    })
                    .collect();

                let mut parsed_data: Vec<HashMap<String, Py<PyAny>>> =
                    Vec::with_capacity(data.len());
                for item in data {
                    let model = redis_utils::parse_model(&fields, self, item)?;
                    let dict = model.dict()?;
                    parsed_data.push(dict);
                }

                Ok(parsed_data)
            }
        }
    }
}
