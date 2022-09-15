use std::collections::HashMap;

use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyType;
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
        execute_if_model_exists(self, model_name, |store, model_meta| {
            redis_utils::run_in_transaction(store, |store_in_tx, pipe| {
                Python::with_gil(|py| {
                    let data: Model = data.extract(py)?;
                    let key = data.get(&model_meta.primary_key_field)?;
                    let key = format!("{}", key);
                    let record = Record::Full { data };
                    redis_utils::insert_on_pipeline(
                        store_in_tx,
                        pipe,
                        model_name,
                        life_span,
                        &key,
                        &record,
                    )?;
                    Ok(())
                })
            })
        })
    }

    pub fn insert_many(
        &mut self,
        model_name: &str,
        data: Vec<Py<Model>>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            redis_utils::run_in_transaction(store, |store_in_tx, pipe| {
                Python::with_gil(|py| {
                    for item in data {
                        let item: Model = item.extract(py)?;
                        let key = item.get(&model_meta.primary_key_field)?;
                        let key = format!("{}", key);
                        let record = Record::Full { data: item };
                        redis_utils::insert_on_pipeline(
                            store_in_tx,
                            pipe,
                            model_name,
                            life_span,
                            &key,
                            &record,
                        )?;
                    }

                    Ok(())
                })
            })
        })
    }

    pub fn update_one(
        &mut self,
        model_name: &str,
        id: Py<PyAny>,
        data: HashMap<String, Py<PyAny>>,
        life_span: Option<usize>,
    ) -> PyResult<()> {
        execute_if_model_exists(self, model_name, |store, _model_meta| {
            let key = format!("{}", id);
            let record = Record::Partial { data };
            redis_utils::run_in_transaction(store, |store_in_tx, pipe| {
                redis_utils::insert_on_pipeline(
                    store_in_tx,
                    pipe,
                    model_name,
                    life_span,
                    &key,
                    &record,
                )?;

                Ok(())
            })
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
        execute_if_model_exists(self, model_name, |store, model_meta| {
            let fields = model_meta.fields.clone();
            let model_type = model_meta.model_type.clone();
            let key = format!("{}", id);
            let primary_key = redis_utils::get_primary_key(model_name, &key);
            let model = find_one_by_raw_id(store, fields, &primary_key)?;
            match model {
                None => Python::with_gil(|py| Ok(py.None())),
                Some(model) => model.to_subclass_instance(&model_type),
            }
        })
    }

    pub fn find_many(&mut self, model_name: &str, ids: Vec<Py<PyAny>>) -> PyResult<Vec<Py<PyAny>>> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            let fields = model_meta.fields.clone();
            let model_type = &model_meta.model_type.clone();
            let keys = ids
                .into_iter()
                .map(|id| {
                    let key = format!("{}", id);
                    redis_utils::get_primary_key(model_name, &key)
                })
                .collect();
            find_many_by_raw_ids(store, fields, &keys, model_type)
        })
    }

    pub fn find_all(&mut self, model_name: &str) -> PyResult<Vec<Py<PyAny>>> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            let fields = model_meta.fields.clone();
            let model_type = &model_meta.model_type.clone();
            let keys = get_all_ids_for_model(store, model_name)?;
            find_many_by_raw_ids(store, fields, &keys, model_type)
        })
    }

    pub fn find_partial_one(
        &mut self,
        model_name: &str,
        id: Py<PyAny>,
        columns: Vec<&str>,
    ) -> PyResult<Py<PyAny>> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            let fields = model_meta.fields.clone();
            let key = format!("{}", id);
            let primary_key = redis_utils::get_primary_key(model_name, &key);
            let dict = find_one_partial_by_raw_id(store, fields, &primary_key, &columns)?;
            match dict {
                None => Python::with_gil(|py| Ok(py.None())),
                Some(dict) => {
                    Python::with_gil(|py| -> PyResult<Py<PyAny>> { Ok(dict.into_py(py)) })
                }
            }
        })
    }

    pub fn find_partial_many(
        &mut self,
        model_name: &str,
        ids: Vec<Py<PyAny>>,
        columns: Vec<&str>,
    ) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            let fields = model_meta.fields.clone();
            let keys = ids
                .into_iter()
                .map(|id| {
                    let key = format!("{}", id);
                    redis_utils::get_primary_key(model_name, &key)
                })
                .collect();
            find_partial_many_by_raw_ids(store, fields, &keys, &columns)
        })
    }

    pub fn find_partial_all(
        &mut self,
        model_name: &str,
        columns: Vec<&str>,
    ) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            let fields = model_meta.fields.clone();
            let keys = get_all_ids_for_model(store, model_name)?;
            find_partial_many_by_raw_ids(store, fields, &keys, &columns)
        })
    }
}

pub fn execute_if_model_exists<T, F>(store: &mut Store, model_name: &str, closure: F) -> PyResult<T>
where
    F: FnOnce(&mut Store, &ModelMeta) -> PyResult<T>,
{
    let models = store.models.clone();
    let model_meta = models.get(model_name).clone();
    match model_meta {
        None => Err(PyValueError::new_err(format!(
            "{} is not a model in this store",
            model_name
        ))),
        Some(model_meta) => closure(store, model_meta),
    }
}

pub(crate) fn find_one_by_raw_id(
    store: &mut Store,
    fields: HashMap<String, Py<PyAny>>,
    id: &str,
) -> PyResult<Option<Model>> {
    let data = redis_utils::run_without_transaction(
        store,
        |_store, pipe| -> PyResult<Vec<HashMap<String, String>>> {
            let key = format!("{}", id);
            pipe.hgetall(key);
            Ok(vec![])
        },
    )?;

    match data.get(0) {
        None => Ok(None),
        Some(item) => {
            let model = redis_utils::parse_model(&fields, store, item)?;
            Ok(Some(model))
        }
    }
}

pub(crate) fn find_one_partial_by_raw_id(
    store: &mut Store,
    fields: HashMap<String, Py<PyAny>>,
    id: &str,
    columns: &Vec<&str>,
) -> PyResult<Option<HashMap<String, Py<PyAny>>>> {
    let data = redis_utils::run_without_transaction(
        store,
        |_store, pipe| -> PyResult<Vec<Vec<String>>> {
            let key = format!("{}", id);
            pipe.cmd("HMGET").arg(key).arg(&columns);
            Ok(vec![])
        },
    )?;

    match data.get(0) {
        None => Ok(None),
        Some(item) => {
            let record = item
                .into_iter()
                .zip(columns)
                .map(|(v, k)| (k.to_string(), v.to_string()))
                .collect::<HashMap<String, String>>();
            let model = redis_utils::parse_model(&fields, store, &record)?;
            let dict = model.dict()?;
            Ok(Some(dict))
        }
    }
}

fn find_many_by_raw_ids(
    store: &mut Store,
    fields: HashMap<String, Py<PyAny>>,
    ids: &Vec<String>,
    model_type: &Py<PyType>,
) -> PyResult<Vec<Py<PyAny>>> {
    let data = redis_utils::run_without_transaction(
        store,
        |_store, pipe| -> PyResult<Vec<HashMap<String, String>>> {
            for id in ids {
                pipe.hgetall(id);
            }

            Ok(vec![])
        },
    )?;

    let mut records: Vec<Py<PyAny>> = Vec::with_capacity(data.len());
    let number_of_fields = fields.len();
    for item in data {
        if number_of_fields > 0 && item.len() == 0 {
            // skip empty items
            continue;
        }

        let model = redis_utils::parse_model(&fields, store, &item)?;
        let item = model.to_subclass_instance(model_type)?;
        records.push(item);
    }

    Ok(records)
}

pub fn find_partial_many_by_raw_ids(
    store: &mut Store,
    fields: HashMap<String, Py<PyAny>>,
    ids: &Vec<String>,
    columns: &Vec<&str>,
) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
    let raw = redis_utils::run_without_transaction(
        store,
        |_store, pipe| -> PyResult<Vec<Vec<String>>> {
            for id in ids {
                pipe.cmd("HMGET").arg(id).arg(columns);
            }

            Ok(vec![])
        },
    )?;

    let data: Vec<HashMap<String, String>> = raw
        .into_iter()
        .map(|item| {
            item.into_iter()
                .zip(columns)
                .map(|(v, k)| (k.to_string(), v))
                .collect::<HashMap<String, String>>()
        })
        .collect();

    let mut parsed_data: Vec<HashMap<String, Py<PyAny>>> = Vec::with_capacity(data.len());
    let number_of_fields = fields.len();
    for item in data {
        if number_of_fields > 0 && item.len() == 0 {
            // skip empty items
            continue;
        }

        let model = redis_utils::parse_model(&fields, store, &item)?;
        let dict = model.dict()?;
        parsed_data.push(dict);
    }

    Ok(parsed_data)
}

fn get_all_ids_for_model(store: &mut Store, model_name: &str) -> PyResult<Vec<String>> {
    let conn = store.conn.as_mut();

    match conn {
        None => Err(PyConnectionError::new_err("redis server disconnected")),
        Some(conn) => {
            let model_index = redis_utils::get_model_index(model_name);
            let keys = conn
                .sscan(&model_index)
                .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?
                .collect();
            Ok(keys)
        }
    }
}
