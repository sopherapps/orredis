use std::collections::HashMap;

use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyType};
use pyo3::{Py, PyAny, PyResult, Python};
use redis::ConnectionLike;

use crate::model::ModelMeta;
use crate::redis_utils::get_primary_key;
use crate::{redis_utils, Model};

const SELECT_SOME_FIELDS_FOR_ALL_IDS_SCRIPT: &str = r"local filtered = {} local cursor = '0' local table_unpack = table.unpack or unpack local columns = {} local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do if i > 1 then if args_tracker[k] then nested_columns[k] = true else  table.insert(columns, k) args_tracker[k] = true end end end repeat local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1]) for _, key in ipairs(result[2]) do if redis.call('TYPE', key).ok == 'hash' then  local data = redis.call('HMGET', key, table_unpack(columns)) local parsed_data = {} for i, v in ipairs(data) do table.insert(parsed_data, columns[i]) if nested_columns[columns[i]] then v = redis.call('HGETALL', v) end table.insert(parsed_data, v) end table.insert(filtered, parsed_data) end end cursor = result[1] until (cursor == '0') return filtered";
const SELECT_ALL_FIELDS_FOR_ALL_IDS_SCRIPT: &str = r"local filtered = {} local cursor = '0' local nested_fields = {} for i, key in ipairs(ARGV) do if i > 1 then nested_fields[key] = true end end repeat local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1]) for _, key in ipairs(result[2]) do if redis.call('TYPE', key).ok == 'hash' then local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(filtered, parent) end end cursor = result[1] until (cursor == '0') return filtered";
const SELECT_ALL_FIELDS_FOR_SOME_IDS_SCRIPT: &str = r"local result = {} local nested_fields = {} for _, key in ipairs(ARGV) do nested_fields[key] = true end for _, key in ipairs(KEYS) do local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(result, parent) end return result";
const SELECT_SOME_FIELDS_FOR_SOME_IDS_SCRIPT: &str = r"local result = {} local table_unpack = table.unpack or unpack local columns = { } local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do if args_tracker[k] then nested_columns[k] = true else table.insert(columns, k) args_tracker[k] = true end end for _, key in ipairs(KEYS) do local data = redis.call('HMGET', key, table_unpack(columns)) local parsed_data = {} for i, v in ipairs(data) do table.insert(parsed_data, columns[i]) if nested_columns[columns[i]] then v = redis.call('HGETALL', v) end table.insert(parsed_data, v) end table.insert(result, parsed_data) end return result";

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
                let data = Python::with_gil(|py| -> PyResult<Model> { data.extract::<Model>(py) })?;
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
            let data = redis_utils::run_script(
                store,
                &model_meta.fields,
                |_store, pipe: &mut redis::Pipeline| -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
                    let ids: Vec<String> = ids
                        .into_iter()
                        .map(|k| get_primary_key(model_name, &k.to_string()))
                        .collect();

                    pipe.cmd("EVAL")
                        .arg(SELECT_ALL_FIELDS_FOR_SOME_IDS_SCRIPT)
                        .arg(ids.len())
                        .arg(ids)
                        .arg(&model_meta.nested_fields);

                    Ok(vec![])
                },
            )?;

            convert_to_py_model_instances(model_meta, data)
        })
    }

    pub fn find_all(&mut self, model_name: &str) -> PyResult<Vec<Py<PyAny>>> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            let data = redis_utils::run_script(
                store,
                &model_meta.fields,
                |_store, pipe: &mut redis::Pipeline| -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
                    pipe.cmd("EVAL")
                        .arg(SELECT_ALL_FIELDS_FOR_ALL_IDS_SCRIPT)
                        .arg(0)
                        .arg(get_table_key_pattern(model_name))
                        .arg(&model_meta.nested_fields);

                    Ok(vec![])
                },
            )?;
            convert_to_py_model_instances(model_meta, data)
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
            let keys: Vec<String> = ids
                .into_iter()
                .map(|id| {
                    let key = format!("{}", id);
                    redis_utils::get_primary_key(model_name, &key)
                })
                .collect();

            redis_utils::run_script(
                store,
                &model_meta.fields,
                |_store, pipe: &mut redis::Pipeline| -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
                    pipe.cmd("EVAL")
                        .arg(SELECT_SOME_FIELDS_FOR_SOME_IDS_SCRIPT)
                        .arg(keys.len())
                        .arg(keys)
                        .arg(columns)
                        .arg(&model_meta.nested_fields);

                    Ok(vec![])
                },
            )
        })
    }

    pub fn find_partial_all(
        &mut self,
        model_name: &str,
        columns: Vec<&str>,
    ) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
        execute_if_model_exists(self, model_name, |store, model_meta| {
            redis_utils::run_script(
                store,
                &model_meta.fields,
                |_store, pipe: &mut redis::Pipeline| -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
                    pipe.cmd("EVAL")
                        .arg(SELECT_SOME_FIELDS_FOR_ALL_IDS_SCRIPT)
                        .arg(0)
                        .arg(get_table_key_pattern(model_name))
                        .arg(columns)
                        .arg(&model_meta.nested_fields);

                    Ok(vec![])
                },
            )
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
            pipe.hgetall(id.to_string());
            Ok(vec![])
        },
    )?;

    match data.get(0) {
        None => Ok(None),
        Some(item) => {
            if item.len() == 0 && fields.len() > 0 {
                Ok(None)
            } else {
                let model = redis_utils::parse_model(&fields, store, item)?;
                Ok(Some(model))
            }
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
            if item.len() == 0 && columns.len() > 0 {
                Ok(None)
            } else {
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
}

/// Generates the key pattern to use when selecting keys in redis of a given model
#[inline]
fn get_table_key_pattern(model_name: &str) -> String {
    format!("{}_%&_*", model_name)
}

/// Converts a list of parsed hashmaps to python objects of the given model_type in the model_meta
fn convert_to_py_model_instances(
    model_meta: &ModelMeta,
    data: Vec<HashMap<String, Py<PyAny>>>,
) -> Result<Vec<Py<PyAny>>, PyErr> {
    let number_of_fields = model_meta.fields.len();
    let model_type = model_meta.model_type.clone();
    let mut records: Vec<Py<PyAny>> = Vec::with_capacity(data.len());
    for dict in data {
        if number_of_fields > 0 && dict.len() == 0 {
            // skip empty items
            continue;
        }

        let instance = Python::with_gil(|py| {
            let dict = dict.into_py_dict(py);
            model_type.call(py, (), Some(dict))
        })?;

        records.push(instance);
    }

    Ok(records)
}
