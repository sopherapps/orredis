use std::collections::HashMap;
use std::time::Duration;

use mobc;
use pyo3::exceptions::{PyConnectionError, PyKeyError};
use pyo3::prelude::*;
use pyo3::types::PyType;
use redis::aio::Connection;

use crate::schema::Schema;
use crate::{async_utils, asyncio, mobc_redis, store, utils};

#[pyclass(subclass)]
pub(crate) struct AsyncStore {
    collections_meta: HashMap<String, store::CollectionMeta>,
    primary_key_field_map: HashMap<String, String>,
    model_type_map: HashMap<String, Py<PyType>>,
    pool: mobc::Pool<mobc_redis::RedisConnectionManager>,
    default_ttl: Option<u64>,
    is_in_use: bool,
}

#[pymethods]
impl AsyncStore {
    /// Initializes the Store
    #[args(
        url,
        pool_size = 5,
        default_ttl = "None",
        timeout = "None",
        max_lifetime = "None"
    )]
    #[new]
    pub fn new(
        url: String,
        pool_size: u64,
        default_ttl: Option<u64>,
        timeout: Option<u64>,
        max_lifetime: Option<u64>,
    ) -> PyResult<Self> {
        let client =
            redis::Client::open(url).map_err(|e| PyConnectionError::new_err(e.to_string()))?;
        let manager = mobc_redis::RedisConnectionManager::new(client);
        let mut pool = mobc::Pool::builder().max_open(pool_size);

        if let Some(timeout) = timeout {
            pool = pool.get_timeout(Some(Duration::from_millis(timeout)));
        }

        if let Some(max_lifetime) = max_lifetime {
            pool = pool.max_lifetime(Some(Duration::from_millis(max_lifetime)));
        }

        let pool = pool.build(manager);

        Ok(AsyncStore {
            collections_meta: Default::default(),
            pool,
            default_ttl,
            primary_key_field_map: Default::default(),
            model_type_map: Default::default(),
            is_in_use: false,
        })
    }

    /// Clears all keys on this redis instance
    #[args(asynchronous = "false")]
    #[pyo3(text_signature = "($self, asynchronous)")]
    pub fn clear<'a>(&mut self, py: Python<'a>, asynchronous: bool) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let pool = self.pool.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                let mut conn = pool
                    .get()
                    .await
                    .map_err(|e| PyConnectionError::new_err(e.to_string()))?;
                let arg = if asynchronous { "ASYNC" } else { "SYNC" };

                redis::cmd("FLUSHALL")
                    .arg(arg)
                    .query_async(&mut conn as &mut Connection)
                    .await
                    .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;
                Ok(Python::with_gil(|py| py.None()))
            }),
        )
    }

    /// Creates a new collection for the given model and adds it to the store instance
    pub(crate) fn create_collection(
        &mut self,
        model: Py<PyType>,
        primary_key_field: String,
    ) -> PyResult<()> {
        if self.is_in_use {
            return Err(PyConnectionError::new_err(
                "a call to 'create_collection()' cannot come after a call to 'get_collection()'.",
            ));
        }

        Python::with_gil(|py| {
            let schema = model.getattr(py, "schema")?.call0(py)?;
            let schema =
                Schema::from_py_schema(schema, &self.primary_key_field_map, &self.model_type_map)?;
            let nested_fields = schema.extract_nested_fields();
            let model_name: String = model.getattr(py, "__qualname__")?.extract(py)?;
            let meta = store::CollectionMeta::new(
                Box::new(schema),
                model.clone(),
                primary_key_field.clone(),
                nested_fields,
            );
            self.collections_meta.insert(model_name.clone(), meta);
            self.primary_key_field_map
                .insert(model_name.clone(), primary_key_field);
            self.model_type_map.insert(model_name, model);
            Ok(())
        })
    }

    /// Instantiates an independent collection from the store for the given model
    pub(crate) fn get_collection(&mut self, model: Py<PyType>) -> PyResult<AsyncCollection> {
        let model_name: String =
            Python::with_gil(|py| model.getattr(py, "__qualname__")?.extract(py))?;
        if let Some(meta) = self.collections_meta.get(&model_name) {
            self.is_in_use = true;
            let pool = self.pool.clone();
            Ok(AsyncCollection::new(
                model_name,
                pool,
                meta.clone(),
                self.default_ttl,
            ))
        } else {
            Err(PyKeyError::new_err(format!(
                "{} has not yet been created on the store",
                model_name
            )))
        }
    }
}

#[pyclass(subclass)]
pub(crate) struct AsyncCollection {
    pub(crate) name: String,
    pub(crate) meta: store::CollectionMeta,
    pub(crate) pool: mobc::Pool<mobc_redis::RedisConnectionManager>,
    pub(crate) default_ttl: Option<u64>,
}

#[pymethods]
impl AsyncCollection {
    /// inserts one model instance into the redis store for this collection
    pub(crate) fn add_one<'a>(
        &self,
        py: Python<'a>,
        item: Py<PyAny>,
        ttl: Option<u64>,
    ) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let name = self.name.clone();
        let schema = self.meta.schema.clone();
        let pk_field = self.meta.primary_key_field.clone();
        let default_ttl = self.default_ttl.clone();
        let pool = self.pool.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                let records =
                    utils::prepare_record_to_insert(&name, &schema, &item, &pk_field, None)?;
                let ttl = match ttl {
                    None => default_ttl,
                    Some(v) => Some(v),
                };
                async_utils::insert_records_async(&pool, &records, &ttl).await
            }),
        )
    }

    /// Inserts many model instances into the redis store for this collection all in a batch.
    /// This is more efficient than repeatedly calling add_one() because only one network request is made to redis
    pub(crate) fn add_many<'a>(
        &self,
        py: Python<'a>,
        items: Vec<Py<PyAny>>,
        ttl: Option<u64>,
    ) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let name = self.name.clone();
        let schema = self.meta.schema.clone();
        let pk_field = self.meta.primary_key_field.clone();
        let default_ttl = self.default_ttl.clone();
        let pool = self.pool.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                let mut records: Vec<(String, Vec<(String, String)>)> =
                    Vec::with_capacity(2 * items.len());
                for item in items {
                    let mut records_to_insert =
                        utils::prepare_record_to_insert(&name, &schema, &item, &pk_field, None)?;
                    records.append(&mut records_to_insert);
                }

                let ttl = match ttl {
                    None => default_ttl,
                    Some(v) => Some(v),
                };

                async_utils::insert_records_async(&pool, &records, &ttl).await
            }),
        )
    }

    /// Updates the record of the given id with the provided data
    pub(crate) fn update_one<'a>(
        &self,
        py: Python<'a>,
        id: &str,
        data: Py<PyAny>,
        ttl: Option<u64>,
    ) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let name = self.name.clone();
        let schema = self.meta.schema.clone();
        let pk_field = self.meta.primary_key_field.clone();
        let default_ttl = self.default_ttl.clone();
        let pool = self.pool.clone();
        let id = id.to_owned();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                let records =
                    utils::prepare_record_to_insert(&name, &schema, &data, &pk_field, Some(&id))?;

                let ttl = match ttl {
                    None => default_ttl,
                    Some(v) => Some(v),
                };

                async_utils::insert_records_async(&pool, &records, &ttl).await
            }),
        )
    }

    /// Deletes the records that correspond to the given ids for this collection
    pub(crate) fn delete_many<'a>(&self, py: Python<'a>, ids: Vec<String>) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let name = self.name.clone();
        let pool = self.pool.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                let primary_keys: Vec<String> = ids
                    .iter()
                    .map(|id| utils::generate_hash_key(&name, id))
                    .collect();
                async_utils::remove_records_async(&pool, &primary_keys).await
            }),
        )
    }

    /// Gets the record that corresponds to the given id
    pub(crate) fn get_one<'a>(&self, py: Python<'a>, id: &str) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let pool = self.pool.clone();
        let name = self.name.clone();
        let meta = self.meta.clone();
        let id = id.to_owned();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                let mut records: Vec<Py<PyAny>> =
                    async_utils::get_records_by_id_async(&pool, &name, &meta, &vec![id]).await?;
                match records.pop() {
                    None => Python::with_gil(|py| Ok(py.None())),
                    Some(record) => Ok(record),
                }
            }),
        )
    }

    /// Returns all the records found in this collection; returning them as models
    pub(crate) fn get_all<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let pool = self.pool.clone();
        let name = self.name.clone();
        let meta = self.meta.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                async_utils::get_all_records_in_collection_async(&pool, &name, &meta).await
            }),
        )
    }

    /// Returns the records whose ids are as given for this collection
    pub(crate) fn get_many<'a>(&self, py: Python<'a>, ids: Vec<String>) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let pool = self.pool.clone();
        let name = self.name.clone();
        let meta = self.meta.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                async_utils::get_records_by_id_async(&pool, &name, &meta, &ids).await
            }),
        )
    }

    /// Returns the record that corresponds to the given id in this collection
    /// returning it as a dictionary with only the fields specified
    pub(crate) fn get_one_partially<'a>(
        &self,
        py: Python<'a>,
        id: &str,
        fields: Vec<String>,
    ) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let pool = self.pool.clone();
        let name = self.name.clone();
        let meta = self.meta.clone();
        let id = id.to_owned();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                let mut records: Vec<Py<PyAny>> = async_utils::get_partial_records_by_id_async(
                    &pool,
                    &name,
                    &meta,
                    &vec![id],
                    &fields,
                )
                .await?;
                match records.pop() {
                    None => Python::with_gil(|py| Ok(py.None())),
                    Some(record) => Ok(record),
                }
            }),
        )
    }

    /// Retrieves the all records in this collection, only returning the specified fields
    /// for each given record
    pub(crate) fn get_all_partially<'a>(
        &self,
        py: Python<'a>,
        fields: Vec<String>,
    ) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let pool = self.pool.clone();
        let name = self.name.clone();
        let meta = self.meta.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                async_utils::get_all_partial_records_in_collection_async(
                    &pool, &name, &meta, &fields,
                )
                .await
            }),
        )
    }

    /// Retrieves the records with the given ids in this collection, only returning
    /// the specified fields for each record
    pub(crate) fn get_many_partially<'a>(
        &self,
        py: Python<'a>,
        ids: Vec<String>,
        fields: Vec<String>,
    ) -> PyResult<&'a PyAny> {
        let locals = asyncio::async_std::get_current_locals(py)?;
        let pool = self.pool.clone();
        let name = self.name.clone();
        let meta = self.meta.clone();

        asyncio::async_std::future_into_py_with_locals(
            py,
            locals.clone(),
            // Store the current locals in task-local data
            asyncio::async_std::scope(locals.clone(), async move {
                async_utils::get_partial_records_by_id_async(&pool, &name, &meta, &ids, &fields)
                    .await
            }),
        )
    }
}

impl AsyncCollection {
    /// Instantiates a new collection. This is not accessible to python and thus a collection
    /// cannot be directly instantiated in python
    pub(crate) fn new(
        name: String,
        pool: mobc::Pool<mobc_redis::RedisConnectionManager>,
        meta: store::CollectionMeta,
        default_ttl: Option<u64>,
    ) -> Self {
        Self {
            name,
            meta,
            pool,
            default_ttl,
        }
    }
}
