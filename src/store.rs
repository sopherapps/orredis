extern crate pyo3;
extern crate r2d2;
extern crate redis;

use std::collections::HashMap;
use std::ops::DerefMut;
use std::time::Duration;

use pyo3::exceptions::{PyConnectionError, PyKeyError};
use pyo3::prelude::*;
use pyo3::types::PyType;

use crate::schema::Schema;
use crate::utils;

#[pyclass(subclass)]
pub(crate) struct Store {
    collections_meta: HashMap<String, CollectionMeta>,
    primary_key_field_map: HashMap<String, String>,
    model_type_map: HashMap<String, Py<PyType>>,
    pool: r2d2::Pool<redis::Client>,
    default_ttl: Option<u64>,
    is_in_use: bool,
}

#[derive(Clone)]
#[pyclass(subclass)]
pub(crate) struct CollectionMeta {
    pub(crate) schema: Box<Schema>,
    pub(crate) model_type: Py<PyType>,
    pub(crate) primary_key_field: String,
    pub(crate) nested_fields: Vec<String>,
}

#[pymethods]
impl Store {
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
        pool_size: u32,
        default_ttl: Option<u64>,
        timeout: Option<u64>,
        max_lifetime: Option<u64>,
    ) -> PyResult<Self> {
        let client =
            redis::Client::open(url).map_err(|e| PyConnectionError::new_err(e.to_string()))?;
        let mut pool = r2d2::Pool::builder().max_size(pool_size);

        if let Some(timeout) = timeout {
            pool = pool.connection_timeout(Duration::from_millis(timeout));
        }

        if let Some(max_lifetime) = max_lifetime {
            pool = pool.max_lifetime(Some(Duration::from_millis(max_lifetime)));
        }

        let pool = pool
            .build(client)
            .map_err(|e| PyConnectionError::new_err(e.to_string()))?;

        Ok(Store {
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
    pub fn clear(&mut self, asynchronous: bool) -> PyResult<()> {
        let mut conn = self
            .pool
            .get()
            .map_err(|e| PyConnectionError::new_err(e.to_string()))?;
        let arg = if asynchronous { "ASYNC" } else { "SYNC" };

        redis::cmd("FLUSHALL")
            .arg(arg)
            .query(conn.deref_mut())
            .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))
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
            let meta = CollectionMeta::new(
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
    pub(crate) fn get_collection(&mut self, model: Py<PyType>) -> PyResult<Collection> {
        let model_name: String =
            Python::with_gil(|py| model.getattr(py, "__qualname__")?.extract(py))?;
        if let Some(meta) = self.collections_meta.get(&model_name) {
            self.is_in_use = true;
            let pool = self.pool.clone();
            Ok(Collection::new(
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

impl CollectionMeta {
    /// Instantiates a new collection meta
    pub(crate) fn new(
        schema: Box<Schema>,
        model_type: Py<PyType>,
        primary_key_field: String,
        nested_fields: Vec<String>,
    ) -> Self {
        CollectionMeta {
            schema,
            model_type,
            primary_key_field,
            nested_fields,
        }
    }
}

#[pyclass(subclass)]
pub(crate) struct Collection {
    pub(crate) name: String,
    pub(crate) meta: CollectionMeta,
    pub(crate) pool: r2d2::Pool<redis::Client>,
    pub(crate) default_ttl: Option<u64>,
}

#[pymethods]
impl Collection {
    /// inserts one model instance into the redis store for this collection
    pub(crate) fn add_one(&self, item: Py<PyAny>, ttl: Option<u64>) -> PyResult<()> {
        let records = utils::prepare_record_to_insert(
            &self.name,
            &self.meta.schema,
            &item,
            &self.meta.primary_key_field,
            None,
        )?;
        let ttl = match ttl {
            None => self.default_ttl,
            Some(v) => Some(v),
        };
        utils::insert_records(&self.pool, &records, &ttl)
    }

    /// Inserts many model instances into the redis store for this collection all in a batch.
    /// This is more efficient than repeatedly calling add_one() because only one network request is made to redis
    pub(crate) fn add_many(&self, items: Vec<Py<PyAny>>, ttl: Option<u64>) -> PyResult<()> {
        let mut records: Vec<(String, Vec<(String, String)>)> = Vec::with_capacity(2 * items.len());
        for item in items {
            let mut records_to_insert = utils::prepare_record_to_insert(
                &self.name,
                &self.meta.schema,
                &item,
                &self.meta.primary_key_field,
                None,
            )?;
            records.append(&mut records_to_insert);
        }

        let ttl = match ttl {
            None => self.default_ttl,
            Some(v) => Some(v),
        };

        utils::insert_records(&self.pool, &records, &ttl)
    }

    /// Updates the record of the given id with the provided data
    pub(crate) fn update_one(&self, id: &str, data: Py<PyAny>, ttl: Option<u64>) -> PyResult<()> {
        let records = utils::prepare_record_to_insert(
            &self.name,
            &self.meta.schema,
            &data,
            &self.meta.primary_key_field,
            Some(id),
        )?;

        let ttl = match ttl {
            None => self.default_ttl,
            Some(v) => Some(v),
        };

        utils::insert_records(&self.pool, &records, &ttl)
    }

    /// Deletes the records that correspond to the given ids for this collection
    pub(crate) fn delete_many(&self, ids: Vec<String>) -> PyResult<()> {
        let primary_keys: Vec<String> = ids
            .iter()
            .map(|id| utils::generate_hash_key(&self.name, id))
            .collect();
        utils::remove_records(&self.pool, &primary_keys)
    }

    /// Gets the record that corresponds to the given id
    pub(crate) fn get_one(&self, id: &str) -> PyResult<Py<PyAny>> {
        let mut records: Vec<Py<PyAny>> =
            utils::get_records_by_id(&self.pool, &self.name, &self.meta, &vec![id.to_string()])?;
        match records.pop() {
            None => Python::with_gil(|py| Ok(py.None())),
            Some(record) => Ok(record),
        }
    }

    /// Returns all the records found in this collection; returning them as models
    pub(crate) fn get_all(&self) -> PyResult<Vec<Py<PyAny>>> {
        utils::get_all_records_in_collection(&self.pool, &self.name, &self.meta)
    }

    /// Returns the records whose ids are as given for this collection
    pub(crate) fn get_many(&self, ids: Vec<String>) -> PyResult<Vec<Py<PyAny>>> {
        utils::get_records_by_id(&self.pool, &self.name, &self.meta, &ids)
    }

    /// Returns the record that corresponds to the given id in this collection
    /// returning it as a dictionary with only the fields specified
    pub(crate) fn get_one_partially(&self, id: &str, fields: Vec<String>) -> PyResult<Py<PyAny>> {
        let mut records: Vec<Py<PyAny>> = utils::get_partial_records_by_id(
            &self.pool,
            &self.name,
            &self.meta,
            &vec![id.to_string()],
            &fields,
        )?;
        match records.pop() {
            None => Python::with_gil(|py| Ok(py.None())),
            Some(record) => Ok(record),
        }
    }

    /// Retrieves the all records in this collection, only returning the specified fields
    /// for each given record
    pub(crate) fn get_all_partially(&self, fields: Vec<String>) -> PyResult<Vec<Py<PyAny>>> {
        utils::get_all_partial_records_in_collection(&self.pool, &self.name, &self.meta, &fields)
    }

    /// Retrieves the records with the given ids in this collection, only returning
    /// the specified fields for each record
    pub(crate) fn get_many_partially(
        &self,
        ids: Vec<String>,
        fields: Vec<String>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        utils::get_partial_records_by_id(&self.pool, &self.name, &self.meta, &ids, &fields)
    }
}

impl Collection {
    /// Instantiates a new collection. This is not accessible to python and thus a collection
    /// cannot be directly instantiated in python
    pub(crate) fn new(
        name: String,
        pool: r2d2::Pool<redis::Client>,
        meta: CollectionMeta,
        default_ttl: Option<u64>,
    ) -> Self {
        Collection {
            name,
            meta,
            pool,
            default_ttl,
        }
    }
}
