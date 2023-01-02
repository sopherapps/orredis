use std::collections::HashMap;
use std::ops::DerefMut;

use pyo3::exceptions::PyConnectionError;
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;

use crate::shared::collections::CollectionMeta;
use crate::shared::macros::{py_key_error, py_value_error};
use crate::shared::parsers::redis_to_py;
use crate::shared::utils as shared_utils;

/// Inserts the (primary key, record) tuples passed to it in a batch into the redis store
pub(crate) fn insert_records(
    pool: &r2d2::Pool<redis::Client>,
    records: &Vec<(String, Vec<(String, String)>)>,
    ttl: &Option<u64>,
) -> PyResult<()> {
    let mut conn = pool
        .get()
        .map_err(|e| PyConnectionError::new_err(e.to_string()))?;
    let mut pipe = redis::pipe();

    // start transaction
    pipe.cmd("MULTI");
    for (pk, record) in records {
        pipe.hset_multiple(pk, &record);

        if let Some(life_span) = ttl {
            pipe.expire(pk, *life_span as usize);
        }
    }
    // end transaction
    pipe.cmd("EXEC");

    pipe.query(conn.deref_mut())
        .map_err(|e| PyConnectionError::new_err(e.to_string()))
}

/// Removes the given keys from the redis store
pub(crate) fn remove_records(pool: &r2d2::Pool<redis::Client>, keys: &Vec<String>) -> PyResult<()> {
    let mut conn = pool
        .get()
        .map_err(|e| PyConnectionError::new_err(e.to_string()))?;
    let mut pipe = redis::pipe();

    pipe.del(keys);

    pipe.query(conn.deref_mut())
        .map_err(|e| PyConnectionError::new_err(e.to_string()))
}

/// Gets the records for the given collection name in redis, with the given ids
pub(crate) fn get_records_by_id(
    pool: &r2d2::Pool<redis::Client>,
    collection_name: &str,
    meta: &CollectionMeta,
    ids: &Vec<String>,
) -> PyResult<Vec<Py<PyAny>>> {
    let ids: Vec<String> = ids
        .into_iter()
        .map(|k| shared_utils::generate_hash_key(collection_name, &k.to_string()))
        .collect();

    run_script(
        pool,
        meta,
        |pipe| {
            pipe.cmd("EVAL")
                .arg(shared_utils::SELECT_ALL_FIELDS_FOR_SOME_IDS_SCRIPT)
                .arg(ids.len())
                .arg(ids)
                .arg(&meta.nested_fields);
            Ok(())
        },
        |data| Python::with_gil(|py| meta.model_type.call(py, (), Some(data.into_py_dict(py)))),
    )
}

/// Gets records in the collection of the given name from redis with the given ids,
/// returning a vector of dictionaries with only the fields specified for each record
pub(crate) fn get_partial_records_by_id(
    pool: &r2d2::Pool<redis::Client>,
    collection_name: &str,
    meta: &CollectionMeta,
    ids: &Vec<String>,
    fields: &Vec<String>,
) -> PyResult<Vec<Py<PyAny>>> {
    let ids: Vec<String> = ids
        .into_iter()
        .map(|k| shared_utils::generate_hash_key(collection_name, &k.to_string()))
        .collect();

    run_script(
        pool,
        meta,
        |pipe| {
            pipe.cmd("EVAL")
                .arg(shared_utils::SELECT_SOME_FIELDS_FOR_SOME_IDS_SCRIPT)
                .arg(ids.len())
                .arg(ids)
                .arg(fields)
                .arg(&meta.nested_fields);
            Ok(())
        },
        |data| Ok(Python::with_gil(|py| data.into_py(py))),
    )
}

/// Gets all records in the collection of the given name from redis,
/// returning a vector of dictionaries with only the fields specified for each record
pub(crate) fn get_all_partial_records_in_collection(
    pool: &r2d2::Pool<redis::Client>,
    collection_name: &str,
    meta: &CollectionMeta,
    fields: &Vec<String>,
) -> PyResult<Vec<Py<PyAny>>> {
    run_script(
        pool,
        meta,
        |pipe| {
            pipe.cmd("EVAL")
                .arg(shared_utils::SELECT_SOME_FIELDS_FOR_ALL_IDS_SCRIPT)
                .arg(0)
                .arg(shared_utils::generate_collection_key_pattern(
                    collection_name,
                ))
                .arg(fields)
                .arg(&meta.nested_fields);
            Ok(())
        },
        |data| Ok(Python::with_gil(|py| data.into_py(py))),
    )
}

/// Gets all the records that are in the given collection
pub(crate) fn get_all_records_in_collection(
    pool: &r2d2::Pool<redis::Client>,
    collection_name: &str,
    meta: &CollectionMeta,
) -> PyResult<Vec<Py<PyAny>>> {
    run_script(
        pool,
        meta,
        |pipe| {
            pipe.cmd("EVAL")
                .arg(shared_utils::SELECT_ALL_FIELDS_FOR_ALL_IDS_SCRIPT)
                .arg(0)
                .arg(shared_utils::generate_collection_key_pattern(
                    collection_name,
                ))
                .arg(&meta.nested_fields);
            Ok(())
        },
        |data| Python::with_gil(|py| meta.model_type.call(py, (), Some(data.into_py_dict(py)))),
    )
}

/// Runs a lua script, and handles the response, transforming it into a list of hashmaps which
/// is then transformed into a list of Py<PyAny> using the item_parser function
pub(crate) fn run_script<T, F>(
    pool: &r2d2::Pool<redis::Client>,
    meta: &CollectionMeta,
    script: T,
    item_parser: F,
) -> PyResult<Vec<Py<PyAny>>>
where
    T: FnOnce(&mut redis::Pipeline) -> PyResult<()>,
    F: FnOnce(HashMap<String, Py<PyAny>>) -> PyResult<Py<PyAny>> + Copy,
{
    let mut conn = pool
        .get()
        .map_err(|e| PyConnectionError::new_err(e.to_string()))?;
    let mut pipe = redis::pipe();

    script(&mut pipe)?;

    let result: redis::Value = pipe
        .query(conn.deref_mut())
        .or_else(|e| Err(PyConnectionError::new_err(e.to_string())))?;

    let results = result
        .as_sequence()
        .ok_or_else(|| py_value_error!(result, "Response from redis is of unexpected shape"))?
        .get(0)
        .ok_or_else(|| py_value_error!(result, "Response from redis is of unexpected shape"))?
        .as_sequence()
        .ok_or_else(|| py_value_error!(result, "Response from redis is of unexpected shape"))?;

    let empty_value = redis::Value::Bulk(vec![]);
    let mut list_of_results: Vec<Py<PyAny>> = Vec::with_capacity(results.len());

    for item in results {
        if *item != empty_value {
            match item.as_map_iter() {
                None => return Err(py_value_error!(item, "redis value is not a map")),
                Some(item) => {
                    let data = item
                        .map(|(k, v)| {
                            let key = redis_to_py::<String>(k)?;
                            let value = match meta.schema.get_type(&key) {
                                Some(field_type) => field_type.redis_to_py(v),
                                None => {
                                    Err(py_key_error!(&key, "key found in data but not in schema"))
                                }
                            }?;
                            Ok((key, value))
                        })
                        .collect::<PyResult<HashMap<String, Py<PyAny>>>>()?;
                    let data = item_parser(data)?;
                    list_of_results.push(data);
                }
            }
        }
    }

    Ok(list_of_results)
}
