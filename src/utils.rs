use std::collections::HashMap;
use std::ops::DerefMut;

use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{timezone_utc, IntoPyDict, PyDate, PyDateTime};

use crate::field_types::FieldType;
use crate::parsers::redis_to_py;
use crate::schema::Schema;
use crate::store::CollectionMeta;

const SELECT_SOME_FIELDS_FOR_ALL_IDS_SCRIPT: &str = r"local filtered = {} local cursor = '0' local table_unpack = table.unpack or unpack local columns = {} local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do if i > 1 then if args_tracker[k] then nested_columns[k] = true else  table.insert(columns, k) args_tracker[k] = true end end end repeat local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1]) for _, key in ipairs(result[2]) do if redis.call('TYPE', key).ok == 'hash' then  local data = redis.call('HMGET', key, table_unpack(columns)) local parsed_data = {} for i, v in ipairs(data) do table.insert(parsed_data, columns[i]) if nested_columns[columns[i]] then v = redis.call('HGETALL', v) end table.insert(parsed_data, v) end table.insert(filtered, parsed_data) end end cursor = result[1] until (cursor == '0') return filtered";
const SELECT_ALL_FIELDS_FOR_ALL_IDS_SCRIPT: &str = r"local filtered = {} local cursor = '0' local nested_fields = {} for i, key in ipairs(ARGV) do if i > 1 then nested_fields[key] = true end end repeat local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1]) for _, key in ipairs(result[2]) do if redis.call('TYPE', key).ok == 'hash' then local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(filtered, parent) end end cursor = result[1] until (cursor == '0') return filtered";
const SELECT_ALL_FIELDS_FOR_SOME_IDS_SCRIPT: &str = r"local result = {} local nested_fields = {} for _, key in ipairs(ARGV) do nested_fields[key] = true end for _, key in ipairs(KEYS) do local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(result, parent) end return result";
const SELECT_SOME_FIELDS_FOR_SOME_IDS_SCRIPT: &str = r"local result = {} local table_unpack = table.unpack or unpack local columns = { } local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do if args_tracker[k] then nested_columns[k] = true else table.insert(columns, k) args_tracker[k] = true end end for _, key in ipairs(KEYS) do local data = redis.call('HMGET', key, table_unpack(columns)) local parsed_data = {} for i, v in ipairs(data) do if v then table.insert(parsed_data, columns[i]) if nested_columns[columns[i]] then v = redis.call('HGETALL', v) end table.insert(parsed_data, v) end end table.insert(result, parsed_data) end return result";

macro_rules! py_value_error {
    ($v:expr, $det:expr) => {
        PyValueError::new_err(format!("{:?} (value was {:?})", $det, $v))
    };
}

macro_rules! py_key_error {
    ($v:expr, $det:expr) => {
        PyKeyError::new_err(format!("{:?} (key was {:?})", $det, $v))
    };
}

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
        .map(|k| generate_hash_key(collection_name, &k.to_string()))
        .collect();

    run_script(
        pool,
        meta,
        |pipe| {
            pipe.cmd("EVAL")
                .arg(SELECT_ALL_FIELDS_FOR_SOME_IDS_SCRIPT)
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
        .map(|k| generate_hash_key(collection_name, &k.to_string()))
        .collect();

    run_script(
        pool,
        meta,
        |pipe| {
            pipe.cmd("EVAL")
                .arg(SELECT_SOME_FIELDS_FOR_SOME_IDS_SCRIPT)
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
                .arg(SELECT_SOME_FIELDS_FOR_ALL_IDS_SCRIPT)
                .arg(0)
                .arg(generate_collection_key_pattern(collection_name))
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
                .arg(SELECT_ALL_FIELDS_FOR_ALL_IDS_SCRIPT)
                .arg(0)
                .arg(generate_collection_key_pattern(collection_name))
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

/// Prepares the records for inserting. It may receive a model instance or a dictionary
pub(crate) fn prepare_record_to_insert(
    collection_name: &str,
    schema: &Box<Schema>,
    obj: &Py<PyAny>,
    primary_key_field: &str,
    id: Option<&str>,
) -> PyResult<Vec<(String, Vec<(String, String)>)>> {
    let obj = Python::with_gil(|py| match obj.extract::<HashMap<String, Py<PyAny>>>(py) {
        Ok(v) => Ok(v),
        Err(_) => obj.getattr(py, "dict")?.call0(py)?.extract(py),
    })?;

    let mut results: Vec<(String, Vec<(String, String)>)> = Vec::with_capacity(2);
    let mut parent_record: Vec<(String, String)> = Vec::with_capacity(obj.len());

    for (field, type_) in &schema.mapping {
        if let Some(v) = obj.get(field) {
            match type_ {
                FieldType::Nested {
                    model_name,
                    primary_key_field: nested_pk_field,
                    schema: nested_schema,
                    ..
                } => {
                    let mut data = prepare_record_to_insert(
                        &model_name,
                        &nested_schema,
                        v,
                        &nested_pk_field,
                        None,
                    )?;
                    if let Some((k, _)) = data.last() {
                        parent_record.push((field.clone(), k.clone()));
                        results.append(&mut data);
                    }
                }
                FieldType::Datetime => Python::with_gil(|py| -> PyResult<()> {
                    // convert every datetime into a UTC datetime
                    let v = v
                        .getattr(py, "astimezone")?
                        .call(py, (timezone_utc(py),), None)?;
                    parent_record.push((field.clone(), v.to_string()));
                    Ok(())
                })?,
                FieldType::Bool => {
                    let v = v.to_string().to_lowercase();
                    parent_record.push((field.clone(), v));
                }
                _ => {
                    parent_record.push((field.clone(), v.to_string()));
                }
            };
        }
    }

    let primary_key = match id {
        None => {
            let pk = obj.get(primary_key_field).ok_or_else(|| {
                py_key_error!(
                    primary_key_field,
                    format!("primary key field missing in {:?}", obj)
                )
            })?;
            generate_hash_key(collection_name, &pk.to_string())
        }
        Some(id) => generate_hash_key(collection_name, id),
    };

    results.push((primary_key, parent_record));
    Ok(results)
}

/// Constructs a unique key for saving a hashmap such that it can be distinguished from
/// hashes of other collections even if they had the same id
#[inline]
pub(crate) fn generate_hash_key(collection_name: &str, id: &str) -> String {
    format!("{}_%&_{}", collection_name, id)
}

/// Constructs a pattern for the keys that belong to a given collection
#[inline]
pub(crate) fn generate_collection_key_pattern(collection_name: &str) -> String {
    format!("{}_%&_*", collection_name)
}

/// Converts a timestamp into a python date/datetime
pub(crate) fn timestamp_to_py_date(timestamp: i64) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        let v = PyDate::from_timestamp(py, timestamp)?;
        Ok(Py::from(v))
    })
}

/// Converts a timestamp into a python date/datetime
pub(crate) fn timestamp_to_py_datetime(timestamp: i64) -> PyResult<Py<PyAny>> {
    Python::with_gil(|py| -> PyResult<Py<PyAny>> {
        let v = PyDateTime::from_timestamp(py, timestamp as f64, Some(timezone_utc(py)))?;
        Ok(Py::from(v))
    })
}
