use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use pyo3::types::timezone_utc;
use pyo3::{Py, PyAny, PyResult, Python};

use crate::shared::field_types::FieldType;
use crate::shared::macros::py_key_error;
use crate::shared::schema::Schema;

pub(crate) const SELECT_SOME_FIELDS_FOR_ALL_IDS_SCRIPT: &str = r"local filtered = {} local cursor = '0' local table_unpack = table.unpack or unpack local columns = {} local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do if i > 1 then if args_tracker[k] then nested_columns[k] = true else  table.insert(columns, k) args_tracker[k] = true end end end repeat local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1]) for _, key in ipairs(result[2]) do if redis.call('TYPE', key).ok == 'hash' then  local data = redis.call('HMGET', key, table_unpack(columns)) local parsed_data = {} for i, v in ipairs(data) do table.insert(parsed_data, columns[i]) if nested_columns[columns[i]] then v = redis.call('HGETALL', v) end table.insert(parsed_data, v) end table.insert(filtered, parsed_data) end end cursor = result[1] until (cursor == '0') return filtered";
pub(crate) const SELECT_ALL_FIELDS_FOR_ALL_IDS_SCRIPT: &str = r"local filtered = {} local cursor = '0' local nested_fields = {} for i, key in ipairs(ARGV) do if i > 1 then nested_fields[key] = true end end repeat local result = redis.call('SCAN', cursor, 'MATCH', ARGV[1]) for _, key in ipairs(result[2]) do if redis.call('TYPE', key).ok == 'hash' then local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(filtered, parent) end end cursor = result[1] until (cursor == '0') return filtered";
pub(crate) const SELECT_ALL_FIELDS_FOR_SOME_IDS_SCRIPT: &str = r"local result = {} local nested_fields = {} for _, key in ipairs(ARGV) do nested_fields[key] = true end for _, key in ipairs(KEYS) do local parent = redis.call('HGETALL', key) for i, k in ipairs(parent) do if nested_fields[k] then local nested = redis.call('HGETALL', parent[i + 1]) parent[i + 1] = nested end end table.insert(result, parent) end return result";
pub(crate) const SELECT_SOME_FIELDS_FOR_SOME_IDS_SCRIPT: &str = r"local result = {} local table_unpack = table.unpack or unpack local columns = { } local nested_columns = {} local args_tracker = {} for i, k in ipairs(ARGV) do if args_tracker[k] then nested_columns[k] = true else table.insert(columns, k) args_tracker[k] = true end end for _, key in ipairs(KEYS) do local data = redis.call('HMGET', key, table_unpack(columns)) local parsed_data = {} for i, v in ipairs(data) do if v then table.insert(parsed_data, columns[i]) if nested_columns[columns[i]] then v = redis.call('HGETALL', v) end table.insert(parsed_data, v) end end table.insert(result, parsed_data) end return result";

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

/// Tries to get a value from a given map so as to return an owned value
/// or an python error
#[inline(always)]
pub(crate) fn try_get<K, V>(data: &HashMap<K, V>, key: &K, msg: &str) -> PyResult<V>
where
    K: Hash + Eq + Debug,
    V: Clone,
{
    match data.get(&key) {
        Some(k) => Ok(k.clone()),
        None => Err(py_key_error!(&key, msg)),
    }
}
