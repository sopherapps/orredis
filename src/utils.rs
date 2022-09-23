use std::collections::HashMap;

use pyo3::prelude::*;
use r2d2::Pool;
use redis::Client;

use crate::records::Record;
use crate::store::CollectionMeta;

/// Inserts the (primary key, record) tuples passed to it in a batch into the redis store
pub(crate) fn insert_records(
    pool: &r2d2::Pool<redis::Client>,
    records: &Vec<(String, Record)>,
    option: &Option<u64>,
) -> PyResult<()> {
    todo!()
}

/// Removes the given keys from the redis store
pub(crate) fn remove_records(pool: &r2d2::Pool<redis::Client>, keys: &Vec<String>) -> PyResult<()> {
    todo!()
}

/// Gets the records for the given collection name in redis, with the given ids
pub(crate) fn get_records_by_id(
    pool: &r2d2::Pool<redis::Client>,
    collection_name: &str,
    meta: &CollectionMeta,
    ids: &Vec<String>,
) -> PyResult<Vec<Py<PyAny>>> {
    todo!()
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
    todo!()
}

/// Gets all records in the collection of the given name from redis,
/// returning a vector of dictionaries with only the fields specified for each record
pub(crate) fn get_all_partial_records_in_collection(
    pool: &r2d2::Pool<redis::Client>,
    collection_name: &str,
    fields: &Vec<String>,
) -> PyResult<Vec<Py<PyAny>>> {
    todo!()
}

/// Gets all the records that are in the given collection
pub(crate) fn get_all_records_in_collection(collection_name: &str) -> PyResult<Vec<Py<PyAny>>> {
    todo!()
}

/// Prepares the records for inserting
pub(crate) fn prepare_record_to_insert(
    collection_name: &str,
    meta: &CollectionMeta,
    primary_key_field_map: &HashMap<String, String>,
    mut parent_record: Record,
    id: Option<&str>,
) -> PyResult<Vec<(String, Record)>> {
    let mut records: Vec<(String, Record)> = Vec::with_capacity(2);
    let primary_key = match id {
        None => parent_record.generate_primary_key(collection_name, &meta.primary_key_field)?,
        Some(id) => generate_hash_key(collection_name, id),
    };
    let mut nested_records = parent_record.pop_nested_records(&primary_key_field_map)?;
    records.append(&mut nested_records);
    records.push((primary_key, parent_record));
    Ok(records)
}

/// Constructs a unique key for saving a hashmap such that it can be distinguished from
/// hashes of other collections even if they had the same id
#[inline]
pub(crate) fn generate_hash_key(collection_name: &str, id: &str) -> String {
    todo!()
}
