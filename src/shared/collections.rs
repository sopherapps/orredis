use pyo3::types::PyType;
use pyo3::Py;

use crate::pyclass;
use crate::shared::schema::Schema;

#[derive(Clone)]
#[pyclass(subclass)]
pub(crate) struct CollectionMeta {
    pub(crate) schema: Box<Schema>,
    pub(crate) model_type: Py<PyType>,
    pub(crate) primary_key_field: String,
    pub(crate) nested_fields: Vec<String>,
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
