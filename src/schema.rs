use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

use crate::field_types::FieldType;

#[derive(Clone, Debug)]
pub(crate) struct Schema {
    pub mapping: HashMap<String, FieldType>,
}

impl Schema {
    pub(crate) fn from_py_schema(
        ob: Py<PyAny>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
    ) -> PyResult<Self> {
        Python::with_gil(|py| {
            let ob = ob.into_py(py);
            let ob: &PyDict = ob.extract(py)?;
            if let Some(props) = ob.get_item("properties") {
                let definitions: HashMap<String, Py<PyAny>> = match ob.get_item("definitions") {
                    None => Default::default(),
                    Some(def) => def.extract()?,
                };
                Schema::from_py_any(props, &definitions, primary_key_field_map, model_type_map)
            } else {
                Err(PyValueError::new_err(
                    "Invalid schema. No 'properties' found",
                ))
            }
        })
    }

    /// Extracts all nested fields in this schema instance
    pub(crate) fn extract_nested_fields(&self) -> Vec<String> {
        self.mapping
            .iter()
            .filter_map(|(k, v)| {
                if let FieldType::Nested { .. } = v {
                    Some(k.to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Gets the FieldType corresponding to the given field_name
    #[inline]
    pub(crate) fn get_type(&self, field_name: &str) -> Option<&FieldType> {
        self.mapping.get(field_name)
    }

    /// Creates an empty schema
    pub(crate) fn empty() -> Self {
        Self {
            mapping: Default::default(),
        }
    }

    /// Converts a PyAny dictionary like object into a schema. e.g.
    ///  {'title': 'A', 'type': 'object', 'properties': {'height': {'title': 'Height', 'type': 'integer'}}
    pub(crate) fn from_py_any(
        props: &PyAny,
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
    ) -> PyResult<Self> {
        let props: &PyDict = props.downcast()?;
        let keys = props.keys();
        let mapping = keys
            .iter()
            .map(|key| {
                let value = props.get_item(key).unwrap();
                let key: String = key.extract()?;
                let value: FieldType = FieldType::extract_from_py_schema(
                    value,
                    definitions,
                    primary_key_field_map,
                    model_type_map,
                )?;
                Ok((key, value))
            })
            .collect::<PyResult<HashMap<String, FieldType>>>()?;
        Ok(Self { mapping })
    }
}
