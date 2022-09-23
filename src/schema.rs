use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::records::FieldType;

#[derive(Clone)]
pub(crate) struct Schema {
    pub mapping: HashMap<String, FieldType>,
}

impl<'source> FromPyObject<'source> for Schema {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let ob: &PyDict = ob.downcast()?;
        if let Some(props) = ob.get_item("properties") {
            let props: &PyDict = props.downcast()?;
            let keys = props.keys();
            let mapping: PyResult<HashMap<String, FieldType>> = keys
                .iter()
                .map(|key| {
                    let value = props.get_item(key).unwrap();
                    let key: String = key.extract()?;
                    let value: FieldType = value.extract()?;
                    Ok((key, value))
                })
                .collect();

            Ok(Schema { mapping: mapping? })
        } else {
            Err(PyValueError::new_err(
                "Invalid schema. No 'properties' found",
            ))
        }
    }
}

impl Schema {
    pub(crate) fn extract_nested_fields(&self) -> HashMap<String, String> {
        self.mapping
            .iter()
            .filter_map(|(k, v)| {
                if let FieldType::Nested {
                    model_name,
                    data: _,
                } = v
                {
                    Some((k.to_string(), model_name.to_string()))
                } else {
                    None
                }
            })
            .collect()
    }
}
