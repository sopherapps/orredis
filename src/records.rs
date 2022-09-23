use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};

use crate::schema::Schema;

#[derive(Clone)]
pub(crate) enum Record {
    Full { data: HashMap<String, FieldType> },
    Partial { data: HashMap<String, FieldType> },
}

#[derive(Clone)]
pub(crate) enum FieldType {
    Nested {
        model_name: String,
        data: Option<Record>,
    },
    Dict {
        key: Box<FieldType>,
        value: Box<FieldType>,
        data: Option<HashMap<FieldType, FieldType>>,
    },
    List {
        items: Box<FieldType>,
        data: Option<Vec<FieldType>>,
    },
    Tuple {
        items: Vec<FieldType>,
        data: Option<Vec<FieldType>>,
    },
    Str {
        data: Option<String>,
    },
    Int {
        data: Option<i64>,
    },
    Float {
        data: Option<f64>,
    },
    Bool {
        data: Option<bool>,
    },
    None,
}

impl<'source> FromPyObject<'source> for FieldType {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        // https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
        let ob: &PyDict = ob.downcast()?;
        if let Some(data_type) = ob.get_item("type") {
            let data_type: &str = data_type.extract()?;
            match data_type {
                "null" => Ok(Self::None),
                "boolean" => Ok(Self::Bool { data: None }),
                "string" => Ok(Self::Str { data: None }),
                "number" => Ok(Self::Float { data: None }),
                "integer" => Ok(Self::Int { data: None }),
                "object" => Ok(Self::Dict {
                    key: Box::new(Self::Str { data: None }),
                    value: Box::new(Self::Str { data: None }),
                    data: None,
                }),
                "array" => {
                    if let Some(items) = ob.get_item("items") {
                        if items.is_instance_of::<PyList>()? {
                            Ok(Self::Tuple {
                                items: items.extract()?,
                                data: None,
                            })
                        } else {
                            Ok(Self::List {
                                items: Box::new(items.extract()?),
                                data: None,
                            })
                        }
                    } else {
                        Ok(Self::List {
                            items: Box::new(Self::Str { data: None }),
                            data: None,
                        })
                    }
                }
                // FIXME: implement more
                &_ => Ok(Self::Str { data: None }),
            }
        } else if let Some(schema_ref) = ob.get_item("$ref") {
            let schema_ref: String = schema_ref.extract()?;
            let model_name = match schema_ref.rsplit("/").next() {
                None => {
                    return Err(PyValueError::new_err(
                        "nested model's reference not found as expected",
                    ))
                }
                Some(v) => v.to_string(),
            };

            Ok(Self::Nested {
                model_name,
                data: None,
            })
        } else {
            Ok(Self::Str { data: None })
        }
    }
}

impl Record {
    /// Converts a python object into a full Record. The python object is a model.
    /// This is useful when getting data from python
    pub(crate) fn from_py_object(data: &Py<PyAny>, schema: &Schema) -> PyResult<Self> {
        todo!()
    }

    /// Converts a python object into a partial Record. The python object is a dictionary.
    /// This is useful when getting data from python
    pub(crate) fn from_py_dict(data: &Py<PyAny>, schema: &Schema) -> PyResult<Self> {
        todo!()
    }

    /// Converts a redis value into a Record. This is useful when getting data from redis
    pub(crate) fn from_redis_value(data: &redis::Value, schema: &Schema) -> PyResult<Self> {
        todo!()
    }

    /// Generates the key-value pairs for this instance to be inserted into redis.
    pub(crate) fn to_redis_key_value_pairs(&self) -> PyResult<Vec<(String, String)>> {
        todo!()
    }

    /// Generates a primary key basing on the model name and the primary key field.
    /// This is used when inserting the record into redis
    pub(crate) fn generate_primary_key(
        &self,
        model_name: &str,
        primary_key_field: &str,
    ) -> PyResult<String> {
        todo!()
    }

    /// Converts the current instance into a py model object.
    pub(crate) fn to_py_object(&self, model_type: &Py<PyType>) -> PyResult<Py<PyAny>> {
        todo!()
    }

    /// Converts the current instance into a py dictionary.
    pub(crate) fn to_py_dict(&self, model_type: &Py<PyType>) -> PyResult<Py<PyAny>> {
        todo!()
    }

    /// Extracts any nested records and replaces them with a string which is their primary key
    /// It returns tuples of the primary keys and records
    pub(crate) fn pop_nested_records(
        &mut self,
        primary_key_field_map: &HashMap<String, String>,
    ) -> PyResult<Vec<(String, Self)>> {
        todo!()
    }
}
