use std::collections::HashMap;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};
use pyo3::PyDowncastError;
use redis::FromRedisValue;

use crate::parsers;
use crate::schema::Schema;

macro_rules! py_key_error {
    ($v:expr, $det:expr) => {
        PyKeyError::new_err(format!("{:?} (key was {:?})", $det, $v))
    };
}

macro_rules! py_value_error {
    ($v:expr, $det:expr) => {
        PyValueError::new_err(format!("{:?} (value was {:?})", $det, $v))
    };
}

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
        schema: Schema,
    },
    Dict {
        value: Box<FieldType>,
        data: Option<HashMap<String, FieldType>>,
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

impl FieldType {
    pub(crate) fn from_py_any(field_type: &Self, data: &Py<PyAny>) -> PyResult<FieldType> {
        let value = match field_type {
            FieldType::Nested {
                model_name,
                schema: nested_schema,
                ..
            } => Self::Nested {
                model_name: model_name.to_string(),
                data: Some(Record::from_py_object(data, nested_schema)?),
                schema: nested_schema.clone(),
            },
            FieldType::Dict { value, .. } => {
                let data: HashMap<String, Py<PyAny>> = Python::with_gil(|py| data.extract(py))?;
                let data: PyResult<HashMap<String, FieldType>> = data
                    .into_iter()
                    .map(|(k, v)| {
                        let v = Self::from_py_any(value, &v)?;
                        Ok((k, v))
                    })
                    .collect();
                Self::Dict {
                    value: value.clone(),
                    data: Some(data?),
                }
            }
            FieldType::List { items, .. } => {
                let data: Vec<Py<PyAny>> = Python::with_gil(|py| data.extract(py))?;
                let data: PyResult<Vec<FieldType>> =
                    data.iter().map(|v| Self::from_py_any(items, v)).collect();
                Self::List {
                    items: items.clone(),
                    data: Some(data?),
                }
            }
            FieldType::Tuple { items, .. } => {
                let data: Vec<Py<PyAny>> = Python::with_gil(|py| data.extract(py))?;
                let data: PyResult<Vec<FieldType>> = data
                    .iter()
                    .zip(items)
                    .map(|(v, type_)| Self::from_py_any(type_, v))
                    .collect();
                Self::Tuple {
                    items: items.clone(),
                    data: Some(data?),
                }
            }
            FieldType::Str { .. } => Self::Str {
                data: Some(data.to_string()),
            },
            FieldType::Int { .. } => {
                let v = Python::with_gil(|py| data.extract(py))?;
                Self::Int { data: Some(v) }
            }
            FieldType::Float { .. } => {
                let v = Python::with_gil(|py| data.extract(py))?;
                Self::Float { data: Some(v) }
            }
            FieldType::Bool { .. } => {
                let v = Python::with_gil(|py| data.extract(py))?;
                Self::Bool { data: Some(v) }
            }
            FieldType::None => Self::None,
        };
        Ok(value)
    }

    pub(crate) fn from_redis_value(field_type: &Self, data: &redis::Value) -> PyResult<FieldType> {
        let value = match field_type {
            FieldType::Nested {
                model_name,
                schema: nested_schema,
                ..
            } => Self::Nested {
                model_name: model_name.to_string(),
                data: Some(Record::from_redis_value(data, nested_schema, true)?),
                schema: nested_schema.clone(),
            },
            FieldType::Dict { value: type_, .. } => {
                let v = parsers::redis_to_py::<String>(data)?;
                let data: HashMap<String, FieldType> = Self::parse_dict_str(&v, type_)?;
                Self::Dict {
                    value: type_.clone(),
                    data: Some(data),
                }
            }
            FieldType::List { items: type_, .. } => {
                let v = parsers::redis_to_py::<String>(data)?;
                let data: Vec<FieldType> = Self::parse_list_str(&v, type_)?;
                Self::List {
                    items: type_.clone(),
                    data: Some(data),
                }
            }
            FieldType::Tuple {
                items: type_list, ..
            } => {
                let v = parsers::redis_to_py::<String>(data)?;
                let data: Vec<FieldType> = FieldType::parse_tuple_str(&v, type_list)?;
                Self::Tuple {
                    items: type_list.clone(),
                    data: Some(data),
                }
            }
            FieldType::Str { .. } => Self::Str {
                data: Some(parsers::redis_to_py::<String>(data)?),
            },
            FieldType::Int { .. } => Self::Int {
                data: Some(parsers::redis_to_py::<i64>(data)?),
            },
            FieldType::Float { .. } => Self::Float {
                data: Some(parsers::redis_to_py::<f64>(data)?),
            },
            FieldType::Bool { .. } => Self::Bool {
                data: Some(parsers::redis_to_py::<bool>(data)?),
            },
            FieldType::None => Self::None,
        };
        Ok(value)
    }

    /// Annotates a hashmap of py objects with field types
    fn annotate_py_hashmap(
        schema: &Schema,
        dict: HashMap<String, Py<PyAny>>,
    ) -> PyResult<HashMap<String, FieldType>> {
        dict.iter()
            .map(|(k, v)| {
                let value = match schema.mapping.get(k) {
                    None => Self::Str {
                        data: Some(v.to_string()),
                    },
                    Some(f) => Self::from_py_any(f, v)?,
                };
                Ok((k.to_string(), value))
            })
            .collect()
    }

    /// Parses a string representation of a dictionary into a hashmap
    pub fn parse_dict_str(value: &str, type_: &FieldType) -> PyResult<HashMap<String, FieldType>> {
        let mut v: HashMap<String, FieldType> = Default::default();
        let items = parsers::extract_str_portions(value, "{", "}", ",");

        for item in items {
            let kv_items = parsers::extract_str_portions(item, "", "", ":");

            if kv_items.len() == 2 {
                let (key, value) = (kv_items[0], kv_items[1]);
                let value = FieldType::from_str(value, type_)?;

                v.insert(key.to_string(), value);
            }
        }

        Ok(v)
    }

    /// Converts a string that represents a list (a python list) into a FieldType
    pub fn parse_list_str(value: &str, type_: &FieldType) -> PyResult<Vec<FieldType>> {
        let items = parsers::extract_str_portions(value, "[", "]", ",");
        items
            .into_iter()
            .map(|item| FieldType::from_str(item, type_))
            .collect()
    }

    /// Converts a string that represents a tuple (a python tuple) into a FieldType
    pub fn parse_tuple_str(value: &str, types_: &Vec<FieldType>) -> PyResult<Vec<FieldType>> {
        let items = parsers::extract_str_portions(value, "(", ")", ",");
        items
            .into_iter()
            .zip(types_)
            .map(|(item, type_)| FieldType::from_str(item, type_))
            .collect()
    }

    /// Converts a string into a FieldType
    pub(crate) fn from_str(data: &str, type_: &FieldType) -> PyResult<FieldType> {
        let result = match type_ {
            FieldType::Nested {
                model_name, schema, ..
            } => {
                let data = Record::from_str(data, schema, true)?;
                Self::Nested {
                    model_name: model_name.to_string(),
                    data: Some(data),
                    schema: schema.clone(),
                }
            }
            FieldType::Dict { value, .. } => {
                let data = Self::parse_dict_str(data, value)?;
                Self::Dict {
                    value: value.clone(),
                    data: Some(data),
                }
            }
            FieldType::List { items, .. } => {
                let data = Self::parse_list_str(data, items)?;
                Self::List {
                    items: items.clone(),
                    data: Some(data),
                }
            }
            FieldType::Tuple { items, .. } => {
                let data = Self::parse_tuple_str(data, items)?;
                Self::Tuple {
                    items: items.clone(),
                    data: Some(data),
                }
            }
            // FIXME: add datetime and date
            FieldType::Str { .. } => Self::Str {
                data: Some(data.to_string()),
            },
            FieldType::Int { .. } => Self::Int {
                data: Some(data.parse::<i64>()?),
            },
            FieldType::Float { .. } => Self::Float {
                data: Some(data.parse::<f64>()?),
            },
            FieldType::Bool { .. } => Self::Bool {
                data: Some(data.to_lowercase().parse::<bool>()?),
            },
            FieldType::None => Self::None,
        };

        Ok(result)
    }

    pub(crate) fn extract_from_py_schema(
        prop: &PyAny,
        definitions: &HashMap<String, Py<PyAny>>,
    ) -> PyResult<Self> {
        // https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
        let prop: &PyDict = prop.downcast()?;
        if let Some(data_type) = prop.get_item("type") {
            let data_type: &str = data_type.extract()?;
            match data_type {
                "null" => Ok(Self::None),
                "boolean" => Ok(Self::Bool { data: None }),
                "string" => Ok(Self::Str { data: None }),
                "number" => Ok(Self::Float { data: None }),
                "integer" => Ok(Self::Int { data: None }),
                "object" => Ok(Self::Dict {
                    value: Box::new(Self::Str { data: None }),
                    data: None,
                }),
                "array" => {
                    if let Some(items) = prop.get_item("items") {
                        match items.downcast::<PyList>() {
                            Ok(type_list) => {
                                let items = type_list
                                    .into_iter()
                                    .map(|v| Self::extract_from_py_schema(v, definitions))
                                    .collect::<PyResult<Vec<FieldType>>>()?;
                                Ok(Self::Tuple { items, data: None })
                            }
                            Err(_) => Ok(Self::List {
                                items: Box::new(Self::extract_from_py_schema(items, definitions)?),
                                data: None,
                            }),
                        }
                    } else {
                        Ok(Self::List {
                            items: Box::new(Self::Str { data: None }),
                            data: None,
                        })
                    }
                }
                // FIXME: implement more like date, datetime etc
                &_ => Ok(Self::Str { data: None }),
            }
        } else if let Some(schema_ref) = prop.get_item("$ref") {
            let schema_ref: String = schema_ref.extract()?;
            let mut name_sections = schema_ref.rsplit("/");
            let model_name = match name_sections.next() {
                None => Err(py_value_error!("model name missing", schema_ref)),
                Some(v) => Ok(v.to_string()),
            }?;
            let schema = match definitions.get(&model_name) {
                None => Ok(Schema::empty()),
                Some(v) => Python::with_gil(|py| {
                    let v = v.as_ref(py);
                    match v.get_item("properties") {
                        Ok(props) => Schema::from_py_any(props, definitions),
                        Err(_) => Ok(Schema::empty()),
                    }
                }),
            }?;

            Ok(Self::Nested {
                model_name,
                data: None,
                schema,
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
        let data = Python::with_gil(|py| -> PyResult<HashMap<String, FieldType>> {
            let dict: HashMap<String, Py<PyAny>> =
                data.getattr(py, "dict")?.call0(py)?.extract(py)?;
            FieldType::annotate_py_hashmap(schema, dict)
        })?;
        Ok(Record::Full { data })
    }

    /// Converts a python object into a partial Record. The python object is a dictionary.
    /// This is useful when getting data from python
    pub(crate) fn from_py_dict(data: &Py<PyAny>, schema: &Schema) -> PyResult<Self> {
        let data = Python::with_gil(|py| -> PyResult<HashMap<String, FieldType>> {
            let dict: HashMap<String, Py<PyAny>> = data.extract(py)?;
            FieldType::annotate_py_hashmap(schema, dict)
        })?;

        Ok(Record::Partial { data })
    }

    /// Converts a redis value into a Record. This is useful when getting data from redis
    pub(crate) fn from_redis_value(
        data: &redis::Value,
        schema: &Schema,
        is_full: bool,
    ) -> PyResult<Self> {
        let value_as_map = data.as_map_iter();

        match value_as_map {
            None => {
                let v = parsers::redis_to_py::<String>(data)?;
                Ok(Self::Partial {
                    data: HashMap::from([("key".to_string(), FieldType::Str { data: Some(v) })]),
                })
            }
            Some(value) => {
                let data = value
                    .map(|(k, v)| {
                        let k = parsers::redis_to_py::<String>(k)?;
                        let value = match schema.mapping.get(&k) {
                            None => FieldType::Str {
                                data: Some(parsers::redis_to_py::<String>(v)?),
                            },
                            Some(f) => FieldType::from_redis_value(f, v)?,
                        };
                        Ok((k, value))
                    })
                    .collect::<PyResult<HashMap<String, FieldType>>>()?;
                match is_full {
                    true => Ok(Self::Full { data }),
                    false => Ok(Self::Partial { data }),
                }
            }
        }
    }

    /// Converts a string value into a Record
    pub(crate) fn from_str(value: &str, schema: &Schema, is_full: bool) -> PyResult<Self> {
        let mut data: HashMap<String, FieldType> = Default::default();
        let items = parsers::extract_str_portions(value, "{", "}", ",");

        for item in items {
            let kv_items = parsers::extract_str_portions(item, "", "", ":");

            if kv_items.len() == 2 {
                let (key, value) = (kv_items[0], kv_items[1]);
                if let Some(type_) = schema.get_type(key) {
                    let value = FieldType::from_str(value, type_)?;
                    data.insert(key.to_string(), value);
                }
            }
        }

        match is_full {
            true => Ok(Self::Full { data }),
            false => Ok(Self::Partial { data }),
        }
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
