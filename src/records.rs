use std::collections::HashMap;
use std::str::FromStr;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict, PyList, PyType};
use pyo3::PyDowncastError;
use redis::FromRedisValue;

use crate::schema::Schema;
use crate::utils::generate_hash_key;
use crate::{parsers, utils};

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

macro_rules! to_py {
    ($v:expr) => {
        Ok(Python::with_gil(|py| $v.into_py(py)))
    };
}

#[derive(Clone, Debug)]
pub(crate) enum Record {
    Full { data: HashMap<String, Py<PyAny>> },
    Partial { data: HashMap<String, Py<PyAny>> },
}

#[derive(Clone, Debug)]
pub(crate) enum FieldType {
    Nested {
        model_name: String,
        schema: Schema,
        primary_key_field: String,
        model_type: Py<PyType>,
    },
    Dict {
        value: Box<FieldType>,
    },
    List {
        items: Box<FieldType>,
    },
    Tuple {
        items: Vec<FieldType>,
    },
    Str,
    Int,
    Float,
    Bool,
    Datetime,
    Date,
    None,
}

impl FieldType {
    /// Converts a Py<PyAny> reference into a String.
    /// This is useful in converting data to be inserted into redis, from python to rust
    pub(crate) fn py_to_str(&self, data: &Py<PyAny>) -> PyResult<String> {
        match self {
            FieldType::Nested {
                model_name,
                primary_key_field,
                ..
            } => {
                let data: HashMap<String, Py<PyAny>> = Python::with_gil(|py| data.extract(py))?;
                match data.get(primary_key_field) {
                    None => Err(py_key_error!(
                        primary_key_field,
                        "primary key field not found in model"
                    )),
                    Some(id) => {
                        let key = generate_hash_key(model_name, &id.to_string());
                        Ok(key)
                    }
                }
            }
            FieldType::Bool => {
                let v = data.to_string().to_lowercase();
                Ok(v)
            }
            _ => Ok(data.to_string()),
        }
    }

    /// Converts data got from redis into a FieldType.
    /// This is useful when getting data from redis to return it in python
    pub(crate) fn redis_to_py(&self, data: &redis::Value) -> PyResult<Py<PyAny>> {
        match self {
            FieldType::Nested {
                schema, model_type, ..
            } => match data.as_map_iter() {
                None => Ok(Python::with_gil(|py| py.None())),
                Some(data) => {
                    let nested_data = data
                        .map(|(k, v)| {
                            let key = parsers::redis_to_py::<String>(k)?;
                            let value = match schema.get_type(&key) {
                                Some(type_) => type_.redis_to_py(v),
                                None => {
                                    Err(py_value_error!(&key, "unexpected field in nested object"))
                                }
                            }?;
                            Ok((key, value))
                        })
                        .collect::<PyResult<HashMap<String, Py<PyAny>>>>()?;
                    Python::with_gil(|py| {
                        model_type.call(py, (), Some(nested_data.into_py_dict(py)))
                    })
                }
            },
            FieldType::Dict { value: type_, .. } => {
                let data = parsers::redis_to_py::<String>(data)?;
                let data: HashMap<String, Py<PyAny>> = Self::parse_dict_str(&data, type_)?;
                to_py!(data)
            }
            FieldType::List { items: type_, .. } => {
                let data = parsers::redis_to_py::<String>(data)?;
                let data: Vec<Py<PyAny>> = Self::parse_list_str(&data, type_)?;
                to_py!(data)
            }
            FieldType::Tuple {
                items: type_list, ..
            } => {
                let data = parsers::redis_to_py::<String>(data)?;
                let data: Vec<Py<PyAny>> = FieldType::parse_tuple_str(&data, type_list)?;
                Python::with_gil(|py| {
                    let data = data.into_py(py);
                    let builtins = PyModule::import(py, "builtins")?;
                    builtins
                        .getattr("tuple")?
                        .call1((&data,))?
                        .extract::<Py<PyAny>>()
                })
            }
            FieldType::Str => {
                let v = parsers::redis_to_py::<String>(data)?;
                to_py!(v)
            }
            FieldType::Int => {
                let v = parsers::redis_to_py::<i64>(data)?;
                to_py!(v)
            }
            FieldType::Float => {
                let v = parsers::redis_to_py::<f64>(data)?;
                to_py!(v)
            }
            FieldType::Bool => {
                let data = parsers::redis_to_py::<String>(data)?;
                let v = parsers::parse_str::<bool>(&data)?;
                to_py!(v)
            }
            FieldType::Datetime => {
                let v = parsers::redis_to_py::<String>(data)?;
                let timestamp = parsers::parse_datetime_to_timestamp(&v)?;
                utils::timestamp_to_py_date(timestamp)
            }
            FieldType::Date => {
                let v = parsers::redis_to_py::<String>(data)?;
                let timestamp = parsers::parse_date_to_timestamp(&v)?;
                utils::timestamp_to_py_date(timestamp)
            }
            FieldType::None => Ok(Python::with_gil(|py| py.None())),
        }
    }

    /// Parses a string representation of a dictionary into a hashmap of py objects
    pub fn parse_dict_str(value: &str, type_: &FieldType) -> PyResult<HashMap<String, Py<PyAny>>> {
        let mut v: HashMap<String, Py<PyAny>> = Default::default();
        let items = parsers::extract_str_portions(value, "{", "}", ",");

        for item in items {
            let kv_items = parsers::extract_str_portions(item, "", "", ":");

            if kv_items.len() == 2 {
                let (key, value) = (kv_items[0], kv_items[1]);
                let value = FieldType::str_to_py(value, type_)?;

                v.insert(key.to_string(), value);
            }
        }

        Ok(v)
    }

    /// Converts a string that represents a list (a python list) into a FieldType
    pub fn parse_list_str(value: &str, type_: &FieldType) -> PyResult<Vec<Py<PyAny>>> {
        let items = parsers::extract_str_portions(value, "[", "]", ",");
        items
            .into_iter()
            .map(|item| FieldType::str_to_py(item, type_))
            .collect()
    }

    /// Converts a string that represents a tuple (a python tuple) into a FieldType
    pub fn parse_tuple_str(value: &str, types_: &Vec<FieldType>) -> PyResult<Vec<Py<PyAny>>> {
        let items = parsers::extract_str_portions(value, "(", ")", ",");
        items
            .into_iter()
            .zip(types_)
            .map(|(item, type_)| FieldType::str_to_py(item, type_))
            .collect()
    }

    /// Converts a string into a Py<PyAny>
    pub(crate) fn str_to_py(data: &str, type_: &FieldType) -> PyResult<Py<PyAny>> {
        match type_ {
            FieldType::Nested { .. } => {
                to_py!(data.to_string())
            }
            FieldType::Dict { value, .. } => {
                let data = Self::parse_dict_str(data, value)?;
                to_py!(data)
            }
            FieldType::List { items, .. } => {
                let data = Self::parse_list_str(data, items)?;
                to_py!(data)
            }
            FieldType::Tuple { items, .. } => {
                let data = Self::parse_tuple_str(data, items)?;
                to_py!(data)
            }
            FieldType::Str => to_py!(data.to_string()),
            FieldType::Int => {
                let data = parsers::parse_str::<i64>(data)?;
                to_py!(data)
            }
            FieldType::Float => {
                let data = parsers::parse_str::<f64>(data)?;
                to_py!(data)
            }
            FieldType::Bool => {
                let data = parsers::parse_str::<bool>(data)?;
                to_py!(data)
            }
            FieldType::Datetime => {
                let timestamp = parsers::parse_datetime_to_timestamp(data)?;
                utils::timestamp_to_py_date(timestamp)
            }
            FieldType::Date => {
                let timestamp = parsers::parse_date_to_timestamp(data)?;
                utils::timestamp_to_py_date(timestamp)
            }
            FieldType::None => Ok(Python::with_gil(|py| py.None())),
        }
    }

    /// Given a schema property and a hashmap of definitions, this method extracts the right FieldType
    /// for that property. It is used when creating a representation of the python-generated schema
    /// within rust
    pub(crate) fn extract_from_py_schema(
        prop: &PyAny,
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
    ) -> PyResult<Self> {
        // https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
        let prop: &PyDict = prop.downcast()?;
        if let Some(data_type) = prop.get_item("type") {
            let data_type: &str = data_type.extract()?;
            match data_type {
                "null" => Ok(Self::None),
                "boolean" => Ok(Self::Bool),
                "string" => match prop.get_item("format") {
                    None => Ok(Self::Str),
                    Some(format) => {
                        let format = format.to_string();
                        match format.as_str() {
                            "date-time" => Ok(Self::Datetime),
                            "date" => Ok(Self::Date),
                            _ => Ok(Self::Str),
                        }
                    }
                },
                "number" => Ok(Self::Float),
                "integer" => Ok(Self::Int),
                "object" => Ok(Self::Dict {
                    value: Box::new(Self::Str),
                }),
                "array" => {
                    if let Some(items) = prop.get_item("items") {
                        match items.downcast::<PyList>() {
                            Ok(type_list) => {
                                let items = type_list
                                    .into_iter()
                                    .map(|v| {
                                        Self::extract_from_py_schema(
                                            v,
                                            definitions,
                                            primary_key_field_map,
                                            model_type_map,
                                        )
                                    })
                                    .collect::<PyResult<Vec<FieldType>>>()?;
                                Ok(Self::Tuple { items })
                            }
                            Err(_) => Ok(Self::List {
                                items: Box::new(Self::extract_from_py_schema(
                                    items,
                                    definitions,
                                    primary_key_field_map,
                                    model_type_map,
                                )?),
                            }),
                        }
                    } else {
                        Ok(Self::List {
                            items: Box::new(Self::Str),
                        })
                    }
                }
                // FIXME: implement more like date, datetime etc
                &_ => Ok(Self::Str),
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
                        Ok(props) => Schema::from_py_any(
                            props,
                            definitions,
                            primary_key_field_map,
                            model_type_map,
                        ),
                        Err(_) => Ok(Schema::empty()),
                    }
                }),
            }?;
            let primary_key_field = match primary_key_field_map.get(&model_name) {
                Some(k) => Ok(k.to_string()),
                None => Err(py_key_error!(
                    &model_name,
                    format!(
                        "model name missing in primary key field map. \
                    Try to create the {} collection first",
                        &model_name
                    )
                )),
            }?;

            let model_type = match model_type_map.get(&model_name) {
                Some(k) => Ok(k.to_owned()),
                None => Err(py_key_error!(
                    &model_name,
                    "model name missing in model type map"
                )),
            }?;

            Ok(Self::Nested {
                model_name,
                schema,
                primary_key_field,
                model_type,
            })
        } else {
            Ok(Self::Str)
        }
    }
}

impl Record {
    /// Generates the key-value pairs for this instance to be inserted into redis.
    pub(crate) fn to_redis_key_value_pairs(
        &self,
        schema: &Schema,
    ) -> PyResult<Vec<(String, String)>> {
        let data = self.extract_data();

        data.iter()
            .map(|(k, v)| {
                let key = k.clone();
                let value = match schema.get_type(&key) {
                    None => Err(py_key_error!(
                        &key,
                        format!(
                            "missing key in schema: {:?}, for record: {:?}",
                            schema, self
                        )
                    )),
                    Some(type_) => type_.py_to_str(v),
                }?;
                Ok((key, value))
            })
            .collect()
    }

    /// Generates a primary key basing on the model name and the primary key field.
    /// This is used when inserting the record into redis
    pub(crate) fn generate_primary_key(
        &self,
        model_name: &str,
        primary_key_field: &str,
    ) -> PyResult<String> {
        let data = self.extract_data();
        match data.get(primary_key_field) {
            None => Err(py_key_error!(
                primary_key_field,
                "primary key field missing on record"
            )),
            Some(key) => Ok(utils::generate_hash_key(model_name, &key.to_string())),
        }
    }

    /// Extracts the data hidden inside the record
    fn extract_data(&self) -> &HashMap<String, Py<PyAny>> {
        match self {
            Record::Full { data } => data,
            Record::Partial { data } => data,
        }
    }

    /// Converts the current instance into a py model object.
    pub(crate) fn to_py_object(&self, model_type: &Py<PyType>) -> PyResult<Py<PyAny>> {
        let data = self.extract_data();
        Python::with_gil(|py| model_type.call(py, (), Some(data.into_py_dict(py))))
    }

    /// Converts the current instance into a py dictionary.
    pub(crate) fn to_py_dict(&self) -> PyResult<Py<PyAny>> {
        let data = self.extract_data();
        to_py!(data.clone())
    }

    /// Extracts any nested records
    /// It returns tuples of the primary keys and records
    pub(crate) fn pop_nested_records(
        record: &Self,
        schema: &Schema,
    ) -> PyResult<Vec<(String, Self, Schema)>> {
        let data = record.extract_data();
        let mut result: Vec<(String, Self, Schema)> = vec![];

        for (k, v) in &schema.mapping {
            if let FieldType::Nested {
                primary_key_field,
                model_name,
                schema,
                ..
            } = v
            {
                if let Some(obj) = data.get(k) {
                    Python::with_gil(|py| -> PyResult<()> {
                        let nested_data: HashMap<String, Py<PyAny>> = obj.extract(py)?;
                        match nested_data.get(primary_key_field) {
                            None => Err(py_key_error!(
                                primary_key_field,
                                "primary key missing in in nested model"
                            )),
                            Some(pk) => {
                                let pk = pk.to_string();
                                result.push((
                                    utils::generate_hash_key(model_name, &pk),
                                    Self::Full { data: nested_data },
                                    // FIXME: consider using Box to reduce the memory usage for cloning a schema
                                    schema.clone(),
                                ));
                                Ok(())
                            }
                        }
                    })?;
                }
            }
        }

        return Ok(result);
    }

    /// Creates a Record (Full) instance from a python object
    pub(crate) fn from_py_object(obj: &Py<PyAny>) -> PyResult<Self> {
        let data: HashMap<String, Py<PyAny>> =
            Python::with_gil(|py| obj.getattr(py, "dict")?.call0(py)?.extract(py))?;
        return Ok(Self::Full { data });
    }

    /// Creates a Record (Half) instance from a python dictionary
    pub(crate) fn from_py_dict(obj: &Py<PyAny>) -> PyResult<Self> {
        let data: HashMap<String, Py<PyAny>> = Python::with_gil(|py| obj.extract(py))?;
        return Ok(Self::Partial { data });
    }
}
