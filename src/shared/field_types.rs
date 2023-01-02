use std::collections::HashMap;
use std::fmt::Debug;

use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict, PyList, PyType};
use redis::Value;

use crate::shared::macros::{py_value_error, to_py};
use crate::shared::parsers;
use crate::shared::schema::Schema;
use crate::shared::utils::try_get;

#[derive(Clone, Debug)]
pub(crate) enum FieldType {
    Nested {
        model_name: String,
        schema: Box<Schema>,
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
    /// Converts data got from redis into a FieldType.
    /// This is useful when getting data from redis to return it in python
    pub(crate) fn redis_to_py(&self, data: &redis::Value) -> PyResult<Py<PyAny>> {
        match self {
            FieldType::Nested {
                schema, model_type, ..
            } => Self::__redis_to_nested_model_py(schema, model_type, data),
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
                FieldType::parse_tuple_str(&data, type_list)
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
                parsers::timestamp_to_py_datetime(timestamp)
            }
            FieldType::Date => {
                let v = parsers::redis_to_py::<String>(data)?;
                let timestamp = parsers::parse_date_to_timestamp(&v)?;
                parsers::timestamp_to_py_date(timestamp)
            }
            FieldType::None => Ok(Python::with_gil(|py| py.None())),
        }
    }

    /// Converts a redis representation of a nested model into a python nested model
    #[inline]
    fn __redis_to_nested_model_py(
        schema: &Box<Schema>,
        model_type: &Py<PyType>,
        data: &Value,
    ) -> Result<Py<PyAny>, PyErr> {
        match data.as_map_iter() {
            None => Ok(Python::with_gil(|py| py.None())),
            Some(data) => {
                let nested_data = data
                    .map(|(k, v)| {
                        let key = parsers::redis_to_py::<String>(k)?;
                        let value = match schema.get_type(&key) {
                            Some(type_) => type_.redis_to_py(v),
                            None => Err(py_value_error!(&key, "unexpected field in nested object")),
                        }?;
                        Ok((key, value))
                    })
                    .collect::<PyResult<HashMap<String, Py<PyAny>>>>()?;
                Python::with_gil(|py| model_type.call(py, (), Some(nested_data.into_py_dict(py))))
            }
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
    pub fn parse_tuple_str(value: &str, types_: &Vec<FieldType>) -> PyResult<Py<PyAny>> {
        let items = parsers::extract_str_portions(value, "(", ")", ",");
        let parsed_data: PyResult<Vec<Py<PyAny>>> = items
            .into_iter()
            .zip(types_)
            .map(|(item, type_)| FieldType::str_to_py(item, type_))
            .collect();

        Python::with_gil(|py| {
            let parsed_data = parsed_data?.into_py(py);
            let builtins = PyModule::import(py, "builtins")?;
            builtins
                .getattr("tuple")?
                .call1((&parsed_data,))?
                .extract::<Py<PyAny>>()
        })
    }

    /// Converts a string into a Py<PyAny>
    pub(crate) fn str_to_py(data: &str, type_: &FieldType) -> PyResult<Py<PyAny>> {
        match type_ {
            FieldType::Nested { .. } => {
                to_py!(data.to_string()) // FIXME: Does this really have to be a string
                                         //        or redis_to_py?
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
                parsers::timestamp_to_py_datetime(timestamp)
            }
            FieldType::Date => {
                let timestamp = parsers::parse_date_to_timestamp(data)?;
                parsers::timestamp_to_py_date(timestamp)
            }
            FieldType::None => Ok(Python::with_gil(|py| py.None())),
        }
    }

    /// Given a schema property and a hashmap of definitions, this method extracts the right FieldType
    /// for that property. It is used when creating a representation of the python-generated schema
    /// within rust
    #[inline]
    pub(crate) fn extract_from_py_schema(
        prop: &PyAny,
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
    ) -> PyResult<Self> {
        let prop: &PyDict = prop.downcast()?;
        if let Some(data_type) = prop.get_item("type") {
            Self::__extract_non_nested_model_field(
                prop,
                definitions,
                primary_key_field_map,
                model_type_map,
                data_type,
            )
        } else if let Some(schema_ref) = prop.get_item("$ref") {
            Self::__extract_nested_model_field(
                definitions,
                primary_key_field_map,
                model_type_map,
                schema_ref,
            )
        } else {
            Ok(Self::Str)
        }
    }

    /// Extracts a nested model field type from a '$ref' value got from JSONSchema
    /// as seen at https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline]
    fn __extract_nested_model_field(
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
        schema_ref: &PyAny,
    ) -> Result<FieldType, PyErr> {
        let model_name = Self::__extract_nested_model_name(schema_ref)?;
        let schema = Self::__extract_schema_from_definitions_by_ref(
            definitions,
            primary_key_field_map,
            model_type_map,
            &model_name,
        )?;
        let primary_key_field = try_get(
            primary_key_field_map,
            &model_name,
            "[primary keys] Try to create the model collection first",
        )?;
        let model_type = try_get(
            model_type_map,
            &model_name,
            "[model types] Try to create the model collection first",
        )?;

        Ok(Self::Nested {
            model_name,
            schema: Box::new(schema),
            primary_key_field,
            model_type,
        })
    }

    /// Extracts a non-nested model field from a 'type' value got from JSONSchema
    /// as seen at https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline]
    fn __extract_non_nested_model_field(
        prop: &PyDict,
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
        data_type: &PyAny,
    ) -> PyResult<FieldType> {
        let data_type: &str = data_type.extract()?;
        match data_type {
            "null" => Ok(Self::None),
            "boolean" => Ok(Self::Bool),
            "string" => Self::__extract_str_or_datetime_field(prop),
            "number" => Ok(Self::Float),
            "integer" => Ok(Self::Int),
            "object" => Ok(Self::Dict {
                value: Box::new(Self::Str), // FIXME: Does this always have to be a string value?
            }),
            "array" => Self::__extract_array_or_tuple_field(
                prop,
                definitions,
                primary_key_field_map,
                model_type_map,
            ),
            &_ => Ok(Self::Str),
        }
    }

    /// Extracts the schema from the 'definitions' of JSONSchema given '$ref' as seen in
    /// the pydantic reference https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline(always)]
    fn __extract_schema_from_definitions_by_ref(
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
        model_name: &String,
    ) -> PyResult<Schema> {
        match definitions.get(model_name) {
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
        }
    }

    /// Extracts the name of the nested model from JSONSchema's '$ref' property
    /// as seen in hte pydantic reference at
    /// https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline(always)]
    fn __extract_nested_model_name(schema_ref: &PyAny) -> PyResult<String> {
        let schema_ref: String = schema_ref.extract()?;
        let mut name_sections = schema_ref.rsplit("/");
        let model_name = match name_sections.next() {
            None => Err(py_value_error!("model name missing", schema_ref)),
            Some(v) => Ok(v.to_string()),
        }?;
        Ok(model_name)
    }

    /// Extracts an array or tuple field from a JSONSchema definition's key-value pair
    /// basing on the pydantic refreence
    /// https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline(always)]
    fn __extract_array_or_tuple_field(
        prop: &PyDict,
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
    ) -> PyResult<FieldType> {
        if let Some(items) = prop.get_item("items") {
            match items.downcast::<PyList>() {
                Ok(type_list) => Self::__extract_tuple_field(
                    definitions,
                    primary_key_field_map,
                    model_type_map,
                    type_list,
                ),
                Err(_) => Self::__extract_list_field(
                    definitions,
                    primary_key_field_map,
                    model_type_map,
                    items,
                ),
            }
        } else {
            Ok(Self::List {
                items: Box::new(Self::Str), // FIXME: Does this always have to be a string value?
            })
        }
    }

    /// Extracts a List field from items got from a JSONSchema list property as shown in
    /// the pydantic reference https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline(always)]
    fn __extract_list_field(
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
        items: &PyAny,
    ) -> PyResult<FieldType> {
        Ok(Self::List {
            items: Box::new(Self::extract_from_py_schema(
                items,
                definitions,
                primary_key_field_map,
                model_type_map,
            )?),
        })
    }

    /// Extracts a tuple field given a list of types as got from a JSONSchema property
    /// basing on the pydantic reference
    /// https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline(always)]
    fn __extract_tuple_field(
        definitions: &HashMap<String, Py<PyAny>>,
        primary_key_field_map: &HashMap<String, String>,
        model_type_map: &HashMap<String, Py<PyType>>,
        type_list: &PyList,
    ) -> PyResult<FieldType> {
        let items = type_list
            .into_iter()
            .map(|v| {
                Self::extract_from_py_schema(v, definitions, primary_key_field_map, model_type_map)
            })
            .collect::<PyResult<Vec<FieldType>>>()?;
        Ok(Self::Tuple { items })
    }

    /// Extracts a Str or Date or Datetime from JSONSchema values basing on the
    /// pydantic reference https://pydantic-docs.helpmanual.io/usage/schema/#json-schema-types
    #[inline(always)]
    fn __extract_str_or_datetime_field(prop: &PyDict) -> PyResult<FieldType> {
        match prop.get_item("format") {
            None => Ok(Self::Str),
            Some(format) => {
                let format = format.to_string();
                match format.as_str() {
                    "date-time" => Ok(Self::Datetime),
                    "date" => Ok(Self::Date),
                    _ => Ok(Self::Str),
                }
            }
        }
    }
}
