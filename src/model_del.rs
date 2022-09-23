extern crate redis;

use std::collections::hash_map;
use std::collections::HashMap;
use std::fmt::format;

use pyo3::exceptions::{PyAttributeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyType};

/// The Model is a schema for each record to be saved in a given collection in redis
#[derive(Clone)]
#[pyclass(subclass, dict)]
pub struct Model {
    pub(crate) _data: HashMap<String, Py<PyAny>>,
}

#[pymethods]
impl Model {
    #[new]
    #[args(kwargs = "**")]
    pub fn new(kwargs: Option<&PyDict>) -> PyResult<Self> {
        kwargs.map_or(
            Ok(Model {
                _data: Default::default(),
            }),
            |k| {
                let mut _data: HashMap<String, Py<PyAny>> = k.extract()?;
                Ok(Model { _data })
            },
        )
    }

    #[classmethod]
    fn get_fields(cls: &PyType) -> PyResult<HashMap<String, Py<PyAny>>> {
        Python::with_gil(|py| -> PyResult<HashMap<String, Py<PyAny>>> {
            // FIXME: Try to use cls.__annotations__ here instead
            let typing = PyModule::import(py, "typing")?;
            let field_types: HashMap<String, Py<PyAny>> =
                typing.getattr("get_type_hints")?.call1((cls,))?.extract()?;
            let public_fields: HashMap<String, Py<PyAny>> = field_types
                .into_iter()
                .filter(|(k, _)| !k.starts_with("_"))
                .collect();
            Ok(public_fields)
        })
    }

    // FIXME: Try to convert this to a class property
    #[classmethod]
    fn get_name(cls: &PyType) -> PyResult<String> {
        Python::with_gil(|_py| -> PyResult<String> {
            let name: String = cls.getattr("__name__")?.extract()?;
            let name = name.to_lowercase();
            Ok(name)
        })
    }

    #[classmethod]
    fn get_primary_key_field(cls: &PyType) -> PyResult<String> {
        match cls.getattr("_primary_key_field") {
            Ok(pk_field) => match pk_field.extract::<String>() {
                Ok(pk_field) => Ok(pk_field),
                Err(_) => Err(PyValueError::new_err("_primary_key_field must be a string")),
            },
            Err(_) => Err(PyAttributeError::new_err(
                "should have a _primary_key_field",
            )),
        }
    }

    pub fn dict(&self) -> PyResult<HashMap<String, Py<PyAny>>> {
        Ok(self._data.clone())
    }

    pub fn __getattr__(slf: PyRef<'_, Self>, name: &str) -> PyResult<Py<PyAny>> {
        let result = slf._data.get(name);
        match result {
            Some(v) => Ok(v.clone()),
            None => Python::with_gil(|py| -> PyResult<Py<PyAny>> {
                let defaults = slf.into_py(py).getattr(py, "__defaults");

                match defaults {
                    Ok(defaults) => {
                        let defaults: HashMap<String, Py<PyAny>> = defaults.extract(py)?;
                        defaults
                            .get(name)
                            .ok_or(PyAttributeError::new_err(format!("{}", name)))
                            .and_then(|v| Ok(v.clone()))
                    }
                    Err(_) => Err(PyAttributeError::new_err(format!("{}", name))),
                }
            }),
        }
    }

    pub fn __setattr__(&mut self, name: String, value: Py<PyAny>) -> PyResult<()> {
        self._data.insert(name, value);
        Ok(())
    }

    pub fn __delattr__(&mut self, name: String) -> PyResult<()> {
        self._data.remove(&name);
        Ok(())
    }

    pub fn __eq__(slf: PyRef<'_, Self>, other: Py<PyAny>) -> PyResult<bool> {
        Python::with_gil(|py| -> PyResult<bool> {
            let other = other.into_py(py);
            let other_type = other.as_ref(py).get_type();
            let other_type_name = other_type.name()?;
            let other_model: Model = other.extract(py)?;
            let mut this_data = slf._data.clone();
            let mut other_data = other_model._data;
            let instance = slf.into_py(py);
            let instance = instance.as_ref(py);
            let this_type = instance.get_type();
            let this_type_name = this_type.name()?;
            let fields = this_type.getattr("__annotations__")?;
            let fields: HashMap<String, Py<PyAny>> = fields.extract()?;

            let default_values = this_type.getattr("__defaults").unwrap_or(PyDict::new(py));
            let default_values: HashMap<String, Py<PyAny>> = default_values.extract()?;

            for (k, v) in &default_values {
                if let None = this_data.get(k) {
                    this_data.insert(k.to_string(), v.clone());
                }

                if let None = other_data.get(k) {
                    other_data.insert(k.to_string(), v.clone());
                }
            }

            if this_type_name != other_type_name {
                return Ok(false);
            }

            for (k, _) in fields {
                let this_value = this_data.get(&k);
                if let Some(other) = &other_data.get(&k) {
                    // if value exists in other_data, check if it equal to the value in this_data
                    match this_value {
                        // if value does not exist on this, then this_data and other_data are not equal
                        None => return Ok(false),
                        Some(v) => {
                            let v = v.into_py(py);
                            let other = other.into_py(py);
                            let v_eq_method = v.getattr(py, "__eq__")?;

                            let bool_value = match v_eq_method.call1(py, (&other,)) {
                                Ok(value) => {
                                    let value_as_str = value.to_string().to_lowercase();
                                    if value_as_str == "notimplemented" {
                                        let other_eq_method = other.getattr(py, "__eq__")?;
                                        match other_eq_method.call1(py, (&v,)) {
                                            Ok(value) => value,
                                            Err(_) => PyBool::new(py, false).into_py(py),
                                        }
                                    } else {
                                        value
                                    }
                                }
                                Err(e) => Err(e)?,
                            };

                            let bool_value: bool = bool_value.extract(py)?;

                            if !bool_value {
                                return Ok(bool_value);
                            }
                        }
                    }
                } else if let Some(_) = this_value {
                    // if value does not exist on other_data but exists on this_data, the two are not equal
                    return Ok(false);
                }
            }

            Ok(true)
        })
    }

    pub fn __str__(slf: PyRef<'_, Self>) -> PyResult<String> {
        Python::with_gil(|py| -> PyResult<String> {
            let dict = slf.dict()?;
            let instance = slf.into_py(py);
            let instance = instance.as_ref(py);
            let instance = Py::from(instance);
            Ok(format!(
                "{} {:?}",
                Self::get_instance_model_name(instance)?,
                &dict
            ))
        })
    }
}

impl Model {
    pub fn empty() -> Self {
        Model {
            _data: Default::default(),
        }
    }

    pub fn get(&self, name: &str) -> PyResult<Py<PyAny>> {
        let result = self._data.get(name);
        match result {
            Some(v) => Ok(v.clone()),
            None => Err(PyAttributeError::new_err(format!("{}", name))),
        }
    }

    pub(crate) fn get_instance_model_name(value: Py<PyAny>) -> PyResult<String> {
        let name = Python::with_gil(|py| {
            let value = value.as_ref(py).get_type();
            value.getattr("__name__")?.extract::<String>()
        })?;
        let name = name.to_lowercase();
        Ok(name)
    }

    pub fn set_default_values(
        &mut self,
        default_values: &HashMap<String, Py<PyAny>>,
    ) -> PyResult<()> {
        for (k, v) in default_values {
            if let None = self._data.get(k) {
                self._data.insert(k.to_string(), v.clone());
            }
        }

        Ok(())
    }
}

impl IntoIterator for Model {
    type Item = (String, Py<PyAny>);
    type IntoIter = hash_map::IntoIter<String, Py<PyAny>>;

    fn into_iter(self) -> Self::IntoIter {
        self._data.into_iter()
    }
}

/// Class that holds the meta for each given Model
#[derive(Clone)]
pub struct ModelMeta {
    pub(crate) fields: HashMap<String, Py<PyAny>>,
    pub(crate) primary_key_field: String,
    pub(crate) model_type: Py<PyType>,
    pub(crate) nested_fields: Vec<String>,
    pub(crate) default_values: HashMap<String, Py<PyAny>>,
}

impl ModelMeta {
    pub fn new(model_type: &PyType) -> PyResult<Self> {
        if is_model(model_type)? {
            let fields: HashMap<String, Py<PyAny>> =
                model_type.getattr("__annotations__")?.extract()?;
            let mut nested_fields: Vec<String> = Default::default();
            let default_values = model_type.getattr("__defaults");
            let mut default_values = match default_values {
                Ok(values) => values.extract::<HashMap<String, Py<PyAny>>>()?,
                Err(_) => Default::default(),
            };

            for (k, v) in &fields {
                Python::with_gil(|py| -> PyResult<()> {
                    let v = v.into_py(py);
                    let field_type = v.as_ref(py).downcast::<PyType>();
                    match field_type {
                        Ok(field_type) => {
                            if is_model(field_type)? {
                                nested_fields.push(k.to_string())
                            }
                        }
                        Err(_) => {}
                    }

                    // Add default values to the meta, and delete them from the Model class
                    if !k.starts_with("_") {
                        match model_type.getattr(k) {
                            Ok(default_value) => {
                                // delete the attribute from the class to avoid confusing __getattr__
                                model_type.delattr(k)?;
                                default_values.insert(k.to_string(), Py::from(default_value))
                            }
                            Err(_) => None,
                        };
                    }

                    Ok(())
                })?;
            }

            model_type.setattr("__defaults", default_values.clone())?;

            Ok(ModelMeta {
                fields,
                primary_key_field: model_type
                    .call_method0("get_primary_key_field")?
                    .extract()?,
                model_type: Py::from(model_type),
                nested_fields,
                default_values,
            })
        } else {
            Err(PyValueError::new_err(format!(
                "{} is not of type Model",
                model_type
            )))
        }
    }
}

fn is_model(model_type: &PyType) -> PyResult<bool> {
    Python::with_gil(|py| -> PyResult<bool> {
        let builtins = PyModule::import(py, "builtins")?;
        let model_type_ref = PyType::new::<Model>(py);
        let is_model: bool = builtins
            .getattr("issubclass")?
            .call((model_type, model_type_ref), None)?
            .extract()?;
        Ok(is_model)
    })
}
