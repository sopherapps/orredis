extern crate redis;

use std::collections::hash_map;
use std::collections::HashMap;

use pyo3::exceptions::{PyAttributeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyBool, PyDict, PyType};

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

    pub fn overwrite(&mut self, kwargs: &PyDict) -> PyResult<()> {
        let _data: HashMap<String, Py<PyAny>> = kwargs.extract()?;
        self._data = _data;
        Ok(())
    }

    #[classmethod]
    fn set_default_values(cls: &PyType, instance: Py<PyAny>) -> PyResult<()> {
        Python::with_gil(|py| {
            let defaults = match cls.getattr("__defaults") {
                Ok(defaults) => defaults.extract::<HashMap<String, Py<PyAny>>>(),
                Err(_) => {
                    let mut defaults: HashMap<String, Py<PyAny>> = Default::default();
                    let fields = cls.getattr("get_fields")?.call0()?;
                    let fields: HashMap<String, Py<PyAny>> = fields.extract()?;

                    for (key, _) in fields {
                        if let Ok(value) = cls.getattr(&key) {
                            defaults.insert(key.clone(), value.into_py(py));
                        }

                        // delete the field from the class so that __getattribute__()
                        // may not keep skipping our custom __getattr__()
                        cls.delattr(&key).ok();
                    }

                    // update the __defaults attribute on the class
                    cls.setattr("__defaults", defaults.clone())?;

                    Ok(defaults)
                }
            }?;

            let instance = instance.as_ref(py);
            let mut data: HashMap<String, Py<PyAny>> =
                instance.getattr("dict")?.call0()?.extract()?;

            for (key, value) in defaults {
                if let None = data.get(&key) {
                    data.insert(key, value);
                }
            }

            let dict: &PyDict = data.into_py_dict(py);
            instance.getattr("overwrite")?.call1((dict,))?;

            Ok(())
        })
    }

    #[classmethod]
    fn get_fields(cls: &PyType) -> PyResult<HashMap<String, Py<PyAny>>> {
        Python::with_gil(|py| -> PyResult<HashMap<String, Py<PyAny>>> {
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

    pub fn __getattr__(&self, name: &str) -> PyResult<Py<PyAny>> {
        let result = self._data.get(name);
        match result {
            Some(v) => Ok(v.clone()),
            None => Err(PyAttributeError::new_err(format!("{}", name))),
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
            let other_type = other.as_ref(py).get_type().name()?;
            let other_model: Model = other.extract(py)?;
            let this_data = slf._data.clone();
            let other_data = other_model._data;
            let instance = slf.into_py(py);
            let instance = instance.as_ref(py);
            let this_type = instance.get_type().name()?;
            let fields = instance.getattr("get_fields")?.call0()?;
            let fields: HashMap<String, Py<PyAny>> = fields.extract()?;

            if this_type != other_type {
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
        Python::with_gil(|py| -> PyResult<String> {
            let value = value.as_ref(py).get_type();
            let name: String = value.getattr("__name__")?.extract()?;
            let name = name.to_lowercase();
            Ok(name)
        })
    }

    /// This converts the model to the native class by consuming the given pointer
    pub(crate) fn to_subclass_instance(self, model_type: &Py<PyType>) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| -> PyResult<Py<PyAny>> {
            let dict = self.dict()?;
            let dict = dict.into_py_dict(py);
            model_type.call(py, (), Some(dict))
        })
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
}

impl ModelMeta {
    pub fn new(model_type: &PyType) -> PyResult<Self> {
        if is_model(model_type)? {
            Ok(ModelMeta {
                fields: model_type.call_method0("get_fields")?.extract()?,
                primary_key_field: model_type
                    .call_method0("get_primary_key_field")?
                    .extract()?,
                model_type: Py::from(model_type),
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
        let sample_model = Model::empty();
        let sample_model = sample_model.into_py(py);
        let sample_model = sample_model.as_ref(py);
        let ref_model_type = sample_model.get_type();
        let is_model: bool = builtins
            .getattr("issubclass")?
            .call((model_type, ref_model_type), None)?
            .extract()?;
        Ok(is_model)
    })
}
