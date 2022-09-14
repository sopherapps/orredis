extern crate redis;

use std::collections::hash_map;
use std::collections::HashMap;

use pyo3::exceptions::{PyAttributeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

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
                let _data: HashMap<String, Py<PyAny>> = k.extract()?;
                Ok(Model { _data })
            },
        )
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
        self._data.insert(name, value).unwrap();
        Ok(())
    }

    pub fn __delattr__(&mut self, name: String) -> PyResult<()> {
        self._data.remove(&name).unwrap();
        Ok(())
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
}

impl IntoIterator for Model {
    type Item = (String, Py<PyAny>);
    type IntoIter = hash_map::IntoIter<String, Py<PyAny>>;

    fn into_iter(self) -> Self::IntoIter {
        self._data.into_iter()
    }
}

/// Class that holds the meta for each given Model
pub struct ModelMeta {
    pub(crate) fields: HashMap<String, Py<PyAny>>,
    pub(crate) primary_key_field: String,
}

impl ModelMeta {
    pub fn new(model_type: &PyType) -> PyResult<Self> {
        if is_model(model_type)? {
            Ok(ModelMeta {
                fields: model_type.call_method0("get_fields")?.extract()?,
                primary_key_field: model_type
                    .call_method0("get_primary_key_field")?
                    .extract()?,
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
