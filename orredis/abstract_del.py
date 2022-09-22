"""
Module containing the abstract classes.
These ease when cross-referring classes that have cyclic dependencies
"""
from typing import Optional, Dict, Any, Union, List

from orredis.orredis import Store, Model


class BaseModel(Model):
    """
    An abstract class to help with typings for Model class
    """
    __defaults: Dict[str, Any] = {}
    _store: Store
    _primary_key_field: str
    _life_span: Optional[int] = None

    @classmethod
    def insert(cls, data: Union[List["BaseModel"], "BaseModel"], life_span_seconds: Optional[float] = None):
        life_span = life_span_seconds if life_span_seconds is not None else cls._life_span
        if isinstance(data, list):
            return cls._store.insert_many(model_name=cls.get_name(), data=data, life_span=life_span)
        elif isinstance(data, cls):
            return cls._store.insert_one(model_name=cls.get_name(), data=data, life_span=life_span)
        raise ValueError("data should be a list of models or a single model instance")

    @classmethod
    def update(cls, _id: Any, data: Dict[str, Any],
               life_span_seconds: Optional[float] = None):
        life_span = life_span_seconds if life_span_seconds is not None else cls._life_span
        return cls._store.update_one(model_name=cls.get_name(), id=_id, data=data, life_span=life_span, )

    @classmethod
    def delete(cls, ids: Union[Any, List[Any]]):
        if isinstance(ids, list):
            return cls._store.delete_many(model_name=cls.get_name(), ids=ids)
        elif ids is not None:
            return cls._store.delete_one(model_name=cls.get_name(), id=ids)
        raise ValueError("ids should be either a list or any non-None value")

    @classmethod
    def select(cls, columns: Optional[List[str]] = None, ids: Optional[Union[List[Any], Any]] = None) -> Optional[
        Union["BaseModel", List["BaseModel"]]]:
        if ids is None:
            if columns is None:
                return cls._store.find_all(model_name=cls.get_name())
            elif isinstance(columns, list):
                return cls._store.find_partial_all(model_name=cls.get_name(), columns=columns)
        elif isinstance(ids, list):
            if columns is None:
                return cls._store.find_many(model_name=cls.get_name(), ids=ids)
            elif isinstance(columns, list):
                return cls._store.find_partial_many(model_name=cls.get_name(), columns=columns, ids=ids)
        else:
            if columns is None:
                return cls._store.find_one(model_name=cls.get_name(), id=ids)
            elif isinstance(columns, list):
                return cls._store.find_partial_one(model_name=cls.get_name(), columns=columns, id=ids)

    def __str__(self) -> str:
        """String representation of the object"""
        return f"{self.__class__.__qualname__} {self.dict()}"
