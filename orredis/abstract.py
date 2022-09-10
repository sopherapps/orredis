"""
Module containing the abstract classes.
These ease when cross-referring classes that have cyclic dependencies
"""
from typing import Optional, Dict, Any, Union, List

from pydantic.main import BaseModel

from orredis.config import RedisConfig
from orredis.connection import Connection


class _AbstractStore(BaseModel):
    """
    An abstract class of a store
    """
    name: str
    redis_config: RedisConfig
    connection: Optional[Connection] = None
    life_span_in_seconds: Optional[int] = None

    class Config:
        arbitrary_types_allowed = True
        orm_mode = True

    def clear(self):
        """Clears all the keys attached to this redis store"""
        with self.connection as redis_store:
            redis_store.flushall()


class _AbstractModel(BaseModel):
    """
    An abstract class to help with typings for Model class
    """
    _store: _AbstractStore
    _primary_key_field: str

    @classmethod
    def get_primary_key_field(cls):
        """Gets the protected _primary_key_field"""
        return cls._primary_key_field

    @classmethod
    def insert(cls, data: Union[List[Any], Any]):
        raise NotImplementedError("insert should be implemented")

    @classmethod
    def update(cls, primary_key_value: Union[Any, Dict[str, Any]], data: Dict[str, Any]):
        raise NotImplementedError("update should be implemented")

    @classmethod
    def delete(cls, primary_key_value: Union[Any, Dict[str, Any]]):
        raise NotImplementedError("delete should be implemented")

    @classmethod
    def select(cls, columns: Optional[List[str]] = None):
        """Should later allow AND, OR"""
        raise NotImplementedError("select should be implemented")

    class Config:
        arbitrary_types_allowed = True
