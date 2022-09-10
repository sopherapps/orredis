"""module containing the specification for an orredis model"""
import typing
from typing import Union, List, Any, Dict, Optional

from orredis.abstract import _AbstractModel


class Model(_AbstractModel):
    """
    A specification for records stored in a redis store.
    Inherit this to create specific collections within the store.
    Each collection will have the lower case name of the model class
    e.g. Author will have "author" collection in the redis store.
    """

    @classmethod
    def __get_table_name(cls):
        """Returns the name of the table or collection"""
        return cls.__name__.lower()

    @classmethod
    def __get_primary_key(cls, primary_key_value: Any):
        """
        Returns the primary key value concatenated to the table name for uniqueness
        """
        table_name = cls.__name__.lower()
        return f"{table_name}_%&_{primary_key_value}"

    @classmethod
    def insert(cls, data: Union[List[_AbstractModel], _AbstractModel], life_span_seconds: Optional[float] = None):
        """
        Inserts a given row or sets of rows into the table
        """
        life_span = life_span_seconds if life_span_seconds is not None else cls._store.life_span_in_seconds
        table = cls.__get_table_name()
        key_field = cls.get_primary_key_field()

        with cls._store.connection as store:

            if isinstance(data, list):
                data_list = [record.dict() for record in data]
                return store.insert_dict_list(
                    table=table,
                    key_field=key_field,
                    data=data_list,
                    life_span=life_span,
                )
            elif isinstance(data, _AbstractModel):
                return store.insert_dict(
                    table=table,
                    key_field=key_field,
                    data=data.dict(),
                    life_span=life_span,
                )
            else:
                raise ValueError(f"data should be a model or a list of models not {type(data)}")

    @classmethod
    def update(cls, _id: Any, data: Dict[str, Any],
               life_span_seconds: Optional[float] = None):
        """
        Updates a given row or sets of rows in the table
        """
        life_span = life_span_seconds if life_span_seconds is not None else cls._store.life_span_in_seconds
        table = cls.__get_table_name()

        with cls._store.connection as store:
            if not isinstance(data, dict):
                raise ValueError(f"data should be a dict not {type(data)}")

            return store.update(
                table=table,
                key=_id,
                data=data,
                life_span=life_span,
            )

    @classmethod
    def delete(cls, ids: Union[Any, List[Any]]):
        """
        deletes a given row or sets of rows in the table
        """
        table = cls.__get_table_name()
        primary_keys = []
        if isinstance(ids, list):
            primary_keys = ids
        elif ids is not None:
            primary_keys = [ids]

        with cls._store.connection as store:
            return store.delete(
                table=table, ids=primary_keys
            )

    @classmethod
    def select(cls, columns: Optional[List[str]] = None, ids: Optional[List[Any]] = None):
        """
        Selects given rows or sets of rows in the table
        """
        table = cls.__get_table_name()
        nested_columns = cls.__get_nested_columns(columns)

        with cls._store.connection as store:
            response = store.select(
                table=table,
                ids=ids,
                columns=columns,
                nested_columns=nested_columns
            )

        if columns:
            return cls.__parse_partially(response)
        else:
            return [cls(**item) for item in response]

    @classmethod
    def __get_nested_columns(cls, columns: Optional[List[str]]) -> Dict[str, str]:
        """
        Gets the map of nested column name to collection name
        if columns is None, it returns all nested columns in the model
        """
        field_types = typing.get_type_hints(cls)
        nested_columns = {}

        if columns is None:
            for field, field_type in field_types.items():
                if isinstance(field_type, type(Model)):
                    nested_columns[field] = field_type.__class__.__name__

        else:
            for col in columns:
                field_type = field_types.get(col, str)
                if isinstance(field_type, type(Model)):
                    nested_columns[col] = field_type.__class__.__name__

        return nested_columns

    @classmethod
    def __parse_partially(cls, data: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """
        Parses a list of dicts of <str,str> to a list of dicts <str,Any> to match data types of cls
        It parses the kind of items that would not pass validation with the base model due to missing
        required fields
        """
        field_types = typing.get_type_hints(cls)
        parsed_data = []

        for item in data:
            new_value = {}
            for k, v in item.items():
                new_value[k] = field_types.get(k, str)(v)

            parsed_data.append(new_value)
        return parsed_data
