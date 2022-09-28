"""Module with abstract classes for orredis"""
from datetime import datetime, timezone
from typing import Dict, Any

from pydantic import BaseModel


class Model(BaseModel):
    """Base model for all models that can be saved and retrieved from redis using orredis"""

    def with_changes(self, changes: Dict[str, Any]) -> "Model":
        """
        Creates a new instance of Model basing on the properties the current one has
        but with modifications using changes
        """
        return self.__class__(**{**self.dict(), **changes})

    def __eq__(self, other):
        datetime_fields = self.__get_datetime_fields()
        if len(datetime_fields) == 0:
            return super().__eq__(other=other)
        else:
            return self.__eq_with_datetime_fields(datetime_fields=datetime_fields, other=other)

    def __get_datetime_fields(self):
        """Retrieves the fields that have datetime as their field type"""
        datetime_fields = getattr(self.__class__, "__datetime_fields", None)

        if datetime_fields is None:
            datetime_fields = {}
            for (k, v) in self.__fields__.items():
                try:
                    if issubclass(v, datetime):
                        datetime_fields[k] = True
                except:
                    pass

            setattr(self.__class__, "__datetime_fields", datetime_fields)

        return datetime_fields

    def __eq_with_datetime_fields(self, datetime_fields, other):
        """
        compares the self to other to determine equality if datetime_fields are present
        since timezone-aware fields seem to cause wrong behaviour

        :param datetime_fields: the fields that have datetime as their type
        :param other: the model or dict to compare self to
        :return: whether self is equal to other
        """
        current_dict = self.dict()
        other_dict = other.dict() if isinstance(other, BaseModel) else other

        if len(current_dict) != len(other_dict):
            return False

        for key in current_dict:
            try:
                current_item = current_dict[key]
                other_item = other_dict[key]

                if datetime_fields.get(key, False) and current_item is not None:
                    # compare the values from the two dicts, with both shifted to UTC.
                    # any error here will be taken as the two instances are not equal
                    # see the except-branch
                    if current_item.astimezone(timezone.utc) != other_item.astimezone(timezone.utc):
                        return False
                elif current_item != other_item:
                    return False
            except:
                # in case of any error, return False.
                return False

        return True
