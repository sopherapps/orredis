"""Module with abstract classes for orredis"""
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
