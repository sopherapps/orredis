from pydantic import BaseModel as PydanticBaseModel
from .abstract import BaseModel


class PydanticModel(BaseModel, PydanticBaseModel):
    """
    A base model to be used for creating models for Redis based on pydantic
    Import this and inherit from it
    """
    pass
