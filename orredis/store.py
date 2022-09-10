"""module containing the specification for store"""
from typing import Optional, Dict, Any

from orredis.abstract import _AbstractStore
from orredis.config import RedisConfig
from orredis.connection import Connection
from orredis.model import Model


class Store(_AbstractStore):
    """
    An abstraction over the actual store in the redis database
    """
    models: Dict[str, type(Model)] = {}

    def __init__(
            self,
            name: str,
            redis_config: RedisConfig,
            connection: Optional[Connection] = None,
            life_span_in_seconds: Optional[int] = None,
            **data: Any
    ):
        super().__init__(
            name=name,
            redis_config=redis_config,
            connection=connection,
            life_span_in_seconds=life_span_in_seconds,
            **data)

        self.connection = Connection(url=redis_config.redis_url)

    def register_model(self, model_class: type(Model)):
        """Registers the model to this store"""
        if not isinstance(model_class.get_primary_key_field(), str):
            raise NotImplementedError(f"{model_class.__name__} should have a _primary_key_field")

        model_class._store = self
        self.models[model_class.__name__.lower()] = model_class

    def model(self, name: str) -> Model:
        """Gets a model by name: case-insensitive"""
        return self.models[name.lower()]
