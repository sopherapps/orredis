from typing import Optional, Type, List, Dict, Any

from .abstract import Model


class Collection:
    """
    The Collection represents a group of similar records within redis
    e.g. records of model Car will be in the Car collection.
    This has all the methods for manipulating data as well as retrieving it
    from redis
    """

    def add_one(self, item: Model, ttl: Optional[int]) -> None:
        """
        Adds a single Model instance to the collection. The model instance should
        be of the same type as the model used to get this collection.

        :param item: the model object to add
        :param ttl: the optional time-to-live for this item in redis; default: None (i.e. never expires).
                    if however, the default_ttl was set on the store, it will default to that
        """

    def add_many(self, items: List[Model], ttl: Optional[int]) -> None:
        """
        Adds a list of Model instances into this collection. The model instances should
        be of the same type as the model used to get this collection.

        :param items: the list of model objects to add
        :param ttl: the optional time-to-live for all these items in redis; default: None (i.e. never expires)
                    if however, the default_ttl was set on the store, it will default to that
        """

    def update_one(self, id: str, data: Dict[str, Any], ttl: Optional[int]) -> None:
        """
        Updates the model instance in redis that has the given id

        :param id: the id of the record to update
        :param data: the new changes to add to the record
        :param ttl: the optional time-to-live for all these items in redis; default: None (i.e. never expires)
                    if however, the default_ttl was set on the store, it will default to that
        """

    def get_one(self, id: str) -> Model:
        """
        Retrieves one record of the given id or None if it does not exist

        :param id: the id of the model record to return
        :return: the model object for the given id in this collection
        """

    def get_many(self, ids: List[str]) -> List[Model]:
        """
        Retrieves a list of records in this collection corresponding to the ids passed

        :param ids: the list of ids whose records are to be returned
        :return: the list of model objects that correspond to the list of ids. Non-existent ids
               are simply skipped
        """

    def get_all(self) -> List[Model]:
        """
        Retrieves a list of all records in this collection at ago

        :return: the list of model objects in this collection
        """

    def get_one_partially(self, id: str, fields: List[str]) -> Dict[str, Any]:
        """
        Retrieves a dictionary containing the provided fields from the record of the given id

        :param id: the id of the record whose data is to be retrieved
        :param fields: the list of fields to be returned in the data
        :return: the dict with the given fields as keys and the values got from the record of the given id
        """

    def get_many_partially(self, ids: List[str], fields: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieves a list of dictionaries for records of the given ids,
        only returning the specified fields for each record

        :param ids: the list of ids of the records to be queried
        :param fields: the fields to be returned in each item
        :return: the list of dicts, each with the given fields as keys and the values for each record returned.
               non-existent ids are ignored
        """

    def get_all_partially(self, fields: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieves a list of dictionaries for all records in the store,
        only returning the specified fields for each record

        :param fields: the fields to be returned in each item
        :return: the list of dicts, each with the given fields as keys and the values for each record returned
        """

    def delete_many(self, ids: List[str]) -> None:
        """
        Removes all records belonging to the given ids

        :param ids: the ids of the records to be removed
        """

class AsyncCollection:
    """
    The AsyncCollection represents a group of similar records within redis
    e.g. records of model Car will be in the Car collection.
    This has all the methods for manipulating data as well as retrieving it
    from redis but asynchronously. For the synchronous API, use Collection
    """

    async def add_one(self, item: Model, ttl: Optional[int]) -> None:
        """
        Adds a single Model instance to the collection. The model instance should
        be of the same type as the model used to get this collection.

        :param item: the model object to add
        :param ttl: the optional time-to-live for this item in redis; default: None (i.e. never expires).
                    if however, the default_ttl was set on the store, it will default to that
        """

    async def add_many(self, items: List[Model], ttl: Optional[int]) -> None:
        """
        Adds a list of Model instances into this collection. The model instances should
        be of the same type as the model used to get this collection.

        :param items: the list of model objects to add
        :param ttl: the optional time-to-live for all these items in redis; default: None (i.e. never expires)
                    if however, the default_ttl was set on the store, it will default to that
        """

    async def update_one(self, id: str, data: Dict[str, Any], ttl: Optional[int]) -> None:
        """
        Updates the model instance in redis that has the given id

        :param id: the id of the record to update
        :param data: the new changes to add to the record
        :param ttl: the optional time-to-live for all these items in redis; default: None (i.e. never expires)
                    if however, the default_ttl was set on the store, it will default to that
        """

    async def get_one(self, id: str) -> Model:
        """
        Retrieves one record of the given id or None if it does not exist

        :param id: the id of the model record to return
        :return: the model object for the given id in this collection
        """

    async def get_many(self, ids: List[str]) -> List[Model]:
        """
        Retrieves a list of records in this collection corresponding to the ids passed

        :param ids: the list of ids whose records are to be returned
        :return: the list of model objects that correspond to the list of ids. Non-existent ids
               are simply skipped
        """

    async def get_all(self) -> List[Model]:
        """
        Retrieves a list of all records in this collection at ago

        :return: the list of model objects in this collection
        """

    async def get_one_partially(self, id: str, fields: List[str]) -> Dict[str, Any]:
        """
        Retrieves a dictionary containing the provided fields from the record of the given id

        :param id: the id of the record whose data is to be retrieved
        :param fields: the list of fields to be returned in the data
        :return: the dict with the given fields as keys and the values got from the record of the given id
        """

    async def get_many_partially(self, ids: List[str], fields: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieves a list of dictionaries for records of the given ids,
        only returning the specified fields for each record

        :param ids: the list of ids of the records to be queried
        :param fields: the fields to be returned in each item
        :return: the list of dicts, each with the given fields as keys and the values for each record returned.
               non-existent ids are ignored
        """

    async def get_all_partially(self, fields: List[str]) -> List[Dict[str, Any]]:
        """
        Retrieves a list of dictionaries for all records in the store,
        only returning the specified fields for each record

        :param fields: the fields to be returned in each item
        :return: the list of dicts, each with the given fields as keys and the values for each record returned
        """

    async def delete_many(self, ids: List[str]) -> None:
        """
        Removes all records belonging to the given ids

        :param ids: the ids of the records to be removed
        """

class Store:
    """
    The Store containing all collections that are stored in redis.

    :param url: the redis url e.g. redis://localhost:6379/0
    :param pool_size: the maximum number of connections in the connection pool to redis; default: 5
    :param default_ttl: the default time-to-live for each record in milliseconds; default: None i.e. no expiry
    :param timeout: the time in milliseconds beyond which a timeout error is raised on failure to
                    get a connection to redis from the connection pool; default is 30000 (30 seconds)
    :param max_lifetime: the maximum lifetime in milliseconds connections in the pool; default is 1800000 (30 minutes)
    """

    def __init__(self,
                 url: str,
                 pool_size: int,
                 default_ttl: Optional[int],
                 timeout: Optional[int],
                 max_lifetime: Optional[int]) -> None: ...

    def clear(self, asynchronous: bool = False) -> None:
        """
        Removes all records in the redis store

        :param asynchronous: whether the FLUSHALL should be done asynchronously or synchronously. default: False
        """

    def create_collection(self,
                          model: Type[Model],
                          primary_key_field: str) -> None:
        """
        Creates a new Collection within the store for the given model supplied

        :param model: the Model schema to be used for this collection
        :param primary_key_field: the field that contains the unique primary key for each model instance e.g.
                                a book's primary key might be its ISBN
        """

    def get_collection(self, model: Type[Model]) -> Collection:
        """
        Retrieves a handle on the collection for a given model to manipulate the data within or
        query it

        :param model: the Model schema whose collection is to be retrieved
        :return: the collection instance to be used to manipulate data or query it using collection.add_one() etc.
        """


class AsyncStore:
    """
    The AsyncStore containing all async_collections that are stored in redis. It is meant to be used
    where async-await is used

    :param url: the redis url e.g. redis://localhost:6379/0
    :param pool_size: the maximum number of connections in the connection pool to redis; default: 5
    :param default_ttl: the default time-to-live for each record in milliseconds; default: None i.e. no expiry
    :param timeout: the time in milliseconds beyond which a timeout error is raised on failure to
                    get a connection to redis from the connection pool; default is 30000 (30 seconds)
    :param max_lifetime: the maximum lifetime in milliseconds connections in the pool; default is 1800000 (30 minutes)
    """

    def __init__(self,
                 url: str,
                 pool_size: int,
                 default_ttl: Optional[int],
                 timeout: Optional[int],
                 max_lifetime: Optional[int]) -> None: ...

    async def clear(self, asynchronous: bool = False) -> None:
        """
        Removes all records in the redis store

        :param asynchronous: whether the FLUSHALL should be done asynchronously or synchronously. default: False
        """

    def create_collection(self,
                          model: Type[Model],
                          primary_key_field: str) -> None:
        """
        Creates a new Collection within the store for the given model supplied

        :param model: the Model schema to be used for this collection
        :param primary_key_field: the field that contains the unique primary key for each model instance e.g.
                                a book's primary key might be its ISBN
        """

    def get_collection(self, model: Type[Model]) -> AsyncCollection:
        """
        Retrieves a handle on the collection for a given model to manipulate the data within or
        query it

        :param model: the Model schema whose collection is to be retrieved
        :return: the collection instance to be used to manipulate data or query it using collection.add_one() etc.
        """
