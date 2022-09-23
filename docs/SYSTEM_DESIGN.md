# System Design

## Behaviour

This is a description of what exactly is happening under the hood.

- When a store is initialized, a rust struct called `Store` is instantiated. It has python bindings thanks
  to [pyo3](https://pyo3.rs/)
    - That store instance has the following fields
        - `collections_meta` - a hashmap with the metadata for each collection including `default_ttl`, `schema`
          , `model_class`, `nested_fields`, `primary_key_field` etc.
        - `pool` - an [r2d2](https://github.com/sfackler/r2d2) pool for [redis-rs](https://github.com/redis-rs/redis-rs)
          connections
- The `store.create_collection()` method does the following:
    - It receives the [pydantic](https://pydantic-docs.helpmanual.io/) `model` argument passed to it and
      calls [`model.schema()`](https://pydantic-docs.helpmanual.io/usage/schema/) on it in order to get
      the [JSONSchema](https://json-schema.org/) representation of that model
    - It saves the schema in hashmap form in the `collections_meta` hashmap on the store. The key associated with this
      schema is the model's `model.__qualname__`
    - It also updates the other properties of that collection on that hashmap including `default_ttl`
      , `primary_key_field`, `nested_fields` etc.
    - The 'nested_fields' are obtained from the schema itself from 'properties' that have `'$ref'` in them.
      **Note: Only one-level of nesting will bring you eager-loading out of the box. Any extra levels will require you
      to make extra calls to the redis store**
- The `store.get_collection()` method does the following:
    - It receives the [pydantic](https://pydantic-docs.helpmanual.io/) `model` argument
    - It creates a new instance of the `Collection` rust struct (with python pyo3 bindings), passing it the
      collections_meta data corresponding to the `model`'s `__qualname`
    - It also clones the `pool` on the store and passes that clone to the `Collection` instance
- The `collection.add_one()` method does the following:
    - It receives an instance of a [pydantic](https://pydantic-docs.helpmanual.io/) `model` and retrieves all its fields
      as a hashmap of field name and field value
    - It generates a unique key basing on the `primary_key_field` of that collection and the `name` of that
      collection
    - If there are `nested_fields`, it generates the unique key for those also basing on the collection they are
      attached to. It then replaces the values for those fields with those unique keys.
    - The data that corresponded to the `nested_fields` keys is then put in a pipeline to be inserted into redis.
    - It then calls the [`HSET` command](https://redis.io/commands/hset/) of redis using a connection from the pool. The
      command is done for each `nested_field` and then finally for the model itself.
    - If there is a `ttl` argument passed to it, or if the `default_ttl` of the collection is not `None` then a call
      to [`EXPIRE`](https://redis.io/commands/expire/) is also made
- The `collection.add_many()` method does similar things as `collection.add_one()` except it does it for many model
  instances. It is more efficient than multiple `add_one()` calls due
  to [pipelining](https://redis.io/docs/manual/pipelining/).
- The `collection.get_one()` method does the following:
    - It generates a unique key basing on the first argument passed to it, the id
    - It then calls the [`HGETALL` command](https://redis.io/commands/hgetall/) with the given key.
    - If the collection has `nested_fields`, it pre-populates (eagerly loads) those using the `HGETALL` command since
      their values are unique keys (like foreign keys) corresponding to the nested model's hashmap in redis.
    - In order to be more efficient especially when models are nested, all these calls are made
      in [a lua script](../lua_scripts/select_all_fields_for_some_ids.lua) using [EVAL](https://redis.io/commands/eval/)
    - It then converts the value got from redis into a hashmap, using the `schema` that is found attached to the
      collection
    - Then the `model_class` is called, passing it the hashmap produced from the previous step as key-word arguments
      just like a [pydantic model](https://pydantic-docs.helpmanual.io/#example) expects.
    - The instance created is returned.
    - If the value got from redis is empty, a python `None` is returned
- The `collection.get_all()` method does the following:
    - It generates a unique pattern for this collection's keys basing on the `__qualname__` of the collection's model.
    - It then calls the [`HSCAN` command](https://redis.io/commands/hscan/) on redis, passing it the said pattern and
      filtering on only hash data types
    - The filtering is done using the [`TYPE` command](https://redis.io/commands/type/) on redis.
    - It then calls the [`HGETALL` command](https://redis.io/commands/hgetall/) with each key found.
    - If the collection has `nested_fields`, it pre-populates (eagerly loads) those using the `HGETALL` command since
      their values are unique keys (like foreign keys) corresponding to the nested model's hashmap in redis.
    - Since `HSCAN` returns a cursor, to retrieve all keys, we loop till the cursor returned from redis is '0' meaning
      no more data can be fetched.
    - In order to be more efficient, all these calls are made
      in [a lua script](../lua_scripts/select_all_fields_for_all_ids.lua) using [EVAL](https://redis.io/commands/eval/)
    - It then converts each record got from redis into a hashmap, using the `schema` that is found attached to the
      collection
    - Then the `model_class` is called for each record, passing it the hashmap produced from the previous step as
      key-word arguments just like a [pydantic model](https://pydantic-docs.helpmanual.io/#example) expects.
    - The instances created are returned as a list.
    - If the value got from redis is empty, an empty list is returned
- The `collection.get_many()` method does the what `collection.get_one()` does but for many ids in a batch more
  efficiently than multiple calls to `get_one` since one network request is made.
- The `collection.get_one_partially()` method does the following:
    - It generates a unique key basing on the first argument passed to it, the id
    - It then calls the [`HMGET` command](https://redis.io/commands/hmget/) passing it the key and the fields specified
      as the second argument
    - If the collection has `nested_fields` and some of those have been included in the list of fields passed to this
      method,
      it pre-populates (eagerly loads) those using the `HGETALL` command since their values are unique keys (like
      foreign keys) corresponding to the nested model's hashmap in redis.
    - In order to be more efficient, all these calls are made
      in [a lua script](../lua_scripts/select_some_fields_for_some_ids.lua)
      using [EVAL](https://redis.io/commands/eval/)
    - It then converts each record got from redis into a hashmap, using the `schema` that is found attached to the
      collection.
    - These hashmaps are converted into python dictionaries and returned.
    - If the value got from redis is empty, a python `None` is returned
- The `collection.get_all_partially()` method does the following:
    - It generates a unique pattern for this collection's keys basing on the `__qualname__` of the collection's model.
    - It then calls the [`HSCAN` command](https://redis.io/commands/hscan/) on redis, passing it the said pattern and
      filtering on only hash data types
    - The filtering is done using the [`TYPE` command](https://redis.io/commands/type/) on redis.
    - It then calls the [`HMGET` command](https://redis.io/commands/hmget/) for each key found, passing also the fields
      specified as the second argument
    - If the collection has `nested_fields` and some of those have been included in the list of fields passed to this
      method,
      it pre-populates (eagerly loads) those using the `HGETALL` command since their values are unique keys (like
      foreign keys) corresponding to the nested model's hashmap in redis.
    - Since `HSCAN` returns a cursor, to retrieve all keys, we loop till the cursor returned from redis is '0' meaning
      no more data can be fetched.
    - In order to be more efficient, all these calls are made
      in [a lua script](../lua_scripts/select_some_fields_for_all_ids.lua) using [EVAL](https://redis.io/commands/eval/)
    - It then converts each record got from redis into a hashmap, using the `schema` that is found attached to the
      collection
    - These hashmaps are converted into python dictionaries and returned.
    - If the value got from redis is empty, an empty list is returned.
- The `collection.get_many_partially()` method does the what `collection.get_one_partially()` does but for many ids in a
  batch.
  This is more efficient than multiple calls to `get_one_partially()` because only one network request is made.
- The `collection.update_one()` method does what `collection.add_one()` does except that its second argument is already
  a hashmap.
- The `collection.delete_many()` method does the following:
    - It receives the ids that are to be deleted and converts them to unique keys basing on the collection's
      s `name`.
    - It then calls the [`DEL` command](https://redis.io/commands/del/) on each of them
      in [a pipeline](https://redis.io/docs/manual/pipelining/)
    - That's it!

## Storage

- Each record is simply stored as a [hash](https://redis.io/docs/data-types/#hashes)
- In order to group hashes in collections, each hash key is automatically given a suffix that includes the collection's
  name
  for instance "Oliver Twist" which belongs to the "Book" collection becomes "Book_%&_Oliver Twist". This way if there
  is
  an "Oliver Twist" in the "User" collection, the latter will not be picked when "Oliver Twist" the book is queried for.
