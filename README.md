# orredis

A fast pydantic-based ORM for redis supporting synchronous and asynchronous interaction with pooled connections to
redis. It is written in rust but runs in python v3.7+

## Purpose

An object-relational-mapping makes writing business logic intuitive because the data representation is closer to what
the real-life situation is. It helps decouple the way such data is programmed from the way such data is actually
persisted
in any of the data persistence technologies we have, typically a database.

Take the example of a book.
In code, one will represent a book as an object with a number of properties such as "title", "edition", "author" etc.

```python
class Book(Model):
  title: str
  edition: int
  author: Author
```

However, in the underlying data store, the same book could be saved as say, a row in a table for a relational database
like PostgreSQL,
or as a document in a document-based NoSQL databases like MongoDB or as a hashmap in redis.
Of these, the document-based NoSQL databases are the closest to the definition in code.

For MongoDB, the same book might be represented as the object below:

```json
{
  "id": "some-random-string",
  "title": "the title of the book",
  "edition": 2,
  "author": {
    "name": "Charles Payne",
    "yearsActive": [
      1992,
      2008
    ]
  }
}
```

As you can see, it is still quite different.

However, for redis, the representation is even going to be further off.
It will most likely be saved as hashmap, with a given key. The properties of book will be 'fields' for that hashmap.

In order to interact with the book representation in the redis server, one has to write commands like:

```shell
# to save the book in the data store
HSET "some key" "title" "the title of the book" "edition" 2 "author" "{\"name\":\"Charles Payne\",\"yearsActive\":[1992,2008]}"
# to retrieve the entire book
HGETALL "some key"
# to retrieve just a few details of the book
HMGET "some key" "title" "edition"
# to update the book - see the confusion? are you saving a new book or updating one?
HSET "some key" "edition" 2
# to delete the book
DEL "some key"
```

The above is so unrelated to the business logic that most of us will take a number of minutes or hours trying to
understand what kind of data is even being saved. Is it a book or some random stuff?

Now consider something like this:

```python
book = Book(title="some title", edition=2, author=Author(name="Charles Payne", years_active=(1992, 2008)))
store = Store(url="redis://localhost:6379/0", pool_size=5, default_ttl=3000, timeout=1)
store.create_collection(model=Book, primary_key_field="title")
book_collection = store.get_collection(Book)
# Do I even need to add a comment here?
# to save books
book_collection.add_one(book)
book_collection.add_many([book, book.with_changes(edition=2), book.with_changes(title="another title")])
# to retrieve books
book_collection.get_one(id="some title")
book_collection.get_all()
book_collection.get_many(ids=["some title", "another title"])
# to get a few details of books (returns a dictionary)
book_collection.get_one_partially("some title", fields=["title", "edition"])
book_collection.get_all_partiallly(fields=["title", "edition"])
book_collection.get_many_partially(ids=["some title", "another title"], __fields=["title", "edition"])

# or to update
book_collection.update_one("some title", data={"edition": 1})
# or to delete
book_collection.delete_many(ids=["some title", "another title"])

# clear all data in store
store.clear()
```

Beautiful, isn't it?

Now imagine using all that, and getting the extra perk of your code running really really fast because it was
implemented
in rust, just for the fun of it.

Uh? You like?

That is the purpose of this project.

## Dependencies

- python +v3.7
- redis server (yes, you need have a redis server somewhere)
- [pydantic](https://github.com/samuelcolvin/pydantic/)

## Quick Start (Synchronous API)

- Install the package

  ```bash
  pip install orredis
  ```

- Import the `Store` and the `Model` classes and use accordingly

```python
from datetime import date
from typing import Tuple, List
from orredis import Model, Store


# type annotations are the schema. 
# Don't leave them out or you will just be getting strings for every property when you retrieve an object
class Author(Model):
  name: str
  active_years: Tuple[int, int]


class Book(Model):
  title: str
  author: Author
  rating: float
  published_on: date
  tags: List[str] = []
  in_stock: bool = True


class Library(Model):
  name: str
  address: str


# Create the store and add create a collection for each model
# - `default_ttl` is the default time to live for each record is the store. 
#   records never expire if there is no default_ttl set, and no `ttl` is given when adding that record to the store
# - `timeout` is the number of milliseconds beyond which the connection to redis will raise a timeout error if
#   it fails to establish a connection.
store = Store(url="redis://localhost:6379/0", pool_size=5, default_ttl=3000, timeout=1000)
# - `identifier_fields` are the properties on the model that uniquely identify a single record. They form an id.
store.create_collection(model=Author, primary_key_field="name")
store.create_collection(model=Book, primary_key_field="title")
store.create_collection(model=Library, primary_key_field="name")

# sample authors. You can create as many as you wish anywhere in the code

authors = {
  "charles": Author(name="Charles Dickens", active_years=(1220, 1280)),
  "jane": Author(name="Jane Austen", active_years=(1580, 1640)),
}

# Sample books.
books = [
  Book(title="Oliver Twist", author=authors["charles"], published_on=date(year=1215, month=4, day=4),
       in_stock=False, rating=2, tags=["Classic"]),
  Book(title="Great Expectations", author=authors["charles"], published_on=date(year=1220, month=4, day=4),
       rating=5,
       tags=["Classic"]),
  Book(title="Jane Eyre", author=authors["charles"], published_on=date(year=1225, month=6, day=4), in_stock=False,
       rating=3.4, tags=["Classic", "Romance"]),
  Book(title="Wuthering Heights", author=authors["jane"], published_on=date(year=1600, month=4, day=4),
       rating=4.0,
       tags=["Classic", "Romance"]),
]

# Some library objects
libraries = [
  Library(name="The Grand Library", address="Kinogozi, Hoima, Uganda"),
  Library(name="Christian Library", address="Buhimba, Hoima, Uganda")
]

# Get the collections
book_collection = store.get_collection(
  model=Book)  # you can have as many instances of this collection as you wish to have
library_collection = store.get_collection(model=Library)
author_collection = store.get_collection(model=Author)

# insert the data
book_collection.add_many(books)  # (the associated authors will be automatically inserted)
library_collection.add_many(libraries,
                            ttl=3000)  # you can even specify the ttl for only these libraries when adding them

# Get all books
all_books = book_collection.get_all()
print(
  all_books)  # Will print [Book(title="Oliver Twist", author="Charles Dickens", published_on=date(year=1215, month=4, day=4), in_stock=False), Book(...]

# Or get some books
some_books = book_collection.get_many(ids=["Oliver Twist", "Jane Eyre"])
print(some_books)  # Will print only those two books

# Or select some authors
some_authors = author_collection.get_many(ids=["Jane Austen"])
print(some_authors)  # Will print Jane Austen even though you didn't explicitly insert her in the Author's collection

# Or only get a few some properties of the book. THIS RETURNS DICTIONARIES not MODEL Instances
books_with_few_fields = book_collection.get_all_partially(fields=["author", "in_stock"])
print(books_with_few_fields)  # Will print [{"author": "'Charles Dickens", "in_stock": "True"},...]
# there is also get_one_partially, get_many_partially

# Update any book or library
book_collection.update_one("Oliver Twist", data={"author": authors["jane"]})
# You could even update a given author's details by nesting their new data in a book update
updated_jane = authors["jane"].with_changes(
  {"active_years": (1999, 2008)})  # create a new record from an old one, with only a few changes
book_collection.update_one("Oliver Twist", data={"author": updated_jane})
# Trying to retrieve jane directly will return her with the new details
# All other books that have Jane Austen as author will also have their data updated. (like a real relationship)
author_collection.get_one("Jane Austen")

# Delete any number of items
library_collection.delete_many(ids=["The Grand Library"])
```

## Quick Start (Asynchronous API)

- Install the package

  ```bash
  pip install orredis
  ```

- Import the `AsyncStore` and the `Model` classes and use accordingly

```python
import asyncio
from datetime import date
from typing import Tuple, List
from orredis import Model, AsyncStore


# type annotations are the schema.
# Don't leave them out or you will just be getting strings for every property when you retrieve an object
class Author(Model):
  name: str
  active_years: Tuple[int, int]


class Book(Model):
  title: str
  author: Author
  rating: float
  published_on: date
  tags: List[str] = []
  in_stock: bool = True


class Library(Model):
  name: str
  address: str


# Create the store and add create a collection for each model
# - `default_ttl` is the default time to live for each record is the store.
#   records never expire if there is no default_ttl set, and no `ttl` is given when adding that record to the store
# - `timeout` is the number of milliseconds beyond which the connection to redis will raise a timeout error if
#   it fails to establish a connection.
store = AsyncStore(url="redis://localhost:6379/0", pool_size=5, default_ttl=3000, timeout=1000)
# - `identifier_fields` are the properties on the model that uniquely identify a single record. They form an id.
store.create_collection(model=Author, primary_key_field="name")
store.create_collection(model=Book, primary_key_field="title")
store.create_collection(model=Library, primary_key_field="name")

# sample authors. You can create as many as you wish anywhere in the code

authors = {
  "charles": Author(name="Charles Dickens", active_years=(1220, 1280)),
  "jane": Author(name="Jane Austen", active_years=(1580, 1640)),
}

# Sample books.
books = [
  Book(title="Oliver Twist", author=authors["charles"], published_on=date(year=1215, month=4, day=4),
       in_stock=False, rating=2, tags=["Classic"]),
  Book(title="Great Expectations", author=authors["charles"], published_on=date(year=1220, month=4, day=4),
       rating=5,
       tags=["Classic"]),
  Book(title="Jane Eyre", author=authors["charles"], published_on=date(year=1225, month=6, day=4), in_stock=False,
       rating=3.4, tags=["Classic", "Romance"]),
  Book(title="Wuthering Heights", author=authors["jane"], published_on=date(year=1600, month=4, day=4),
       rating=4.0,
       tags=["Classic", "Romance"]),
]

# Some library objects
libraries = [
  Library(name="The Grand Library", address="Kinogozi, Hoima, Uganda"),
  Library(name="Christian Library", address="Buhimba, Hoima, Uganda")
]


async def run_async_example():
  # Get the collections
  book_collection = store.get_collection(
    model=Book)  # you can have as many instances of this collection as you wish to have
  library_collection = store.get_collection(model=Library)
  author_collection = store.get_collection(model=Author)

  # insert the data
  await book_collection.add_many(books)  # (the associated authors will be automatically inserted)
  await library_collection.add_many(libraries,
                                    ttl=3000)  # you can even specify the ttl for only these libraries when adding them

  # Get all books
  all_books = await book_collection.get_all()
  print(
    all_books)  # Will print [Book(title="Oliver Twist", author="Charles Dickens", published_on=date(year=1215, month=4, day=4), in_stock=False), Book(...]

  # Or get some books
  some_books = await book_collection.get_many(ids=["Oliver Twist", "Jane Eyre"])
  print(some_books)  # Will print only those two books

  # Or select some authors
  some_authors = await author_collection.get_many(ids=["Jane Austen"])
  print(
    some_authors)  # Will print Jane Austen even though you didn't explicitly insert her in the Author's collection

  # Or only get a few some properties of the book. THIS RETURNS DICTIONARIES not MODEL Instances
  books_with_few_fields = await book_collection.get_all_partially(fields=["author", "in_stock"])
  print(books_with_few_fields)  # Will print [{"author": "'Charles Dickens", "in_stock": "True"},...]
  # there is also get_one_partially, get_many_partially

  # Update any book or library
  await book_collection.update_one("Oliver Twist", data={"author": authors["jane"]})
  # You could even update a given author's details by nesting their new data in a book update
  updated_jane = authors["jane"].with_changes(
    {"active_years": (1999, 2008)})  # create a new record from an old one, with only a few changes
  await book_collection.update_one("Oliver Twist", data={"author": updated_jane})
  # Trying to retrieve jane directly will return her with the new details
  # All other books that have Jane Austen as author will also have their data updated. (like a real relationship)
  await author_collection.get_one("Jane Austen")

  # Delete any number of items
  await library_collection.delete_many(ids=["The Grand Library"])


asyncio.run(run_async_example())
```

## Benchmarks

This package has been benchmarked against some of the pre-existing ORMs for redis and this is how it stacks up against
them:

### orredis (asynchronous)

```
---------------------------------------------------------- benchmark: 11 tests ----------------------------------------------------------
Name (time in us)                                                                Mean                Min                    Max          
-----------------------------------------------------------------------------------------------------------------------------------------
benchmark_async_delete[async_book_collection-Wuthering Heights]               21.3601 (1.66)      6.1050 (1.0)      19,299.5530 (17.09)  
benchmark_async_add_one[async_book_collection-book0]                          12.8834 (1.0)       6.1730 (1.01)      1,281.1460 (1.13)   
benchmark_async_update_one[async_book_collection-Wuthering Heights-data0]     13.8155 (1.07)      6.3400 (1.04)     15,867.4010 (14.05)  
benchmark_async_add_many[async_book_collection]                               17.5144 (1.36)      7.1700 (1.17)      1,129.5450 (1.0)    
benchmark_async_bulk_delete[async_book_collection]                            25.1278 (1.95)      7.1840 (1.18)     23,385.2130 (20.70)  
benchmark_async_get_all[async_book_collection]                                23.2657 (1.81)      8.2150 (1.35)      3,417.0570 (3.03)   
benchmark_async_get_one[async_book_collection-book0]                          22.6506 (1.76)      8.2610 (1.35)      1,202.5950 (1.06)   
benchmark_async_get_many_partially[async_book_collection]                     25.1589 (1.95)     10.7620 (1.76)      1,369.5500 (1.21)   
benchmark_async_get_one_partially[async_book_collection-book0]                25.0272 (1.94)     11.4470 (1.88)     15,109.6220 (13.38)  
benchmark_async_get_many[async_book_collection]                               24.9438 (1.94)     11.6200 (1.90)      2,231.5890 (1.98)   
benchmark_async_get_all_partially[async_book_collection]                      25.7168 (2.00)     11.8590 (1.94)     17,399.2010 (15.40)  
-----------------------------------------------------------------------------------------------------------------------------------------

```

### orredis (synchronous)

``` 
----------------------------------------------------- benchmark: 11 tests -----------------------------------------------------
Name (time in us)                                                     Mean                 Min                    Max          
-------------------------------------------------------------------------------------------------------------------------------
benchmark_delete[book_collection-Wuthering Heights]                73.2168 (1.0)       59.7780 (1.0)         293.7510 (1.20)   
benchmark_bulk_delete[book_collection]                             76.1323 (1.04)      63.0270 (1.05)        244.3730 (1.0)    
benchmark_update_one[book_collection-Wuthering Heights-data0]     124.3903 (1.70)     102.1310 (1.71)        296.9740 (1.22)   
benchmark_add_one[book_collection-book0]                          155.5704 (2.12)     129.7560 (2.17)        393.7910 (1.61)   
benchmark_get_one_partially[book_collection-book0]                169.0863 (2.31)     137.9540 (2.31)        338.4000 (1.38)   
benchmark_get_one[book_collection-book0]                          202.6351 (2.77)     167.3440 (2.80)        580.5420 (2.38)   
benchmark_get_many_partially[book_collection]                     213.8824 (2.92)     181.9030 (3.04)        513.8720 (2.10)   
benchmark_get_many[book_collection]                               265.6097 (3.63)     221.0640 (3.70)        641.8550 (2.63)   
benchmark_get_all_partially[book_collection]                      298.5290 (4.08)     258.2100 (4.32)        606.2200 (2.48)   
benchmark_add_many[book_collection]                               352.7892 (4.82)     287.3370 (4.81)     15,414.2120 (63.08)  
benchmark_get_all[book_collection]                                398.7546 (5.45)     356.0230 (5.96)        813.4560 (3.33)   
-------------------------------------------------------------------------------------------------------------------------------

```

### [pydantic-redis (pure-python)](https://github.com/sopherapps/pydantic-redis)

``` 
---------------------------------------------------- benchmark: 11 tests --------------------------------------------------
Name (time in us)                                                Mean                 Min                   Max          
---------------------------------------------------------------------------------------------------------------------------
benchmark_delete[redis_store-Wuthering Heights]              166.7813 (1.0)        151.4560 (1.0)        537.9630 (1.0)    
benchmark_bulk_delete[redis_store]                           197.9722 (1.19)       169.5110 (1.12)       751.8540 (1.40)   
benchmark_update[redis_store-Wuthering Heights-data0]        372.0253 (2.23)       328.2080 (2.17)     1,825.8930 (3.39)   
benchmark_single_insert[redis_store-book0]                   425.9535 (2.55)       389.1270 (2.57)       637.8000 (1.19)   
benchmark_select_columns_for_one_id[redis_store-book0]       478.0758 (2.87)       440.3790 (2.91)       878.7980 (1.63)   
benchmark_select_all_for_one_id[redis_store-book0]           544.4497 (3.26)       490.4720 (3.24)     1,346.3720 (2.50)   
benchmark_select_columns_for_some_items[redis_store]         609.6302 (3.66)       526.4430 (3.48)     3,640.8700 (6.77)   
benchmark_select_some_items[redis_store]                     726.0504 (4.35)       679.9930 (4.49)     2,105.7010 (3.91)   
benchmark_select_columns[redis_store]                        919.1155 (5.51)       823.5510 (5.44)     2,066.8600 (3.84)   
benchmark_bulk_insert[redis_store]                         1,124.5457 (6.74)       995.9630 (6.58)     1,410.6550 (2.62)   
benchmark_select_default[redis_store]                      1,189.2962 (7.13)     1,067.1630 (7.05)     2,223.7830 (4.13)   
---------------------------------------------------------------------------------------------------------------------------
```

## Contributions

Contributions are welcome. The docs have to maintained, the code has to be made cleaner, more idiomatic and faster,
and there might be need for someone else to take over this repo in case I move on to other things. It happens!

First thing is you probably need to know a bit of rust. Consider reading
the [rust book](https://doc.rust-lang.org/book/).
It can be a very interesting read, albeit a long one.

When you are ready, look at the [CONTRIBUTIONS GUIDELINES](./docs/CONTRIBUTING.md)

Then you can read through the [SYSTEM DESIGN](./docs/SYSTEM_DESIGN.md) document to get a feel of what exactly is going
on under the hood.

### TODO:

- [ ] Add pagination for `collection.get_all()` and `collection.get_all_partially()`
- [x] Add an asynchronous API with same exact data manipulation and querying methods
- [ ] Add and host proper documentation

### How to Test


- Clone the repo and enter its root folder

  ```bash
  git clone https://github.com/sopherapps/orredis.git && cd orredis
  ```

- Create a virtual environment and activate it

  ```bash
  virtualenv -p /usr/bin/python3.7 env && source env/bin/activate
  ```

- Install the dependencies

  ```bash
  pip install -r requirements.txt
  ```

- Install orredis package in the virtual environment

  ```bash
  maturin develop
  ```

  For optimized build use:

  ```bash
  maturin develop -r
  ```

- Run the tests command

  ```bash
  pytest --benchmark-disable
  ```

- Run benchmarks

  ```bash
  pytest --benchmark-compare --benchmark-autosave
  ```

  OR the summary

  ```shell
  # synchronous API
  pytest test/test_benchmarks.py --benchmark-columns=mean,min,max --benchmark-name=short 
  ```

  ```shell
  # asynchronous API
  pytest test/test_async_benchmarks.py --benchmark-columns=mean,min,max --benchmark-name=short
  ```

## Acknowledgement

- The asyncio module code was adapted from [pyo3-asyncio](https://github.com/awestlake87/pyo3-asyncio)
- The python-rust bindings were got from [the pyo3 project](https://github.com/PyO3)

## License

Licensed under both the [MIT License](./LICENSE-MIT) and the [APACHE (2.0) License](./LICENSE-APACHE)
Copyright (c) 2022 [Martin Ahindura](https://github.com/tinitto)
Copyright (c) 2017-present PyO3 Project and Contributors

## Gratitude

> "Jesus answered him(Thomas), 'I am the way, the truth and the life; no one goes to the Father except by Me'"
> 
> -- John 14: 6

All glory be to God.
