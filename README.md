# orredis

A fast ORM for redis supporting both asynchronous and synchronous interaction with pooled or single connections to
redis.

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

## Quick Start

- Install the package

  ```bash
  pip install pydantic-redis
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
store.create_collection(model=Book, primary_key_field="title")
store.create_collection(model=Library, primary_key_field="name")
store.create_collection(model=Author, primary_key_field="name")

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

## Benchmarks

This package has been benchmarked against some of the pre-existing ORMs for redis and this is how it stacks up against
them:

### Speed

TBD

### Memory

TBD

## Contributions

Contributions are welcome. The docs have to maintained, the code has to be made cleaner, more idiomatic and faster,
and there might be need for someone else to take over this repo in case I move on to other things. It happens!

First thing is you probably need to know a bit of rust. Consider reading
the [rust book](https://doc.rust-lang.org/book/).
It can be a very interesting read, albeit a long one.

When you are ready, look at the [CONTRIBUTIONS GUIDELINES](./docs/CONTRIBUTIONS_GUIDELINE.md)

Then you can read through the [SYSTEM DESIGN](./docs/SYSTEM_DESIGN.md) document to get a feel of what exactly is going
on under the hood.

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

- Run the tests command

  ```bash
  pytest --benchmark-disable
  ```

- Run benchmarks

  ```bash
  pytest --benchmark-compare --benchmark-autosave
  ```

## Gratitude

You might wish to offer support to the project and its maintainers. Please do.
Martin, the initiator of the project, is adamant in staying in Uganda because there is more he loses by moving out of
Uganda
in search of employment opportunities. The opportunities in Uganda are very scarce. This is not the EU or US, so, yeah,
If you wish to support him, please do on [his patreon page](https://www.patreon.com/user?u=78878966).

Or...

Better still, you could hire Martin or convince your company to hire him as a remote engineer :-)

Martin is very (very) grateful.
