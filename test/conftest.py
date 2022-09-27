import socket
from datetime import date
from typing import Tuple, List

import pytest
import redislite
from pytest_lazyfixture import lazy_fixture

from orredis import Store, Model


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

authors = {
    "charles": Author(name="Charles Dickens", active_years=(1220, 1280)),
    "jane": Author(name="Jane Austen", active_years=(1580, 1640)),
}

books = [
    Book(title="Oliver Twist", author=authors["charles"], published_on=date(year=1215, month=4, day=4),
         in_stock=False, rating=2, tags=["Classic"]),
    Book(title="Great Expectations", author=authors["charles"], published_on=date(year=1220, month=4, day=4), rating=5,
         tags=["Classic"]),
    Book(title="Jane Eyre", author=authors["charles"], published_on=date(year=1225, month=6, day=4), in_stock=False,
         rating=3.4, tags=["Classic", "Romance"]),
    Book(title="Wuthering Heights", author=authors["jane"], published_on=date(year=1600, month=4, day=4), rating=4.0,
         tags=["Classic", "Romance"]),
]

redis_store_fixture = [(lazy_fixture("redis_store"))]
book_collection_fixture = [(lazy_fixture("book_collection"))]
books_fixture = [(lazy_fixture("book_collection"), book) for book in books[-1:]]
update_books_fixture = [
    (lazy_fixture("book_collection"), book.title, {"author": authors["jane"], "in_stock": not book.in_stock})
    for book in books[-1:]
]
delete_books_fixture = [(lazy_fixture("book_collection"), book.title) for book in books[-1:]]


@pytest.fixture()
def unused_tcp_port():
    """Creates an unused TCP port"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    port = f"{sock.getsockname()[1]}"
    sock.close()
    yield port


@pytest.fixture()
def redis_server(unused_tcp_port):
    """Sets up a fake redis server we can use for tests"""
    instance = redislite.Redis(serverconfig={"port": unused_tcp_port})
    yield unused_tcp_port
    instance.shutdown()


@pytest.fixture()
def redis_store(redis_server):
    """Sets up a redis store using the redis_server fixture and adds the book model to it"""
    store = Store(url=f"redis://localhost:{redis_server}/1")
    store.create_collection(Author, primary_key_field="name")
    store.create_collection(Book, primary_key_field="title")
    yield store
    store.clear()


@pytest.fixture()
def book_collection(redis_store):
    """Returns a collection for manipulating book records"""
    yield redis_store.get_collection(Book)


@pytest.fixture()
def author_collection(redis_store):
    """Returns a collection for manipulating author records"""
    yield redis_store.get_collection(Author)
