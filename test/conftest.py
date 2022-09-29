import socket
from datetime import date, datetime, timezone
from typing import Tuple, List

import pytest
import pytest_asyncio
import redislite
from pytest_lazyfixture import lazy_fixture

from orredis import Store, Model, AsyncStore


class Author(Model):
    name: str
    active_years: Tuple[int, int]

class Book(Model):
    title: str
    author: Author
    rating: float
    published_on: date
    last_updated: datetime = datetime(year=2022, month=9, day=17, hour=1, minute=30, tzinfo=timezone.utc)
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

# sync
redis_store_fixture = [(lazy_fixture("redis_store"))]
book_collection_fixture = [(lazy_fixture("book_collection"))]
books_fixture = [(lazy_fixture("book_collection"), book) for book in books[-1:]]
update_books_fixture = [
    (lazy_fixture("book_collection"), book.title, {"author": authors["jane"], "in_stock": not book.in_stock})
    for book in books[-1:]
]
delete_books_fixture = [(lazy_fixture("book_collection"), book.title) for book in books[-1:]]

# async
async_redis_store_fixture = [(lazy_fixture("async_redis_store"))]
async_book_collection_fixture = [(lazy_fixture("async_book_collection"))]
async_books_fixture = [(lazy_fixture("async_book_collection"), book) for book in books[-1:]]
async_update_books_fixture = [
    (lazy_fixture("async_book_collection"), book.title, {"author": authors["jane"], "in_stock": not book.in_stock})
    for book in books[-1:]
]
async_delete_books_fixture = [(lazy_fixture("async_book_collection"), book.title) for book in books[-1:]]


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


@pytest_asyncio.fixture
async def async_redis_store(redis_server):
    """Sets up an asynchronous redis store using the redis_server fixture and adds the book model to it"""
    store = AsyncStore(url=f"redis://localhost:{redis_server}/1")
    store.create_collection(Author, primary_key_field="name")
    store.create_collection(Book, primary_key_field="title")
    yield store
    await store.clear()


@pytest.fixture()
def book_collection(redis_store):
    """Returns a collection for manipulating book records"""
    yield redis_store.get_collection(Book)


@pytest.fixture()
def author_collection(redis_store):
    """Returns a collection for manipulating author records"""
    yield redis_store.get_collection(Author)


@pytest_asyncio.fixture
def async_book_collection(async_redis_store):
    """Returns an asynchronous collection for manipulating book records"""
    yield async_redis_store.get_collection(Book)


@pytest.fixture()
def aio_benchmark(benchmark):
    """
    A fixture for benchmarking coroutines courtesy of [Marcello Bello](https://github.com/mbello)
    as shared in this issue:
    https://github.com/ionelmc/pytest-benchmark/issues/66#issuecomment-575853801
    """
    import asyncio
    import threading

    class Sync2Async:
        def __init__(self, coro, *args, **kwargs):
            self.coro = coro
            self.args = args
            self.kwargs = kwargs
            self.custom_loop = None
            self.thread = None

        def start_background_loop(self) -> None:
            asyncio.set_event_loop(self.custom_loop)
            self.custom_loop.run_forever()

        def __call__(self):
            evloop = None
            awaitable = self.coro(*self.args, **self.kwargs)
            try:
                evloop = asyncio.get_running_loop()
            except:
                pass
            if evloop is None:
                return asyncio.run(awaitable)
            else:
                if not self.custom_loop or not self.thread or not self.thread.is_alive():
                    self.custom_loop = asyncio.new_event_loop()
                    self.thread = threading.Thread(target=self.start_background_loop, daemon=True)
                    self.thread.start()

                return asyncio.run_coroutine_threadsafe(awaitable, self.custom_loop).result()

    def _wrapper(func, *args, **kwargs):
        if asyncio.iscoroutinefunction(func):
            benchmark(Sync2Async(func, *args, **kwargs))
        else:
            benchmark(func, *args, **kwargs)

    return _wrapper
