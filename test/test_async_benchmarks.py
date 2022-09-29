"""Tests for benchmarks for the asyncio part"""
import pytest

from test.conftest import (
    async_book_collection_fixture as book_collection_fixture,
    books,
    async_books_fixture as books_fixture,
    async_update_books_fixture as update_books_fixture,
    async_delete_books_fixture as delete_books_fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", book_collection_fixture)
async def test_benchmark_async_add_many(aio_benchmark, collection):
    """Benchmarks the async add_many operation"""
    aio_benchmark(collection.add_many, books)


@pytest.mark.asyncio
@pytest.mark.parametrize("collection, book", books_fixture)
async def test_benchmark_async_add_one(aio_benchmark, collection, book):
    """Benchmarks the async add_one() operation"""
    aio_benchmark(collection.add_one, book)


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", book_collection_fixture)
async def test_benchmark_async_get_all(aio_benchmark, collection):
    """Benchmarks the async get_all() operation"""
    await collection.add_many(books)
    aio_benchmark(collection.get_all)


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", book_collection_fixture)
async def test_benchmark_async_get_all_partially(aio_benchmark, collection):
    """Benchmarks the async get_all_partially() operation"""
    await collection.add_many(books)
    aio_benchmark(collection.get_all_partially, fields=['title', 'author', 'in_stock'])


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", book_collection_fixture)
async def test_benchmark_async_get_many(aio_benchmark, collection):
    """Benchmarks the async get_many() operation"""
    await collection.add_many(books)
    ids = [book.title for book in books[:2]]
    aio_benchmark(collection.get_many, ids=ids)


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", book_collection_fixture)
async def test_benchmark_async_get_many_partially(aio_benchmark, collection):
    """Benchmarks the async get_many_partially() operation"""
    await collection.add_many(books)
    ids = [book.title for book in books[:2]]
    aio_benchmark(collection.get_many_partially, ids=ids, fields=['title', 'author', 'in_stock'])


@pytest.mark.asyncio
@pytest.mark.parametrize("collection, book", books_fixture)
async def test_benchmark_async_get_one_partially(aio_benchmark, collection, book):
    """Benchmarks the get_one_partially() operation"""
    await collection.add_many(books)
    aio_benchmark(collection.get_one_partially, id=book.title, fields=['title', 'author', 'in_stock'])


@pytest.mark.asyncio
@pytest.mark.parametrize("collection, book", books_fixture)
async def test_benchmark_async_get_one(aio_benchmark, collection, book):
    """Benchmarks the get_one() operation"""
    await collection.add_many(books)
    aio_benchmark(collection.get_one, id=book.title)


@pytest.mark.asyncio
@pytest.mark.parametrize("collection, title, data", update_books_fixture)
async def test_benchmark_async_update_one(aio_benchmark, collection, title, data):
    """Benchmarks the update_one() operation"""
    await collection.add_many(books)
    aio_benchmark(collection.update_one, title, data=data)


@pytest.mark.asyncio
@pytest.mark.parametrize("collection, title", delete_books_fixture)
async def test_benchmark_async_delete(aio_benchmark, collection, title):
    """Benchmarks the async delete_many() for one id only operation"""
    await collection.add_many(books)
    aio_benchmark(collection.delete_many, ids=[title])


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", book_collection_fixture)
async def test_benchmark_async_bulk_delete(aio_benchmark, collection):
    """Benchmarks the async bulk delete_many() operation"""
    await collection.add_many(books)
    aio_benchmark(collection.delete_many, ids=[book.title for book in books])
