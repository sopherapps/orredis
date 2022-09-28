"""Tests for benchmarks"""
import pytest

from test.conftest import book_collection_fixture, books, books_fixture, update_books_fixture, delete_books_fixture


@pytest.mark.parametrize("collection", book_collection_fixture)
def test_benchmark_add_many(benchmark, collection):
    """Benchmarks the add_many operation"""
    benchmark(collection.add_many, books)


@pytest.mark.parametrize("collection, book", books_fixture)
def test_benchmark_add_one(benchmark, collection, book):
    """Benchmarks the add_one() operation"""
    benchmark(collection.add_one, book)


@pytest.mark.parametrize("collection", book_collection_fixture)
def test_benchmark_get_all(benchmark, collection):
    """Benchmarks the get_all() operation"""
    collection.add_many(books)
    benchmark(collection.get_all)


@pytest.mark.parametrize("collection", book_collection_fixture)
def test_benchmark_get_all_partially(benchmark, collection):
    """Benchmarks the get_all_partially() operation"""
    collection.add_many(books)
    benchmark(collection.get_all_partially, fields=['title', 'author', 'in_stock'])


@pytest.mark.parametrize("collection", book_collection_fixture)
def test_benchmark_get_many(benchmark, collection):
    """Benchmarks the get_many() operation"""
    collection.add_many(books)
    ids = [book.title for book in books[:2]]
    benchmark(collection.get_many, ids=ids)


@pytest.mark.parametrize("collection", book_collection_fixture)
def test_benchmark_get_many_partially(benchmark, collection):
    """Benchmarks the get_many_partially() operation"""
    collection.add_many(books)
    ids = [book.title for book in books[:2]]
    benchmark(collection.get_many_partially, ids=ids, fields=['title', 'author', 'in_stock'])


@pytest.mark.parametrize("collection, book", books_fixture)
def test_benchmark_get_one_partially(benchmark, collection, book):
    """Benchmarks the get_one_partially() operation"""
    collection.add_many(books)
    benchmark(collection.get_one_partially, id=book.title, fields=['title', 'author', 'in_stock'])


@pytest.mark.parametrize("collection, book", books_fixture)
def test_benchmark_get_one(benchmark, collection, book):
    """Benchmarks the get_one() operation"""
    collection.add_many(books)
    benchmark(collection.get_one, id=book.title)


@pytest.mark.parametrize("collection, title, data", update_books_fixture)
def test_benchmark_update_one(benchmark, collection, title, data):
    """Benchmarks the update_one() operation"""
    collection.add_many(books)
    benchmark(collection.update_one, title, data=data)


@pytest.mark.parametrize("collection, title", delete_books_fixture)
def test_benchmark_delete(benchmark, collection, title):
    """Benchmarks the delete_many() for one id only operation"""
    collection.add_many(books)
    benchmark(collection.delete_many, ids=[title])


@pytest.mark.parametrize("collection", book_collection_fixture)
def test_benchmark_bulk_delete(benchmark, collection):
    """Benchmarks the bulk delete_many() operation"""
    collection.add_many(books)
    benchmark(collection.delete_many, ids=[book.title for book in books])
