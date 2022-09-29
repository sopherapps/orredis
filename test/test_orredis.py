"""Tests for the orredis"""
from datetime import datetime, timezone, timedelta

import pytest

from orredis import Model
from test.conftest import Book, redis_store_fixture, books, authors, Author


def test_model_eq():
    """the custom eq should be able to handle datetime objects"""

    class Data(Model):
        size: int
        name: str

    class Task(Data):
        time: datetime

    ts = datetime(year=2022, month=7, day=9, minute=8, second=7, tzinfo=timezone(timedelta(hours=5)))
    other_ts = datetime(year=2022, month=7, day=9, minute=8, second=7, tzinfo=timezone(timedelta(hours=2)))

    assert Data(size=9, name="foo") == Data(size=9, name="foo")
    assert Data(size=9, name="foo") != Data(size=9, name="foor")
    assert Data(size=9, name="foo") != Data(size=59, name="foo")

    assert Data(size=9, name="foo") == dict(size=9, name="foo")
    assert Data(size=9, name="foo") != dict(size=9, name="foor")
    assert Data(size=9, name="foo") != dict(size=59, name="foo")

    assert Task(time=ts, size=9, name="foo") == Task(time=ts.astimezone(timezone.utc), size=9, name="foo")
    assert Task(time=ts, size=9, name="foo") == Task(time=ts, size=9, name="foo")
    assert Task(time=ts, size=9, name="foo") != Task(time=ts.astimezone(timezone.utc), size=9, name="foor")
    assert Task(time=ts, size=9, name="foo") != Task(time=ts.astimezone(timezone.utc), size=59, name="foo")
    assert Task(time=ts, size=9, name="foo") != Task(time=other_ts, size=9, name="foo")

    assert Task(time=ts, size=9, name="foo") == dict(time=ts.astimezone(timezone.utc), size=9, name="foo")
    assert Task(time=ts, size=9, name="foo") == dict(time=ts, size=9, name="foo")
    assert Task(time=ts, size=9, name="foo") != dict(time=ts.astimezone(timezone.utc), size=9, name="foor")
    assert Task(time=ts, size=9, name="foo") != dict(time=ts.astimezone(timezone.utc), size=59, name="foo")
    assert Task(time=ts, size=9, name="foo") != dict(time=other_ts, size=9, name="foo")


def test_model_with_changes():
    """with_changes creates a new instance with the right modifications"""
    book = books[0]
    changes = {"title": "Yooo another book", "in_stock": not book.in_stock}
    new_book = book.with_changes(changes=changes)
    assert book != new_book
    for k, v in changes.items():
        value = getattr(new_book, k)
        assert value == v


def test_create_collection_without_primary_key(redis_store):
    """Throws error when a collection is created without a primary_kry_field"""

    class ModelWithoutPrimaryKey(Model):
        title: str

    with pytest.raises(TypeError, match=r"primary_key_field"):
        redis_store.create_collection(ModelWithoutPrimaryKey)

    with pytest.raises(TypeError,
                       match=r"argument 'primary_key_field': 'int' object cannot be converted to 'PyString'"):
        redis_store.create_collection(ModelWithoutPrimaryKey, primary_key_field=3)


@pytest.mark.parametrize("store", redis_store_fixture)
def test_store_clear(store):
    """Clears all the keys in the redis store"""
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)

    book_collection.add_many(books)
    books_in_store_before_clear = book_collection.get_all()
    authors_in_store_before_clear = author_collection.get_all()

    store.clear()

    books_in_store_after_clear = book_collection.get_all()
    authors_in_store_after_clear = author_collection.get_all()

    assert books_in_store_before_clear != []
    assert authors_in_store_before_clear != []
    assert books_in_store_after_clear == []
    assert authors_in_store_after_clear == []


@pytest.mark.parametrize("store", redis_store_fixture)
def test_add_many(store):
    """Adds many model instances into the redis data store"""
    book_collection = store.get_collection(Book)
    book_collection.add_many(books)
    books_in_store = book_collection.get_all()
    print(f"books: {[f'{bk}' for bk in books]}\n\nbooks_in_store: {[f'{bk}' for bk in books_in_store]}")
    assert sorted(books, key=lambda x: x.title) == sorted(books_in_store, key=lambda x: x.title)


@pytest.mark.parametrize("store", redis_store_fixture)
def test_nested_add_many(store):
    """add_many also upserts any nested records in redis"""
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)

    authors_in_store_before_insert = author_collection.get_all()

    book_collection.add_many(books)

    authors_in_store_after_insert = sorted(author_collection.get_all(), key=lambda x: x.name)

    assert authors_in_store_before_insert == []
    assert authors_in_store_after_insert == sorted(authors.values(), key=lambda x: x.name)


@pytest.mark.parametrize("store", redis_store_fixture)
def test_add_one(store):
    """
    add_one inserts a single record into redis
    """
    book_collection = store.get_collection(Book)

    book = book_collection.get_one(id=books[0].title)
    assert book is None

    book_collection.add_one(books[0])

    book = book_collection.get_one(id=books[0].title)
    assert books[0] == book


@pytest.mark.parametrize("store", redis_store_fixture)
def test_nested_add_one(store):
    """
    add_one also any nested model into redis
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)

    key = books[0].author.name
    author = author_collection.get_one(id=key)
    assert author is None

    book_collection.add_one(books[0])

    author = author_collection.get_one(id=key)
    assert books[0].author == author


@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_all(store):
    """get_all() returns all the book models"""
    book_collection = store.get_collection(Book)
    book_collection.add_many(books)
    response = book_collection.get_all()
    sorted_books = sorted(books, key=lambda x: x.title)
    sorted_response = sorted(response, key=lambda x: x.title)
    assert sorted_books == sorted_response


@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_all_partially(store):
    """
    get_all_partially() returns a list of dictionaries of all books models with only those columns
    """
    book_collection = store.get_collection(Book)

    book_collection.add_many(books)
    books_dict = {book.title: book for book in books}
    columns = ['title', 'author', 'in_stock']
    response = book_collection.get_all_partially(fields=['title', 'author', 'in_stock'])
    response_dict = {book['title']: book for book in response}

    for title, book in books_dict.items():
        book_in_response = response_dict[title]
        assert isinstance(book_in_response, dict)
        assert sorted(book_in_response.keys()) == sorted(columns)

        for column in columns:
            if column == 'author':
                assert book_in_response[column] == getattr(book, column)
            else:
                v = getattr(book, column)
                assert f"{book_in_response[column]}" == f"{v}"


@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_many_partially(store):
    """
    get_many_partially() returns a list of dictionaries of book models of the selected ids
    with only those fields
    """
    book_collection = store.get_collection(Book)

    book_collection.add_many(books)
    ids = [book.title for book in books[:2]]
    books_dict = {book.title: book for book in books[:2]}
    fields = ['title', 'author', 'in_stock']
    response = book_collection.get_many_partially(ids=ids, fields=['title', 'author', 'in_stock'])
    response_dict = {book['title']: book for book in response}

    assert len(response) == len(ids)

    for title, book in books_dict.items():
        book_in_response = response_dict[title]
        assert isinstance(book_in_response, dict)
        assert sorted(book_in_response.keys()) == sorted(fields)

        for column in fields:
            if column == 'author':
                assert book_in_response[column] == getattr(book, column)
            else:
                v = getattr(book, column)
                assert f"{book_in_response[column]}" == f"{v}"


@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_many(store):
    """
    get_many() returns only those elements with the given ids
    """
    book_collection = store.get_collection(Book)
    book_collection.add_many(books)
    ids = [book.title for book in books[:2]]
    response = book_collection.get_many(ids=ids)
    assert response == books[:2]


@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_one_non_existent_id(store):
    """
    get_one() for a non-existent id returns None
    """
    book_collection = store.get_collection(Book)
    book_collection.add_many(books)
    response = book_collection.get_one(id="Some strange book")
    assert response is None


# FIXME: add test for non-existent columns for both single and multiple record retrieval

@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_one_partially_non_existent_id_with_existent_columns(store):
    """
    get_one_partially() for a non-existent id even when columns are okay returns None
    """
    book_collection = store.get_collection(Book)
    book_collection.add_many(books)
    response = book_collection.get_one_partially(id="Some strange book", fields=["author", "title"])
    assert response is None


@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_one(store):
    """
    get_one() returns only the elements with the given id
    """
    book_collection = store.get_collection(Book)
    book_collection.add_many(books)
    for book in books:
        response = book_collection.get_one(id=book.title)
        assert response == book


@pytest.mark.parametrize("store", redis_store_fixture)
def test_get_one_partially(store):
    """
    get_one_partially() returns only the fields given for the elements with the given id
    """
    book_collection = store.get_collection(Book)
    book_collection.add_many(books)
    fields = ['title', "author", 'in_stock']

    for book in books:
        response = book_collection.get_one_partially(id=book.title, fields=fields)
        expected = {key: getattr(book, key) for key in fields}
        assert expected == response


@pytest.mark.parametrize("store", redis_store_fixture)
def test_update_one(store):
    """
    update_one() for a given primary key updates it in redis
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)
    book_collection.add_many(books)
    title = books[0].title
    new_in_stock = not books[0].in_stock
    new_author = Author(name='John Doe', active_years=(2000, 2009))
    new_author_key = new_author.name

    old_book = book_collection.get_one(id=title)
    assert old_book == books[0]
    assert old_book.author != new_author

    book_collection.update_one(id=title, data={"author": new_author, "in_stock": new_in_stock})

    book = book_collection.get_one(id=title)
    author = author_collection.get_one(id=new_author_key)
    assert book.author == new_author
    assert author == new_author
    assert book.title == old_book.title
    assert book.in_stock == new_in_stock
    assert book.published_on == old_book.published_on


@pytest.mark.parametrize("store", redis_store_fixture)
def test_nested_update_one(store):
    """
    Updating a nested model, without changing its primary key, also updates it its collection in redis
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)
    book_collection.add_many(books)

    new_in_stock = not books[0].in_stock
    updated_author = Author(**books[0].author.dict())
    updated_author.active_years = (2020, 2045)
    book_key = books[0].title
    author_key = updated_author.name

    old_author = author_collection.get_one(id=author_key)
    old_book = book_collection.get_one(id=book_key)
    assert old_book == books[0]
    assert old_author == books[0].author
    assert old_author != updated_author

    book_collection.update_one(id=books[0].title, data={"author": updated_author, "in_stock": new_in_stock})

    book = book_collection.get_one(id=book_key)
    author = author_collection.get_one(id=author_key)

    assert book.author == updated_author
    assert author == updated_author
    assert book.title == old_book.title
    assert book.in_stock == new_in_stock
    assert book.published_on == old_book.published_on


@pytest.mark.parametrize("store", redis_store_fixture)
def test_delete_many(store):
    """
    delete_many() removes the items of the given ids from redis,
    but leaves the nested models intact
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)
    book_collection.add_many(books)
    books_to_delete = books[:2]
    books_to_be_left_in_db = books[2:]

    ids_to_delete = [book.title for book in books_to_delete]
    ids_to_leave_intact = [book.title for book in books_to_be_left_in_db]

    book_collection.delete_many(ids=ids_to_delete)
    deleted_books_select_response = book_collection.get_many(ids=ids_to_delete)

    books_left = book_collection.get_many(ids=ids_to_leave_intact)
    authors_left = sorted(author_collection.get_all(), key=lambda x: x.name)

    assert deleted_books_select_response == []
    assert books_left == books_to_be_left_in_db
    assert authors_left == sorted(authors.values(), key=lambda x: x.name)
