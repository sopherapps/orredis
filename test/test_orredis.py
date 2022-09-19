"""Tests for the redis orm"""

import pytest

from orredis import BaseModel
from test.conftest import Book, redis_store_fixture, books, authors, Author


def test_register_model_without_primary_key(redis_store):
    """Throws error when a model without the _primary_key_field class variable set is registered"""

    class ModelWithoutPrimaryKey(BaseModel):
        title: str

    with pytest.raises(AttributeError, match=r"_primary_key_field"):
        redis_store.register_model(ModelWithoutPrimaryKey)

    ModelWithoutPrimaryKey._primary_key_field = None

    with pytest.raises(ValueError, match=r"_primary_key_field must be a string"):
        redis_store.register_model(ModelWithoutPrimaryKey)


@pytest.mark.parametrize("store", redis_store_fixture)
def test_store_clear(store):
    """Clears all the keys in the redis store"""
    Book.insert(books)
    books_in_store_before_clear = Book.select()
    authors_in_store_before_clear = Author.select()

    store.clear()

    books_in_store_after_clear = Book.select()
    authors_in_store_after_clear = Author.select()

    assert books_in_store_before_clear != []
    assert authors_in_store_before_clear != []
    assert books_in_store_after_clear == []
    assert authors_in_store_after_clear == []


@pytest.mark.parametrize("store", redis_store_fixture)
def test_bulk_insert(store):
    """Providing a list of Model instances to the insert method inserts the records in redis"""
    Book.insert(books)
    books_in_store = Book.select()
    print(f"books: {[f'{bk}' for bk in books]}\n\nbooks_in_store: {[f'{bk}' for bk in books_in_store]}")
    assert sorted(books, key=lambda x: x.title) == sorted(books_in_store, key=lambda x: x.title)


@pytest.mark.parametrize("store", redis_store_fixture)
def test_bulk_nested_insert(store):
    """Providing a list of Model instances to the insert method also upserts their nested records in redis"""
    authors_in_store_before_insert = Author.select()

    Book.insert(books)

    authors_in_store_after_insert = sorted(Author.select(), key=lambda x: x.name)

    assert authors_in_store_before_insert == []
    assert authors_in_store_after_insert == sorted(authors.values(), key=lambda x: x.name)


@pytest.mark.parametrize("store", redis_store_fixture)
def test_insert_single(store):
    """
    Providing a single Model instance inserts that record in redis
    """
    book = Book.select(ids=books[0].title)
    assert book is None

    Book.insert(books[0])

    book = Book.select(ids=books[0].title)
    assert books[0] == book


@pytest.mark.parametrize("store", redis_store_fixture)
def test_insert_single_nested(store):
    """
    Providing a single Model instance upserts also any nested model into redis
    """
    key = books[0].author.name
    author = Author.select(ids=key)
    assert author is None

    Book.insert(books[0])

    author = Author.select(ids=key)
    assert books[0].author == author


@pytest.mark.parametrize("store", redis_store_fixture)
def test_select_default(store):
    """Selecting without arguments returns all the book models"""
    Book.insert(books)
    response = Book.select()
    sorted_books = sorted(books, key=lambda x: x.title)
    sorted_response = sorted(response, key=lambda x: x.title)
    assert sorted_books == sorted_response


@pytest.mark.parametrize("store", redis_store_fixture)
def test_select_some_columns(store):
    """
    Selecting some columns returns a list of dictionaries of all books models with only those columns
    """
    Book.insert(books)
    books_dict = {book.title: book for book in books}
    columns = ['title', 'author', 'in_stock']
    response = Book.select(columns=['title', 'author', 'in_stock'])
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
def test_select_some_columns_for_some_items(store):
    """
    Selecting some columns, for some ids only, returns a list of dictionaries of book models of the selected ids
    with only those columns
    """
    Book.insert(books)
    ids = [book.title for book in books[:2]]
    books_dict = {book.title: book for book in books[:2]}
    columns = ['title', 'author', 'in_stock']
    response = Book.select(columns=['title', 'author', 'in_stock'], ids=ids)
    response_dict = {book['title']: book for book in response}

    assert len(response) == len(ids)

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
def test_select_some_ids(store):
    """
    Selecting some ids returns only those elements with the given ids
    """
    Book.insert(books)
    ids = [book.title for book in books[:2]]
    response = Book.select(ids=ids)
    assert response == books[:2]


@pytest.mark.parametrize("store", redis_store_fixture)
def test_select_non_existent_id(store):
    """
    Selecting non-existent id returns None
    """
    Book.insert(books)
    response = Book.select(ids="Some strange book")
    assert response is None


@pytest.mark.parametrize("store", redis_store_fixture)
def test_select_one_id(store):
    """
    Selecting one id returns only the elements with the given id
    """
    Book.insert(books)
    for book in books:
        response = Book.select(ids=book.title)
        assert response == book


@pytest.mark.parametrize("store", redis_store_fixture)
def test_select_some_columns_for_one_id(store):
    """
    Selecting one id returns only the columns for the elements with the given id
    """
    Book.insert(books)
    columns = ['title', 'author', 'in_stock']

    for book in books:
        response = Book.select(ids=book.title, columns=columns)
        assert response == {key: getattr(book, key) for key in columns}


@pytest.mark.parametrize("store", redis_store_fixture)
def test_update(store):
    """
    Updating an item of a given primary key updates it in redis
    """
    Book.insert(books)
    title = books[0].title
    new_in_stock = not books[0].in_stock
    new_author = Author(name='John Doe', active_years=(2000, 2009))
    new_author_key = new_author.name

    old_book = Book.select(ids=title)
    assert old_book == books[0]
    assert old_book.author != new_author

    Book.update(_id=title, data={"author": new_author, "in_stock": new_in_stock})

    book = Book.select(ids=title)
    author = Author.select(ids=new_author_key)
    assert book.author == new_author
    assert author == new_author
    assert book.title == old_book.title
    assert book.in_stock == new_in_stock
    assert book.published_on == old_book.published_on


@pytest.mark.parametrize("store", redis_store_fixture)
def test_update_nested_model(store):
    """
    Updating a nested model, without changing its primary key, also updates it its collection in redis
    """
    Book.insert(books)

    new_in_stock = not books[0].in_stock
    updated_author = Author(**books[0].author.dict())
    updated_author.active_years = (2020, 2045)
    book_key = books[0].title
    author_key = updated_author.name

    old_author = Author.select(ids=[author_key])[0]
    old_book = Book.select(ids=book_key)
    assert old_book == books[0]
    assert old_author == books[0].author
    assert old_author != updated_author

    Book.update(_id=books[0].title, data={"author": updated_author, "in_stock": new_in_stock})

    book = Book.select(ids=book_key)
    author = Author.select(ids=author_key)

    assert book.author == updated_author
    assert author == updated_author
    assert book.title == old_book.title
    assert book.in_stock == new_in_stock
    assert book.published_on == old_book.published_on


@pytest.mark.parametrize("store", redis_store_fixture)
def test_delete_multiple(store):
    """
    Providing a list of ids to the delete function will remove the items from redis,
    but leave the nested models intact
    """
    Book.insert(books)
    books_to_delete = books[:2]
    books_to_be_left_in_db = books[2:]

    ids_to_delete = [book.title for book in books_to_delete]
    ids_to_leave_intact = [book.title for book in books_to_be_left_in_db]

    Book.delete(ids=ids_to_delete)
    deleted_books_select_response = Book.select(ids=ids_to_delete)

    books_left = Book.select(ids=ids_to_leave_intact)
    authors_left = sorted(Author.select(), key=lambda x: x.name)

    assert deleted_books_select_response == []
    assert books_left == books_to_be_left_in_db
    assert authors_left == sorted(authors.values(), key=lambda x: x.name)
