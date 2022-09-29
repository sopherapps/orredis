"""Tests for the asynchronous part of orredis"""
import pytest

from orredis import AsyncStore
from test.conftest import Book, async_redis_store_fixture, books, authors, Author


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_store_clear_async(store: AsyncStore):
    """Clears all the keys in the redis store"""
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)

    await book_collection.add_many(books)
    books_in_store_before_clear = await book_collection.get_all()
    authors_in_store_before_clear = await author_collection.get_all()

    await store.clear(asynchronous=True)

    books_in_store_after_clear = await book_collection.get_all()
    authors_in_store_after_clear = await author_collection.get_all()

    assert books_in_store_before_clear != []
    assert authors_in_store_before_clear != []
    assert books_in_store_after_clear == []
    assert authors_in_store_after_clear == []


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_add_many_async(store):
    """Adds many model instances into the redis data store"""
    book_collection = store.get_collection(Book)
    await book_collection.add_many(books)
    books_in_store = await book_collection.get_all()
    print(f"books: {[f'{bk}' for bk in books]}\n\nbooks_in_store: {[f'{bk}' for bk in books_in_store]}")
    assert sorted(books, key=lambda x: x.title) == sorted(books_in_store, key=lambda x: x.title)


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_nested_add_many_async(store):
    """add_many also upserts any nested records in redis"""
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)

    authors_in_store_before_insert = await author_collection.get_all()

    await book_collection.add_many(books)

    authors_in_store_after_insert = sorted(await author_collection.get_all(), key=lambda x: x.name)

    assert authors_in_store_before_insert == []
    assert authors_in_store_after_insert == sorted(authors.values(), key=lambda x: x.name)


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_add_one_async(store):
    """
    add_one inserts a single record into redis
    """
    book_collection = store.get_collection(Book)

    book = await book_collection.get_one(id=books[0].title)
    assert book is None

    await book_collection.add_one(books[0])

    book = await book_collection.get_one(id=books[0].title)
    assert books[0] == book


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_nested_add_one_async(store):
    """
    add_one also any nested model into redis
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)

    key = books[0].author.name
    author = await author_collection.get_one(id=key)
    assert author is None

    await book_collection.add_one(books[0])

    author = await author_collection.get_one(id=key)
    assert books[0].author == author


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_all_async(store):
    """get_all() returns all the book models"""
    book_collection = store.get_collection(Book)
    await book_collection.add_many(books)
    response = await book_collection.get_all()
    sorted_books = sorted(books, key=lambda x: x.title)
    sorted_response = sorted(response, key=lambda x: x.title)
    assert sorted_books == sorted_response


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_all_partially_async(store):
    """
    get_all_partially() returns a list of dictionaries of all books models with only those columns
    """
    book_collection = store.get_collection(Book)

    await book_collection.add_many(books)
    books_dict = {book.title: book for book in books}
    columns = ['title', 'author', 'in_stock']
    response = await book_collection.get_all_partially(fields=['title', 'author', 'in_stock'])
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


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_many_partially_async(store):
    """
    get_many_partially() returns a list of dictionaries of book models of the selected ids
    with only those fields
    """
    book_collection = store.get_collection(Book)

    await book_collection.add_many(books)
    ids = [book.title for book in books[:2]]
    books_dict = {book.title: book for book in books[:2]}
    fields = ['title', 'author', 'in_stock']
    response = await book_collection.get_many_partially(ids=ids, fields=['title', 'author', 'in_stock'])
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


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_many_async(store):
    """
    get_many() returns only those elements with the given ids
    """
    book_collection = store.get_collection(Book)
    await book_collection.add_many(books)
    ids = [book.title for book in books[:2]]
    response = await book_collection.get_many(ids=ids)
    assert response == books[:2]


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_one_non_existent_id_async(store):
    """
    get_one() for a non-existent id returns None
    """
    book_collection = store.get_collection(Book)
    await book_collection.add_many(books)
    response = await book_collection.get_one(id="Some strange book")
    assert response is None


# FIXME: add test for non-existent columns for both single and multiple record retrieval

@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_one_partially_non_existent_id_with_existent_columns_async(store):
    """
    get_one_partially() for a non-existent id even when columns are okay returns None
    """
    book_collection = store.get_collection(Book)
    await book_collection.add_many(books)
    response = await book_collection.get_one_partially(id="Some strange book", fields=["author", "title"])
    assert response is None


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_one_async(store):
    """
    get_one() returns only the elements with the given id
    """
    book_collection = store.get_collection(Book)
    await book_collection.add_many(books)
    for book in books:
        response = await book_collection.get_one(id=book.title)
        assert response == book


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_get_one_partially_async(store):
    """
    get_one_partially() returns only the fields given for the elements with the given id
    """
    book_collection = store.get_collection(Book)
    await book_collection.add_many(books)
    fields = ['title', "author", 'in_stock']

    for book in books:
        response = await book_collection.get_one_partially(id=book.title, fields=fields)
        expected = {key: getattr(book, key) for key in fields}
        assert expected == response


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_update_one_async(store):
    """
    update_one() for a given primary key updates it in redis
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)
    await book_collection.add_many(books)
    title = books[0].title
    new_in_stock = not books[0].in_stock
    new_author = Author(name='John Doe', active_years=(2000, 2009))
    new_author_key = new_author.name

    old_book = await book_collection.get_one(id=title)
    assert old_book == books[0]
    assert old_book.author != new_author

    await book_collection.update_one(id=title, data={"author": new_author, "in_stock": new_in_stock})

    book = await book_collection.get_one(id=title)
    author = await author_collection.get_one(id=new_author_key)
    assert book.author == new_author
    assert author == new_author
    assert book.title == old_book.title
    assert book.in_stock == new_in_stock
    assert book.published_on == old_book.published_on


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_nested_update_one_async(store):
    """
    Updating a nested model, without changing its primary key, also updates it its collection in redis
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)
    await book_collection.add_many(books)

    new_in_stock = not books[0].in_stock
    updated_author = Author(**books[0].author.dict())
    updated_author.active_years = (2020, 2045)
    book_key = books[0].title
    author_key = updated_author.name

    old_author = await author_collection.get_one(id=author_key)
    old_book = await book_collection.get_one(id=book_key)
    assert old_book == books[0]
    assert old_author == books[0].author
    assert old_author != updated_author

    await book_collection.update_one(id=books[0].title, data={"author": updated_author, "in_stock": new_in_stock})

    book = await book_collection.get_one(id=book_key)
    author = await author_collection.get_one(id=author_key)

    assert book.author == updated_author
    assert author == updated_author
    assert book.title == old_book.title
    assert book.in_stock == new_in_stock
    assert book.published_on == old_book.published_on


@pytest.mark.asyncio
@pytest.mark.parametrize("store", async_redis_store_fixture)
async def test_delete_many_async(store):
    """
    delete_many() removes the items of the given ids from redis,
    but leaves the nested models intact
    """
    book_collection = store.get_collection(Book)
    author_collection = store.get_collection(Author)
    await book_collection.add_many(books)
    books_to_delete = books[:2]
    books_to_be_left_in_db = books[2:]

    ids_to_delete = [book.title for book in books_to_delete]
    ids_to_leave_intact = [book.title for book in books_to_be_left_in_db]

    await book_collection.delete_many(ids=ids_to_delete)
    deleted_books_select_response = await book_collection.get_many(ids=ids_to_delete)

    books_left = await book_collection.get_many(ids=ids_to_leave_intact)
    authors_left = sorted(await author_collection.get_all(), key=lambda x: x.name)

    assert deleted_books_select_response == []
    assert books_left == books_to_be_left_in_db
    assert authors_left == sorted(authors.values(), key=lambda x: x.name)
