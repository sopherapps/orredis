"""Module with the abstraction over a redis connection"""
from orredis.orredis import Redis


class Connection:
    """Manages the connection to a redis instance"""

    def __init__(self, url: str):
        self.__instance = Redis(url)

    def __enter__(self):
        if not self.__instance.is_open():
            self.__instance.reopen()

        return self.__instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__instance.close()
