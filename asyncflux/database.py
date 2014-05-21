# -*- coding: utf-8 -*-
"""Database level operations"""
from asyncflux.util import asyncflux_coroutine


class Database(object):

    def __init__(self, client, name):
        self.__client = client
        self.__name = name

    @property
    def client(self):
        return self.__client

    @property
    def name(self):
        return self.__name

    @asyncflux_coroutine
    def create(self):
        yield self.client.create_database(self.name)

    @asyncflux_coroutine
    def delete(self):
        yield self.client.delete_database(self.name)

    def __repr__(self):
        return "Database(%r, %r)" % (self.client, self.name)
