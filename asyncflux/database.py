# -*- coding: utf-8 -*-
"""Database level operations"""
from tornado import gen

from asyncflux import user
from asyncflux.util import asyncflux_coroutine, snake_case_dict


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

    @asyncflux_coroutine
    def get_user_names(self):
        users = yield self.client.request('/db/%(database)s/users',
                                          {'database': self.name})
        raise gen.Return([u['name'] for u in users])

    @asyncflux_coroutine
    def get_users(self):
        us = yield self.client.request('/db/%(database)s/users',
                                       {'database': self.name})
        users = [user.User(self, **snake_case_dict(u)) for u in us]
        raise gen.Return(users)

    @asyncflux_coroutine
    def create_user(self, username, password, read_from='.*', write_to='.*'):
        payload = {'name': username, 'password': password,
                   'readFrom': read_from, 'writeTo': write_to}
        yield self.client.request('/db/%(database)s/users',
                                  {'database': self.name}, method='POST',
                                  body=payload)

    @asyncflux_coroutine
    def delete_user(self, username):
        yield self.client.request('/db/%(database)s/users/%(username)s',
                                  {'database': self.name, 'username': username},
                                  method='DELETE')

    def __repr__(self):
        return "Database(%r, %r)" % (self.client, self.name)
