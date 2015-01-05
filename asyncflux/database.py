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
    def create_user(self, username, password, is_admin=False, read_from=None,
                    write_to=None):
        payload = {'name': username, 'password': password, 'isAdmin': is_admin}
        if bool(read_from) != bool(write_to):
            raise ValueError('You have to provide read and write permissions')
        elif read_from and write_to:
            payload['readFrom'] = read_from
            payload['writeTo'] = write_to
        yield self.client.request('/db/%(database)s/users',
                                  {'database': self.name}, method='POST',
                                  body=payload)
        read_from = read_from or user.User.READ_FROM
        write_to = write_to or user.User.WRITE_TO
        new_user = user.User(self, username, is_admin=is_admin,
                             read_from=read_from, write_to=write_to)
        raise gen.Return(new_user)

    @asyncflux_coroutine
    def change_user_password(self, username, new_password):
        yield self.client.request('/db/%(database)s/users/%(username)s',
                                  {'database': self.name, 'username': username},
                                  method='POST',
                                  body={'password': new_password})

    @asyncflux_coroutine
    def delete_user(self, username):
        yield self.client.request('/db/%(database)s/users/%(username)s',
                                  {'database': self.name, 'username': username},
                                  method='DELETE')

    def __repr__(self):
        return "Database(%r, %r)" % (self.client, self.name)
