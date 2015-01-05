# -*- coding: utf-8 -*-
"""Database level operations"""
from tornado import gen

from asyncflux import user
from asyncflux.errors import AsyncfluxError
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

    def __validate_permission_params(self, read_from=None, write_to=None,
                                     allow_nulls=True):
        if allow_nulls:
            condition = bool(read_from) != bool(write_to)
        else:
            condition = not(read_from and write_to)
        if condition:
            raise ValueError('You have to provide read and write permissions')

    @asyncflux_coroutine
    def create_user(self, username, password, is_admin=False, read_from=None,
                    write_to=None):
        self.__validate_permission_params(read_from=read_from,
                                          write_to=write_to)
        payload = {'name': username, 'password': password, 'isAdmin': is_admin}
        if read_from and write_to:
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
    def update_user(self, username, new_password=None, is_admin=None,
                    read_from=None, write_to=None):
        self.__validate_permission_params(read_from=read_from,
                                          write_to=write_to)
        payload = {}
        if new_password:
            payload['password'] = new_password
        if is_admin:
            payload['isAdmin'] = is_admin
        if read_from and write_to:
            payload['readFrom'] = read_from
            payload['writeTo'] = write_to
        if not payload:
            raise ValueError('You have to set at least one argument')
        yield self.client.request('/db/%(database)s/users/%(username)s',
                                  {'database': self.name, 'username': username},
                                  method='POST', body=payload)

    @asyncflux_coroutine
    def change_user_password(self, username, new_password):
        yield self.update_user(username, new_password=new_password)

    @asyncflux_coroutine
    def change_user_privileges(self, username, is_admin, read_from=None,
                               write_to=None):
        self.__validate_permission_params(read_from=read_from,
                                          write_to=write_to)
        yield self.update_user(username, is_admin=is_admin,
                               read_from=read_from, write_to=write_to)

    @asyncflux_coroutine
    def change_user_permissions(self, username, read_from, write_to):
        self.__validate_permission_params(read_from=read_from,
                                          write_to=write_to,
                                          allow_nulls=False)
        yield self.update_user(username, read_from=read_from,
                               write_to=write_to)

    @asyncflux_coroutine
    def delete_user(self, username):
        yield self.client.request('/db/%(database)s/users/%(username)s',
                                  {'database': self.name, 'username': username},
                                  method='DELETE')

    @asyncflux_coroutine
    def authenticate_user(self, username, password):
        try:
            yield self.client.request('/db/%(database)s/authenticate',
                                      {'database': self.name},
                                      auth_username=username,
                                      auth_password=password)
        except AsyncfluxError:
            raise gen.Return(False)
        raise gen.Return(True)

    def __repr__(self):
        return "Database(%r, %r)" % (self.client, self.name)
