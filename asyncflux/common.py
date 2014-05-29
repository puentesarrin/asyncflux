# -*- coding: utf-8 -*
from tornado import gen

from asyncflux.util import asyncflux_coroutine


class BaseUsers(object):

    FIELD_FOR_NAMES = 'username'

    def __init__(self, client):
        self.__client = client

    @property
    def client(self):
        return self.__client

    def _get_path(self):
        raise NotImplementedError

    @asyncflux_coroutine
    def get_all(self):
        users = yield self.client.request(self._get_path())
        raise gen.Return([u[self.FIELD_FOR_NAMES] for u in users])

    @asyncflux_coroutine
    def add(self, username, password):
        yield self.client.request(self._get_path(), method='POST',
                                  body={'name': username, 'password': password})

    @asyncflux_coroutine
    def update(self, username, new_password):
        yield self.client.request(self._get_path() + '/%(username)s',
                                  path_params={'username': username},
                                  body={'password': new_password},
                                  method='POST')

    @asyncflux_coroutine
    def delete(self, username):
        yield self.client.request(self._get_path() + '/%(username)s',
                                  {'username': username}, method='DELETE')
