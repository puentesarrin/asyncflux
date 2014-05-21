# -*- coding: utf-8 -*-
"""Tools for cluster administration"""
from tornado import gen

from asyncflux.util import asyncflux_coroutine


class ClusterAdmins(object):

    def __init__(self, client):
        self.__client = client

    @property
    def client(self):
        return self.__client

    @asyncflux_coroutine
    def get_all(self):
        users = yield self.client._fetch('/cluster_admins')
        raise gen.Return([u['username'] for u in users])

    @asyncflux_coroutine
    def add(self, username, password):
        yield self.client._fetch('/cluster_admins', method='POST',
                                 body={'name': username, 'password': password})

    @asyncflux_coroutine
    def update(self, username, new_password):
        yield self.client._fetch('/cluster_admins/%(username)s',
                                 path_params={'username': username},
                                 body={'password': new_password},
                                 method='POST')

    @asyncflux_coroutine
    def delete(self, username):
        yield self.client._fetch('/cluster_admins/%(username)s',
                                 {'username': username}, method='DELETE')
