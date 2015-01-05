# -*- coding: utf-8 -*-
"""Tools for cluster administration"""
from asyncflux.util import asyncflux_coroutine


class ClusterAdmin(object):

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
    def change_password(self, new_password):
        yield self.client.change_cluster_admin_password(self.name,
                                                        new_password)

    @asyncflux_coroutine
    def delete(self):
        yield self.client.delete_cluster_admin(self.name)

    def __repr__(self):
        return 'ClusterAdmin(%r, %r)' % (self.client, self.name)
