# -*- coding: utf-8 -*-
"""Tools for databases' retention policies"""
from asyncflux.util import asyncflux_coroutine


class RetentionPolicy(object):

    DEFAULT = False

    def __init__(self, database, name, duration, replication, default=None):
        self.__database = database
        self.__name = name
        self.__duration = duration
        self.__replication = replication
        self.__default = default or self.DEFAULT

    @property
    def database(self):
        return self.__database

    @property
    def name(self):
        return self.__name

    @property
    def duration(self):
        return self.__duration

    @property
    def replication(self):
        return self.__replication

    @property
    def default(self):
        return self.__default

    @asyncflux_coroutine
    def alter(self, duration=None, replication=None, default=False):
        if duration:
            self.__duration = duration
        if replication:
            self.__replication = replication
        self.__default = default
        yield self.database.alter_retention_policy(self.name, duration,
                                                   replication, default)

    @asyncflux_coroutine
    def drop(self):
        yield self.database.drop_retention_policy(self.name)

    def __repr__(self):
        return 'RetentionPolicy(%r, %r)' % (self.database, self.name)
