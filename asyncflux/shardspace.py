# -*- coding: utf-8 -*-
"""Tools for shard spaces administration"""
from asyncflux.database import Database


class ShardSpace(object):

    def __init__(self, client, name, database, regex, retention_policy,
                 shard_duration, replication_factor, split):
        self.__client = client
        self.__name = name
        if isinstance(database, Database):
            self.__database = database
        else:
            self.__database = Database(client, database)
        self.__regex = regex
        self.__retention_policy = retention_policy
        self.__shard_duration = shard_duration
        self.__replication_factor = replication_factor
        self.__split = split

    @property
    def client(self):
        return self.__client

    @property
    def name(self):
        return self.__name

    @property
    def database(self):
        return self.__database

    @property
    def regex(self):
        return self.__regex

    @property
    def retention_policy(self):
        return self.__retention_policy

    @property
    def shard_duration(self):
        return self.__shard_duration

    @property
    def replication_factor(self):
        return self.__replication_factor

    @property
    def split(self):
        return self.__split
