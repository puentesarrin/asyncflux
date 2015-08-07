# -*- coding: utf-8 -*-
"""Tools for InfluxDB users"""
from asyncflux.util import asyncflux_coroutine


class User(object):

    ADMIN = False

    def __init__(self, client, name, admin=None):
        self.__client = client
        self.__name = name
        self.__admin = admin or self.ADMIN

    @property
    def client(self):
        return self.__client

    @property
    def name(self):
        return self.__name

    @property
    def admin(self):
        return self.__admin

    @asyncflux_coroutine
    def change_password(self, new_password):
        yield self.client.change_user_password(self.name, new_password)

    def __get_database_name(self, name_or_database):
        database_name = name_or_database
        from asyncflux import database
        if isinstance(database_name, database.Database):
            database_name = name_or_database.name
        if not isinstance(database_name, basestring):
            raise TypeError("name_or_database must be an instance of "
                            "%s or Database" % (basestring.__name__,))
        return database_name

    @asyncflux_coroutine
    def grant_privilege_on(self, privilege, name_or_database):
        database_name = self.__get_database_name(name_or_database)
        yield self.client.grant_privilege(privilege, self.name, database_name)

    @asyncflux_coroutine
    def revoke_privilege_on(self, privilege, name_or_database):
        database_name = self.__get_database_name(name_or_database)
        yield self.client.revoke_privilege(privilege, self.name, database_name)

    @asyncflux_coroutine
    def drop(self):
        yield self.client.drop_user(self.name)

    def __repr__(self):
        return 'User(%r, %r)' % (self.client, self.name)
