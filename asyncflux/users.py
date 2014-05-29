# -*- coding: utf-8 -*-
"""Tools for database users"""
from asyncflux.common import BaseUsers


class Users(BaseUsers):

    FIELD_FOR_NAMES = 'name'

    def __init__(self, database):
        self.__database = database
        super(Users, self).__init__(database.client)

    @property
    def database(self):
        return self.__database

    def _get_path(self):
        return '/db/%s/users' % self.database.name

    def __repr__(self):
        return 'Users(%r)' % self.database
