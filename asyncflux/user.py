# -*- coding: utf-8 -*-
"""Tools for database users"""


class User(object):

    def __init__(self, database, name, is_admin, write_to, read_from):
        self.__database = database
        self.__client = database.client
        self.__name = name
        self.__is_admin = is_admin
        self.__write_to = write_to
        self.__read_from = read_from

    @property
    def database(self):
        return self.__database

    @property
    def client(self):
        return self.__client

    @property
    def name(self):
        return self.__name

    @property
    def is_admin(self):
        return self.__is_admin

    @property
    def write_to(self):
        return self.__write_to

    @property
    def read_from(self):
        return self.__read_from

    def __repr__(self):
        return 'Users(%r)' % self.database
