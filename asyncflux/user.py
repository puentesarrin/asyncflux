# -*- coding: utf-8 -*-
"""Tools for database users"""
from asyncflux.util import asyncflux_coroutine


class User(object):

    IS_ADMIN = False
    READ_FROM = '.*'
    WRITE_TO = '.*'

    def __init__(self, database, name, is_admin=None, read_from=None,
                 write_to=None):
        self.__database = database
        self.__client = database.client
        self.__name = name
        self.__is_admin = is_admin or self.IS_ADMIN
        self.__read_from = read_from or self.READ_FROM
        self.__write_to = write_to or self.WRITE_TO

    @property
    def database(self):
        return self.__database

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

    def __update_attributes(self, is_admin=None, read_from=None,
                            write_to=None):
        if is_admin:
            self.__is_admin = is_admin
        if read_from:
            self.__read_from = read_from
        if write_to:
            self.__write_to = write_to

    @asyncflux_coroutine
    def update(self, new_password=None, is_admin=None, read_from=None,
               write_to=None):
        yield self.database.update_user(self.name, new_password=new_password,
                                        is_admin=is_admin, read_from=read_from,
                                        write_to=write_to)
        self.__update_attributes(is_admin=is_admin, read_from=read_from,
                                 write_to=write_to)

    @asyncflux_coroutine
    def change_password(self, new_password):
        yield self.database.change_user_password(self.name, new_password)

    @asyncflux_coroutine
    def change_privileges(self, is_admin, read_from=None, write_to=None):
        yield self.database.change_user_privileges(self.name, is_admin,
                                                   read_from=read_from,
                                                   write_to=write_to)
        self.__update_attributes(is_admin=is_admin, read_from=read_from,
                                 write_to=write_to)

    @asyncflux_coroutine
    def change_permissions(self, read_from, write_to):
        yield self.database.change_user_permissions(self.name, read_from,
                                                    write_to)
        self.__update_attributes(read_from=read_from, write_to=write_to)

    @asyncflux_coroutine
    def delete(self):
        yield self.database.delete_user(self.name)

    def __repr__(self):
        return 'User(%r, %r)' % (self.database, self.name)
