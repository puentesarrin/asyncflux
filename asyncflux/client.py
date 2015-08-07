# -*- coding: utf-8 -*-
"""Connection to InfluxDB"""
import json
try:
    from urlparse import urlparse
except ImportError:  # pragma: no cover
    from urllib.parse import urlparse    # pragma: no cover

from tornado import gen, httpclient, httputil, ioloop
from tornado.util import basestring_type
from influxdb.resultset import ResultSet

from asyncflux import database, user
from asyncflux.errors import AsyncfluxError
from asyncflux.util import asyncflux_coroutine


class AsyncfluxClient(object):

    HOST = 'localhost'
    PORT = 8086
    USERNAME = 'root'
    PASSWORD = 'root'

    def __init__(self, host=None, port=None, username=None, password=None,
                 is_secure=False, io_loop=None, **kwargs):
        scheme = 'https' if is_secure else 'http'
        host = host or self.HOST
        port = port or self.PORT
        username = username or self.USERNAME
        password = password or self.PASSWORD
        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        if '://' in host:
            if host.startswith(('http://', 'https://')):
                result = urlparse(host)
                scheme = result.scheme
                host = result.hostname
                port = result.port or port
                username = result.username or username
                password = result.password or password
            else:
                index = host.find("://")
                raise ValueError('Invalid URL scheme: %s' % host[:index])

        self.__scheme = scheme
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = password

        self.__json = kwargs.get('json_module', json)
        self.io_loop = io_loop or ioloop.IOLoop.current()
        self.http_client = httpclient.AsyncHTTPClient(self.io_loop)

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def base_url(self):
        return '%s://%s:%s' % (self.__scheme, self.host, self.port, )

    @property
    def username(self):
        return self.__username

    @username.setter
    def username(self, value):
        self.__username = value

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, value):
        self.__password = value

    def __getattr__(self, name):
        return database.Database(self, name)

    def __getitem__(self, name):
        return self.__getattr__(name)

    @asyncflux_coroutine
    def request(self, path, path_params=None, qs=None, body=None,
                method='GET', auth_username=None, auth_password=None):
        try:
            path_params = path_params or {}
            qs = qs or {}
            auth_username = auth_username or self.username
            auth_password = auth_password or self.password

            url = (self.base_url + path) % path_params
            if isinstance(body, dict):
                body = self.__json.dumps(body)
            response = yield self.http_client.fetch(
                httputil.url_concat(url, qs), body=body, method=method,
                auth_username=auth_username, auth_password=auth_password)
            if hasattr(response, 'body') and response.body:
                raise gen.Return(self.__json.loads(response.body))
        except httpclient.HTTPError as e:
            raise AsyncfluxError(e.response)

    @asyncflux_coroutine
    def ping(self):
        yield self.request('/ping')

    @asyncflux_coroutine
    def query(self, query, params=None, epoch=None, database=None,
              raise_errors=True):
        params = params or {}
        params['q'] = query
        if database:
            params['db'] = database
        if epoch:
            params['epoch'] = epoch

        response = yield self.request('/query', qs=params)
        result_set = [
            ResultSet(result, raise_errors=raise_errors)
            for result
            in response.get('results', [])
        ]
        raise gen.Return(result_set)

    @asyncflux_coroutine
    def get_databases(self):
        result_set = yield self.query('SHOW DATABASES')
        databases = [
            database.Database(self, raw['name'])
            for raw
            in result_set[0].get_points()
        ]
        raise gen.Return(databases)

    @asyncflux_coroutine
    def get_database_names(self):
        result_set = yield self.query('SHOW DATABASES')
        databases = [
            raw['name']
            for raw
            in result_set[0].get_points()
        ]
        raise gen.Return(databases)

    @asyncflux_coroutine
    def create_database(self, name_or_database):
        name = name_or_database
        if isinstance(name, database.Database):
            name = name_or_database.name
        if not isinstance(name, basestring_type):
            raise TypeError("name_or_database must be an instance of "
                            "%s or Database" % (basestring_type.__name__,))
        yield self.query('CREATE DATABASE {}'.format(name))
        new_database = database.Database(self, name)
        raise gen.Return(new_database)

    @asyncflux_coroutine
    def drop_database(self, name_or_database):
        name = name_or_database
        if isinstance(name, database.Database):
            name = name_or_database.name
        if not isinstance(name, basestring_type):
            raise TypeError("name_or_database must be an instance of "
                            "%s or Database" % (basestring_type.__name__,))
        yield self.query('DROP DATABASE {}'.format(name))

    @asyncflux_coroutine
    def get_users(self):
        result_set = yield self.query('SHOW USERS')
        databases = [
            user.User(self, point['user'], point['admin'])
            for point
            in result_set[0].get_points()
        ]
        raise gen.Return(databases)

    @asyncflux_coroutine
    def get_user_names(self):
        result_set = yield self.query('SHOW USERS')
        databases = [
            point['user']
            for point
            in result_set[0].get_points()
        ]
        raise gen.Return(databases)

    @asyncflux_coroutine
    def create_user(self, username, password, admin=False):
        query_list = ["CREATE USER {} WITH PASSWORD '{}'".format(username,
                                                                 password)]
        if admin:
            query_list.append('WITH ALL PRIVILEGES')
        yield self.query(' '.join(query_list))
        new_user = user.User(self, username, admin)
        raise gen.Return(new_user)

    @asyncflux_coroutine
    def change_user_password(self, username, password):
        query_str = "SET PASSWORD FOR {} = '{}'".format(username, password)
        yield self.query(query_str)

    @asyncflux_coroutine
    def drop_user(self, username):
        query_str = 'DROP USER {}'.format(username)
        yield self.query(query_str)

    @asyncflux_coroutine
    def grant_privilege(self, privilege, username, database=None):
        query_list = ['GRANT {}'.format(privilege)]
        if database:
            query_list.append('ON {}'.format(database))
        query_list.append('TO {}'.format(username))
        yield self.query(' '.join(query_list))

    @asyncflux_coroutine
    def revoke_privilege(self, privilege, username, database=None):
        query_list = ['REVOKE {}'.format(privilege)]
        if database:
            query_list.append('ON {}'.format(database))
        query_list.append('FROM {}'.format(username))
        yield self.query(' '.join(query_list))

    @asyncflux_coroutine
    def grant_admin_privileges(self, username):
        yield self.grant_privilege('ALL PRIVILEGES', username)

    @asyncflux_coroutine
    def revoke_admin_privileges(self, username):
        yield self.revoke_privilege('ALL PRIVILEGES', username)

    def __repr__(self):
        return "AsyncfluxClient(%r, %r)" % (self.host, self.port)
