# -*- coding: utf-8 -*-
"""Connection to InfluxDB"""
import json
import sys
try:
    from urlparse import urlparse
except ImportError:  # pragma: no cover
    from urllib.parse import urlparse    # pragma: no cover
if sys.version_info[0] >= 3:
    basestring = str  # pragma: no cover
else:
    basestring = basestring  # pragma: no cover

from tornado import gen, httpclient, httputil, ioloop

from asyncflux import clusteradmin, database
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
        status = yield self.request('/ping')
        raise gen.Return(status)

    @asyncflux_coroutine
    def get_databases(self):
        dbs = yield self.request('/db')
        databases = [database.Database(self, db['name']) for db in dbs]
        raise gen.Return(databases)

    @asyncflux_coroutine
    def get_database_names(self):
        databases = yield self.request('/db')
        raise gen.Return([db['name'] for db in databases])

    @asyncflux_coroutine
    def create_database(self, name_or_database):
        name = name_or_database
        if isinstance(name, database.Database):
            name = name_or_database.name
        if not isinstance(name, basestring):
            raise TypeError("name_or_database must be an instance of "
                            "%s or Database" % (basestring.__name__,))
        yield self.request('/db', body={'name': name}, method='POST')
        new_database = database.Database(self, name)
        raise gen.Return(new_database)

    @asyncflux_coroutine
    def delete_database(self, name_or_database):
        name = name_or_database
        if isinstance(name, database.Database):
            name = name_or_database.name
        if not isinstance(name, basestring):
            raise TypeError("name_or_database must be an instance of "
                            "%s or Database" % (basestring.__name__,))
        yield self.request('/db/%(database)s', {'database': name},
                           method='DELETE')

    @asyncflux_coroutine
    def get_cluster_admin_names(self):
        admins = yield self.request('/cluster_admins')
        raise gen.Return([a['name'] for a in admins])

    @asyncflux_coroutine
    def get_cluster_admins(self):
        cas = yield self.request('/cluster_admins')
        admins = [clusteradmin.ClusterAdmin(self, ca['name']) for ca in cas]
        raise gen.Return(admins)

    @asyncflux_coroutine
    def create_cluster_admin(self, username, password):
        yield self.request('/cluster_admins', method='POST',
                           body={'name': username, 'password': password})
        new_cluster_admin = clusteradmin.ClusterAdmin(self, username)
        raise gen.Return(new_cluster_admin)

    @asyncflux_coroutine
    def change_cluster_admin_password(self, username, new_password):
        yield self.request('/cluster_admins/%(username)s',
                           {'username': username}, method='POST',
                           body={'password': new_password})

    @asyncflux_coroutine
    def delete_cluster_admin(self, username):
        yield self.request('/cluster_admins/%(username)s',
                           {'username': username}, method='DELETE')

    @asyncflux_coroutine
    def authenticate_cluster_admin(self, username, password):
        try:
            yield self.request('/cluster_admins/authenticate',
                               auth_username=username, auth_password=password)
        except AsyncfluxError:
            raise gen.Return(False)
        raise gen.Return(True)

    def __repr__(self):
        return "AsyncfluxClient(%r, %r)" % (self.host, self.port)
