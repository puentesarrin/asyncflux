# -*- coding: utf-8 -*-
"""Connection to InfluxDB"""
import functools
import json
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from tornado import gen, httpclient, httputil, ioloop

from asyncflux import clusteradmins, database
from asyncflux.util import asyncflux_coroutine, InfluxException


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

    @property
    def cluster_admins(self):
        return clusteradmins.ClusterAdmins(self)

    def __getattr__(self, name):
        return database.Database(self, name)

    def __getitem__(self, name):
        return self.__getattr__(name)

    @asyncflux_coroutine
    def request(self, path, path_params={}, qs={}, body=None, method='GET'):
        try:
            url = (self.base_url + path) % path_params
            qs.update({'u': self.username, 'p': self.password})
            if isinstance(body, dict):
                body = self.__json.dumps(body)
            response = yield self.http_client.fetch(
                httputil.url_concat(url, qs), body=body, method=method)
            if hasattr(response, 'body') and response.body:
                raise gen.Return(self.__json.loads(response.body))
        except httpclient.HTTPError as e:
            raise InfluxException(e.response)

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

    def __repr__(self):
        return "AsyncfluxClient(%r, %r)" % (self.host, self.port)
