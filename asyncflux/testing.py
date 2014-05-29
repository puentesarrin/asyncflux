# -*- coding: utf-8 -*-
"""Unit testing support for asynchronous code"""
import json
import random
import string

from tornado.httputil import url_concat
from tornado.httpclient import HTTPClient, HTTPError
from tornado.testing import AsyncTestCase, gen_test


__all__ = ('AsyncfluxTestCase', 'gen_test', 'SynchroInfluxDBClient', )


class SynchroInfluxDBClient(object):
    """A simple and **synchronous** client for InfluxDB.

    .. warning::

       This class is used for unit testing purposes only.
    """

    HOST = 'localhost'
    PORT = 8086
    USERNAME = 'root'
    PASSWORD = 'root'

    @property
    def base_url(self):
        return 'http://%s:%s' % (self.HOST, self.PORT, )

    @property
    def credentials(self):
        return {'u': self.USERNAME, 'p': self.PASSWORD}

    def fetch(self, path, path_params={}, qs={}, body=None, method='GET',
              credentials=None):
        try:
            http_client = HTTPClient()
            url = (self.base_url + path) % path_params
            qs.update(credentials or self.credentials)
            if isinstance(body, dict):
                body = json.dumps(body)
            response = http_client.fetch(url_concat(url, qs), body=body,
                                         method=method)
            if hasattr(response, 'body') and response.body:
                return json.loads(response.body)
        except HTTPError as e:
            if hasattr(e.response, 'body') and e.response.body:
                raise Exception(e.response.body)
            raise Exception(e.message)

    def create_database(self, name):
        return self.fetch('/db', body={'name': name}, method='POST')

    def get_database(self, name):
        return self.fetch('/db/%(database)s', {'database': name})

    def get_databases(self):
        return self.fetch('/db')

    def get_database_names(self):
        response = self.get_databases()
        return [db['name'] for db in response]

    def delete_database(self, name):
        return self.fetch('/db/%(database)s', {'database': name},
                          method='DELETE')

    def delete_all_databases(self):
        for database in self.get_database_names():
            self.delete_database(database)

    def validate_cluster_admin(self, username, password):
        try:
            self.fetch('/db', credentials={'u': username,
                                           'p': password})
            return True
        except:
            return False

    def add_cluster_admin(self, username, password):
        self.fetch('/cluster_admins', method='POST',
                   body={'name': username, 'password': password})

    def get_all_cluster_admins(self):
        return self.fetch('/cluster_admins')

    def get_all_cluster_admin_names(self):
        response = self.get_all_cluster_admins()
        return [u['username'] for u in response]

    def delete_cluster_admin(self, username):
        self.fetch('/cluster_admins/%(username)s', {'username': username},
                   method='DELETE')

    def delete_all_cluster_admins(self):
        admins = self.get_all_cluster_admin_names()
        # Avoid "root" user deletion.
        if 'root' in admins:
            admins.remove('root')
        for user in admins:
            self.delete_cluster_admin(user)

    def validate_db_user(self, database, username, password):
        try:
            self.fetch('/db/%s/authenticate' % database,
                       credentials={'u': username, 'p': password})
            return True
        except Exception as e:
            return False

    def add_db_user(self, database, username, password):
        self.fetch('/db/%s/users' % database, method='POST',
                   body={'name': username, 'password': password})

    def get_all_db_users(self, database):
        return self.fetch('/db/%s/users' % database)

    def get_all_db_user_names(self, database):
        response = self.get_all_db_users(database)
        return [u['name'] for u in response]

    def delete_db_user(self, database, username):
        self.fetch('/db/%(database)s/users/%(username)s',
                   {'database': database, 'username': username},
                   method='DELETE')

    def delete_all_db_users(self, database):
        users = self.get_all_db_user_names(database)
        for user in users:
            self.delete_db_user(database, user)


class AsyncfluxTestCase(AsyncTestCase):

    def setUp(self):
        super(AsyncfluxTestCase, self).setUp()
        self.sync_client = SynchroInfluxDBClient()

    def stop_op(self, result, error):
        if error:
            raise error
        super(AsyncfluxTestCase, self).stop(result)
