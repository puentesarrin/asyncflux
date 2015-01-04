# -*- coding: utf-8 -*-
import json

from asyncflux import AsyncfluxClient
from asyncflux.clusteradmin import ClusterAdmin
from asyncflux.database import Database
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.errors import AsyncfluxError


class AsyncfluxClientTestCase(AsyncfluxTestCase):

    def test_class_instantiation(self):
        client = AsyncfluxClient()
        self.assertEqual(client.host, 'localhost')
        self.assertEqual(client.port, 8086)
        self.assertEqual(client.base_url, 'http://localhost:8086')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient('anotherhost')
        self.assertEqual(client.host, 'anotherhost')
        self.assertEqual(client.port, 8086)
        self.assertEqual(client.base_url, 'http://anotherhost:8086')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient(port=8089)
        self.assertEqual(client.host, 'localhost')
        self.assertEqual(client.port, 8089)
        self.assertEqual(client.base_url, 'http://localhost:8089')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient('http://localhost')
        self.assertEqual(client.host, 'localhost')
        self.assertEqual(client.port, 8086)
        self.assertEqual(client.base_url, 'http://localhost:8086')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient('http://localhost', 8089)
        self.assertEqual(client.host, 'localhost')
        self.assertEqual(client.port, 8089)
        self.assertEqual(client.base_url, 'http://localhost:8089')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient('http://remotehost:8089')
        self.assertEqual(client.host, 'remotehost')
        self.assertEqual(client.port, 8089)
        self.assertEqual(client.base_url, 'http://remotehost:8089')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient('http://remotehost:8086', port=8089)
        self.assertEqual(client.host, 'remotehost')
        self.assertEqual(client.port, 8086)
        self.assertEqual(client.base_url, 'http://remotehost:8086')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient('http://user:pass@remotehost:8089')
        self.assertEqual(client.host, 'remotehost')
        self.assertEqual(client.port, 8089)
        self.assertEqual(client.base_url, 'http://remotehost:8089')
        self.assertEqual(client.username, 'user')
        self.assertEqual(client.password, 'pass')

        client = AsyncfluxClient('https://user:pass@remotehost:8089')
        self.assertEqual(client.host, 'remotehost')
        self.assertEqual(client.port, 8089)
        self.assertEqual(client.base_url, 'https://remotehost:8089')
        self.assertEqual(client.username, 'user')
        self.assertEqual(client.password, 'pass')

        client = AsyncfluxClient(username='me')
        self.assertEqual(client.host, 'localhost')
        self.assertEqual(client.port, 8086)
        self.assertEqual(client.base_url, 'http://localhost:8086')
        self.assertEqual(client.username, 'me')
        self.assertEqual(client.password, 'root')

        client = AsyncfluxClient(password='mysecurepassword')
        self.assertEqual(client.host, 'localhost')
        self.assertEqual(client.port, 8086)
        self.assertEqual(client.base_url, 'http://localhost:8086')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'mysecurepassword')

        client = AsyncfluxClient(is_secure=True)
        self.assertEqual(client.host, 'localhost')
        self.assertEqual(client.port, 8086)
        self.assertEqual(client.base_url, 'https://localhost:8086')
        self.assertEqual(client.username, 'root')
        self.assertEqual(client.password, 'root')

        self.assertRaisesRegexp(ValueError, 'Invalid URL scheme: ftp',
                                AsyncfluxClient, 'ftp://localhost:23')
        self.assertRaisesRegexp(TypeError, 'port must be an instance of int',
                                AsyncfluxClient, port='bar')

    def test_credential_properties_setters(self):
        client = AsyncfluxClient(username='foo', password='bar')
        username = 'new_username'
        password = 'new_password'
        client.username = username
        client.password = password

        self.assertEqual(client.username, username)
        self.assertEqual(client.password, password)

    def test_class_attributes_and_items(self):
        client = AsyncfluxClient()
        databases = ['foo', 'bar', 'fubar']

        for db_name in databases:
            database = getattr(client, db_name)
            self.assertIsInstance(database, Database)
            self.assertEqual(database.client, client)
            self.assertEqual(database.name, db_name)

        for db_name in databases:
            database = client[db_name]
            self.assertIsInstance(database, Database)
            self.assertEqual(database.client, client)
            self.assertEqual(database.name, db_name)

    @gen_test
    def test_ping(self):
        client = AsyncfluxClient()
        response_body = {'status': 'ok'}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.ping()
            self.assertEqual(response, response_body)

            self.assert_mock_args(m, '/ping')

    @gen_test
    def test_get_database_names(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        databases = [{'name': 'foo'}, {'name': 'bar'}]
        db_names = [db['name'] for db in databases]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=databases)
            response = yield client.get_database_names()
            self.assertEqual(response, db_names)

            self.assert_mock_args(m, '/db')

    @gen_test
    def test_get_databases(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        databases = [{'name': 'foo'}, {'name': 'bar'}]
        db_names = [db['name'] for db in databases]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=databases)
            response = yield client.get_databases()
            self.assertEqual(len(response), len(databases))
            for r in response:
                self.assertIsInstance(r, Database)
                self.assertIn(r.name, db_names)

            self.assert_mock_args(m, '/db')

    def test_create_database_cps(self):
        client = AsyncfluxClient()
        db_name = 'foo'

        # Using a string
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 201)
            client.create_database(db_name, callback=self.stop_op)
            response = self.wait()
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db', method='POST',
                                  body=json.dumps({'name': db_name}))

        # Existing database
        response_body = 'database %s exists' % db_name
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 409, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                client.create_database(db_name, callback=self.stop_op)
                response = self.wait()
                self.assertEqual(response, response_body)

            self.assert_mock_args(m, '/db', method='POST',
                                  body=json.dumps({'name': db_name}))

    @gen_test
    def test_create_database(self):
        client = AsyncfluxClient()
        db_name = 'foo'

        # Using a string
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 201)
            response = yield client.create_database(db_name)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db', method='POST',
                                  body=json.dumps({'name': db_name}))

        # Using an instance of Database
        db = Database(client, db_name)
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 201)
            response = yield client.create_database(db)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db', method='POST',
                                  body=json.dumps({'name': db_name}))

        # Using an unsupported type
        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^name_or_database must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield client.create_database(None)
            self.assertFalse(m.called)

        # Existing database
        response_body = 'database %s exists' % db_name
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 409, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                response = yield client.create_database(db_name)
                self.assertEqual(response, response_body)

            self.assert_mock_args(m, '/db', method='POST',
                                  body=json.dumps({'name': db_name}))

    @gen_test
    def test_delete_database(self):
        client = AsyncfluxClient()
        db_name = 'foo'

        # Using a string
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            response = yield client.delete_database(db_name)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s' % db_name, method='DELETE')

        # Using an instance of Database
        db = Database(client, db_name)
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            response = yield client.delete_database(db)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s' % db_name, method='DELETE')

        # Using an unsupported type
        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^name_or_database must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield client.delete_database(None)
            self.assertFalse(m.called)

        # Non-existing database
        response_body = "Database %s doesn't exist" % db_name
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield client.delete_database(db_name)

            self.assert_mock_args(m, '/db/%s' % db_name, method='DELETE')

    @gen_test
    def test_get_cluster_admin_names(self):
        client = AsyncfluxClient()
        admins = [{'name': 'foo'}, {'name': 'bar'}]
        admin_names = [a['name'] for a in admins]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=admins)
            response = yield client.get_cluster_admin_names()
            self.assertListEqual(response, admin_names)

            self.assert_mock_args(m, '/cluster_admins')

    @gen_test
    def test_get_cluster_admins(self):
        client = AsyncfluxClient()
        admins = [{'name': 'foo'}, {'name': 'bar'}]
        admin_names = [a['name'] for a in admins]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=admins)
            response = yield client.get_cluster_admins()
            self.assertEqual(len(response), len(admin_names))
            for r in response:
                self.assertIsInstance(r, ClusterAdmin)
                self.assertIn(r.name, admin_names)

            self.assert_mock_args(m, '/cluster_admins')

    @gen_test
    def test_create_cluster_admin(self):
        client = AsyncfluxClient()
        username = 'foo'
        password = 'fubar'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield client.create_cluster_admin(username, password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/cluster_admins', method='POST',
                                  body=json.dumps({'name': username,
                                                   'password': password}))

        # Existing cluster admin
        response_body = 'User %s already exists' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield client.create_cluster_admin(username, password)

            self.assert_mock_args(m, '/cluster_admins', method='POST',
                                  body=json.dumps({'name': username,
                                                   'password': password}))

        # Invalid password
        password = 'bar'
        response_body = ('Password must be more than 4 and less than 56 '
                         'characters')
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield client.create_cluster_admin(username, password)

            self.assert_mock_args(m, '/cluster_admins', method='POST',
                                  body=json.dumps({'name': username,
                                                   'password': password}))

    @gen_test
    def test_change_cluster_admin_password(self):
        client = AsyncfluxClient()
        username = 'foo'
        password = 'fubar'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield client.change_cluster_admin_password(username,
                                                                  password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='POST',
                                  body=json.dumps({'password': password}))

        # Non-existing cluster admin
        response_body = 'Invalid user name %s' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield client.change_cluster_admin_password(username, password)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='POST',
                                  body=json.dumps({'password': password}))

        # Invalid password
        password = 'bar'
        response_body = ('Password must be more than 4 and less than 56 '
                         'characters')
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield client.change_cluster_admin_password(username, password)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='POST',
                                  body=json.dumps({'password': password}))

    @gen_test
    def test_delete_cluster_admin(self):
        client = AsyncfluxClient()
        username = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield client.delete_cluster_admin(username)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='DELETE')

        # Non-existing cluster admin
        response_body = "User %s doesn't exists" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield client.delete_cluster_admin(username)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='DELETE')

    def test_repr(self):
        host = 'localhost'
        port = 8086
        self.assertEqual(repr(AsyncfluxClient(str(host), port)),
                         "AsyncfluxClient('%s', %d)" % (host, port))
