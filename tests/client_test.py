# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.util import InfluxException


class AsyncfluxClientTestCase(AsyncfluxTestCase):

    def setUp(self):
        super(AsyncfluxClientTestCase, self).setUp()
        self.client = AsyncfluxClient(self.sync_client.base_url,
                                      username=self.sync_client.USERNAME,
                                      password=self.sync_client.PASSWORD,
                                      io_loop=self.io_loop)

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

        self.assertRaisesRegexp(ValueError, 'Invalid URL scheme: ftp',
                                AsyncfluxClient, 'ftp://localhost:23')
        self.assertRaisesRegexp(TypeError, 'port must be an instance of int',
                                AsyncfluxClient, port='bar')

    def test_class_attributes(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            database = getattr(self.client, db_name)
            self.assertEqual(type(database), Database)
            self.assertEqual(database.client, self.client)
            self.assertEqual(database.name, db_name)

    def test_class_items(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            database = self.client[db_name]
            self.assertEqual(type(database), Database)
            self.assertEqual(database.client, self.client)
            self.assertEqual(database.name, db_name)

    def test_create_database(self):
        db_name = 'foo'
        self.client.create_database(db_name, callback=self.stop_op)
        self.wait()
        current_databases = self.sync_client.get_database_names()
        self.assertTrue(db_name in current_databases)
        self.sync_client.delete_database(db_name)

    @gen_test
    def test_create_database_coro(self):
        db_name = 'foo'
        yield self.client.create_database(db_name)
        current_databases = self.sync_client.get_database_names()
        self.assertTrue(db_name in current_databases)
        self.sync_client.delete_database(db_name)

    def test_create_database_class(self):
        db_name = 'foo'
        db = Database(self.client, db_name)
        self.client.create_database(db, callback=self.stop_op)
        self.wait()
        current_databases = self.sync_client.get_database_names()
        self.assertTrue(db_name in current_databases)
        self.sync_client.delete_database(db_name)

    @gen_test
    def test_create_database_class_coro(self):
        db_name = 'foo'
        db = Database(self.client, db_name)
        yield self.client.create_database(db)
        current_databases = self.sync_client.get_database_names()
        self.assertTrue(db_name in current_databases)
        self.sync_client.delete_database(db_name)

    def test_create_database_fails(self):
        db_name = 'foo'
        self.client.create_database(db_name, callback=self.stop_op)
        self.wait()
        self.client.create_database(db_name, callback=self.stop_op)
        self.assertRaisesRegexp(InfluxException,
                                'database %s exists' % db_name,
                                self.wait)
        self.sync_client.delete_database(db_name)

    @gen_test
    def test_create_database_fails_coro(self):
        db_name = 'foo'
        yield self.client.create_database(db_name)
        with self.assertRaisesRegexp(InfluxException,
                                     'database %s exists' % db_name):
            yield self.client.create_database(db_name)
        self.sync_client.delete_database(db_name)

    def test_create_database_type_fails(self):
        with self.assertRaisesRegexp(TypeError,
                                     r'^name_or_database must be an instance'):
            self.client.create_database(None, callback=self.stop_op)
            self.wait()

    @gen_test
    def test_create_database_type_fails_coro(self):
        with self.assertRaisesRegexp(TypeError,
                                     r'^name_or_database must be an instance'):
            yield self.client.create_database(None)

    def test_get_database_names(self):
        db_name = 'foo'
        self.sync_client.create_database(db_name)
        self.client.get_database_names(callback=self.stop_op)
        current_databases = self.wait()
        self.assertTrue(db_name in current_databases)
        self.sync_client.delete_database(db_name)

    @gen_test
    def test_get_database_names_coro(self):
        db_name = 'foo'
        self.sync_client.create_database(db_name)
        current_databases = yield self.client.get_database_names()
        self.assertTrue(db_name in current_databases)
        self.sync_client.delete_database(db_name)

    def test_get_databases(self):
        db_name = 'foo'
        self.sync_client.create_database(db_name)
        self.client.get_databases(callback=self.stop_op)
        database = [db for db in self.wait() if db.name == db_name][0]
        self.assertEqual(database.name, db_name)
        self.assertEqual(type(database), Database)
        self.assertEqual(database.client, self.client)
        self.sync_client.delete_database(db_name)

    @gen_test
    def test_get_databases_coro(self):
        db_name = 'foo'
        self.sync_client.create_database(db_name)
        databases = (yield self.client.get_databases())
        database = [db for db in databases if db.name == db_name][0]
        self.assertEqual(database.name, db_name)
        self.assertEqual(type(database), Database)
        self.assertEqual(database.client, self.client)
        self.sync_client.delete_database(db_name)

    def test_delete_database(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            self.sync_client.create_database(db_name)
            self.client.delete_database(db_name, callback=self.stop_op)
            self.wait()
        current_databases = self.sync_client.get_database_names()
        for db_name in databases:
            self.assertTrue(db_name not in current_databases)

    @gen_test
    def test_delete_database_coro(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            self.sync_client.create_database(db_name)
            yield self.client.delete_database(db_name)
        current_databases = self.sync_client.get_database_names()
        for db_name in databases:
            self.assertTrue(db_name not in current_databases)

    def test_delete_database_class(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            self.sync_client.create_database(db_name)
            database = Database(self.client, db_name)
            self.client.delete_database(database,
                                        callback=self.stop_op)
            self.wait()
        current_databases = self.sync_client.get_database_names()
        for db_name in databases:
            self.assertTrue(db_name not in current_databases)

    @gen_test
    def test_delete_database_class_coro(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            self.sync_client.create_database(db_name)
            database = Database(self.client, db_name)
            yield self.client.delete_database(database)
        current_databases = self.sync_client.get_database_names()
        for db_name in databases:
            self.assertTrue(db_name not in current_databases)

    def test_delete_database_fails(self):
        db_name = 'foo'
        self.sync_client.create_database(db_name)
        self.client.delete_database(db_name, callback=self.stop_op)
        self.wait()
        self.client.delete_database(db_name, callback=self.stop_op)
        self.assertRaisesRegexp(InfluxException,
                                "Database %s doesn't exist" % db_name,
                                self.wait)

    @gen_test
    def test_delete_database_fails_coro(self):
        db_name = 'foo'
        self.sync_client.create_database(db_name)
        yield self.client.delete_database(db_name)
        with self.assertRaisesRegexp(InfluxException,
                                     "Database %s doesn't exist" % db_name):
            yield self.client.delete_database(db_name)

    def test_delete_database_type_fails(self):
        with self.assertRaisesRegexp(TypeError,
                                     r'^name_or_database must be an instance'):
            self.client.delete_database(None, callback=self.stop_op)

    @gen_test
    def test_delete_database_type_fails_coro(self):
        with self.assertRaisesRegexp(TypeError,
                                     r'^name_or_database must be an instance'):
            yield self.client.delete_database(None)

    def test_repr(self):
        host = 'localhost'
        port = 8086
        self.assertEqual(repr(AsyncfluxClient(str(host), port)),
                         "AsyncfluxClient('%s', %d)" % (host, port))
