# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.util import InfluxException


class DatabaseTestCase(AsyncfluxTestCase):

    def setUp(self):
        super(DatabaseTestCase, self).setUp()
        self.client = AsyncfluxClient(self.sync_client.base_url,
                                      username=self.sync_client.USERNAME,
                                      password=self.sync_client.PASSWORD,
                                      io_loop=self.io_loop)

    def test_create(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            db = Database(self.client, db_name)
            db.create(callback=self.stop_op)
            self.wait()
        current_databases = self.sync_client.get_database_names()
        for db_name in databases:
            self.assertTrue(db_name in current_databases)
            self.sync_client.delete_database(db_name)

    @gen_test
    def test_create_coro(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            db = Database(self.client, db_name)
            yield db.create()
        current_databases = self.sync_client.get_database_names()
        for db_name in databases:
            self.assertTrue(db_name in current_databases)
            self.sync_client.delete_database(db_name)

    def test_delete(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            self.sync_client.create_database(db_name)
            db = Database(self.client, db_name)
            db.delete(callback=self.stop_op)
            self.wait()
        current_databases = self.sync_client.get_database_names()
        for db_name in current_databases:
            self.assertTrue(db_name not in current_databases)

    @gen_test
    def test_delete_coro(self):
        databases = ['foo', 'bar', 'fubar']
        for db_name in databases:
            self.sync_client.create_database(db_name)
            db = Database(self.client, db_name)
            yield db.delete()
        current_databases = self.sync_client.get_database_names()
        for db_name in current_databases:
            self.assertTrue(db_name not in current_databases)

    def test_repr(self):
        host = 'localhost'
        port = 8086
        client = AsyncfluxClient(host, port)
        db_name = 'db'
        self.assertEqual(repr(Database(client, db_name)),
                         ("Database(AsyncfluxClient('%s', %d), '%s')" %
                          (host, port, db_name)))
