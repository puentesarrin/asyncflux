# -*- coding: utf-8 -*-
import json

from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.errors import AsyncfluxError
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.user import User


class DatabaseTestCase(AsyncfluxTestCase):

    @gen_test
    def test_create(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = Database(client, db_name)

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            response = yield db.create()
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db', method='POST',
                                  body=json.dumps({'name': db_name}))

        # Existing database
        response_body = 'database %s exists' % db_name
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 409, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                response = yield db.create()
                self.assertEqual(response, response_body)

            self.assert_mock_args(m, '/db', method='POST',
                                  body=json.dumps({'name': db_name}))

    @gen_test
    def test_delete(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = Database(client, db_name)

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            response = yield db.delete()
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s' % db_name, method='DELETE')

        # Non-existing databse
        response_body = "Database %s doesn't exist" % db_name
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.delete()

            self.assert_mock_args(m, '/db/%s' % db_name, method='DELETE')

    @gen_test
    def test_get_user_names(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        users = [{'name': 'foo', 'isAdmin': False, 'writeTo': '.*', 'readFrom': '.*'},
                 {'name': 'bar', 'isAdmin': False, 'writeTo': '.*', 'readFrom': '.*'}]
        user_names = [u['name'] for u in users]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=users)
            response = yield db.get_user_names()
            self.assertListEqual(response, user_names)

            self.assert_mock_args(m, '/db/%s/users' % db_name)

    @gen_test
    def test_get_users(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        users = [{'name': 'foo', 'isAdmin': False, 'writeTo': '.*', 'readFrom': '.*'},
                 {'name': 'bar', 'isAdmin': False, 'writeTo': '.*', 'readFrom': '.*'}]
        user_names = [u['name'] for u in users]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=users)
            response = yield db.get_users()
            self.assertEqual(len(response), len(user_names))
            for r in response:
                self.assertIsInstance(r, User)
                self.assertIn(r.name, user_names)

            self.assert_mock_args(m, '/db/%s/users' % db_name)

    @gen_test
    def test_create_user(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        password = 'fubar'
        read_from = '.*'
        write_to = '.*'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.create_user(username, password)
            self.assertIsNone(response)

            payload = {'name': username, 'password': password,
                       'readFrom': read_from, 'writeTo': write_to}
            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

        # Existing database user
        response_body = 'User %s already exists' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.create_user(username, password)

            payload = {'name': username, 'password': password,
                       'readFrom': read_from, 'writeTo': write_to}
            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

        # Invalid password
        password = 'bar'
        response_body = ('Password must be more than 4 and less than 56 '
                         'characters')
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.create_user(username, password)

            payload = {'name': username, 'password': password,
                       'readFrom': read_from, 'writeTo': write_to}
            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

        # Non-default permissions
        read_from = '^$'
        write_to = '^$'
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.create_user(username, password,
                                            read_from=read_from, write_to=write_to)
            self.assertIsNone(response)

            payload = {'name': username, 'password': password,
                       'readFrom': read_from, 'writeTo': write_to}
            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

    @gen_test
    def test_delete_user(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.delete_user(username)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='DELETE')

        # Non-existing cluster admin
        response_body = "User %s doesn't exists" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.delete_user(username)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='DELETE')

    def test_repr(self):
        host = 'localhost'
        port = 8086
        client = AsyncfluxClient(host, port)
        db_name = 'db'
        self.assertEqual(repr(Database(client, db_name)),
                         ("Database(AsyncfluxClient('%s', %d), '%s')" %
                          (host, port, db_name)))
