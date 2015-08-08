# -*- coding: utf-8 -*-
from influxdb.exceptions import InfluxDBClientError

from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.user import User


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

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            response = yield client.ping()
            self.assertEqual(response, None)

            self.assert_mock_args(m, '/ping')

    @gen_test
    def test_get_databases(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'databases',
                            'columns': ['name'],
                            'values': [['foo'], ['bar']]
                        }
                    ]
                }
            ]
        }
        db_names = ['foo', 'bar']

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.get_databases()

            self.assertEqual(len(response), len(db_names))
            for db, db_name in zip(response, db_names):
                self.assertIsInstance(db, Database)
                self.assertEqual(db.name, db_name)

            self.assert_mock_args(m, '/query', query='SHOW DATABASES')

    @gen_test
    def test_get_database_names(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'databases',
                            'columns': ['name'],
                            'values': [['foo'], ['bar']]
                        }
                    ]
                }
            ]
        }

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.get_database_names()

            self.assertEqual(response, ['foo', 'bar'])

            self.assert_mock_args(m, '/query', query='SHOW DATABASES')

    def test_create_database_cps(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            client.create_database(db_name, callback=self.stop_op)
            response = self.wait()

            self.assertIsInstance(response, Database)
            self.assertEqual(response.name, db_name)

            self.assert_mock_args(m, '/query', query='CREATE DATABASE foo')

    @gen_test
    def test_create_database_using_string(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        response_body = {'results': [{}]}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.create_database(db_name)

            self.assertIsInstance(response, Database)
            self.assertEqual(response.name, db_name)

            self.assert_mock_args(m, '/query', query='CREATE DATABASE foo')

    @gen_test
    def test_create_database_using_class(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        response_body = {'results': [{}]}

        db = Database(client, db_name)
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.create_database(db)

            self.assertIsInstance(response, Database)
            self.assertEqual(response.name, db_name)

            self.assert_mock_args(m, '/query', query='CREATE DATABASE foo')

    @gen_test
    def test_create_database_unsupported_type(self):
        client = AsyncfluxClient()

        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^name_or_database must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield client.create_database(None)

            self.assertFalse(m.called)

    def test_create_database_unsupported_type_cps(self):
        client = AsyncfluxClient()

        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^name_or_database must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                client.create_database(None, callback=self.stop_op)
                self.wait()

            self.assertFalse(m.called)

    @gen_test
    def test_create_database_existing_one(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        response_body = {'results': [{'error': 'database already exists'}]}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.create_database(db_name)

            self.assertEqual(str(cm.exception), 'database already exists')
            self.assert_mock_args(m, '/query', query='CREATE DATABASE foo')

    @gen_test
    def test_drop_database_using_string(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        response_body = {'results': [{}]}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.drop_database(db_name)

            self.assertIsNone(response)

            self.assert_mock_args(m, '/query', query='DROP DATABASE foo')

    @gen_test
    def test_drop_database_using_class(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = Database(client, db_name)
        response_body = {'results': [{}]}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.drop_database(db)

            self.assertIsNone(response)

            self.assert_mock_args(m, '/query', query='DROP DATABASE foo')

    @gen_test
    def test_drop_database_unsupported_type(self):
        client = AsyncfluxClient()

        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^name_or_database must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield client.drop_database(None)

            self.assertFalse(m.called)

    @gen_test
    def test_drop_database_non_existing_one(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        response_body = {
            'results': [{'error': 'database not found: foo'}]
        }

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.drop_database(db_name)

            self.assertEqual(str(cm.exception), 'database not found: foo')
            self.assert_mock_args(m, '/query', query='DROP DATABASE foo')

    @gen_test
    def test_get_users(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'columns': ['user', 'admin'],
                            'values': [
                                ['foo', True],
                                ['bar', False]
                            ]
                        }
                    ]
                }
            ]
        }
        users_data = [
            {'user': 'foo', 'admin': True},
            {'user': 'bar', 'admin': False}
        ]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.get_users()

            self.assertEqual(len(response), len(users_data))
            for user, user_info in zip(response, users_data):
                self.assertIsInstance(user, User)
                self.assertEqual(user.name, user_info['user'])
                self.assertEqual(user.admin, user_info['admin'])

            self.assert_mock_args(m, '/query', query='SHOW USERS')

    @gen_test
    def test_get_user_names(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'columns': ['user', 'admin'],
                            'values': [
                                ['foo', True],
                                ['bar', False]
                            ]
                        }
                    ]
                }
            ]
        }

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.get_user_names()

            self.assertEqual(response, ['foo', 'bar'])

            self.assert_mock_args(m, '/query', query='SHOW USERS')

    @gen_test
    def test_create_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'
        password = 'bar'
        query = "CREATE USER foo WITH PASSWORD 'bar'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.create_user(username, password)

            self.assertIsInstance(response, User)
            self.assertEqual(response.name, username)
            self.assertEqual(response.admin, False)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_create_user_admin(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'
        password = 'bar'
        query = "CREATE USER foo WITH PASSWORD 'bar' WITH ALL PRIVILEGES"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.create_user(username, password, True)

            self.assertIsInstance(response, User)
            self.assertEqual(response.name, username)
            self.assertEqual(response.admin, True)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_create_user_existing_one(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user already exists'}]}
        username = 'foo'
        password = 'bar'
        query = "CREATE USER foo WITH PASSWORD 'bar'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.create_user(username, password)

            self.assertEqual(str(cm.exception), 'user already exists')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_create_user_admin_existing_one(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user already exists'}]}
        username = 'foo'
        password = 'bar'
        query = "CREATE USER foo WITH PASSWORD 'bar' WITH ALL PRIVILEGES"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.create_user(username, password, True)

            self.assertEqual(str(cm.exception), 'user already exists')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_change_user_password(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'
        password = 'bar'
        query = "SET PASSWORD FOR foo = 'bar'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.change_user_password(username, password)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_change_user_password_non_existing(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        username = 'foo'
        password = 'bar'
        query = "SET PASSWORD FOR foo = 'bar'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.change_user_password(username, password)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.drop_user(username)

            self.assert_mock_args(m, '/query', query='DROP USER foo')

    @gen_test
    def test_drop_user_non_existing(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        username = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.drop_user(username)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query='DROP USER foo')

    @gen_test
    def test_grant_privilege(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'
        db_name = 'bar'
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.grant_privilege('ALL', username, db_name)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'
        db_name = 'bar'
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.revoke_privilege('ALL', username, db_name)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_privilege_to_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        username = 'foo'
        db_name = 'bar'
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.grant_privilege('ALL', username, db_name)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege_from_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        username = 'foo'
        db_name = 'bar'
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.revoke_privilege('ALL', username, db_name)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_admin_privileges(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'
        query = 'GRANT ALL PRIVILEGES TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.grant_admin_privileges(username)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_admin_privileges(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        username = 'foo'
        query = 'REVOKE ALL PRIVILEGES FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.revoke_admin_privileges(username)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_admin_privileges_to_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        username = 'foo'
        query = 'GRANT ALL PRIVILEGES TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.grant_admin_privileges(username)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_admin_privileges_from_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        username = 'foo'
        query = 'REVOKE ALL PRIVILEGES FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.revoke_admin_privileges(username)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    def test_repr(self):
        host = 'localhost'
        port = 8086
        self.assertEqual(repr(AsyncfluxClient(str(host), port)),
                         "AsyncfluxClient('%s', %d)" % (host, port))
