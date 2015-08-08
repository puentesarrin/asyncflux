# -*- coding: utf-8 -*-
from influxdb.exceptions import InfluxDBClientError

from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.user import User


class UserTestCase(AsyncfluxTestCase):

    @gen_test
    def test_change_password(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo')
        query = "SET PASSWORD FOR foo = 'bar'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.change_password('bar')

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_change_password_non_existing(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        user = User(client, 'foo')
        query = "SET PASSWORD FOR foo = 'bar'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield user.change_password('bar')

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_privilege_on_using_string(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo')
        db_name = 'bar'
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.grant_privilege_on('ALL', db_name)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_privilege_on_using_class(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo')
        db = Database(client, 'bar')
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.grant_privilege_on('ALL', db)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_privilege_on_unsupported_type(self):
        client = AsyncfluxClient()
        user = User(client, 'foo')

        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^name_or_database must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield user.grant_privilege_on('ALL', None)

            self.assertFalse(m.called)

    @gen_test
    def test_revoke_privilege_on_using_string(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo')
        db_name = 'bar'
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.revoke_privilege_on('ALL', db_name)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege_on_using_class(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo')
        db = Database(client, 'bar')
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.revoke_privilege_on('ALL', db)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege_on_unsupported_type(self):
        client = AsyncfluxClient()
        user = User(client, 'foo')

        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^name_or_database must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield user.revoke_privilege_on('ALL', None)

            self.assertFalse(m.called)

    @gen_test
    def test_grant_privilege_on_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        user = User(client, 'foo')
        db_name = 'bar'
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield user.grant_privilege_on('ALL', db_name)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege_on_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        user = User(client, 'foo')
        db_name = 'bar'
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield user.revoke_privilege_on('ALL', db_name)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_admin_privileges(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo')
        query = 'GRANT ALL PRIVILEGES TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.grant_admin_privileges()

            self.assertTrue(user.admin)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_admin_privileges(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo', admin=True)
        query = 'REVOKE ALL PRIVILEGES FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.revoke_admin_privileges()

            self.assertFalse(user.admin)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_admin_privileges_to_non_existing_one(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        user = User(client, 'foo')
        query = 'GRANT ALL PRIVILEGES TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield user.grant_admin_privileges()

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_admin_privileges_from_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        user = User(client, 'foo')
        query = 'REVOKE ALL PRIVILEGES FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield user.revoke_admin_privileges()

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        user = User(client, 'foo')
        query = 'DROP USER foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield user.drop()

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop_non_existing_one(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        user = User(client, 'foo')
        query = 'DROP USER foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield user.drop()

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    def test_repr(self):
        host = 'localhost'
        port = 8086
        db_name = 'foo'
        db = AsyncfluxClient(host, port)[db_name]
        username = 'foo'
        format_repr = "User(Database(AsyncfluxClient('%s', %d), '%s'), '%s')"
        self.assertEqual(repr(User(db, username)),
                         (format_repr % (host, port, db_name, username)))
