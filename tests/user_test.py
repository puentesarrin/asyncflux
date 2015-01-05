# -*- coding: utf-8 -*-
import json

from asyncflux import AsyncfluxClient
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.user import User
from asyncflux.errors import AsyncfluxError


class UserTestCase(AsyncfluxTestCase):

    @gen_test
    def test_update(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        user = User(db, username)
        password = 'fubar'
        is_admin = True
        read_from = '^$'
        write_to = '^$'

        # Update password
        payload = {'password': password}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield user.update(new_password=password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Update isAdmin value
        payload = {'isAdmin': is_admin}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield user.update(is_admin=is_admin)
            self.assertIsNone(response)
            self.assertEqual(user.is_admin, is_admin)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Update permissions
        payload = {'readFrom': read_from, 'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield user.update(read_from=read_from,
                                         write_to=write_to)
            self.assertIsNone(response)
            self.assertEqual(user.read_from, read_from)
            self.assertEqual(user.write_to, write_to)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Invalid permission argument values
        exc_msg = 'You have to provide read and write permissions'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield user.update(password, is_admin=is_admin, read_from=read_from)

        # Without any arguments
        exc_msg = 'You have to set at least one argument'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield user.update()

    @gen_test
    def test_change_password(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        password = 'fubar'
        user = User(db, username)

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield user.change_password(password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST',
                                  body=json.dumps({'password': password}))

        # Non-existing user
        response_body = 'Invalid username %s' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield user.change_password(password)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST',
                                  body=json.dumps({'password': password}))

        # Invalid password
        password = 'bar'
        response_body = ('Password must be more than 4 and less than 56 '
                         'characters')
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield user.change_password(password)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST',
                                  body=json.dumps({'password': password}))

    @gen_test
    def test_change_privileges(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        user = User(db, username)
        is_admin = True
        read_from = '^$'
        write_to = '^$'

        # Update permissions
        payload = {'isAdmin': is_admin, 'readFrom': read_from,
                   'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield user.change_privileges(is_admin,
                                                    read_from=read_from,
                                                    write_to=write_to)
            self.assertIsNone(response)
            self.assertEqual(user.is_admin, is_admin)
            self.assertEqual(user.read_from, read_from)
            self.assertEqual(user.write_to, write_to)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        payload = {'isAdmin': is_admin}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            yield user.change_privileges(is_admin, None, None)
            self.assertIsNone(response)
            self.assertEqual(user.is_admin, is_admin)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Non-existing user
        payload = {'isAdmin': is_admin, 'readFrom': read_from,
                   'writeTo': write_to}
        response_body = "Invalid username %s" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield user.change_privileges(is_admin, read_from=read_from,
                                             write_to=write_to)
            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Invalid permission argument values
        exc_msg = 'You have to provide read and write permissions'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield user.change_privileges(is_admin, read_from, None)

        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield user.change_privileges(is_admin, None, write_to)

    @gen_test
    def test_change_permissions(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        user = User(db, username)
        read_from = '^$'
        write_to = '^$'

        # Update permissions
        payload = {'readFrom': read_from, 'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield user.change_permissions(read_from=read_from,
                                                     write_to=write_to)
            self.assertIsNone(response)
            self.assertEqual(user.read_from, read_from)
            self.assertEqual(user.write_to, write_to)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Non-existing user
        response_body = "Invalid username %s" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield user.change_permissions(read_from=read_from,
                                              write_to=write_to)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Invalid permission argument values
        exc_msg = 'You have to provide read and write permissions'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield user.change_permissions(None, None)

        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield user.change_permissions(read_from, None)

        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield user.change_permissions(None, write_to)

    @gen_test
    def test_delete(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        user = User(db, username)

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield user.delete()
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='DELETE')

        # Non-existing user
        response_body = "User %s doesn't exist" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield user.delete()

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='DELETE')

    def test_repr(self):
        host = 'localhost'
        port = 8086
        db_name = 'foo'
        db = AsyncfluxClient(host, port)[db_name]
        username = 'foo'
        format_repr = "User(Database(AsyncfluxClient('%s', %d), '%s'), '%s')"
        self.assertEqual(repr(User(db, username)),
                         (format_repr % (host, port, db_name, username)))
