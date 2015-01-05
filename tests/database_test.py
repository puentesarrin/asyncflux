# -*- coding: utf-8 -*-
import json

from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.errors import AsyncfluxError
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.user import User


class DatabaseTestCase(AsyncfluxTestCase):

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
        users = [{'name': 'foo', 'isAdmin': False, 'writeTo': '.*',
                  'readFrom': '.*'},
                 {'name': 'bar', 'isAdmin': False, 'writeTo': '.*',
                  'readFrom': '.*'}]
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
        users = [{'name': 'foo', 'isAdmin': False, 'writeTo': '.*',
                  'readFrom': '.*'},
                 {'name': 'bar', 'isAdmin': False, 'writeTo': '.*',
                  'readFrom': '.*'}]
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
    def test_get_user(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        user = {'name': username, 'isAdmin': False, 'writeTo': '^$',
                'readFrom': '^$'}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=user)
            response = yield db.get_user('foo')
            self.assertIsInstance(response, User)
            self.assertEqual(response.name, user['name'])
            self.assertEqual(response.is_admin, user['isAdmin'])
            self.assertEqual(response.write_to, user['writeTo'])
            self.assertEqual(response.read_from, user['readFrom'])

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username))

        # Non-existing database user
        response_body = 'Invalid username %s' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.get_user(username)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username))

    @gen_test
    def test_create_user(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        password = 'fubar'
        is_admin = True
        read_from = '.*'
        write_to = '.*'

        payload = {'name': username, 'password': password, 'isAdmin': is_admin,
                   'readFrom': read_from, 'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.create_user(username, password,
                                            is_admin=is_admin,
                                            read_from=read_from,
                                            write_to=write_to)
            self.assertIsInstance(response, User)
            self.assertEqual(response.name, username)

            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

        # Without permissions
        payload = {'name': username, 'password': password, 'isAdmin': False}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.create_user(username, password)
            self.assertIsInstance(response, User)
            self.assertEqual(response.name, username)
            self.assertEqual(response.is_admin, False)
            self.assertEqual(response.read_from, '.*')
            self.assertEqual(response.write_to, '.*')

            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

        # Invalid permission argument values
        exc_msg = 'You have to provide read and write permissions'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.create_user(username, password, is_admin=is_admin,
                                 read_from=read_from)

        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.create_user(username, password, is_admin=is_admin,
                                 write_to=write_to)

        # Existing database user
        payload = {'name': username, 'password': password, 'isAdmin': is_admin,
                   'readFrom': read_from, 'writeTo': write_to}
        response_body = 'User %s already exists' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.create_user(username, password, is_admin=is_admin,
                                     read_from=read_from, write_to=write_to)

            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

        # Invalid password
        password = 'bar'
        payload = {'name': username, 'password': password, 'isAdmin': is_admin,
                   'readFrom': read_from, 'writeTo': write_to}
        response_body = ('Password must be more than 4 and less than 56 '
                         'characters')
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.create_user(username, password, is_admin=is_admin,
                                     read_from=read_from, write_to=write_to)

            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

        # Non-default permissions
        read_from = '^$'
        write_to = '^$'
        payload = {'name': username, 'password': password, 'isAdmin': is_admin,
                   'readFrom': read_from, 'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.create_user(username, password,
                                            is_admin=is_admin,
                                            read_from=read_from,
                                            write_to=write_to)
            self.assertIsInstance(response, User)
            self.assertEqual(response.name, username)

            self.assert_mock_args(m, '/db/%s/users' % db_name, method='POST',
                                  body=json.dumps(payload))

    @gen_test
    def test_update_user(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        password = 'fubar'
        is_admin = True
        read_from = '^$'
        write_to = '^$'

        # Update password
        payload = {'password': password}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.update_user(username, password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Update isAdmin value
        payload = {'isAdmin': is_admin}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.update_user(username, is_admin=is_admin)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Update permissions
        payload = {'readFrom': read_from, 'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.update_user(username, read_from=read_from,
                                            write_to=write_to)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Invalid permission argument values
        exc_msg = 'You have to provide read and write permissions'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.update_user(username, password, is_admin=is_admin,
                                 read_from=read_from)

        # Without any arguments
        exc_msg = 'You have to set at least one argument'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.update_user(username)

    @gen_test
    def test_change_user_password(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        password = 'fubar'

        payload = {'password': password}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.change_user_password(username, password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Non-existing user
        response_body = "Invalid username %s" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.change_user_password(username, password)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Invalid password
        password = 'bar'
        payload = {'password': password}
        response_body = ('Password must be more than 4 and less than 56 '
                         'characters')
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.change_user_password(username, password)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

    @gen_test
    def test_change_user_privileges(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        is_admin = True
        read_from = '^$'
        write_to = '^$'

        # Update permissions
        payload = {'isAdmin': is_admin, 'readFrom': read_from,
                   'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.change_user_privileges(username, is_admin,
                                                       read_from=read_from,
                                                       write_to=write_to)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        payload = {'isAdmin': is_admin}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            yield db.change_user_privileges(username, is_admin, None, None)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Non-existing user
        payload = {'isAdmin': is_admin, 'readFrom': read_from,
                   'writeTo': write_to}
        response_body = "Invalid username %s" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.change_user_privileges(username, is_admin,
                                                read_from=read_from,
                                                write_to=write_to)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Invalid permission argument values
        exc_msg = 'You have to provide read and write permissions'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.change_user_privileges(username, is_admin, read_from, None)

        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.change_user_privileges(username, is_admin, None, write_to)

    @gen_test
    def test_change_user_permissions(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        read_from = '^$'
        write_to = '^$'

        # Update permissions
        payload = {'readFrom': read_from, 'writeTo': write_to}
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.change_user_permissions(username,
                                                        read_from=read_from,
                                                        write_to=write_to)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Non-existing user
        response_body = "Invalid username %s" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.change_user_permissions(username, read_from=read_from,
                                                 write_to=write_to)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='POST', body=json.dumps(payload))

        # Invalid permission argument values
        exc_msg = 'You have to provide read and write permissions'
        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.change_user_permissions(username, None, None)

        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.change_user_permissions(username, read_from, None)

        with self.assertRaisesRegexp(ValueError, exc_msg):
            yield db.change_user_permissions(username, None, write_to)

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

        # Non-existing user
        response_body = "User %s doesn't exists" % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield db.delete_user(username)

            self.assert_mock_args(m, '/db/%s/users/%s' % (db_name, username),
                                  method='DELETE')

    @gen_test
    def test_authenticate_user(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        db = client[db_name]
        username = 'foo'
        password = 'bar'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield db.authenticate_user(username, password)
            self.assertTrue(response)

            self.assert_mock_args(m, '/db/%s/authenticate' % db_name,
                                  auth_username=username,
                                  auth_password=password)

        # Invalid credentials
        response_body = 'Invalid username/password'
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 401, body=response_body)
            response = yield db.authenticate_user(username, password)
            self.assertFalse(response)

            self.assert_mock_args(m, '/db/%s/authenticate' % db_name,
                                  auth_username=username,
                                  auth_password=password)

    def test_repr(self):
        host = 'localhost'
        port = 8086
        client = AsyncfluxClient(host, port)
        db_name = 'db'
        self.assertEqual(repr(Database(client, db_name)),
                         ("Database(AsyncfluxClient('%s', %d), '%s')" %
                          (host, port, db_name)))
