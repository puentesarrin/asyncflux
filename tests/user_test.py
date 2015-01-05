# -*- coding: utf-8 -*-
import json

from asyncflux import AsyncfluxClient
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.user import User
from asyncflux.errors import AsyncfluxError


class UserTestCase(AsyncfluxTestCase):

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

        # Non-existing cluster admin
        response_body = 'Invalid user name %s' % username
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

    def test_repr(self):
        host = 'localhost'
        port = 8086
        db_name = 'foo'
        db = AsyncfluxClient(host, port)[db_name]
        username = 'foo'
        format_repr = "User(Database(AsyncfluxClient('%s', %d), '%s'), '%s')"
        self.assertEqual(repr(User(db, username)),
                         (format_repr % (host, port, db_name, username)))
