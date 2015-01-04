# -*- coding: utf-8 -*-
import json

from asyncflux import AsyncfluxClient
from asyncflux.clusteradmin import ClusterAdmin
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.errors import AsyncfluxError


class ClusterAdminTestCase(AsyncfluxTestCase):

    @gen_test
    def test_create(self):
        client = AsyncfluxClient()
        username = 'foo'
        password = 'fubar'
        cluster_admin = ClusterAdmin(client, username)

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield cluster_admin.create(password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/cluster_admins', method='POST',
                                  body=json.dumps({'name': username,
                                                   'password': password}))

        # Existing cluster admin
        response_body = 'User %s already exists' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield cluster_admin.create(password)

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
                yield cluster_admin.create(password)

            self.assert_mock_args(m, '/cluster_admins', method='POST',
                                  body=json.dumps({'name': username,
                                                   'password': password}))

    @gen_test
    def test_change_password(self):
        client = AsyncfluxClient()
        username = 'foo'
        password = 'fubar'
        cluster_admin = ClusterAdmin(client, username)

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield cluster_admin.change_password(password)
            self.assertIsNone(response)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='POST',
                                  body=json.dumps({'password': password}))

        # Non-existing cluster admin
        response_body = 'Invalid user name %s' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield cluster_admin.change_password(password)

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
                yield cluster_admin.change_password(password)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='POST',
                                  body=json.dumps({'password': password}))

    @gen_test
    def test_delete(self):
        client = AsyncfluxClient()
        username = 'foo'
        cluster_admin = ClusterAdmin(client, username)

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200)
            response = yield cluster_admin.delete()
            self.assertIsNone(response)

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='DELETE')

        # Non-existing cluster admin
        response_body = 'User %s already exists' % username
        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaisesRegexp(AsyncfluxError, response_body):
                yield cluster_admin.delete()

            self.assert_mock_args(m, '/cluster_admins/%s' % username,
                                  method='DELETE')

    def test_repr(self):
        host = 'localhost'
        port = 8086
        username = 'foo'
        client = AsyncfluxClient(host, port)
        self.assertEqual(repr(ClusterAdmin(client, username)),
                         ("ClusterAdmin(AsyncfluxClient('%s', %d), %r)" %
                          (host, port, username)))
