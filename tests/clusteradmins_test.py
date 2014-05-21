# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.clusteradmins import ClusterAdmins
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.util import InfluxException


class ClusterAdminsTestCase(AsyncfluxTestCase):

    def setUp(self):
        super(ClusterAdminsTestCase, self).setUp()
        self.client = AsyncfluxClient(self.sync_client.base_url,
                                      username=self.sync_client.USERNAME,
                                      password=self.sync_client.PASSWORD,
                                      io_loop=self.io_loop)
        self.cluster_admins = self.client.cluster_admins

    def test_get_all(self):
        admins = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in admins:
            self.sync_client.add_cluster_admin(username, password)
        self.cluster_admins.get_all(callback=self.stop_op)
        users = self.wait()
        for username, _ in admins:
            self.assertTrue(username in users)
            self.sync_client.delete_cluster_admin(username)

    @gen_test
    def test_get_all_coro(self):
        admins = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in admins:
            self.sync_client.add_cluster_admin(username, password)
        users = yield self.cluster_admins.get_all()
        for username, _ in admins:
            self.assertTrue(username in users)
            self.sync_client.delete_cluster_admin(username)

    def test_add(self):
        admins = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in admins:
            self.cluster_admins.add(username, password,
                                    callback=self.stop_op)
            self.wait()
        users = self.sync_client.get_all_cluster_admin_names()
        for username, _ in admins:
            self.assertTrue(username in users)
            self.sync_client.delete_cluster_admin(username)

    @gen_test
    def test_add_coro(self):
        admins = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in admins:
            yield self.cluster_admins.add(username, password)
        users = self.sync_client.get_all_cluster_admin_names()
        for username, _ in admins:
            self.assertTrue(username in users)
            self.sync_client.delete_cluster_admin(username)

    def test_add_fails(self):
        username, password = 'me', 'mysecurepassword'
        self.cluster_admins.add(username, password,
                                callback=self.stop_op)
        self.wait()
        with self.assertRaisesRegexp(InfluxException,
                                     'User me already exists'):
            self.cluster_admins.add(username, password,
                                    callback=self.stop_op)
            self.wait()
        self.sync_client.delete_cluster_admin(username)

    @gen_test
    def test_add_fails_coro(self):
        username, password = 'me', 'mysecurepassword'
        yield self.cluster_admins.add(username, password)
        with self.assertRaisesRegexp(InfluxException,
                                     'User me already exists'):
            yield self.cluster_admins.add(username, password)
        self.sync_client.delete_cluster_admin(username)

    def test_update(self):
        admins = [('me', 'mysecurepassword', 'newpassword'),
                  ('foo', 'foobar', 'foobarfoobar')]
        for username, password, new_password in admins:
            self.sync_client.add_cluster_admin(username, password)
            self.cluster_admins.update(username, new_password,
                                       callback=self.stop_op)
            self.wait()
        users = self.sync_client.get_all_cluster_admin_names()
        for username, _, password in admins:
            is_valid = self.sync_client.validate_cluster_admin(username,
                                                               password)
            self.assertTrue(is_valid)

        for username, password, _ in admins:
            is_valid = self.sync_client.validate_cluster_admin(username,
                                                               password)
            self.assertTrue(not is_valid)
            self.sync_client.delete_cluster_admin(username)

    @gen_test
    def test_update_coro(self):
        admins = [('me', 'mysecurepassword', 'newpassword'),
                  ('foo', 'foobar', 'foobarfoobar')]
        for username, password, new_password in admins:
            self.sync_client.add_cluster_admin(username, password)
            yield self.cluster_admins.update(username, new_password)
        users = self.sync_client.get_all_cluster_admin_names()
        for username, _, password in admins:
            is_valid = self.sync_client.validate_cluster_admin(username,
                                                               password)
            self.assertTrue(is_valid)

        for username, password, _ in admins:
            is_valid = self.sync_client.validate_cluster_admin(username,
                                                               password)
            self.assertTrue(not is_valid)
            self.sync_client.delete_cluster_admin(username)

    def test_delete(self):
        admins = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in admins:
            self.sync_client.add_cluster_admin(username, password)
            self.cluster_admins.delete(username, callback=self.stop_op)
            self.wait()
        users = self.sync_client.get_all_cluster_admin_names()
        for username, _ in admins:
            self.assertTrue(username not in users)

    @gen_test
    def test_delete_coro(self):
        admins = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in admins:
            self.sync_client.add_cluster_admin(username, 'asecurepassword')
            yield self.cluster_admins.delete(username)
        users = self.sync_client.get_all_cluster_admin_names()
        for username, _ in admins:
            self.assertTrue(username not in users)

    def test_delete_fails(self):
        with self.assertRaisesRegexp(InfluxException,
                                     "User me doesn't exists"):
            self.cluster_admins.delete('me', callback=self.stop_op)
            self.wait()

    @gen_test
    def test_delete_fails_coro(self):
        with self.assertRaisesRegexp(InfluxException,
                                     "User me doesn't exists"):
            yield self.cluster_admins.delete('me')

    def test_repr(self):
        host = 'localhost'
        port = 8086
        client = AsyncfluxClient(host, port)
        self.assertEqual(repr(ClusterAdmins(client)),
                         ("ClusterAdmins(AsyncfluxClient('%s', %d))" %
                          (host, port)))
