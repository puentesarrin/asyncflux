# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.users import Users
from asyncflux.util import InfluxException


class UsersTestCase(AsyncfluxTestCase):

    def setUp(self):
        super(UsersTestCase, self).setUp()
        self.client = AsyncfluxClient(self.sync_client.base_url,
                                      username=self.sync_client.USERNAME,
                                      password=self.sync_client.PASSWORD,
                                      io_loop=self.io_loop)
        self.db_name = 'test_db'
        self.sync_client.create_database(self.db_name)
        self.db_users = self.client[self.db_name].users

    def tearDown(self):
        self.sync_client.delete_database(self.db_name)
        super(UsersTestCase, self).tearDown()

    def test_get_all(self):
        users = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in users:
            self.sync_client.add_db_user(self.db_name, username, password)
        self.db_users.get_all(callback=self.stop_op)
        created_users = self.wait()
        for username, _ in users:
            self.assertTrue(username in created_users)
            self.sync_client.delete_db_user(self.db_name, username)

    @gen_test
    def test_get_all_coro(self):
        users = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in users:
            self.sync_client.add_db_user(self.db_name, username, password)
        created_users = yield self.db_users.get_all()
        for username, _ in users:
            self.assertTrue(username in created_users)
            self.sync_client.delete_db_user(self.db_name, username)

    def test_add(self):
        users = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in users:
            self.db_users.add(username, password, callback=self.stop_op)
            self.wait()
        created_users = self.sync_client.get_all_db_user_names(self.db_name)
        for username, _ in users:
            self.assertTrue(username in created_users)
            self.sync_client.delete_db_user(self.db_name, username)

    @gen_test
    def test_add_coro(self):
        users = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in users:
            yield self.db_users.add(username, password)
        created_users = self.sync_client.get_all_db_user_names(self.db_name)
        for username, _ in users:
            self.assertTrue(username in created_users)
            self.sync_client.delete_db_user(self.db_name, username)

    def test_add_fails(self):
        username, password = 'me', 'mysecurepassword'
        self.db_users.add(username, password, callback=self.stop_op)
        self.wait()
        with self.assertRaisesRegexp(InfluxException,
                                     'User me already exists'):
            self.db_users.add(username, password,
                              callback=self.stop_op)
            self.wait()
        self.sync_client.delete_db_user(self.db_name, username)

    @gen_test
    def test_add_fails_coro(self):
        username, password = 'me', 'mysecurepassword'
        yield self.db_users.add(username, password)
        with self.assertRaisesRegexp(InfluxException,
                                     'User me already exists'):
            yield self.db_users.add(username, password)
        self.sync_client.delete_db_user(self.db_name, username)

    def test_update(self):
        users = [('me', 'mysecurepassword', 'newpassword'),
                 ('foo', 'foobar', 'foobarfoobar')]
        for username, password, new_password in users:
            self.sync_client.add_db_user(self.db_name, username, password)
            self.db_users.update(username, new_password,
                                 callback=self.stop_op)
            self.wait()
        created_users = self.sync_client.get_all_db_user_names(self.db_name)
        for username, _, password in users:
            is_valid = self.sync_client.validate_db_user(self.db_name,
                                                         username, password)
            self.assertTrue(is_valid)

        for username, password, _ in users:
            is_valid = self.sync_client.validate_db_user(self.db_name,
                                                         username, password)
            self.assertTrue(not is_valid)
            self.sync_client.delete_db_user(self.db_name, username)

    @gen_test
    def test_update_coro(self):
        users = [('me', 'mysecurepassword', 'newpassword'),
                 ('foo', 'foobar', 'foobarfoobar')]
        for username, password, new_password in users:
            self.sync_client.add_db_user(self.db_name, username, password)
            yield self.db_users.update(username, new_password)
        created_users = self.sync_client.get_all_db_user_names(self.db_name)
        for username, _, password in users:
            is_valid = self.sync_client.validate_db_user(self.db_name,
                                                         username, password)
            self.assertTrue(is_valid)

        for username, password, _ in users:
            is_valid = self.sync_client.validate_db_user(self.db_name,
                                                         username, password)
            self.assertTrue(not is_valid)
            self.sync_client.delete_db_user(self.db_name, username)

    def test_delete(self):
        users = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in users:
            self.sync_client.add_db_user(self.db_name, username, password)
            self.db_users.delete(username, callback=self.stop_op)
            self.wait()
        created_users = self.sync_client.get_all_db_user_names(self.db_name)
        for username, _ in created_users:
            self.assertTrue(username not in users)

    @gen_test
    def test_delete_coro(self):
        users = [('me', 'mysecurepassword'), ('foo', 'foobar')]
        for username, password in users:
            self.sync_client.add_db_user(self.db_name, username, password)
            yield self.db_users.delete(username)
        created_users = self.sync_client.get_all_db_user_names(self.db_name)
        for username, _ in created_users:
            self.assertTrue(username not in users)

    def test_delete_fails(self):
        with self.assertRaisesRegexp(InfluxException,
                                     "User me doesn't exist"):
            self.db_users.delete('me', callback=self.stop_op)
            self.wait()

    @gen_test
    def test_delete_fails_coro(self):
        with self.assertRaisesRegexp(InfluxException,
                                     "User me doesn't exist"):
            yield self.db_users.delete('me')

    def test_repr(self):
        host = 'localhost'
        port = 8086
        client = AsyncfluxClient(host, port)
        self.assertEqual(repr(Users(client)),
                         ("Users(AsyncfluxClient('%s', %d))" %
                          (host, port)))
