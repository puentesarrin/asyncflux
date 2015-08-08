# -*- coding: utf-8 -*-
from influxdb.exceptions import InfluxDBClientError

from asyncflux import AsyncfluxClient
from asyncflux.retentionpolicy import RetentionPolicy
from asyncflux.testing import AsyncfluxTestCase, gen_test


class RetentionPolicyTestCase(AsyncfluxTestCase):

    @gen_test
    def test_alter_duration(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 30d'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.alter(duration='30d')

            self.assertEqual(retention_policy.name, rp_name)
            self.assertEqual(retention_policy.duration, '30d')
            self.assertEqual(retention_policy.replication, 1)
            self.assertFalse(retention_policy.default)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_replication(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'ALTER RETENTION POLICY bar ON foo REPLICATION 2'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.alter(replication=2)

            self.assertEqual(retention_policy.name, rp_name)
            self.assertEqual(retention_policy.duration, '5d')
            self.assertEqual(retention_policy.replication, 2)
            self.assertFalse(retention_policy.default)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_default(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'ALTER RETENTION POLICY bar ON foo DEFAULT'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.alter(default=True)

            self.assertEqual(retention_policy.name, rp_name)
            self.assertEqual(retention_policy.duration, '5d')
            self.assertEqual(retention_policy.replication, 1)
            self.assertTrue(retention_policy.default)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_duration_and_replication(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 30d REPLICATION 2'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.alter(duration='30d', replication=2)

            self.assertEqual(retention_policy.name, rp_name)
            self.assertEqual(retention_policy.duration, '30d')
            self.assertEqual(retention_policy.replication, 2)
            self.assertFalse(retention_policy.default)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_duration_and_default(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 30d DEFAULT'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.alter(duration='30d', default=True)

            self.assertEqual(retention_policy.name, rp_name)
            self.assertEqual(retention_policy.duration, '30d')
            self.assertEqual(retention_policy.replication, 1)
            self.assertTrue(retention_policy.default)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_replication_and_default(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'ALTER RETENTION POLICY bar ON foo REPLICATION 2 DEFAULT'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.alter(replication=2, default=True)

            self.assertEqual(retention_policy.name, rp_name)
            self.assertEqual(retention_policy.duration, '5d')
            self.assertEqual(retention_policy.replication, 2)
            self.assertTrue(retention_policy.default)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_all_arguments(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = ('ALTER RETENTION POLICY bar ON foo DURATION 30d REPLICATION 2 '
                 'DEFAULT')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.alter(duration='30d', replication=2,
                                         default=True)

            self.assertEqual(retention_policy.name, rp_name)
            self.assertEqual(retention_policy.duration, '30d')
            self.assertEqual(retention_policy.replication, 2)
            self.assertTrue(retention_policy.default)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_non_existing_retention_one(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'retention policy not found'}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 30d REPLICATION 1'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield retention_policy.alter(duration='30d', replication=1)

            self.assertEqual(str(cm.exception), 'retention policy not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'DROP RETENTION POLICY bar ON foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield retention_policy.drop()

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop_non_existing_database(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'database not found'}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'DROP RETENTION POLICY bar ON foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield retention_policy.drop()

            self.assertEqual(str(cm.exception), 'database not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop_non_existing_one(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'retention policy not found'}]}
        database = client.foo
        rp_name = 'bar'
        retention_policy = RetentionPolicy(database, rp_name, '5d', 1)
        query = 'DROP RETENTION POLICY bar ON foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield retention_policy.drop()

            self.assertEqual(str(cm.exception), 'retention policy not found')
            self.assert_mock_args(m, '/query', query=query)
