# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.shardspace import ShardSpace
from asyncflux.testing import AsyncfluxTestCase, gen_test


class ShardSpaceTestCase(AsyncfluxTestCase):

    def test_class_instantiation(self):
        client = AsyncfluxClient()

        name = 'default'
        database = 'foo'
        regex = '/.*/'
        retention_policy = 'inf'
        shard_duration = '7d'
        replication_factor = 5
        split = 3
        shard_space = ShardSpace(client, name=name, database=database,
                                 regex=regex, retention_policy=retention_policy,
                                 shard_duration=shard_duration,
                                 replication_factor=replication_factor,
                                 split=split)
        self.assertIsInstance(shard_space.database, Database)
        self.assertEqual(shard_space.database.name, database)

        database = Database(client, 'foo')
        shard_space = ShardSpace(client, name=name, database=database,
                                 regex=regex, retention_policy=retention_policy,
                                 shard_duration=shard_duration,
                                 replication_factor=replication_factor,
                                 split=split)
        self.assertIsInstance(shard_space.database, Database)
        self.assertEqual(shard_space.database.name, database.name)

    @gen_test
    def test_get_shard_spaces(self):
        client = AsyncfluxClient()
        shard_spaces = [{'name': 'default', 'database': 'foo', 'regex': '/.*/',
                         'retentionPolicy': 'inf', 'shardDuration': '7d',
                         'replicationFactor': 1, 'split': 1},
                        {'name': 'second', 'database': 'bar', 'regex': '/.*/',
                         'retentionPolicy': '365d', 'shardDuration': '15m',
                         'replicationFactor': 5, 'split': 5},
                        {'name': 'default', 'database': 'bar', 'regex': '/.*/',
                         'retentionPolicy': 'inf', 'shardDuration': '7d',
                         'replicationFactor': 1, 'split': 1}]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=shard_spaces)
            response = yield client.get_shard_spaces()
            for actual, expected in zip(response, shard_spaces):
                self.assertIsInstance(actual, ShardSpace)
                self.assertEqual(actual.client, client)
                self.assertEqual(actual.name, expected['name'])
                self.assertIsInstance(actual.database, Database)
                self.assertEqual(actual.database.name, expected['database'])
                self.assertEqual(actual.regex, expected['regex'])
                self.assertEqual(actual.retention_policy,
                                 expected['retentionPolicy'])
                self.assertEqual(actual.shard_duration,
                                 expected['shardDuration'])
                self.assertEqual(actual.replication_factor,
                                 expected['replicationFactor'])
                self.assertEqual(actual.split, expected['split'])

                self.assert_mock_args(m, '/cluster/shard_spaces')
