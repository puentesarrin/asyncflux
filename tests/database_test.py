# -*- coding: utf-8 -*-
from mock import call
from influxdb.exceptions import InfluxDBClientError

from asyncflux import AsyncfluxClient
from asyncflux.database import Database
from asyncflux.retentionpolicy import RetentionPolicy
from asyncflux.testing import AsyncfluxTestCase, gen_test
from asyncflux.user import User


class DatabaseTestCase(AsyncfluxTestCase):

    @gen_test
    def test_write(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400000000000\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data)

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name})

    @gen_test
    def test_write_non_default_retention_policy(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400000000000\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data, retention_policy='rp_name')

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name, 'rp': 'rp_name'})

    @gen_test
    def test_write_with_precision(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data, precision='s')

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name, 'precision': 's'})

    @gen_test
    def test_write_with_consistency(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400000000000\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data, consistency='all')

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name, 'consistency': 'all'})

    @gen_test
    def test_write_non_default_retention_policy_with_precision(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data, retention_policy='rp_name',
                                        precision='s')

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name, 'rp': 'rp_name',
                                      'precision': 's'})

    @gen_test
    def test_write_non_default_retention_policy_with_consistency(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400000000000\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data, retention_policy='rp_name',
                                        consistency='all')

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name, 'rp': 'rp_name',
                                      'consistency': 'all'})

    @gen_test
    def test_write_with_precision_and_consistency(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data, precision='s', consistency='all')

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name, 'precision': 's',
                                      'consistency': 'all'})

    @gen_test
    def test_write_with_all_arguments(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        data = {
            'measurement': 'cpu_load',
            'points': [
                {
                    'tags': {
                        'host': 'server01',
                        'region': 'us-east-a1'
                    },
                    'time': '2015-11-10T23:00:00Z',
                    'fields': {
                        'value': 0.55
                    }
                }
            ]
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write(data, retention_policy='rp_name',
                                        precision='s', consistency='all')

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name, 'rp': 'rp_name',
                                      'precision': 's', 'consistency': 'all'})

    @gen_test
    def test_write_points(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        points = [
            {
                'time': '2015-11-10T23:00:00Z',
                'fields': {
                    'value': 0.55
                }
            }
        ]
        body = ('cpu_load value=0.55 1447196400000000000\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write_points('cpu_load', points)

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name})

    @gen_test
    def test_write_points_with_tags(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        points = [
            {
                'time': '2015-11-10T23:00:00Z',
                'fields': {
                    'value': 0.55
                }
            }
        ]
        tags = {
            'host': 'server01',
            'region': 'us-east-a1'
        }
        body = ('cpu_load,host=server01,region=us-east-a1 value=0.55 '
                '1447196400000000000\n')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write_points('cpu_load', points,
                                               tags=tags)

            self.assert_mock_args(m, '/write', method='POST', body=body,
                                  qs={'db': db_name})

    @gen_test
    def test_write_points_batches(self):
        client = AsyncfluxClient()
        db_name = 'foo'
        points = [
            {
                'time': '2015-11-10T23:00:00Z',
                'fields': {
                    'value': 0.55
                }
            },
            {
                'time': '2015-10-10T22:00:00Z',
                'fields': {
                    'value': 0.62
                }
            }
        ]
        body1 = 'cpu_load value=0.55 1447196400000000000\n'
        body2 = 'cpu_load value=0.62 1444514400000000000\n'
        qs = {'db': db_name}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 204)
            yield client[db_name].write_points('cpu_load', points,
                                               batch_size=1)

            calls = [
                call('/write', method='POST', body=body1, qs=qs),
                call('/write', method='POST', body=body2, qs=qs)
            ]
            self.assert_multiple_mock_args(m, calls)

    @gen_test
    def test_get_measurements(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'measurements',
                            'columns': ['name'],
                            'values': [["cpu_load"]]
                        }
                    ]
                }
            ]
        }
        db_name = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].get_measurements()

            self.assertEqual(response, ['cpu_load'])
            self.assert_mock_args(m, '/query', query='SHOW MEASUREMENTS',
                                  qs={'db': db_name})

    @gen_test
    def test_get_measurements_with_tags(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'measurements',
                            'columns': ['name'],
                            'values': [["cpu_load"]]
                        }
                    ]
                }
            ]
        }
        db_name = 'foo'
        query = "SHOW MEASUREMENTS WHERE host='server01'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].get_measurements({'host':
                                                               'server01'})

            self.assertEqual(response, ['cpu_load'])
            self.assert_mock_args(m, '/query', query=query, qs={'db': db_name})

    @gen_test
    def test_get_tag_keys(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'cpu_load',
                            'columns': ['tagKey'],
                            'values': [['host'], ['region']]
                        },
                        {
                            'name': 'disk_free',
                            'columns': ['tagKey'],
                            'values': [['hostname']]
                        }
                    ]
                }
            ]
        }
        db_name = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].get_tag_keys()

            expected = [
                ('cpu_load', ['host', 'region']),
                ('disk_free', ['hostname'])
            ]
            self.assertListEqual(response, expected)
            self.assert_mock_args(m, '/query', query='SHOW TAG KEYS',
                                  qs={'db': db_name})

    @gen_test
    def test_get_tag_keys_with_measurement(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'cpu_load',
                            'columns': ['tagKey'],
                            'values': [['host'], ['region']]
                        }
                    ]
                }
            ]
        }
        db_name = 'foo'
        measurement = 'cpu_load'
        query = 'SHOW TAG KEYS FROM cpu_load'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].get_tag_keys(measurement)

            expected = [
                (measurement, ['host', 'region'])
            ]
            self.assertListEqual(response, expected)
            self.assert_mock_args(m, '/query', query=query, qs={'db': db_name})

    @gen_test
    def test_get_tag_values(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'hostTagValues',
                            'columns': ['host'],
                            'values': [
                                ['server01'], ['server02'], ['server03']
                            ]
                        }
                    ]
                }
            ]
        }
        query = 'SHOW TAG VALUES WITH KEY IN ("host")'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.foo.get_tag_values('host')

            expected = [('host', ['server01', 'server02', 'server03'])]
            self.assertListEqual(response, expected)
            self.assert_mock_args(m, '/query', query=query, qs={'db': 'foo'})

    @gen_test
    def test_get_tag_values_from_measurement(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'regionTagValues',
                            'columns': ['region'],
                            'values': [['us-east-a1']]
                        }
                    ]
                }
            ]
        }
        query = 'SHOW TAG VALUES FROM "cpu_load" WITH KEY IN ("region")'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.foo.get_tag_values('region',
                                                       measurement='cpu_load')

            self.assertListEqual(response, [('region', ['us-east-a1'])])
            self.assert_mock_args(m, '/query', query=query, qs={'db': 'foo'})

    @gen_test
    def test_get_tag_values_multiple_keys(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'hostTagValues',
                            'columns': ['host'],
                            'values': [['server01']]
                        },
                        {
                            'name': 'regionTagValues',
                            'columns': ['region'],
                            'values': [['us-west-a1'], ['us-east-a1']]
                        }
                    ]
                }
            ]
        }
        query = 'SHOW TAG VALUES WITH KEY IN ("host","region")'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client.foo.get_tag_values(['host', 'region'])

            expected = [
                ('host', ['server01']),
                ('region', ['us-west-a1', 'us-east-a1'])
            ]
            self.assertListEqual(response, expected)
            self.assert_mock_args(m, '/query', query=query, qs={'db': 'foo'})

    @gen_test
    def test_get_series(self):
        client = AsyncfluxClient()
        serie_values = [
            [
                'cpu_load_short,host=server01,region=us-east-a1',
                'server01',
                'us-east-a1'
            ],
            [
                'cpu_load_short,host=server02,region=us-east-a1',
                'server02',
                'us-east-a1'
            ]
        ]
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'cpu_load',
                            'columns': ['_key', 'host', 'region'],
                            'values': serie_values
                        }
                    ]
                }
            ]
        }
        db_name = 'foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].get_series()

            self.assertEqual(len(response[0].get('tags', [])),
                             len(serie_values))
            self.assert_mock_args(m, '/query', query='SHOW SERIES',
                                  qs={'db': db_name})

    @gen_test
    def test_drop_series(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        query = 'DROP SERIES FROM "cpu_load"'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.foo.drop_series('cpu_load')

            self.assert_mock_args(m, '/query', query=query,
                                  qs={'db': db_name})

    @gen_test
    def test_drop_series_using_tags(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        query = "DROP SERIES FROM \"cpu_load\" WHERE region='us-east-a1'"

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.foo.drop_series('cpu_load',
                                         tags={'region': 'us-east-a1'})

            self.assert_mock_args(m, '/query', query=query,
                                  qs={'db': db_name})

    @gen_test
    def test_drop_series_non_existing_measurement(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [{'error': 'measurement not found: cpu_load'}]
        }
        db_name = 'foo'
        query = 'DROP SERIES FROM "cpu_load"'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.foo.drop_series('cpu_load')

            self.assertEqual(str(cm.exception),
                             'measurement not found: cpu_load')
            self.assert_mock_args(m, '/query', query=query,
                                  qs={'db': db_name})

    @gen_test
    def test_grant_privilege_to_using_string(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        privilege = 'ALL'
        username = 'foo'
        db_name = 'bar'
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].grant_privilege_to(privilege, username)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_privilege_to_using_class(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        privilege = 'ALL'
        user = User(client, 'foo')
        db_name = 'bar'
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].grant_privilege_to(privilege, user)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_grant_privilege_to_unsupported_type(self):
        client = AsyncfluxClient()
        privilege = 'ALL'
        db_name = 'bar'

        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^username_or_user must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield client[db_name].grant_privilege_to(privilege, None)

            self.assertFalse(m.called)

    @gen_test
    def test_grant_privilege_to_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        privilege = 'ALL'
        username = 'foo'
        db_name = 'bar'
        query = 'GRANT ALL ON bar TO foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].grant_privilege_to(privilege, username)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege_from_using_string(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        privilege = 'ALL'
        username = 'foo'
        db_name = 'bar'
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].revoke_privilege_from(privilege, username)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege_from_using_class(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        privilege = 'ALL'
        user = User(client, 'foo')
        db_name = 'bar'
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].revoke_privilege_from(privilege, user)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_revoke_privilege_from_unsupported_type(self):
        client = AsyncfluxClient()
        privilege = 'ALL'
        db_name = 'bar'

        with self.patch_fetch_mock(client) as m:
            re_exc_msg = r'^username_or_user must be an instance'
            with self.assertRaisesRegexp(TypeError, re_exc_msg):
                yield client[db_name].revoke_privilege_from(privilege, None)

            self.assertFalse(m.called)

    @gen_test
    def test_revoke_privilege_from_non_existing_user(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'user not found'}]}
        privilege = 'ALL'
        username = 'foo'
        db_name = 'bar'
        query = 'REVOKE ALL ON bar FROM foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].revoke_privilege_from(privilege, username)

            self.assertEqual(str(cm.exception), 'user not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_get_retention_policies(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'columns': [
                                'name', 'duration', 'replicaN', 'default'
                            ],
                            'values': [
                                ['default', '0', 1, True]
                            ]
                        }
                    ]
                }
            ]
        }
        db_name = 'foo'
        query = 'SHOW RETENTION POLICIES ON foo'
        retentions = [{
            'name': 'default',
            'duration': '0',
            'replicaN': 1,
            'default': True
        }]

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].get_retention_policies()

            self.assertEqual(len(response), len(retentions))
            for raw, retention in zip(response, retentions):
                self.assertIsInstance(raw, RetentionPolicy)
                self.assertEqual(raw.name, retention['name'])
                self.assertEqual(raw.duration, retention['duration'])
                self.assertEqual(raw.replication, retention['replicaN'])
                self.assertEqual(raw.default, retention['default'])
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_get_retention_policy_names(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'columns': [
                                'name', 'duration', 'replicaN', 'default'
                            ],
                            'values': [
                                ['default', '0', 1, True]
                            ]
                        }
                    ]
                }
            ]
        }
        db_name = 'foo'
        query = 'SHOW RETENTION POLICIES ON foo'
        retention_names = ['default']

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].get_retention_policy_names()

            self.assertEqual(len(response), len(retention_names))
            for raw, retention in zip(response, retention_names):
                self.assertEqual(raw, retention)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_get_retention_policy_names_non_existing_database(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'database not found'}]}
        db_name = 'foo'
        query = 'SHOW RETENTION POLICIES ON foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].get_retention_policy_names()

            self.assertEqual(str(cm.exception), 'database not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_create_retention_policy(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'CREATE RETENTION POLICY bar ON foo DURATION 30d REPLICATION 1'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].create_retention_policy(rp_name,
                                                                     '30d', 1)

            self.assertIsInstance(response, RetentionPolicy)
            self.assertEqual(response.name, rp_name)
            self.assertEqual(response.duration, '30d')
            self.assertEqual(response.replication, 1)
            self.assertEqual(response.default, False)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_create_retention_policy_default(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = ('CREATE RETENTION POLICY bar ON foo DURATION 30d '
                 'REPLICATION 1 DEFAULT')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            response = yield client[db_name].create_retention_policy(rp_name,
                                                                     '30d', 1,
                                                                     True)

            self.assertIsInstance(response, RetentionPolicy)
            self.assertEqual(response.name, rp_name)
            self.assertEqual(response.duration, '30d')
            self.assertEqual(response.replication, 1)
            self.assertEqual(response.default, True)
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_create_retention_policy_non_existing_database(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'database not found'}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'CREATE RETENTION POLICY bar ON foo DURATION 30d REPLICATION 1'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].create_retention_policy(rp_name, '30d',
                                                              1)

            self.assertEqual(str(cm.exception), 'database not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_create_retention_policy_already_existing_retention(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [{'error': 'retention policy already exists'}]
        }
        db_name = 'foo'
        rp_name = 'bar'
        query = 'CREATE RETENTION POLICY bar ON foo DURATION 30d REPLICATION 1'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].create_retention_policy(rp_name, '30d',
                                                              1)

            self.assertEqual(str(cm.exception),
                             'retention policy already exists')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_duration(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 5d'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].alter_retention_policy(rp_name,
                                                         duration='5d')

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_replication(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo REPLICATION 2'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].alter_retention_policy(rp_name,
                                                         replication=2)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_default(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo DEFAULT'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].alter_retention_policy(rp_name,
                                                         default=True)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_duration_and_replication(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 5d REPLICATION 2'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].alter_retention_policy(rp_name,
                                                         duration='5d',
                                                         replication=2)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_duration_and_default(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 5d DEFAULT'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].alter_retention_policy(rp_name,
                                                         duration='5d',
                                                         default=True)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_replication_and_default(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo REPLICATION 2 DEFAULT'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].alter_retention_policy(rp_name,
                                                         replication=2,
                                                         default=True)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_all_arguments(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = ('ALTER RETENTION POLICY bar ON foo DURATION 5d REPLICATION 2 '
                 'DEFAULT')

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].alter_retention_policy(rp_name,
                                                         duration='5d',
                                                         replication=2,
                                                         default=True)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_non_existing_database(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'database not found'}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 30d REPLICATION 1'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].alter_retention_policy(rp_name, '30d', 1)

            self.assertEqual(str(cm.exception), 'database not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_alter_retention_policy_non_existing_retention(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'retention policy not found'}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'ALTER RETENTION POLICY bar ON foo DURATION 30d REPLICATION 1'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].alter_retention_policy(rp_name, '30d', 1)

            self.assertEqual(str(cm.exception), 'retention policy not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop_retention_policy(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'DROP RETENTION POLICY bar ON foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client[db_name].drop_retention_policy(rp_name)

            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop_retention_policy_non_existing_database(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'database not found'}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'DROP RETENTION POLICY bar ON foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].drop_retention_policy(rp_name)

            self.assertEqual(str(cm.exception), 'database not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop_retention_policy_non_existing_retention(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'retention policy not found'}]}
        db_name = 'foo'
        rp_name = 'bar'
        query = 'DROP RETENTION POLICY bar ON foo'

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client[db_name].drop_retention_policy(rp_name)

            self.assertEqual(str(cm.exception), 'retention policy not found')
            self.assert_mock_args(m, '/query', query=query)

    @gen_test
    def test_drop(self):
        client = AsyncfluxClient()
        response_body = {'results': [{}]}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.foo.drop()

            self.assert_mock_args(m, '/query', query='DROP DATABASE foo')

    @gen_test
    def test_drop_non_existing_one(self):
        client = AsyncfluxClient()
        response_body = {'results': [{'error': 'database not found: foo'}]}

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            with self.assertRaises(InfluxDBClientError) as cm:
                yield client.foo.drop()

            self.assertEqual(str(cm.exception), 'database not found: foo')
            self.assert_mock_args(m, '/query', query='DROP DATABASE foo')

    def test_repr(self):
        host = 'localhost'
        port = 8086
        client = AsyncfluxClient(host, port)
        db_name = 'db'
        self.assertEqual(repr(Database(client, db_name)),
                         ("Database(AsyncfluxClient('%s', %d), '%s')" %
                          (host, port, db_name)))
