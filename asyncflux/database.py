# -*- coding: utf-8 -*-
"""Database level operations"""
from tornado import gen
from tornado.util import basestring_type

from influxdb.line_protocol import make_lines

from asyncflux import retentionpolicy, user
from asyncflux.util import asyncflux_coroutine, batches


class Database(object):

    BATCH_SIZE = 5000

    def __init__(self, client, name):
        self.__client = client
        self.__name = name

    @property
    def client(self):
        return self.__client

    @property
    def name(self):
        return self.__name

    @asyncflux_coroutine
    def query(self, query, params=None, raise_errors=True):
        result_set = yield self.client.query(query, params, database=self.name,
                                             raise_errors=raise_errors)
        raise gen.Return(result_set)

    @asyncflux_coroutine
    def write(self, data, retention_policy=None, precision=None,
              consistency=None):
        qs = {'db': self.name}
        if retention_policy:
            qs['rp'] = retention_policy
        if precision:
            qs['precision'] = precision
        if consistency:
            qs['consistency'] = consistency
        body = make_lines(data, precision)
        yield self.client.request('/write', method='POST', qs=qs, body=body)

    @asyncflux_coroutine
    def write_points(self, measurement, points, tags=None,
                     retention_policy=None, precision=None, consistency=None,
                     batch_size=None):
        batch_size = batch_size or self.BATCH_SIZE
        for batch in batches(points, batch_size):
            data = {
                'measurement': measurement,
                'points': batch
            }
            if tags:
                data['tags'] = tags

            yield self.write(data, retention_policy=retention_policy,
                             precision=precision, consistency=consistency)

    @asyncflux_coroutine
    def get_measurements(self, tags=None):
        tags = tags or {}
        query_list = ['SHOW MEASUREMENTS']
        if tags:
            tags_str = ' and '.join(["{}='{}'".format(k, v)
                                     for k, v in tags.items()])
            query_list.append('WHERE {}'.format(tags_str))
        result_set = yield self.query(' '.join(query_list))
        measurements = [
            point['name']
            for point
            in result_set[0].get_points()
        ]
        raise gen.Return(measurements)

    @asyncflux_coroutine
    def get_series(self):
        result_set = yield self.query('SHOW SERIES')
        series = []
        for serie in result_set[0].items():
            series.append({'name': serie[0][0],
                           'tags': list(serie[1])})
        raise gen.Return(series)

    @asyncflux_coroutine
    def drop_series(self, measurement=None, tags=None):
        query_list = ['DROP SERIES']
        if measurement:
            query_list.append('FROM "{}"'.format(measurement))
        if tags:
            tags_str = ' and '.join(["{}='{}'".format(k, v)
                                     for k, v in tags.items()])
            query_list.append('WHERE {}'.format(tags_str))
        yield self.query(' '.join(query_list))

    def __get_username(self, username_or_user):
        username = username_or_user
        if isinstance(username, user.User):
            username = username_or_user.name
        if not isinstance(username, basestring_type):
            raise TypeError("username_or_user must be an instance of "
                            "%s or User" % (basestring_type.__name__,))
        return username

    @asyncflux_coroutine
    def grant_privilege_to(self, privilege, username_or_user):
        username = self.__get_username(username_or_user)
        yield self.client.grant_privilege(privilege, username, self.name)

    @asyncflux_coroutine
    def revoke_privilege_from(self, privilege, username_or_user):
        username = self.__get_username(username_or_user)
        yield self.client.revoke_privilege(privilege, username, self.name)

    @asyncflux_coroutine
    def get_retention_policies(self):
        query_str = 'SHOW RETENTION POLICIES ON {}'.format(self.name)
        result_set = yield self.client.query(query_str)
        retention_policies = [
            retentionpolicy.RetentionPolicy(self, point['name'],
                                            point['duration'],
                                            point['replicaN'],
                                            point['default'])
            for point
            in result_set[0].get_points()
        ]
        raise gen.Return(retention_policies)

    @asyncflux_coroutine
    def get_retention_policy_names(self):
        query_str = 'SHOW RETENTION POLICIES ON {}'.format(self.name)
        result_set = yield self.client.query(query_str)
        retention_policies = [
            point['name']
            for point
            in result_set[0].get_points()
        ]
        raise gen.Return(retention_policies)

    @asyncflux_coroutine
    def create_retention_policy(self, retention_name, duration, replication,
                                default=False):
        query_format = ('CREATE RETENTION POLICY {} ON {} '
                        'DURATION {} REPLICATION {}')
        query_list = [
            query_format.format(retention_name, self.name, duration,
                                replication)
        ]
        if default:
            query_list.append('DEFAULT')
        yield self.client.query(' '.join(query_list))
        new_retention_policy = retentionpolicy.RetentionPolicy(self,
                                                               retention_name,
                                                               duration,
                                                               replication,
                                                               default)
        raise gen.Return(new_retention_policy)

    @asyncflux_coroutine
    def alter_retention_policy(self, retention_name, duration=None,
                               replication=None, default=False):
        query_list = ['ALTER RETENTION POLICY {} ON {}'.format(retention_name,
                                                               self.name)]
        if duration:
            query_list.append('DURATION {}'.format(duration))
        if replication:
            query_list.append('REPLICATION {}'.format(replication))
        if default:
            query_list.append('DEFAULT')
        yield self.client.query(' '.join(query_list))

    @asyncflux_coroutine
    def drop_retention_policy(self, retention_name):
        query_str = 'DROP RETENTION POLICY {} ON {}'.format(retention_name,
                                                            self.name)
        yield self.client.query(query_str)

    @asyncflux_coroutine
    def drop(self):
        yield self.client.drop_database(self.name)

    def __repr__(self):
        return "Database(%r, %r)" % (self.client, self.name)
