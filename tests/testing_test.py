# -*- coding: utf-8 -*-
from tornado.httpclient import HTTPError

from asyncflux import AsyncfluxClient
from asyncflux.testing import AsyncfluxTestCase, gen_test


class HTTPMockClientTestCase(AsyncfluxTestCase):

    @gen_test
    def test_http_request_ok(self):
        client = AsyncfluxClient()
        response_body = {
            'results': [
                {
                    'series': [
                        {
                            'name': 'databases',
                            'columns': ['name'],
                            'values': [['foo'], ['bar']]
                        }
                    ]
                }
            ]
        }

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 200, body=response_body)
            yield client.get_database_names()

            self.assert_mock_args(m, '/query', query='SHOW DATABASES')

    @gen_test
    def test_http_request_error(self):
        client = AsyncfluxClient()
        response_body = {
            'error': ('error parsing query: found EOF, expected CONTINUOUS, '
                      'DATABASES, FIELD, GRANTS, MEASUREMENTS, RETENTION, '
                      'SERIES, SERVERS, TAG, USERS at line 1, char 6')
        }

        with self.patch_fetch_mock(client) as m:
            self.setup_fetch_mock(m, 400, body=response_body)
            with self.assertRaises(HTTPError):
                yield client.query('SHOW')

            self.assert_mock_args(m, '/query', query='SHOW')
