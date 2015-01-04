# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.testing import AsyncfluxTestCase
from asyncflux.util import snake_case, snake_case_dict


class TestAsyncfluxCoroutine(AsyncfluxTestCase):

    def test_non_callable(self):
        client = AsyncfluxClient()
        with self.assertRaisesRegexp(TypeError, 'callback must be a callable'):
            client.get_databases(callback='this is not a callable')


class TestSnakeCase(AsyncfluxTestCase):

    def test_snake_case(self):
        values = [
            ('CamelCase', 'camel_case'),
            ('isAdmin', 'is_admin'),
            ('writeTo', 'write_to'),
            ('readFrom', 'read_from')
        ]
        for raw_string, snake in values:
            self.assertEqual(snake_case(raw_string), snake)

    def test_snake_case_dict(self):
        raw_dict = {
            'name': 'foo',
            'isAdmin': False,
            'writeTo': '.*',
            'readFrom': '.*'
        }
        snake_dict = {
            'name': 'foo',
            'is_admin': False,
            'write_to': '.*',
            'read_from': '.*'
        }
        self.assertDictEqual(snake_case_dict(raw_dict), snake_dict)
