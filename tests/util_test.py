# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.testing import AsyncfluxTestCase
from asyncflux.util import batches, sanitize_url


class TestAsyncfluxCoroutine(AsyncfluxTestCase):

    def test_non_callable(self):
        client = AsyncfluxClient()
        with self.assertRaisesRegexp(TypeError, 'callback must be a callable'):
            client.get_databases(callback='this is not a callable')


class SanitizeURLTestCase(AsyncfluxTestCase):

    def test_sanitize_url(self):
        url1 = 'http://puentesarr.in/?foo=bar&fu=baz'
        url2 = 'http://puentesarr.in/?fu=baz&foo=bar'
        sanitized_url1 = sanitize_url(url1)
        sanitized_url2 = sanitize_url(url2)

        self.assertEqual(sanitized_url1, sanitized_url2)


class BatchesGeneratorTestCase(AsyncfluxTestCase):

    def test_batches(self):
        result = batches(range(10), 3)
        self.assertEquals(list(result), [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]])

        result = batches(range(3), 3)
        self.assertEquals(list(result), [[0, 1, 2]])

        result = batches([], 3)
        self.assertEquals(list(result), [])
