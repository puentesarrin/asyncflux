# -*- coding: utf-8 -*-
from asyncflux import AsyncfluxClient
from asyncflux.testing import AsyncfluxTestCase


class TestAsyncFluxCoroutine(AsyncfluxTestCase):

    def test_non_callable(self):
        client = AsyncfluxClient()
        with self.assertRaisesRegexp(TypeError, 'callback must be a callable'):
            client.get_databases(callback='this is not a callable')
