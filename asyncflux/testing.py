# -*- coding: utf-8 -*-
"""Unit testing support for asynchronous code"""
import json
import mock
try:
    from StringIO import StringIO
except:
    from io import StringIO

from tornado.gen import coroutine, Return
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
from tornado.testing import AsyncTestCase, gen_test

__all__ = ('AsyncfluxTestCase', 'gen_test', )


class AsyncfluxTestCase(AsyncTestCase):

    def patch_fetch_mock(self, client):
        return mock.patch.object(client.http_client, 'fetch')

    def setup_fetch_mock(self, fetch_mock, status_code, **kwargs):
        @coroutine
        def side_effect(request, **_):
            if request is not HTTPRequest:
                request = HTTPRequest(request)
            body = kwargs.pop('body', None)
            if body:
                if isinstance(body, (dict, list)):
                    body = json.dumps(body)
                kwargs['buffer'] = StringIO(body)
            response = HTTPResponse(request, status_code, **kwargs)
            if status_code < 200 or status_code >= 300:
                raise HTTPError(status_code, response=response)
            raise Return(response)

        fetch_mock.side_effect = side_effect

    def assert_mock_args(self, fetch_mock, path, method='GET', body=None,
                         auth_username='root', auth_password='root', *args,
                         **kwargs):
        url = 'http://localhost:8086' + path
        fetch_mock.assert_called_once_with(url, method=method, body=body,
                                           auth_username=auth_username,
                                           auth_password=auth_password,
                                           *args, **kwargs)

    def stop_op(self, result, error):
        if error:
            raise error
        super(AsyncfluxTestCase, self).stop(result)
