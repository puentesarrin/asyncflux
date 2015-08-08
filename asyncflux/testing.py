# -*- coding: utf-8 -*-
"""Unit testing support for asynchronous code"""
import functools
import json
import mock

from collections import OrderedDict
try:
    from StringIO import StringIO
except ImportError:  # pragma: no cover
    from io import StringIO  # pragma: no cover
try:
    from urlparse import urlparse
except ImportError:  # pragma: no cover
    from urllib.parse import urlparse  # pragma: no cover

from tornado import httputil
from tornado.escape import parse_qs_bytes
from tornado.gen import coroutine, Return
from tornado.httpclient import HTTPError, HTTPRequest, HTTPResponse
from tornado.testing import AsyncTestCase, gen_test

__all__ = ('AsyncfluxTestCase', 'gen_test', )


def __sanitize_request_url(method):

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        args_type = type(args[0])
        request_args, request_kwargs = args[0]

        url = request_args[0]
        base_url, qs = url[:url.find('?')], parse_qs_bytes(urlparse(url).query)
        ordered_qs = OrderedDict(sorted(qs.items(), key=lambda x: x[1]))
        sanitized_url = httputil.url_concat(base_url, ordered_qs)

        args = (args_type([(sanitized_url, ), request_kwargs, ]), )
        return method(self, *args, **kwargs)

    return wrapper

mock.NonCallableMock._call_matcher = \
    __sanitize_request_url(mock.NonCallableMock._call_matcher)


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

    def assert_mock_args(self, fetch_mock, path, query=None, qs=None,
                         method='GET', body=None, auth_username='root',
                         auth_password='root', *args, **kwargs):
        qs = qs or {}
        if query:
            qs['q'] = query
        url = httputil.url_concat('http://localhost:8086{}'.format(path), qs)
        fetch_mock.assert_called_once_with(url, method=method, body=body,
                                           auth_username=auth_username,
                                           auth_password=auth_password,
                                           *args, **kwargs)

    def stop_op(self, result, error):
        if error:
            raise error
        super(AsyncfluxTestCase, self).stop(result)
