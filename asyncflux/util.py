# -*- coding: utf-8 -*-
"""General-purpose utilities"""
import functools

from collections import OrderedDict

from tornado import gen, httputil
from tornado.escape import parse_qs_bytes
try:
    from urlparse import urlparse
except ImportError:  # pragma: no cover
    from urllib.parse import urlparse  # pragma: no cover
try:
    xrange
except NameError:  # pragma: no cover
    xrange = range  # pragma: no cover


def asyncflux_coroutine(f):
    """A coroutine that accepts an optional callback.

    Given a callback, the function returns None, and the callback is run
    with (result, error). Without a callback the function returns a Future.
    """
    coro = gen.coroutine(f)

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        callback = kwargs.pop('callback', None)
        if callback and not callable(callback):
            raise TypeError("callback must be a callable")
        future = coro(*args, **kwargs)
        if callback:
            def _callback(future):
                try:
                    result = future.result()
                    callback(result, None)
                except Exception as e:
                    callback(None, e)
            future.add_done_callback(_callback)
        else:
            return future
    return wrapper


def sanitize_url(url):
    base_url, qs = url[:url.find('?')], parse_qs_bytes(urlparse(url).query)
    ordered_qs = OrderedDict(sorted(qs.items(), key=lambda x: x[1]))
    return httputil.url_concat(base_url, ordered_qs)


def batches(source, batch_size):
    source = iter(source)
    while True:
        batch = []
        for i in xrange(batch_size):
            try:
                batch.append(next(source))
            except StopIteration:
                if batch:
                    yield batch
                raise StopIteration
        yield batch
