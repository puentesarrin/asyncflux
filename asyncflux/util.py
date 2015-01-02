# -*- coding: utf-8 -*-
"""General-purpose utilities"""
import functools

from tornado import gen


class InfluxException(Exception):

    def __init__(self, http_response):
        self.response = http_response
        self.message = http_response.body
        super(InfluxException, self).__init__(self.message)


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
