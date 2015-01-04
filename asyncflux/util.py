# -*- coding: utf-8 -*-
"""General-purpose utilities"""
import functools
import re

from tornado import gen


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

_SNAKE_RE = re.compile('(?!^)([A-Z]+)')


def snake_case(string):
    return re.sub(_SNAKE_RE, r'_\1', string).lower()


def snake_case_dict(_dict):
    raw_dict = _dict.copy()
    result = {}
    try:
        while 1:
            key, value = raw_dict.popitem()
            result[snake_case(key)] = value
    except KeyError:
        return result
