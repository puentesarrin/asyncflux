# -*- coding: utf-8 -*-
"""Asynchronous client for InfluxDB and Tornado."""
import sys

__all__ = ('__author__', '__since__', '__version__', 'version',
           'AsyncfluxClient', )

version_tuple = (0, 0, '+')

if sys.version_info[0] >= 3:
    basestring = str  # pragma: no cover
else:
    basestring = basestring  # pragma: no cover


def get_version_string():
    if isinstance(version_tuple[-1], basestring):
        return '.'.join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return '.'.join(map(str, version_tuple))

version = get_version_string()
"""Current version of Asyncflux."""

__author__ = 'Jorge Puente-Sarrín <puentesarrin@gmail.com>'
__since__ = '2014-05-18'
__version__ = version


from asyncflux.client import AsyncfluxClient
