# -*- coding: utf-8 -*-
"""Asynchronous client for InfluxDB and Tornado."""
version_tuple = (0, 0, '+')

try:
    basestring
except:
    basestring = str


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
