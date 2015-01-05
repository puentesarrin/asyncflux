# -*- coding: utf-8 -*-
"""Test the asyncflux module itself."""
import unittest
import asyncflux


class TestAsyncflux(unittest.TestCase):

    def test_asyncflux_client_alias(self):
        # Testing that asyncflux module imports client.AsyncfluxClient
        c = asyncflux.AsyncfluxClient()
        self.assertIsNotNone(c)

    def test_version_string(self):
        asyncflux.version_tuple = (0, 0, 0)
        self.assertEqual(asyncflux.get_version_string(), '0.0.0')
        asyncflux.version_tuple = (1, 0, 0)
        self.assertEqual(asyncflux.get_version_string(), '1.0.0')
        asyncflux.version_tuple = (5, 0, '+')
        self.assertEqual(asyncflux.get_version_string(), '5.0+')
        asyncflux.version_tuple = (0, 4, 'b')
        self.assertEqual(asyncflux.get_version_string(), '0.4b')
