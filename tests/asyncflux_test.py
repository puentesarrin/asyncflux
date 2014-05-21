# -*- coding: utf-8 -*-
"""Test the asyncflux module itself."""
import unittest
import asyncflux


class TestAsyncflux(unittest.TestCase):

    def test_asyncflux_client_alias(self):
        # Testing that asyncflux module imports client.AsyncfluxClient
        c = asyncflux.AsyncfluxClient()
