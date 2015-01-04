# -*- coding: utf-8 -*-


class AsyncfluxError(Exception):

    def __init__(self, http_response):
        self.response = http_response
        self.message = http_response.body
        super(AsyncfluxError, self).__init__(self.message)
