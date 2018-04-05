# -*- coding: utf-8 -*-
from __future__ import absolute_import


class NoCredentialsException(Exception):
    def __init__(self):
        super(Exception, self).__init__(
            'No RGW credentials found, '
            'please consult the documentation on how to enable RGW for '
            'the dashboard.')
