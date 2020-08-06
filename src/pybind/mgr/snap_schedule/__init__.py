# -*- coding: utf-8 -*-

from os import environ

if 'UNITTEST' in environ:
    import tests

from .module import Module
