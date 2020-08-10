# -*- coding: utf-8 -*-

from os import environ

if 'UNITTEST' in environ:
    import tests
else:
    from .module import Module
