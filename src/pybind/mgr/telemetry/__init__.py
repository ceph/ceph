import os

if 'UNITTEST' in os.environ:
    import tests

try:
    from .module import Module
except ImportError:
    pass
