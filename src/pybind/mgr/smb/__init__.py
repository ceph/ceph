# enable unit test automagical mocks
import os

if 'UNITTEST' in os.environ:
    import tests  # noqa: F401

from .module import Module

__all__ = ['Module']
