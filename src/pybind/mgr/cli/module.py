"""
CLI Module - expose Ceph-Mgr Python API via CLI
"""
import functools
import inspect
import json
import logging
import re
from threading import Event
from typing import Callable, Any

from mgr_module import MgrModule, HandleCommandResult, CLICommand, API


logger = logging.getLogger()

class CephCommander:
    """
    Utility class to inspect Python functions and generate corresponding
    CephCommand signatures (see src/mon/MonCommand.h for details)
    """

    def __init__(self, func: Callable):
        self.func = func
        self.signature = inspect.signature(func)
        self.params = self.signature.parameters

    def to_ceph_signature(self):
        """
        Generate CephCommand signature (dict-like)
        """
        return {
            'prefix': f'mgr cli {self.func.__name__}',
            'args': self.format_args(),
            'desc': self.format_desc(),
            'perm': API.perm.get(self.func),
        }

    def format_args(self):
        params = iter(self.params.values())
        # Skip first param ('self')
        next(params)
        return ' '.join(self.format_param(param) for param in params)

    def format_desc(self):
        # Remove all whitespace
        # TODO: try to parse docstring (first line, params, etc.)
        return ' '.join(self.func.__doc__.split()) if self.func.__doc__ else ''

    @classmethod
    def format_param(cls, param: inspect.Parameter):
        return f'name={param.name},type={cls.python_type_to_ceph_type(param.annotation)},req={cls.is_required(param.default)}'

    @staticmethod
    def is_required(default: Any):
        return (default == inspect.Parameter.empty)

    @staticmethod
    def python_type_to_ceph_type(_type: type):
        PY_TO_CEPH = {
            str: 'CephString',
            int: 'CephInt'
        }
        return PY_TO_CEPH.get(_type, 'CephArgtype')


class MgrAPIReflector(type):
    """
    Metaclass to register COMMANDS and Command Handlers via CLICommand
    decorator
    """

    def __new__(cls, name, bases, dct):
        klass = super().__new__(cls, name, bases, dct)
        for base in bases:
            for name, func in inspect.getmembers(base, cls.is_public):
                # However not necessary (CLICommand uses a registry)
                # save functions to klass._cli_{name}() methods. This
                # can help on unit testing
                setattr(
                    klass,
                    f'_cli_{name}',
                    CLICommand(**CephCommander(func).to_ceph_signature())(
                        functools.partial(cls.func_wrapper, func))
                )
        return klass

    @staticmethod
    def is_public(func: Callable):
        return (
            inspect.isfunction(func) and
            not func.__name__.startswith('_') and
            not API.hook.get(func) and
            not API.internal.get(func)
        )

    @staticmethod
    def func_wrapper(func, self, *args, **kwargs):
        return HandleCommandResult(
            0,
            json.dumps(func(self, *args, **kwargs), sort_keys=True, indent=4)
        )


class CLI(MgrModule, metaclass=MgrAPIReflector):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop = Event()

    def serve(self):
        logger.info("Starting")
        while not self.stop.wait(self.get_ceph_option('mgr_tick_period')):
            pass

    def shutdown(self):
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        logger.info('Stopping')
        self.stop.set()
