import concurrent.futures
import functools
import inspect
import logging
import time
import errno
from typing import Any, Callable, Dict, List

from mgr_module import MgrModule, HandleCommandResult, CLICommand, API

logger = logging.getLogger()
get_time = time.perf_counter


def pretty_json(obj: Any) -> Any:
    import json
    return json.dumps(obj, sort_keys=True, indent=2)


class CephCommander:
    """
    Utility class to inspect Python functions and generate corresponding
    CephCommand signatures (see src/mon/MonCommand.h for details)
    """

    def __init__(self, func: Callable):
        self.func = func
        self.signature = inspect.signature(func)
        self.params = self.signature.parameters

    def to_ceph_signature(self) -> Dict[str, str]:
        """
        Generate CephCommand signature (dict-like)
        """
        return {
            'prefix': f'mgr cli {self.func.__name__}',
            'perm': API.perm.get(self.func)
        }


class MgrAPIReflector(type):
    """
    Metaclass to register COMMANDS and Command Handlers via CLICommand
    decorator
    """

    def __new__(cls, name, bases, dct):  # type: ignore
        klass = super().__new__(cls, name, bases, dct)
        cls.threaded_benchmark_runner = None
        for base in bases:
            for name, func in inspect.getmembers(base, cls.is_public):
                # However not necessary (CLICommand uses a registry)
                # save functions to klass._cli_{n}() methods. This
                # can help on unit testing
                wrapper = cls.func_wrapper(func)
                command = CLICommand(**CephCommander(func).to_ceph_signature())(  # type: ignore
                    wrapper)
                setattr(
                    klass,
                    f'_cli_{name}',
                    command)
        return klass

    @staticmethod
    def is_public(func: Callable) -> bool:
        return (
            inspect.isfunction(func)
            and not func.__name__.startswith('_')
            and API.expose.get(func)
        )

    @staticmethod
    def func_wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> HandleCommandResult:  # type: ignore
            return HandleCommandResult(stdout=pretty_json(
                func(self, *args, **kwargs)))

        # functools doesn't change the signature when wrapping a function
        # so we do it manually
        signature = inspect.signature(func)
        wrapper.__signature__ = signature  # type: ignore
        return wrapper


class CLI(MgrModule, metaclass=MgrAPIReflector):
    @CLICommand('mgr cli_benchmark')
    def benchmark(self, iterations: int, threads: int, func_name: str,
                  func_args: List[str] = None) -> HandleCommandResult:  # type: ignore
        func_args = () if func_args is None else func_args
        if iterations and threads:
            try:
                func = getattr(self, func_name)
            except AttributeError:
                return HandleCommandResult(errno.EINVAL,
                                           stderr="Could not find the public "
                                           "function you are requesting")
        else:
            raise BenchmarkException("Number of calls and number "
                                     "of parallel calls must be greater than 0")

        def timer(*args: Any) -> float:
            time_start = get_time()
            func(*func_args)
            return get_time() - time_start

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            results_iter = executor.map(timer, range(iterations))
        results = list(results_iter)

        stats = {
            "avg": sum(results) / len(results),
            "max": max(results),
            "min": min(results),
        }
        return HandleCommandResult(stdout=pretty_json(stats))


class BenchmarkException(Exception):
    pass
