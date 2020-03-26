import sys
import time


try:
    from typing import Any
except ImportError:
    pass

import pytest


from tests import mock
from .fixtures import cephadm_module, wait
from ..module import trivial_completion, forall_hosts


class TestCompletion(object):

    def test_trivial(self, cephadm_module):
        @trivial_completion
        def run(x):
            return x+1
        assert wait(cephadm_module, run(1)) == 2

    @pytest.mark.parametrize("input,expected", [
        ([], []),
        ([1], ["(1,)"]),
        (["hallo"], ["('hallo',)"]),
        ("hi", ["('h',)", "('i',)"]),
        (list(range(5)), [str((x, )) for x in range(5)]),
        ([(1, 2), (3, 4)], ["(1, 2)", "(3, 4)"]),
    ])
    def test_async_map(self, input, expected, cephadm_module):
        @forall_hosts
        def run_forall(*args):
            return str(args)
        assert run_forall(input) == expected


    @pytest.mark.parametrize("input,expected", [
        ([], []),
        ([1], ["(1,)"]),
        (["hallo"], ["('hallo',)"]),
        ("hi", ["('h',)", "('i',)"]),
        (list(range(5)), [str((x, )) for x in range(5)]),
        ([(1, 2), (3, 4)], ["(1, 2)", "(3, 4)"]),
    ])
    def test_async_map_self(self, input, expected, cephadm_module):
        class Run(object):
            def __init__(self):
                self.attr = 1

            @forall_hosts
            def run_forall(self, *args):
                assert self.attr == 1
                return str(args)

        assert Run().run_forall(input) == expected
