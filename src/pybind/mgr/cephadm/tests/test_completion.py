import sys
import time


try:
    from typing import Any
except ImportError:
    pass

import pytest


from orchestrator import raise_if_exception, Completion
from .fixtures import cephadm_module
from ..module import trivial_completion, async_completion, async_map_completion, CephadmOrchestrator


class TestCompletion(object):
    def _wait(self, m, c):
        # type: (CephadmOrchestrator, Completion) -> Any
        m.process([c])
        m.process([c])

        for _ in range(30):
            if c.is_finished:
                raise_if_exception(c)
                return c.result
            time.sleep(0.1)
        assert False, "timeout" + str(c._state)

    def test_trivial(self, cephadm_module):
        @trivial_completion
        def run(x):
            return x+1
        assert self._wait(cephadm_module, run(1)) == 2

    @pytest.mark.parametrize("input", [
        ((1, ), ),
        ((1, 2), ),
        (("hallo", ), ),
        (("hallo", "foo"), ),
    ])
    def test_async(self, input, cephadm_module):
        @async_completion
        def run(*args):
            return str(args)

        assert self._wait(cephadm_module, run(*input)) == str(input)

    @pytest.mark.parametrize("input,expected", [
        ([], []),
        ([1], ["(1,)"]),
        (["hallo"], ["('hallo',)"]),
        ("hi", ["('h',)", "('i',)"]),
        (list(range(5)), [str((x, )) for x in range(5)]),
        ([(1, 2), (3, 4)], ["(1, 2)", "(3, 4)"]),
    ])
    def test_async_map(self, input, expected, cephadm_module):
        @async_map_completion
        def run(*args):
            return str(args)

        c = run(input)
        self._wait(cephadm_module, c)
        assert c.result == expected

    def test_async_self(self, cephadm_module):
        class Run(object):
            def __init__(self):
                self.attr = 1

            @async_completion
            def run(self, x):
                assert self.attr == 1
                return x + 1

        assert self._wait(cephadm_module, Run().run(1)) == 2

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

            @async_map_completion
            def run(self, *args):
                assert self.attr == 1
                return str(args)

        c = Run().run(input)
        self._wait(cephadm_module, c)
        assert c.result == expected

    def test_then1(self, cephadm_module):
        @async_map_completion
        def run(x):
            return x+1

        assert self._wait(cephadm_module, run([1,2]).then(str)) == '[2, 3]'

    def test_then2(self, cephadm_module):
        @async_map_completion
        def run(x):
            time.sleep(0.1)
            return x+1

        @async_completion
        def async_str(results):
            return str(results)

        c = run([1,2]).then(async_str)

        self._wait(cephadm_module, c)
        assert c.result == '[2, 3]'

    def test_then3(self, cephadm_module):
        @async_map_completion
        def run(x):
            time.sleep(0.1)
            return x+1

        def async_str(results):
            return async_completion(str)(results)

        c = run([1,2]).then(async_str)

        self._wait(cephadm_module, c)
        assert c.result == '[2, 3]'

    def test_then4(self, cephadm_module):
        @async_map_completion
        def run(x):
            time.sleep(0.1)
            return x+1

        def async_str(results):
            return async_completion(str)(results).then(lambda x: x + "hello")

        c = run([1,2]).then(async_str)

        self._wait(cephadm_module, c)
        assert c.result == '[2, 3]hello'

    @pytest.mark.skip(reason="see limitation of async_map_completion")
    def test_then5(self, cephadm_module):
        @async_map_completion
        def run(x):
            time.sleep(0.1)
            return async_completion(str)(x+1)

        c = run([1,2])

        self._wait(cephadm_module, c)
        assert c.result == "['2', '3']"

    def test_raise(self, cephadm_module):
        @async_completion
        def run(x):
            raise ZeroDivisionError()

        with pytest.raises(ZeroDivisionError):
            self._wait(cephadm_module, run(1))
