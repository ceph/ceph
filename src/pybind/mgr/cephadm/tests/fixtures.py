import time
import fnmatch
from contextlib import contextmanager

try:
    from typing import Any
except ImportError:
    pass
import pytest

from cephadm import CephadmOrchestrator
from orchestrator import raise_if_exception, Completion, HostSpec
from tests import mock



def get_ceph_option(_, key):
    return __file__


def _run_cephadm(ret):
    def foo(*args, **kwargs):
        return ret, '', 0
    return foo


def match_glob(val, pat):
    ok = fnmatch.fnmatchcase(val, pat)
    if not ok:
        assert pat in val


def mon_command(*args, **kwargs):
    return 0, '', ''


@pytest.yield_fixture()
def cephadm_module():
    with mock.patch("cephadm.module.CephadmOrchestrator.get_ceph_option", get_ceph_option),\
            mock.patch("cephadm.module.CephadmOrchestrator.remote"),\
            mock.patch("cephadm.module.CephadmOrchestrator.send_command"), \
            mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command):

        m = CephadmOrchestrator.__new__ (CephadmOrchestrator)
        m.__init__('cephadm', 0, 0)
        m._cluster_fsid = "fsid"
        yield m


def wait(m, c):
    # type: (CephadmOrchestrator, Completion) -> Any
    m.process([c])

    try:
        import pydevd  # if in debugger
        in_debug = True
    except ImportError:
        in_debug = False

    if in_debug:
        while True:    # don't timeout
            if c.is_finished:
                raise_if_exception(c)
                return c.result
            time.sleep(0.1)
    else:
        for i in range(30):
            if i % 10 == 0:
                m.process([c])
            if c.is_finished:
                raise_if_exception(c)
                return c.result
            time.sleep(0.1)
    assert False, "timeout" + str(c._state)


@contextmanager
def with_host(m:CephadmOrchestrator, name):
    # type: (CephadmOrchestrator, str) -> None
    wait(m, m.add_host(HostSpec(hostname=name)))
    yield
    wait(m, m.remove_host(name))
