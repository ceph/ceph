import datetime
import time
import fnmatch
from contextlib import contextmanager

from cephadm.module import CEPH_DATEFMT

try:
    from typing import Any
except ImportError:
    pass
import pytest

from cephadm import CephadmOrchestrator
from cephadm.services.osd import RemoveUtil, OSD
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


@contextmanager
def with_cephadm_module(module_options=None, store=None):
    """
    :param module_options: Set opts as if they were set before module.__init__ is called
    :param store: Set the store before module.__init__ is called
    """
    with mock.patch("cephadm.module.CephadmOrchestrator.get_ceph_option", get_ceph_option),\
            mock.patch("cephadm.services.osd.RemoveUtil._run_mon_cmd"), \
            mock.patch("cephadm.module.CephadmOrchestrator.get_osdmap"), \
            mock.patch("cephadm.services.osd.OSDService.get_osdspec_affinity", return_value='test_spec'), \
            mock.patch("cephadm.module.CephadmOrchestrator.remote"):

        m = CephadmOrchestrator.__new__ (CephadmOrchestrator)
        if module_options is not None:
            for k, v in module_options.items():
                m._ceph_set_module_option('cephadm', k, v)
        if store is None:
            store = {}
        if '_ceph_get/mon_map' not in store:
            store['_ceph_get/mon_map'] = {
                'modified': datetime.datetime.utcnow().strftime(CEPH_DATEFMT),
                'fsid': 'foobar',
            }
        for k, v in store.items():
            m._ceph_set_store(k, v)

        m.__init__('cephadm', 0, 0)
        m._cluster_fsid = "fsid"
        yield m


@pytest.yield_fixture()
def cephadm_module():
    with with_cephadm_module({}) as m:
        yield m


@pytest.yield_fixture()
def rm_util():
    with with_cephadm_module({}) as m:
        r = RemoveUtil.__new__(RemoveUtil)
        r.__init__(m)
        yield r


@pytest.yield_fixture()
def osd_obj():
    with mock.patch("cephadm.services.osd.RemoveUtil"):
        o = OSD(0, mock.MagicMock())
        yield o


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
