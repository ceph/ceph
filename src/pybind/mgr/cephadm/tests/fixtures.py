import time
import fnmatch
try:
    from typing import Any
except ImportError:
    pass
import pytest

from cephadm import CephadmOrchestrator
from orchestrator import raise_if_exception, Completion
from tests import mock


def set_store(self, k, v):
    if v is None:
        del self._store[k]
    else:
        self._store[k] = v


def get_store(self, k):
    return self._store.get(k, None)


def get_store_prefix(self, prefix):
    return {
        k: v for k, v in self._store.items()
        if k.startswith(prefix)
    }


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
            mock.patch("cephadm.module.CephadmOrchestrator._configure_logging", lambda *args: None),\
            mock.patch("cephadm.module.CephadmOrchestrator.remote"),\
            mock.patch("cephadm.module.CephadmOrchestrator.set_store", set_store), \
            mock.patch("cephadm.module.CephadmOrchestrator.get_store", get_store),\
            mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]')), \
            mock.patch("cephadm.module.HostCache.save_host"), \
            mock.patch("cephadm.module.HostCache.rm_host"), \
            mock.patch("cephadm.module.CephadmOrchestrator.send_command"), \
            mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command), \
            mock.patch("cephadm.module.CephadmOrchestrator.get_store_prefix", get_store_prefix):

        CephadmOrchestrator._register_commands('')
        CephadmOrchestrator._register_options('')
        m = CephadmOrchestrator.__new__ (CephadmOrchestrator)
        m._root_logger = mock.MagicMock()
        m._store = {
            'ssh_config': '',
            'ssh_identity_key': '',
            'ssh_identity_pub': '',
            'inventory': {},
            'upgrade_state': None,
        }
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
