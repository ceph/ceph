from contextlib import contextmanager

import pytest

from ssh import SSHOrchestrator
from tests import mock


def set_store(self, k, v):
    if v is None:
        del self._store[k]
    else:
        self._store[k] = v


def get_store(self, k):
    return self._store[k]


def get_store_prefix(self, prefix):
    return {
        k: v for k, v in self._store.items()
        if k.startswith(prefix)
    }


@pytest.yield_fixture()
def ssh_module():
    with mock.patch("ssh.module.SSHOrchestrator.get_ceph_option", lambda _, key: __file__),\
            mock.patch("ssh.module.SSHOrchestrator.set_store", set_store),\
            mock.patch("ssh.module.SSHOrchestrator.get_store", get_store),\
            mock.patch("ssh.module.SSHOrchestrator.get_store_prefix", get_store_prefix):
        m = SSHOrchestrator.__new__ (SSHOrchestrator)
        m._store = {
            'ssh_config': '',
            'ssh_identity_key': '',
            'ssh_identity_pub': '',
            'inventory': {},
        }
        m.__init__('ssh', 0, 0)
        yield m
