# -*- coding: utf-8 -*-
import pytest
from mgr_module import MgrModule


@pytest.fixture
def mgr() -> MgrModule:
    return MgrModule.__new__(MgrModule)


@pytest.fixture
def store(mgr: MgrModule):
    from ceph_secrets.secret_store import SecretStoreMon
    return SecretStoreMon(mgr)


@pytest.fixture
def secret_mgr(store):
    from ceph_secrets.secret_mgr import SecretMgr
    return SecretMgr(store)
