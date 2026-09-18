# type: ignore
from unittest.mock import MagicMock, patch

import pytest

import rados
import mgr_module


def _make_handle(connect_side_effect=None):
    handle = MagicMock()
    handle.connect.side_effect = connect_side_effect
    handle.get_addrs.return_value = '1.2.3.4:0/0'
    return handle


def test_rados_connects_and_caches_the_handle():
    inst = MagicMock()
    inst._rados = None
    handle = _make_handle()
    with patch.object(mgr_module.rados, 'Rados', return_value=handle):
        result = mgr_module.MgrModule.rados.fget(inst)

    handle.connect.assert_called_once()
    assert result is handle
    assert inst._rados is handle
    inst._ceph_register_client.assert_called_once()


def test_rados_leaves_the_shared_config_alone():
    # The handle shares the mgr's CephContext, so setting a config value on it
    # would change that value for every other module in the process and
    # override whatever the operator configured. Retry behaviour comes from
    # ceph-mgr's own default for rados_connect_retries instead.
    inst = MagicMock()
    inst._rados = None
    handle = _make_handle()
    with patch.object(mgr_module.rados, 'Rados', return_value=handle):
        mgr_module.MgrModule.rados.fget(inst)

    handle.conf_set.assert_not_called()


def test_rados_does_not_cache_handle_on_persistent_failure():
    # If the bounded retries inside connect() exhaust, the error propagates
    # and no unconnected handle is left cached on self._rados.
    err = rados.Error('cluster unreachable', errno=1)
    inst = MagicMock()
    inst._rados = None
    handle = _make_handle(connect_side_effect=err)
    with patch.object(mgr_module.rados, 'Rados', return_value=handle):
        with pytest.raises(rados.Error):
            mgr_module.MgrModule.rados.fget(inst)

    assert inst._rados is None
    inst._ceph_register_client.assert_not_called()


def test_rados_reconnects_after_a_failed_connect():
    # A failed connect must not poison the property: the next access builds a
    # new handle and tries again.
    err = rados.Error('cluster unreachable', errno=1)
    inst = MagicMock()
    inst._rados = None
    failing = _make_handle(connect_side_effect=err)
    working = _make_handle()
    with patch.object(mgr_module.rados, 'Rados',
                      side_effect=[failing, working]):
        with pytest.raises(rados.Error):
            mgr_module.MgrModule.rados.fget(inst)
        result = mgr_module.MgrModule.rados.fget(inst)

    assert result is working
    assert inst._rados is working


def test_rados_returns_cached_handle_without_reconnecting():
    cached = MagicMock()
    inst = MagicMock()
    inst._rados = cached
    with patch.object(mgr_module.rados, 'Rados') as ctor:
        result = mgr_module.MgrModule.rados.fget(inst)

    assert result is cached
    ctor.assert_not_called()
    inst._ceph_register_client.assert_not_called()
