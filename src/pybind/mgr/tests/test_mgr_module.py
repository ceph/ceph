# type: ignore
from unittest.mock import MagicMock, patch

import pytest

import rados
import mgr_module


def _make_handle(connect_side_effect=None, retries='0'):
    handle = MagicMock()
    handle.connect.side_effect = connect_side_effect
    handle.get_addrs.return_value = '1.2.3.4:0/0'
    handle.conf_get.return_value = retries
    return handle


def test_rados_enables_connect_retries_by_default():
    # The transient-failure resilience now lives in librados; the module opts
    # in by raising rados_connect_retries before connecting, so a connect
    # during a mon-election window is retried inside connect() rather than
    # failing the module permanently.
    inst = MagicMock()
    inst._rados = None
    handle = _make_handle(retries='0')
    with patch.object(mgr_module.rados, 'Rados', return_value=handle):
        result = mgr_module.MgrModule.rados.fget(inst)

    handle.conf_set.assert_called_once_with('rados_connect_retries', '5')
    handle.connect.assert_called_once()
    assert result is handle
    assert inst._rados is handle
    inst._ceph_register_client.assert_called_once()


def test_rados_respects_operator_configured_retries():
    # If the operator has already tuned rados_connect_retries we must not
    # clobber it.
    inst = MagicMock()
    inst._rados = None
    handle = _make_handle(retries='2')
    with patch.object(mgr_module.rados, 'Rados', return_value=handle):
        mgr_module.MgrModule.rados.fget(inst)

    handle.conf_set.assert_not_called()
    handle.connect.assert_called_once()


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


def test_rados_returns_cached_handle_without_reconnecting():
    cached = MagicMock()
    inst = MagicMock()
    inst._rados = cached
    with patch.object(mgr_module.rados, 'Rados') as ctor:
        result = mgr_module.MgrModule.rados.fget(inst)

    assert result is cached
    ctor.assert_not_called()
    inst._ceph_register_client.assert_not_called()
