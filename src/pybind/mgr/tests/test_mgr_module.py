# type: ignore
from unittest.mock import MagicMock, patch

import pytest

import rados
import mgr_module


def _invoke_rados_property(connect_side_effect):
    """
    Exercise MgrModule.rados without constructing the (C++-backed) module.

    Returns the fake module instance, the fake librados handle, the property
    result, the patched rados.Rados constructor and the patched time.sleep.
    """
    inst = MagicMock()
    inst._rados = None
    handle = MagicMock()
    handle.connect.side_effect = connect_side_effect
    handle.get_addrs.return_value = '1.2.3.4:0/0'
    with patch.object(mgr_module.rados, 'Rados', return_value=handle) as ctor, \
            patch('mgr_module.time.sleep') as sleep:
        result = mgr_module.MgrModule.rados.fget(inst)
    return inst, handle, result, ctor, sleep


def test_rados_retries_transient_connect_failure():
    # A transient connect() failure (e.g. cephx EPERM during a mon election)
    # must be retried rather than failing the module permanently.
    err = rados.Error('injected transient connect failure', errno=1)
    inst, handle, result, ctor, sleep = _invoke_rados_property([err, err, None])

    assert handle.connect.call_count == 3
    assert sleep.call_count == 2
    # A fresh handle is built per attempt; librados can't reconnect a handle
    # whose connect() failed, so each failed attempt is shut down and discarded.
    assert ctor.call_count == 3
    assert handle.shutdown.call_count == 2
    assert result is handle
    # handle is cached and the client is registered only after a good connect
    assert inst._rados is handle
    inst._ceph_register_client.assert_called_once()


def test_rados_does_not_cache_handle_on_persistent_failure():
    # If the mons stay unreachable the bounded retries exhaust and the error
    # propagates, but no unconnected handle is left cached on self._rados.
    err = rados.Error('cluster unreachable', errno=1)
    inst = MagicMock()
    inst._rados = None
    handle = MagicMock()
    handle.connect.side_effect = err
    with patch.object(mgr_module.rados, 'Rados', return_value=handle) as ctor, \
            patch('mgr_module.time.sleep'):
        with pytest.raises(rados.Error):
            mgr_module.MgrModule.rados.fget(inst)

    # every attempt built a fresh handle and shut the failed one down
    assert ctor.call_count == 5
    assert handle.shutdown.call_count == 5
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
