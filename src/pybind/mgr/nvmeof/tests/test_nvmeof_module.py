
from contextlib import contextmanager
from unittest.mock import MagicMock

import nvmeof.module as nvmeof_mod


class FakeRados:
    def __init__(self, exists: bool):
        self._exists = exists
        self.opened_pools = []

    def pool_exists(self, pool_name: str) -> bool:
        return self._exists

    @contextmanager
    def open_ioctx(self, pool_name: str):
        self.opened_pools.append(pool_name)
        yield object()


def patch_rbd_pool_init(monkeypatch):
    rbd_instance = MagicMock()
    monkeypatch.setattr(nvmeof_mod.rbd, "RBD", lambda: rbd_instance)
    return rbd_instance


def make_mgr(mon_handler, exists: bool, monkeypatch):
    mgr = nvmeof_mod.NVMeoF.__new__(nvmeof_mod.NVMeoF)
    mgr.mon_command = mon_handler
    mgr._print_log = lambda *args, **kwargs: None
    mgr.run = False

    mgr._fake_rados = FakeRados(exists)
    mgr.remote = MagicMock()

    def _pool_exists(self, pool_name: str) -> bool:
        return self._fake_rados.pool_exists(pool_name)

    def _rbd_pool_init(self, pool_name: str):
        with self._fake_rados.open_ioctx(pool_name) as ioctx:
            nvmeof_mod.rbd.RBD().pool_init(ioctx, False)

    monkeypatch.setattr(nvmeof_mod.NVMeoF, "_pool_exists", _pool_exists, raising=True)
    monkeypatch.setattr(nvmeof_mod.NVMeoF, "_rbd_pool_init", _rbd_pool_init, raising=True)

    return mgr


def test_pool_exists_skips_create_calls_enable_and_pool_init(monkeypatch):
    calls = []

    def mon_command(cmd, inbuf):
        calls.append(cmd)
        return 0, "", ""

    rbd_instance = patch_rbd_pool_init(monkeypatch)
    mgr = make_mgr(mon_command, exists=True, monkeypatch=monkeypatch)

    mgr.create_pool_if_not_exists()

    assert not any(c.get("prefix") == "osd pool create" for c in calls)
    assert any(c.get("prefix") == "osd pool application enable" for c in calls)

    assert mgr._fake_rados.opened_pools == [".nvmeof"]
    rbd_instance.pool_init.assert_called_once()


def test_pool_missing_creates_then_enables_then_pool_init(monkeypatch):
    calls = []

    def mon_command(cmd, inbuf):
        calls.append(cmd)
        return 0, "", ""

    rbd_instance = patch_rbd_pool_init(monkeypatch)
    mgr = make_mgr(mon_command, exists=False, monkeypatch=monkeypatch)

    mgr.create_pool_if_not_exists()

    assert any(c.get("prefix") == "osd pool create" for c in calls)
    assert any(c.get("prefix") == "osd pool application enable" for c in calls)

    assert mgr._fake_rados.opened_pools == [".nvmeof"]
    rbd_instance.pool_init.assert_called_once()
