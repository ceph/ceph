from unittest.mock import MagicMock

import nvmeof.module as nvmeof_mod


class FakeRados:
    def __init__(self, exists: bool):
        self._exists = exists

    def pool_exists(self, pool_name: str) -> bool:
        return self._exists


def make_mgr(mon_handler, exists: bool, monkeypatch):
    mgr = nvmeof_mod.NVMeoF.__new__(nvmeof_mod.NVMeoF)
    mgr.mon_command = mon_handler
    mgr._print_log = lambda *args, **kwargs: None
    mgr.run = False

    mgr._fake_rados = FakeRados(exists)
    mgr.remote = MagicMock()

    def _pool_exists(self, pool_name: str) -> bool:
        return self._fake_rados.pool_exists(pool_name)

    monkeypatch.setattr(nvmeof_mod.NVMeoF, "_pool_exists", _pool_exists, raising=True)

    return mgr


def test_pool_exists_skips_pool_creation(monkeypatch):
    calls = []

    def mon_command(cmd, inbuf):
        calls.append(cmd)
        return 0, "", ""

    mgr = make_mgr(mon_command, exists=True, monkeypatch=monkeypatch)

    mgr.create_pool_if_not_exists()

    assert not any(c.get("prefix") == "osd pool create" for c in calls)

    assert not any(c.get("prefix") == "osd pool application enable" for c in calls)


def test_pool_missing_creates_then_enables(monkeypatch):
    calls = []

    def mon_command(cmd, inbuf):
        calls.append(cmd)
        return 0, "", ""

    mgr = make_mgr(mon_command, exists=False, monkeypatch=monkeypatch)

    mgr.create_pool_if_not_exists()

    create_calls = [c for c in calls if c.get("prefix") == "osd pool create"]
    assert len(create_calls) == 1, "Expected one pool create call"

    assert create_calls[0].get("pool") == ".nvmeof", "Expected pool_name to be '.nvmeof'"

    app_enable_calls = [c for c in calls if c.get("prefix") == "osd pool application enable"]
    assert len(app_enable_calls) == 1, "Expected one application enable call"

    assert app_enable_calls[0].get("app") == "nvmeof-meta", "Expected 'nvmeof-meta' application to be enabled"
    assert app_enable_calls[0].get("pool") == ".nvmeof", "Expected pool to be '.nvmeof'"


def test_enable_application_with_custom_app_name(monkeypatch):
    calls = []

    def mon_command(cmd, inbuf):
        calls.append(cmd)
        return 0, "", ""

    mgr = make_mgr(mon_command, exists=True, monkeypatch=monkeypatch)

    mgr._enable_application("test-pool", "custom-app")

    custom_enable_calls = [c for c in calls if c.get("prefix") == "osd pool application enable" and c.get("app") == "custom-app"]
    assert len(custom_enable_calls) == 1, "Expected one 'custom-app' application enable call"
    assert custom_enable_calls[0].get("pool") == "test-pool"
