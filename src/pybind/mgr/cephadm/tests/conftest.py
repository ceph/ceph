import pytest

from cephadm.services.osd import RemoveUtil, OSD
from unittest import mock
from .fixtures import with_cephadm_module, async_side_effect
from cephadm import CephadmOrchestrator
from typing import Generator


@pytest.fixture()
def cephadm_module() -> Generator[CephadmOrchestrator, None, None]:
    with with_cephadm_module({}) as m:
        yield m


@pytest.fixture()
def rm_util():
    with with_cephadm_module({}) as m:
        r = RemoveUtil.__new__(RemoveUtil)
        r.__init__(m)
        yield r


@pytest.fixture()
def osd_obj():
    with mock.patch("cephadm.services.osd.RemoveUtil"):
        o = OSD(0, mock.MagicMock())
        yield o


@pytest.fixture()
def mock_cephadm(monkeypatch):
    import cephadm.module
    import cephadm.serve

    mm = mock.MagicMock()
    mm._run_cephadm.side_effect = async_side_effect(('{}', '', 0))
    monkeypatch.setattr(
        cephadm.serve.CephadmServe, '_run_cephadm', mm._run_cephadm
    )
    monkeypatch.setattr(
        cephadm.module.CephadmOrchestrator,
        '_daemon_action',
        mm._daemon_action,
    )
    mm.serve = cephadm.serve.CephadmServe
    return mm
