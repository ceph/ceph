import pytest

from cephadm.services.osd import RemoveUtil, OSD
from tests import mock

from .fixtures import with_cephadm_module


@pytest.fixture()
def cephadm_module():
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
