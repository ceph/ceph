from osd_support import OSDSupport
import pytest

from tests import mock


@pytest.yield_fixture()
def osd_support_module():
    with mock.patch("osd_support.module.OSDSupport.get_osdmap"), \
         mock.patch("osd_support.module.OSDSupport.osd_df"), \
         mock.patch("osd_support.module.OSDSupport.mon_command", return_value=(0, '', '')):
        m = OSDSupport.__new__(OSDSupport)
        m.__init__('osd_support', 0, 0)
        yield m
