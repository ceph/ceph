import json
import os

import pytest

from smbutil import SMBTestConf


def read_smb_test_meta(conf_file=None):
    if not conf_file:
        conf_file = os.environ.get('SMB_TEST_META')
    if not conf_file:
        return None
    with open(conf_file) as fh:
        json_data = json.load(fh)
    return SMBTestConf(json_data)


@pytest.fixture
def smb_cfg():
    conf = read_smb_test_meta()
    if not conf:
        pytest.skip('no SMB_TEST_META value')
    return conf


def pytest_generate_tests(metafunc):
    if 'share_name' in metafunc.fixturenames:
        conf = read_smb_test_meta()
        if not conf:
            return
        metafunc.parametrize('share_name', conf.shares)
