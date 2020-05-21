from unittest.mock import patch, Mock

import teuthology.lock.util
from teuthology import provision


class TestVpsOsVersionParamCheck(object):

    def setup(self):
        self.fake_ctx = Mock()
        self.fake_ctx.machine_type = 'vps'
        self.fake_ctx.num_to_lock = 1
        self.fake_ctx.lock = False

        def fake_downburst_executable():
            return ''

        self.fake_downburst_executable = fake_downburst_executable

    def test_ubuntu_precise(self):
        self.fake_ctx.os_type = 'ubuntu'
        self.fake_ctx.os_version = 'precise'
        with patch.multiple(
            provision.downburst,
            downburst_executable=self.fake_downburst_executable,
        ):
            check_value = teuthology.lock.util.vps_version_or_type_valid(
                self.fake_ctx.machine_type,
                self.fake_ctx.os_type,
                self.fake_ctx.os_version)

        assert check_value

    def test_ubuntu_number(self):
        self.fake_ctx.os_type = 'ubuntu'
        self.fake_ctx.os_version = '12.04'
        with patch.multiple(
            provision.downburst,
            downburst_executable=self.fake_downburst_executable,
        ):
            check_value = teuthology.lock.util.vps_version_or_type_valid(
                self.fake_ctx.machine_type,
                self.fake_ctx.os_type,
                self.fake_ctx.os_version)
        assert check_value

    def test_mixup(self):
        self.fake_ctx.os_type = '6.5'
        self.fake_ctx.os_version = 'rhel'
        with patch.multiple(
            provision.downburst,
            downburst_executable=self.fake_downburst_executable,
        ):
            check_value = teuthology.lock.util.vps_version_or_type_valid(
                self.fake_ctx.machine_type,
                self.fake_ctx.os_type,
                self.fake_ctx.os_version)
        assert not check_value

    def test_bad_type(self):
        self.fake_ctx.os_type = 'aardvark'
        self.fake_ctx.os_version = '6.5'
        with patch.multiple(
            provision.downburst,
            downburst_executable=self.fake_downburst_executable,
        ):
            check_value = teuthology.lock.util.vps_version_or_type_valid(
                self.fake_ctx.machine_type,
                self.fake_ctx.os_type,
                self.fake_ctx.os_version)
        assert not check_value

    def test_bad_version(self):
        self.fake_ctx.os_type = 'rhel'
        self.fake_ctx.os_version = 'vampire_bat'
        with patch.multiple(
            provision.downburst,
            downburst_executable=self.fake_downburst_executable,
        ):
            check_value = teuthology.lock.util.vps_version_or_type_valid(
                self.fake_ctx.machine_type,
                self.fake_ctx.os_type,
                self.fake_ctx.os_version)
        assert not check_value
