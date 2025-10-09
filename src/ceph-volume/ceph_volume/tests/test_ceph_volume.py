import os
from ceph_volume import AllowLoopDevices, allow_loop_devices
from typing import Any


class TestAllowLoopDevsWarning:
    def setup_method(self) -> None:
        AllowLoopDevices.allow = False
        AllowLoopDevices.warned = False
        self.teardown_method()

    def teardown_method(self) -> None:
        AllowLoopDevices.allow = False
        AllowLoopDevices.warned = False
        if os.environ.get('CEPH_VOLUME_ALLOW_LOOP_DEVICES'):
            os.environ.pop('CEPH_VOLUME_ALLOW_LOOP_DEVICES')

    def test_loop_dev_warning(self, fake_call: Any, caplog: Any) -> None:
        AllowLoopDevices.warned = False
        assert allow_loop_devices() is False
        assert not caplog.records
        os.environ['CEPH_VOLUME_ALLOW_LOOP_DEVICES'] = "y"
        assert allow_loop_devices() is True
        log = caplog.records[0]
        assert log.levelname == "WARNING"
        assert "will never be supported in production" in log.message
