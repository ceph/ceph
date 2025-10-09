import json
import logging
import time

from tasks.mgr.mgr_test_case import MgrTestCase
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)


class TestCephadmCLI(MgrTestCase):
    def _cmd(self, *args) -> str:
        assert self.mgr_cluster is not None
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _orch_cmd(self, *args) -> str:
        return self._cmd("orch", *args)

    def setUp(self):
        super(TestCephadmCLI, self).setUp()

    def test_yaml(self):
        """
        to prevent oddities like

        >>> import yaml
        ... from collections import OrderedDict
        ... assert yaml.dump(OrderedDict()) == '!!python/object/apply:collections.OrderedDict\\n- []\\n'
        """
        out = self._orch_cmd('device', 'ls', '--format', 'yaml')
        self.assertNotIn('!!python', out)

        out = self._orch_cmd('host', 'ls', '--format', 'yaml')
        self.assertNotIn('!!python', out)

        out = self._orch_cmd('ls', '--format', 'yaml')
        self.assertNotIn('!!python', out)

        out = self._orch_cmd('ps', '--format', 'yaml')
        self.assertNotIn('!!python', out)

        out = self._orch_cmd('status', '--format', 'yaml')
        self.assertNotIn('!!python', out)

    def test_pause(self):
        self._orch_cmd('pause')
        self.wait_for_health('CEPHADM_PAUSED', 60)
        self._orch_cmd('resume')
        self.wait_for_health_clear(60)

    def test_daemon_restart(self):
        self._orch_cmd('daemon', 'stop', 'osd.0')
        self.wait_for_health('OSD_DOWN', 60)
        with safe_while(sleep=2, tries=30) as proceed:
            while proceed():
                j = json.loads(self._orch_cmd('ps', '--format', 'json'))
                d = {d['daemon_name']: d for d in j}
                if d['osd.0']['status_desc'] != 'running':
                    break
        time.sleep(5)
        self._orch_cmd('daemon', 'start', 'osd.0')
        self.wait_for_health_clear(120)
        # this sleep is to try and address https://tracker.ceph.com/issues/69526
        time.sleep(5)
        self._orch_cmd('daemon', 'restart', 'osd.0')

    def test_device_ls_wide(self):
        self._orch_cmd('device', 'ls', '--wide')

    def test_cephfs_mirror(self):
        self._orch_cmd('apply', 'cephfs-mirror')
        self.wait_until_true(lambda: 'cephfs-mirror' in self._orch_cmd('ps'), 60)
        self.wait_for_health_clear(60)
        self._orch_cmd('rm', 'cephfs-mirror')
        self.wait_until_true(lambda: 'cephfs-mirror' not in self._orch_cmd('ps'), 60)
