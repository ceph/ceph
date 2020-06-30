import errno
import json
import logging


from .mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestOrchestratorCli(MgrTestCase):
    MGRS_REQUIRED = 1

    def _cmd(self, module, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(module, *args)

    def _orch_cmd(self, *args):
        return self._cmd("orch", *args)

    def _progress_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("progress", *args)

    def _orch_cmd_result(self, *args, **kwargs):
        """
        raw_cluster_cmd doesn't support kwargs.
        """
        return self.mgr_cluster.mon_manager.raw_cluster_cmd_result("orch", *args, **kwargs)

    def _test_orchestrator_cmd_result(self, *args, **kwargs):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd_result("test_orchestrator", *args, **kwargs)

    def setUp(self):
        super(TestOrchestratorCli, self).setUp()

        self._load_module("orchestrator")
        self._load_module("test_orchestrator")
        self._orch_cmd("set", "backend", "test_orchestrator")

    def test_status(self):
        ret = self._orch_cmd("status")
        self.assertIn("test_orchestrator", ret)

    def test_device_ls(self):
        ret = self._orch_cmd("device", "ls")
        self.assertIn("localhost", ret)

    def test_device_ls_refresh(self):
        ret = self._orch_cmd("device", "ls", "--refresh")
        self.assertIn("localhost", ret)

    def test_device_ls_hoshs(self):
        ret = self._orch_cmd("device", "ls", "localhost", "host1")
        self.assertIn("localhost", ret)


    def test_device_ls_json(self):
        ret = self._orch_cmd("device", "ls", "--format", "json")
        self.assertIn("localhost", ret)
        self.assertIsInstance(json.loads(ret), list)

    def test_ps(self):
        ret = self._orch_cmd("ps")
        self.assertIn("mgr", ret)

    def test_ps_json(self):
        ret = self._orch_cmd("ps", "--format", "json")
        self.assertIsInstance(json.loads(ret), list)
        self.assertIn("mgr", ret)


    def test_service_action(self):
        self._orch_cmd("restart", "mds.cephfs")
        self._orch_cmd("stop", "mds.cephfs")
        self._orch_cmd("start", "mds.cephfs")

    def test_service_instance_action(self):
        self._orch_cmd("daemon", "restart", "mds.a")
        self._orch_cmd("daemon", "stop", "mds.a")
        self._orch_cmd("daemon", "start", "mds.a")

    def test_osd_create(self):
        drive_group = """
service_type: osd
service_id: any.sda
placement:
  host_pattern: '*'
data_devices:
  all: True
"""
        res = self._orch_cmd_result("apply", "osd", "-i", "-",
                                    stdin=drive_group)
        self.assertEqual(res, 0)

    def test_blink_device_light(self):
        def _ls_lights(what):
            return json.loads(self._cmd("device", "ls-lights"))[what]

        metadata = json.loads(self._cmd("osd", "metadata"))
        dev_name_ids = [osd["device_ids"] for osd in metadata]
        _, dev_id = [d.split('=') for d in dev_name_ids if len(d.split('=')) == 2][0]

        for t in ["ident", "fault"]:
            self.assertNotIn(dev_id, _ls_lights(t))
            self._cmd("device", "light", "on", dev_id, t)
            self.assertIn(dev_id, _ls_lights(t))

            health = {
                'ident': 'DEVICE_IDENT_ON',
                'fault': 'DEVICE_FAULT_ON',
            }[t]
            self.wait_for_health(health, 30)

            self._cmd("device", "light", "off", dev_id, t)
            self.assertNotIn(dev_id, _ls_lights(t))

        self.wait_for_health_clear(30)

    def test_mds_add(self):
        self._orch_cmd('daemon', 'add', 'mds', 'fsname')

    def test_rgw_add(self):
        self._orch_cmd('daemon', 'add', 'rgw', 'realm', 'zone')

    def test_nfs_add(self):
        self._orch_cmd('daemon', 'add', "nfs", "service_name", "pool", "--namespace", "ns")
        self._orch_cmd('daemon', 'add', "nfs", "service_name", "pool")

    def test_osd_rm(self):
        self._orch_cmd('daemon', "rm", "osd.0", '--force')

    def test_mds_rm(self):
        self._orch_cmd("daemon", "rm", "mds.fsname")

    def test_rgw_rm(self):
        self._orch_cmd("daemon", "rm", "rgw.myrealm.myzone")

    def test_nfs_rm(self):
        self._orch_cmd("daemon", "rm", "nfs.service_name")

    def test_host_ls(self):
        out = self._orch_cmd("host", "ls", "--format=json")
        hosts = json.loads(out)
        self.assertEqual(len(hosts), 1)
        self.assertEqual(hosts[0]["hostname"], "localhost")

    def test_host_add(self):
        self._orch_cmd("host", "add", "hostname")

    def test_host_rm(self):
        self._orch_cmd("host", "rm", "hostname")

    def test_mon_update(self):
        self._orch_cmd("apply", "mon", "3 host1:1.2.3.0/24 host2:1.2.3.0/24 host3:10.0.0.0/8")
        self._orch_cmd("apply", "mon", "3 host1:1.2.3.4 host2:1.2.3.4 host3:10.0.0.1")

    def test_mgr_update(self):
        self._orch_cmd("apply", "mgr", "3")

    def test_nfs_update(self):
        self._orch_cmd("apply", "nfs", "service_name", "2")

    def test_error(self):
        ret = self._orch_cmd_result("host", "add", "raise_no_support")
        self.assertEqual(ret, errno.ENOENT)
        ret = self._orch_cmd_result("host", "add", "raise_bug")
        self.assertEqual(ret, errno.EINVAL)
        ret = self._orch_cmd_result("host", "add", "raise_not_implemented")
        self.assertEqual(ret, errno.ENOENT)
        ret = self._orch_cmd_result("host", "add", "raise_no_orchestrator")
        self.assertEqual(ret, errno.ENOENT)
        ret = self._orch_cmd_result("host", "add", "raise_import_error")
        self.assertEqual(ret, errno.ENOENT)

    def test_load_data(self):
        data = {
            'inventory': [
                {
                    'name': 'host0',
                    'devices': [
                        {
                            'type': 'hdd',
                            'id': '/dev/sda',
                            'size': 1024**4 * 4,
                            'rotates': True
                        }
                    ]
                },
                {
                    'name': 'host1',
                    'devices': [
                        {
                            'type': 'hdd',
                            'id': '/dev/sda',
                            'size': 1024**4 * 4,
                            'rotates': True
                        }
                    ]
                }
            ],
            'daemons': [
                {
                    'hostname': 'host0',
                    'daemon_type': 'mon',
                    'daemon_id': 'a'
                },
                {
                    'hostname': 'host1',
                    'daemon_type': 'osd',
                    'daemon_id': '1'
                }
            ]
        }

        ret = self._test_orchestrator_cmd_result('load_data', '-i', '-', stdin=json.dumps(data))
        self.assertEqual(ret, 0)
        out = self._orch_cmd('device', 'ls', '--format=json')
        inventory = data['inventory']
        inventory_result = json.loads(out)
        self.assertEqual(len(inventory), len(inventory_result))

        out = self._orch_cmd('device', 'ls', 'host0', '--format=json')
        inventory_result = json.loads(out)
        self.assertEqual(len(inventory_result), 1)
        self.assertEqual(inventory_result[0]['name'], 'host0')

        out = self._orch_cmd('ps', '--format=json')
        daemons = data['daemons']
        daemons_result = json.loads(out)
        self.assertEqual(len(daemons), len(daemons_result))

        out = self._orch_cmd('ps', 'host0', '--format=json')
        daemons_result = json.loads(out)
        self.assertEqual(len(daemons_result), 1)
        self.assertEqual(daemons_result[0]['hostname'], 'host0')

        # test invalid input file: invalid json
        json_str = '{ "inventory: '
        ret = self._test_orchestrator_cmd_result('load_data', '-i', '-', stdin=json_str)
        self.assertEqual(ret, errno.EINVAL)

        # test invalid input file: missing key
        json_str = '{ "inventory": [{"devices": []}] }'
        ret = self._test_orchestrator_cmd_result('load_data', '-i', '-', stdin=json_str)
        self.assertEqual(ret, errno.EINVAL)

        # load empty data for other tests
        ret = self._test_orchestrator_cmd_result('load_data', '-i', '-', stdin='{}')
        self.assertEqual(ret, 0)
