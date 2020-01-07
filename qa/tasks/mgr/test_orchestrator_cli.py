import errno
import json
import logging
from time import sleep

from teuthology.exceptions import CommandFailedError

from mgr_test_case import MgrTestCase


log = logging.getLogger(__name__)


class TestOrchestratorCli(MgrTestCase):
    MGRS_REQUIRED = 1

    def _cmd(self, module, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(module, *args)

    def _orch_cmd(self, *args):
        return self._cmd("orchestrator", *args)

    def _progress_cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd("progress", *args)

    def _orch_cmd_result(self, *args, **kwargs):
        """
        raw_cluster_cmd doesn't support kwargs.
        """
        return self.mgr_cluster.mon_manager.raw_cluster_cmd_result("orchestrator", *args, **kwargs)

    def _test_orchestrator_cmd_result(self, *args, **kwargs):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd_result("test_orchestrator", *args, **kwargs)

    def setUp(self):
        super(TestOrchestratorCli, self).setUp()

        self._load_module("orchestrator_cli")
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

    def test_service_ls(self):
        ret = self._orch_cmd("service", "ls")
        self.assertIn("ceph-mgr", ret)

    def test_service_ls_json(self):
        ret = self._orch_cmd("service", "ls", "--format", "json")
        self.assertIsInstance(json.loads(ret), list)
        self.assertIn("ceph-mgr", ret)


    def test_service_action(self):
        self._orch_cmd("service", "restart", "mds", "cephfs")
        self._orch_cmd("service", "stop", "mds", "cephfs")
        self._orch_cmd("service", "start", "mds", "cephfs")

    def test_service_instance_action(self):
        self._orch_cmd("service-instance", "restart", "mds", "a")
        self._orch_cmd("service-instance", "stop", "mds", "a")
        self._orch_cmd("service-instance", "start", "mds", "a")

    def test_osd_create(self):
        self._orch_cmd("osd", "create", "*:device")
        self._orch_cmd("osd", "create", "*:device,device2")

        drive_group = {
            "host_pattern": "*",
            "data_devices": {"paths": ["/dev/sda"]}
        }

        res = self._orch_cmd_result("osd", "create", "-i", "-", stdin=json.dumps(drive_group))
        self.assertEqual(res, 0)

        with self.assertRaises(CommandFailedError):
            self._orch_cmd("osd", "create", "notfound:device")

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
        self._orch_cmd("mds", "add", "service_name")

    def test_rgw_add(self):
        self._orch_cmd("rgw", "add", "myrealm", "myzone")

    def test_nfs_add(self):
        self._orch_cmd("nfs", "add", "service_name", "pool", "--namespace", "ns")
        self._orch_cmd("nfs", "add", "service_name", "pool")

    def test_osd_rm(self):
        self._orch_cmd("osd", "rm", "osd.0")

    def test_mds_rm(self):
        self._orch_cmd("mds", "rm", "foo")

    def test_rgw_rm(self):
        self._orch_cmd("rgw", "rm", "myrealm", "myzone")

    def test_nfs_rm(self):
        self._orch_cmd("nfs", "rm", "service_name")

    def test_host_ls(self):
        out = self._orch_cmd("host", "ls", "--format=json")
        hosts = json.loads(out)
        self.assertEqual(len(hosts), 1)
        self.assertEqual(hosts[0]["host"], "localhost")

    def test_host_add(self):
        self._orch_cmd("host", "add", "hostname")

    def test_host_rm(self):
        self._orch_cmd("host", "rm", "hostname")

    def test_mon_update(self):
        self._orch_cmd("mon", "update", "3", "host1:1.2.3.0/24", "host2:1.2.3.0/24", "host3:10.0.0.0/8")
        self._orch_cmd("mon", "update", "3", "host1:1.2.3.4", "host2:1.2.3.4", "host3:10.0.0.1")

    def test_mgr_update(self):
        self._orch_cmd("mgr", "update", "3")

    def test_nfs_update(self):
        self._orch_cmd("nfs", "update", "service_name", "2")

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

    def test_progress(self):
        self._progress_cmd('clear')
        evs = json.loads(self._progress_cmd('json'))['completed']
        self.assertEqual(len(evs), 0)
        self._orch_cmd("mgr", "update", "4")
        sleep(6)  # There is a sleep(5) in the test_orchestrator.module.serve()
        evs = json.loads(self._progress_cmd('json'))['completed']
        self.assertEqual(len(evs), 1)
        self.assertIn('update_mgrs', evs[0]['message'])

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
            'services': [
                {
                    'nodename': 'host0',
                    'service_type': 'mon',
                    'service_instance': 'a'
                },
                {
                    'nodename': 'host1',
                    'service_type': 'osd',
                    'service_instance': '1'
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

        out = self._orch_cmd('service', 'ls', '--format=json')
        services = data['services']
        services_result = json.loads(out)
        self.assertEqual(len(services), len(services_result))

        out = self._orch_cmd('service', 'ls', 'host0', '--format=json')
        services_result = json.loads(out)
        self.assertEqual(len(services_result), 1)
        self.assertEqual(services_result[0]['nodename'], 'host0')

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
