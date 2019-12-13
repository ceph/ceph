""" Test output wizards
"""
import unittest
from tests import mock

from ..ansible_runner_svc import EVENT_DATA_URL
from ..output_wizards import ProcessHostsList, ProcessPlaybookResult, \
                             ProcessInventory

class  OutputWizardProcessHostsList(unittest.TestCase):
    """Test ProcessHostsList Output Wizard
    """
    RESULT_OK = """
                {
                "status": "OK",
                "msg": "",
                "data": {
                    "hosts": [
                        "host_a",
                        "host_b",
                        "host_c"
                        ]
                    }
                }
                """
    ar_client = mock.Mock()
    test_wizard = ProcessHostsList(ar_client)

    def test_process(self):
        """Test a normal call"""

        nodes_list = self.test_wizard.process("", self.RESULT_OK)
        self.assertEqual([node.name for node in nodes_list],
                         ["host_a", "host_b", "host_c"])

    def test_errors(self):
        """Test different kind of errors processing result"""

         # Malformed json
        host_list = self.test_wizard.process("", """{"msg": """"")
        self.assertEqual(host_list, [])

        # key error
        host_list = self.test_wizard.process("", """{"msg": ""}""")
        self.assertEqual(host_list, [])

        # Hosts not in iterable
        host_list = self.test_wizard.process("", """{"data":{"hosts": 123} }""")
        self.assertEqual(host_list, [])

class OutputWizardProcessPlaybookResult(unittest.TestCase):
    """Test ProcessPlaybookResult Output Wizard
    """
    # Input to process
    INVENTORY_EVENTS = {1:"first event", 2:"second event"}
    EVENT_INFORMATION = "event information\n"

    # Mocked response
    mocked_response = mock.Mock()
    mocked_response.text = EVENT_INFORMATION

    # The Ansible Runner Service client
    ar_client = mock.Mock()
    ar_client.http_get = mock.MagicMock(return_value=mocked_response)

    test_wizard = ProcessPlaybookResult(ar_client)

    def test_process(self):
        """Test a normal call
        """

        operation_id = 24
        result = self.test_wizard.process(operation_id, self.INVENTORY_EVENTS)

        # Check http request are correct and compose expected result
        expected_result = ""
        for key, dummy_data in self.INVENTORY_EVENTS.items():
            http_request = EVENT_DATA_URL % (operation_id, key)
            self.ar_client.http_get.assert_any_call(http_request)
            expected_result += self.EVENT_INFORMATION

        #Check result
        self.assertEqual(result, expected_result)

class OutputWizardProcessInventory(unittest.TestCase):
    """Test ProcessInventory Output Wizard
    """
    # Input to process
    INVENTORY_EVENTS = {'event_uuid_1': {'host': '192.168.121.144',
                                         'task': 'list storage inventory',
                                         'event': 'runner_on_ok'}}
    EVENT_DATA = r"""
    {
    "status": "OK",
    "msg": "",
    "data": {
        "uuid": "5e96d509-174d-4f5f-bd94-e278c3a5b85b",
        "counter": 11,
        "stdout": "changed: [192.168.121.144]",
        "start_line": 17,
        "end_line": 18,
        "runner_ident": "6e98b2ba-3ce1-11e9-be81-2016b900e38f",
        "created": "2019-03-02T11:50:56.582112",
        "pid": 482,
        "event_data": {
            "play_pattern": "osds",
            "play": "query each host for storage device inventory",
            "task": "list storage inventory",
            "task_args": "_ansible_version=2.6.5, _ansible_selinux_special_fs=['fuse', 'nfs', 'vboxsf', 'ramfs', '9p'], _ansible_no_log=False, _ansible_module_name=ceph_volume, _ansible_debug=False, _ansible_verbosity=0, _ansible_keep_remote_files=False, _ansible_syslog_facility=LOG_USER, _ansible_socket=None, action=inventory, _ansible_diff=False, _ansible_remote_tmp=~/.ansible/tmp, _ansible_shell_executable=/bin/sh, _ansible_check_mode=False, _ansible_tmpdir=None",
            "remote_addr": "192.168.121.144",
            "res": {
                "_ansible_parsed": true,
                "stderr_lines": [],
                "changed": true,
                "end": "2019-03-02 11:50:56.554937",
                "_ansible_no_log": false,
                "stdout": "[{\"available\": true, \"rejected_reasons\": [], \"sys_api\": {\"scheduler_mode\": \"noop\", \"rotational\": \"1\", \"vendor\": \"ATA\", \"human_readable_size\": \"50.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {}, \"rev\": \"2.5+\", \"sas_address\": \"\", \"locked\": 0, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/sdc\", \"support_discard\": \"\", \"model\": \"QEMU HARDDISK\", \"ro\": \"0\", \"nr_requests\": \"128\", \"size\": 53687091200.0}, \"lvs\": [], \"path\": \"/dev/sdc\"}, {\"available\": false, \"rejected_reasons\": [\"locked\"], \"sys_api\": {\"scheduler_mode\": \"noop\", \"rotational\": \"1\", \"vendor\": \"ATA\", \"human_readable_size\": \"50.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {}, \"rev\": \"2.5+\", \"sas_address\": \"\", \"locked\": 1, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/sda\", \"support_discard\": \"\", \"model\": \"QEMU HARDDISK\", \"ro\": \"0\", \"nr_requests\": \"128\", \"size\": 53687091200.0}, \"lvs\": [{\"cluster_name\": \"ceph\", \"name\": \"osd-data-dcf8a88c-5546-42d2-afa4-b36f7fb23b66\", \"osd_id\": \"3\", \"cluster_fsid\": \"30d61f3e-7ee4-4bdc-8fe7-2ad5bb3f5317\", \"type\": \"block\", \"block_uuid\": \"fVqujC-9dgh-cN9W-1XD4-zVx1-1UdA-fUS3ha\", \"osd_fsid\": \"8b7cbeba-5e86-44ff-a5f3-2e7df77753fe\"}], \"path\": \"/dev/sda\"}, {\"available\": false, \"rejected_reasons\": [\"locked\"], \"sys_api\": {\"scheduler_mode\": \"noop\", \"rotational\": \"1\", \"vendor\": \"ATA\", \"human_readable_size\": \"50.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {}, \"rev\": \"2.5+\", \"sas_address\": \"\", \"locked\": 1, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/sdb\", \"support_discard\": \"\", \"model\": \"QEMU HARDDISK\", \"ro\": \"0\", \"nr_requests\": \"128\", \"size\": 53687091200.0}, \"lvs\": [{\"cluster_name\": \"ceph\", \"name\": \"osd-data-8c92e986-bd97-4b3d-ba77-2cb88e15d80f\", \"osd_id\": \"1\", \"cluster_fsid\": \"30d61f3e-7ee4-4bdc-8fe7-2ad5bb3f5317\", \"type\": \"block\", \"block_uuid\": \"mgzO7O-vUfu-H3mf-4R3K-2f97-ZMRH-SngBFP\", \"osd_fsid\": \"6d067688-3e1b-45f9-ad03-8abd19e9f117\"}], \"path\": \"/dev/sdb\"}, {\"available\": false, \"rejected_reasons\": [\"locked\"], \"sys_api\": {\"scheduler_mode\": \"mq-deadline\", \"rotational\": \"1\", \"vendor\": \"0x1af4\", \"human_readable_size\": \"41.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {\"vda1\": {\"start\": \"2048\", \"holders\": [], \"sectorsize\": 512, \"sectors\": \"2048\", \"size\": \"1024.00 KB\"}, \"vda3\": {\"start\": \"2101248\", \"holders\": [\"dm-0\", \"dm-1\"], \"sectorsize\": 512, \"sectors\": \"81784832\", \"size\": \"39.00 GB\"}, \"vda2\": {\"start\": \"4096\", \"holders\": [], \"sectorsize\": 512, \"sectors\": \"2097152\", \"size\": \"1024.00 MB\"}}, \"rev\": \"\", \"sas_address\": \"\", \"locked\": 1, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/vda\", \"support_discard\": \"\", \"model\": \"\", \"ro\": \"0\", \"nr_requests\": \"256\", \"size\": 44023414784.0}, \"lvs\": [{\"comment\": \"not used by ceph\", \"name\": \"LogVol00\"}, {\"comment\": \"not used by ceph\", \"name\": \"LogVol01\"}], \"path\": \"/dev/vda\"}]",
                "cmd": [
                    "ceph-volume",
                    "inventory",
                    "--format=json"
                ],
                "rc": 0,
                "start": "2019-03-02 11:50:55.150121",
                "stderr": "",
                "delta": "0:00:01.404816",
                "invocation": {
                    "module_args": {
                        "wal_vg": null,
                        "wal": null,
                        "dmcrypt": false,
                        "block_db_size": "-1",
                        "journal": null,
                        "objectstore": "bluestore",
                        "db": null,
                        "batch_devices": [],
                        "db_vg": null,
                        "journal_vg": null,
                        "cluster": "ceph",
                        "osds_per_device": 1,
                        "containerized": "False",
                        "crush_device_class": null,
                        "report": false,
                        "data_vg": null,
                        "data": null,
                        "action": "inventory",
                        "journal_size": "5120"
                    }
                },
                "stdout_lines": [
                    "[{\"available\": true, \"rejected_reasons\": [], \"sys_api\": {\"scheduler_mode\": \"noop\", \"rotational\": \"1\", \"vendor\": \"ATA\", \"human_readable_size\": \"50.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {}, \"rev\": \"2.5+\", \"sas_address\": \"\", \"locked\": 0, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/sdc\", \"support_discard\": \"\", \"model\": \"QEMU HARDDISK\", \"ro\": \"0\", \"nr_requests\": \"128\", \"size\": 53687091200.0}, \"lvs\": [], \"path\": \"/dev/sdc\"}, {\"available\": false, \"rejected_reasons\": [\"locked\"], \"sys_api\": {\"scheduler_mode\": \"noop\", \"rotational\": \"1\", \"vendor\": \"ATA\", \"human_readable_size\": \"50.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {}, \"rev\": \"2.5+\", \"sas_address\": \"\", \"locked\": 1, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/sda\", \"support_discard\": \"\", \"model\": \"QEMU HARDDISK\", \"ro\": \"0\", \"nr_requests\": \"128\", \"size\": 53687091200.0}, \"lvs\": [{\"cluster_name\": \"ceph\", \"name\": \"osd-data-dcf8a88c-5546-42d2-afa4-b36f7fb23b66\", \"osd_id\": \"3\", \"cluster_fsid\": \"30d61f3e-7ee4-4bdc-8fe7-2ad5bb3f5317\", \"type\": \"block\", \"block_uuid\": \"fVqujC-9dgh-cN9W-1XD4-zVx1-1UdA-fUS3ha\", \"osd_fsid\": \"8b7cbeba-5e86-44ff-a5f3-2e7df77753fe\"}], \"path\": \"/dev/sda\"}, {\"available\": false, \"rejected_reasons\": [\"locked\"], \"sys_api\": {\"scheduler_mode\": \"noop\", \"rotational\": \"1\", \"vendor\": \"ATA\", \"human_readable_size\": \"50.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {}, \"rev\": \"2.5+\", \"sas_address\": \"\", \"locked\": 1, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/sdb\", \"support_discard\": \"\", \"model\": \"QEMU HARDDISK\", \"ro\": \"0\", \"nr_requests\": \"128\", \"size\": 53687091200.0}, \"lvs\": [{\"cluster_name\": \"ceph\", \"name\": \"osd-data-8c92e986-bd97-4b3d-ba77-2cb88e15d80f\", \"osd_id\": \"1\", \"cluster_fsid\": \"30d61f3e-7ee4-4bdc-8fe7-2ad5bb3f5317\", \"type\": \"block\", \"block_uuid\": \"mgzO7O-vUfu-H3mf-4R3K-2f97-ZMRH-SngBFP\", \"osd_fsid\": \"6d067688-3e1b-45f9-ad03-8abd19e9f117\"}], \"path\": \"/dev/sdb\"}, {\"available\": false, \"rejected_reasons\": [\"locked\"], \"sys_api\": {\"scheduler_mode\": \"mq-deadline\", \"rotational\": \"1\", \"vendor\": \"0x1af4\", \"human_readable_size\": \"41.00 GB\", \"sectors\": 0, \"sas_device_handle\": \"\", \"partitions\": {\"vda1\": {\"start\": \"2048\", \"holders\": [], \"sectorsize\": 512, \"sectors\": \"2048\", \"size\": \"1024.00 KB\"}, \"vda3\": {\"start\": \"2101248\", \"holders\": [\"dm-0\", \"dm-1\"], \"sectorsize\": 512, \"sectors\": \"81784832\", \"size\": \"39.00 GB\"}, \"vda2\": {\"start\": \"4096\", \"holders\": [], \"sectorsize\": 512, \"sectors\": \"2097152\", \"size\": \"1024.00 MB\"}}, \"rev\": \"\", \"sas_address\": \"\", \"locked\": 1, \"sectorsize\": \"512\", \"removable\": \"0\", \"path\": \"/dev/vda\", \"support_discard\": \"\", \"model\": \"\", \"ro\": \"0\", \"nr_requests\": \"256\", \"size\": 44023414784.0}, \"lvs\": [{\"comment\": \"not used by ceph\", \"name\": \"LogVol00\"}, {\"comment\": \"not used by ceph\", \"name\": \"LogVol01\"}], \"path\": \"/dev/vda\"}]"
                ]
            },
            "pid": 482,
            "play_uuid": "2016b900-e38f-0e09-19be-00000000000c",
            "task_uuid": "2016b900-e38f-0e09-19be-000000000012",
            "event_loop": null,
            "playbook_uuid": "e80e66f2-4a78-4a96-aaf6-fbe473f11312",
            "playbook": "storage-inventory.yml",
            "task_action": "ceph_volume",
            "host": "192.168.121.144",
            "task_path": "/usr/share/ansible-runner-service/project/storage-inventory.yml:29"
        },
        "event": "runner_on_ok"
    }
    }
    """

    # Mocked response
    mocked_response = mock.Mock()
    mocked_response.text = EVENT_DATA

    # The Ansible Runner Service client
    ar_client = mock.Mock()
    ar_client.http_get = mock.MagicMock(return_value=mocked_response)

    test_wizard = ProcessInventory(ar_client)

    def test_process(self):
        """Test a normal call
        """
        operation_id = 12
        nodes_list = self.test_wizard.process(operation_id, self.INVENTORY_EVENTS)

        for key, dummy_data in self.INVENTORY_EVENTS.items():
            http_request = EVENT_DATA_URL % (operation_id, key)
            self.ar_client.http_get.assert_any_call(http_request)


        # Only one host
        self.assertTrue(len(nodes_list), 1)

        # Host retrieved OK
        self.assertEqual(nodes_list[0].name, "192.168.121.144")

        # Devices
        self.assertTrue(len(nodes_list[0].devices.devices), 4)

        expected_device_ids = ["/dev/sdc", "/dev/sda", "/dev/sdb", "/dev/vda"]
        device_ids = [dev.path for dev in nodes_list[0].devices.devices]

        self.assertEqual(expected_device_ids, device_ids)
