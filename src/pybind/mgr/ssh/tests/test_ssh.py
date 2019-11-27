import json
import time
from contextlib import contextmanager

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection

try:
    from typing import Any
except ImportError:
    pass

from orchestrator import ServiceDescription, raise_if_exception, Completion, InventoryNode, \
    StatelessServiceSpec, PlacementSpec, RGWSpec, parse_host_specs
from ..module import SSHOrchestrator
from tests import mock
from .fixtures import ssh_module


"""
TODOs:
    There is really room for improvement here. I just quickly assembled theses tests.
    I general, everything should be testes in Teuthology as well. Reasons for
    also testing this here is the development roundtrip time.
"""



def _run_ceph_daemon(ret):
    def foo(*args, **kwargs):
        return ret, 0
    return foo

def mon_command(*args, **kwargs):
    return 0, '', ''


class TestSSH(object):
    def _wait(self, m, c):
        # type: (SSHOrchestrator, Completion) -> Any
        m.process([c])
        m.process([c])

        for _ in range(30):
            if c.is_finished:
                raise_if_exception(c)
                return c.result
            time.sleep(0.1)
        assert False, "timeout" + str(c._state)

    @contextmanager
    def _with_host(self, m, name):
        self._wait(m, m.add_host(name))
        yield
        self._wait(m, m.remove_host(name))

    def test_get_unique_name(self, ssh_module):
        existing = [
            ServiceDescription(service_instance='mon.a')
        ]
        new_mon = ssh_module.get_unique_name(existing, 'mon')
        assert new_mon.startswith('mon.')
        assert new_mon != 'mon.a'

    def test_host(self, ssh_module):
        with self._with_host(ssh_module, 'test'):
            assert self._wait(ssh_module, ssh_module.get_hosts()) == [InventoryNode('test')]
        c = ssh_module.get_hosts()
        assert self._wait(ssh_module, c) == []

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('[]'))
    def test_service_ls(self, ssh_module):
        with self._with_host(ssh_module, 'test'):
            c = ssh_module.describe_service()
            assert self._wait(ssh_module, c) == []

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('[]'))
    def test_device_ls(self, ssh_module):
        with self._with_host(ssh_module, 'test'):
            c = ssh_module.get_inventory()
            assert self._wait(ssh_module, c) == [InventoryNode('test')]

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('[]'))
    @mock.patch("ssh.module.SSHOrchestrator.send_command")
    @mock.patch("ssh.module.SSHOrchestrator.mon_command", mon_command)
    @mock.patch("ssh.module.SSHOrchestrator._get_connection")
    def test_mon_update(self, _send_command, _get_connection, ssh_module):
        with self._with_host(ssh_module, 'test'):
            c = ssh_module.update_mons(1, [parse_host_specs('test:0.0.0.0')])
            assert self._wait(ssh_module, c) == ["(Re)deployed mon.test on host 'test'"]

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('[]'))
    @mock.patch("ssh.module.SSHOrchestrator.send_command")
    @mock.patch("ssh.module.SSHOrchestrator.mon_command", mon_command)
    @mock.patch("ssh.module.SSHOrchestrator._get_connection")
    def test_mgr_update(self, _send_command, _get_connection, ssh_module):
        with self._with_host(ssh_module, 'test'):
            c = ssh_module.update_mgrs(1, [parse_host_specs('test:0.0.0.0')])
            [out] = self._wait(ssh_module, c)
            assert "(Re)deployed mgr." in out
            assert " on host 'test'" in out

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('{}'))
    @mock.patch("ssh.module.SSHOrchestrator.send_command")
    @mock.patch("ssh.module.SSHOrchestrator.mon_command", mon_command)
    @mock.patch("ssh.module.SSHOrchestrator._get_connection")
    def test_create_osds(self, _send_command, _get_connection, ssh_module):
        with self._with_host(ssh_module, 'test'):
            dg = DriveGroupSpec('test', DeviceSelection(paths=['']))
            c = ssh_module.create_osds(dg)
            assert self._wait(ssh_module, c) == "Created osd(s) on host 'test'"

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('{}'))
    @mock.patch("ssh.module.SSHOrchestrator.send_command")
    @mock.patch("ssh.module.SSHOrchestrator.mon_command", mon_command)
    @mock.patch("ssh.module.SSHOrchestrator._get_connection")
    def test_mds(self, _send_command, _get_connection, ssh_module):
        with self._with_host(ssh_module, 'test'):
            ps = PlacementSpec(nodes=['test'])
            c = ssh_module.add_mds(StatelessServiceSpec('name', ps))
            [out] = self._wait(ssh_module, c)
            assert "(Re)deployed mds.name." in out
            assert " on host 'test'" in out

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('{}'))
    @mock.patch("ssh.module.SSHOrchestrator.send_command")
    @mock.patch("ssh.module.SSHOrchestrator.mon_command", mon_command)
    @mock.patch("ssh.module.SSHOrchestrator._get_connection")
    def test_rgw(self, _send_command, _get_connection, ssh_module):
        with self._with_host(ssh_module, 'test'):
            ps = PlacementSpec(nodes=['test'])
            c = ssh_module.add_rgw(RGWSpec('name', ps))
            [out] = self._wait(ssh_module, c)
            assert "(Re)deployed rgw.name." in out
            assert " on host 'test'" in out

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon(
        json.dumps([
            dict(
                name='rgw.myrgw.foobar',
                style='ceph-daemon',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    def test_remove_rgw(self, ssh_module):
        ssh_module._cluster_fsid = "fsid"
        with self._with_host(ssh_module, 'test'):
            c = ssh_module.remove_rgw('myrgw')
            out = self._wait(ssh_module, c)
            assert out == ["Removed rgw.myrgw.foobar from host 'test'"]

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('{}'))
    @mock.patch("ssh.module.SSHOrchestrator.send_command")
    @mock.patch("ssh.module.SSHOrchestrator.mon_command", mon_command)
    @mock.patch("ssh.module.SSHOrchestrator._get_connection")
    def test_rbd_mirror(self, _send_command, _get_connection, ssh_module):
        with self._with_host(ssh_module, 'test'):
            ps = PlacementSpec(nodes=['test'])
            c = ssh_module.add_rbd_mirror(StatelessServiceSpec('name', ps))
            [out] = self._wait(ssh_module, c)
            assert "(Re)deployed rbd-mirror." in out
            assert " on host 'test'" in out

    @mock.patch("ssh.module.SSHOrchestrator._run_ceph_daemon", _run_ceph_daemon('{}'))
    @mock.patch("ssh.module.SSHOrchestrator.send_command")
    @mock.patch("ssh.module.SSHOrchestrator.mon_command", mon_command)
    @mock.patch("ssh.module.SSHOrchestrator._get_connection")
    def test_blink_device_light(self, _send_command, _get_connection, ssh_module):
        with self._with_host(ssh_module, 'test'):
            c = ssh_module.blink_device_light('ident', True, [('test', '')])
            assert self._wait(ssh_module, c) == ['Set ident light for test: on']


