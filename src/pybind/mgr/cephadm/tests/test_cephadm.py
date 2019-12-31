import json
import time
from contextlib import contextmanager

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection

try:
    from typing import Any
except ImportError:
    pass

from orchestrator import ServiceDescription, raise_if_exception, Completion, InventoryNode, \
    StatelessServiceSpec, PlacementSpec, RGWSpec, parse_host_specs, StatefulServiceSpec
from ..module import CephadmOrchestrator
from tests import mock
from .fixtures import cephadm_module


"""
TODOs:
    There is really room for improvement here. I just quickly assembled theses tests.
    I general, everything should be testes in Teuthology as well. Reasons for
    also testing this here is the development roundtrip time.
"""



def _run_cephadm(ret):
    def foo(*args, **kwargs):
        return ret, '', 0
    return foo

def mon_command(*args, **kwargs):
    return 0, '', ''


class TestCephadm(object):
    def _wait(self, m, c):
        # type: (CephadmOrchestrator, Completion) -> Any
        m.process([c])

        try:
            import pydevd  # if in debugger
            while True:    # don't timeout
                if c.is_finished:
                    raise_if_exception(c)
                    return c.result
                time.sleep(0.1)
        except ImportError:  # not in debugger
            for i in range(30):
                if i % 10 == 0:
                    m.process([c])
                if c.is_finished:
                    raise_if_exception(c)
                    return c.result
                time.sleep(0.1)
        assert False, "timeout" + str(c._state)

        m.process([c])
        assert False, "timeout" + str(c._state)

    @contextmanager
    def _with_host(self, m, name):
        self._wait(m, m.add_host(name))
        yield
        self._wait(m, m.remove_host(name))

    def test_get_unique_name(self, cephadm_module):
        existing = [
            ServiceDescription(service_instance='mon.a')
        ]
        new_mon = cephadm_module.get_unique_name(existing, 'mon')
        assert new_mon.startswith('mon.')
        assert new_mon != 'mon.a'

    def test_host(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            assert self._wait(cephadm_module, cephadm_module.get_hosts()) == [InventoryNode('test')]
        c = cephadm_module.get_hosts()
        assert self._wait(cephadm_module, c) == []

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    def test_service_ls(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.describe_service()
            assert self._wait(cephadm_module, c) == []

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    def test_device_ls(self, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.get_inventory()
            assert self._wait(cephadm_module, c) == [InventoryNode('test')]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='rgw.myrgw.foobar',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_service_action(self, _send_command, _get_connection, cephadm_module):
        cephadm_module._cluster_fsid = "fsid"
        cephadm_module.service_cache_timeout = 10
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.service_action('redeploy', 'rgw', service_id='myrgw.foobar')
            assert self._wait(cephadm_module, c) == ["Deployed rgw.myrgw.foobar on host 'test'"]

            for what in ('start', 'stop', 'restart'):
                c = cephadm_module.service_action(what, 'rgw', service_id='myrgw.foobar')
                assert self._wait(cephadm_module, c) == [what + " rgw.myrgw.foobar from host 'test'"]


    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_mon_update(self, _send_command, _get_connection, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            c = cephadm_module.update_mons(StatefulServiceSpec(placement=ps))
            assert self._wait(cephadm_module, c) == ["Deployed mon.a on host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('[]'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_mgr_update(self, _send_command, _get_connection, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test:0.0.0.0=a'], count=1)
            c = cephadm_module.update_mgrs(StatefulServiceSpec(placement=ps))
            [out] = self._wait(cephadm_module, c)
            assert "Deployed mgr." in out
            assert " on host 'test'" in out

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_create_osds(self, _send_command, _get_connection, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            dg = DriveGroupSpec('test', DeviceSelection(paths=['']))
            c = cephadm_module.create_osds(dg)
            assert self._wait(cephadm_module, c) == "Created osd(s) on host 'test'"

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='osd.0',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    def test_remove_osds(self, cephadm_module):
        cephadm_module._cluster_fsid = "fsid"
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.remove_osds(['0'])
            out = self._wait(cephadm_module, c)
            assert out == ["Removed osd.0 from host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_mds(self, _send_command, _get_connection, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_mds(StatelessServiceSpec('name', placement=ps))
            [out] = self._wait(cephadm_module, c)
            assert "Deployed mds.name." in out
            assert " on host 'test'" in out

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_rgw(self, _send_command, _get_connection, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_rgw(RGWSpec('realm', 'zone', placement=ps))
            [out] = self._wait(cephadm_module, c)
            assert "Deployed rgw.realm.zone." in out
            assert " on host 'test'" in out

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm(
        json.dumps([
            dict(
                name='rgw.myrgw.foobar',
                style='cephadm',
                fsid='fsid',
                container_id='container_id',
                version='version',
                state='running',
            )
        ])
    ))
    def test_remove_rgw(self, cephadm_module):
        cephadm_module._cluster_fsid = "fsid"
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.remove_rgw('myrgw')
            out = self._wait(cephadm_module, c)
            assert out == ["Removed rgw.myrgw.foobar from host 'test'"]

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_rbd_mirror(self, _send_command, _get_connection, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            ps = PlacementSpec(hosts=['test'], count=1)
            c = cephadm_module.add_rbd_mirror(StatelessServiceSpec(name='name', placement=ps))
            [out] = self._wait(cephadm_module, c)
            assert "Deployed rbd-mirror." in out
            assert " on host 'test'" in out

    @mock.patch("cephadm.module.CephadmOrchestrator._run_cephadm", _run_cephadm('{}'))
    @mock.patch("cephadm.module.CephadmOrchestrator.send_command")
    @mock.patch("cephadm.module.CephadmOrchestrator.mon_command", mon_command)
    @mock.patch("cephadm.module.CephadmOrchestrator._get_connection")
    def test_blink_device_light(self, _send_command, _get_connection, cephadm_module):
        with self._with_host(cephadm_module, 'test'):
            c = cephadm_module.blink_device_light('ident', True, [('test', '', '')])
            assert self._wait(cephadm_module, c) == ['Set ident light for test: on']

