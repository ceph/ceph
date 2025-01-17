
import json
import textwrap

import pytest
import yaml

from ceph.deployment.hostspec import HostSpec
from ceph.deployment.inventory import Devices, Device
from ceph.deployment.service_spec import ServiceSpec
from ceph.deployment import inventory
from ceph.utils import datetime_now
from mgr_module import HandleCommandResult

from test_orchestrator import TestOrchestrator as _TestOrchestrator

from orchestrator import InventoryHost, DaemonDescription, ServiceDescription, DaemonDescriptionStatus, OrchResult
from orchestrator import OrchestratorValidationError
from orchestrator.module import to_format, Format, OrchestratorCli, preview_table_osd
from unittest import mock


def _test_resource(data, resource_class, extra=None):
    # ensure we can deserialize and serialize
    rsc = resource_class.from_json(data)
    assert rsc.to_json() == resource_class.from_json(rsc.to_json()).to_json()

    if extra:
        # if there is an unexpected data provided
        data_copy = data.copy()
        data_copy.update(extra)
        with pytest.raises(OrchestratorValidationError):
            resource_class.from_json(data_copy)


def test_inventory():
    json_data = {
        'name': 'host0',
        'addr': '1.2.3.4',
        'devices': [
            {
                'sys_api': {
                    'rotational': '1',
                    'size': 1024,
                },
                'path': '/dev/sda',
                'available': False,
                'rejected_reasons': [],
                'lvs': []
            }
        ]
    }
    _test_resource(json_data, InventoryHost, {'abc': False})
    for devices in json_data['devices']:
        _test_resource(devices, inventory.Device)

    json_data = [{}, {'name': 'host0', 'addr': '1.2.3.4'}, {'devices': []}]
    for data in json_data:
        with pytest.raises(OrchestratorValidationError):
            InventoryHost.from_json(data)


def test_daemon_description():
    json_data = {
        'hostname': 'test',
        'daemon_type': 'mon',
        'daemon_id': 'a',
        'status': -1,
    }
    _test_resource(json_data, DaemonDescription, {'abc': False})

    dd = DaemonDescription.from_json(json_data)
    assert dd.status.value == DaemonDescriptionStatus.error.value


def test_apply():
    to = _TestOrchestrator('', 0, 0)
    completion = to.apply([
        ServiceSpec(service_type='nfs', service_id='foo'),
        ServiceSpec(service_type='nfs', service_id='foo'),
        ServiceSpec(service_type='nfs', service_id='foo'),
    ])
    res = '<NFSServiceSpec for service_name=nfs.foo>'
    assert completion.result == [res, res, res]


def test_yaml():
    y = """daemon_type: crash
daemon_id: ubuntu
daemon_name: crash.ubuntu
hostname: ubuntu
status: 1
status_desc: starting
is_active: false
events:
- 2020-06-10T10:08:22.933241Z daemon:crash.ubuntu [INFO] "Deployed crash.ubuntu on
  host 'ubuntu'"
---
service_type: crash
service_name: crash
placement:
  host_pattern: '*'
status:
  container_image_id: 74803e884bea289d2d2d3ebdf6d37cd560499e955595695b1390a89800f4e37a
  container_image_name: quay.io/ceph/daemon-base:latest-main-devel
  created: '2020-06-10T10:37:31.051288Z'
  last_refresh: '2020-06-10T10:57:40.715637Z'
  running: 1
  size: 1
events:
- 2020-06-10T10:37:31.139159Z service:crash [INFO] "service was created"
"""
    types = (DaemonDescription, ServiceDescription)

    for y, cls in zip(y.split('---\n'), types):
        data = yaml.safe_load(y)
        object = cls.from_json(data)

        assert to_format(object, Format.yaml, False, cls) == y
        assert to_format([object], Format.yaml, True, cls) == y

        j = json.loads(to_format(object, Format.json, False, cls))
        assert to_format(cls.from_json(j), Format.yaml, False, cls) == y


def test_event_multiline():
    from .._interface import OrchestratorEvent
    e = OrchestratorEvent(datetime_now(), 'service', 'subject', 'ERROR', 'message')
    assert OrchestratorEvent.from_json(e.to_json()) == e

    e = OrchestratorEvent(datetime_now(), 'service',
                          'subject', 'ERROR', 'multiline\nmessage')
    assert OrchestratorEvent.from_json(e.to_json()) == e


def test_handle_command():
    cmd = {
        'prefix': 'orch daemon add',
        'daemon_type': 'mon',
        'placement': 'smithi044:[v2:172.21.15.44:3301,v1:172.21.15.44:6790]=c',
    }
    m = OrchestratorCli('orchestrator', 0, 0)
    r = m._handle_command(None, cmd)
    assert r == HandleCommandResult(
        retval=-2, stdout='', stderr='No orchestrator configured (try `ceph orch set backend`)')


r = OrchResult([ServiceDescription(spec=ServiceSpec(service_type='osd'), running=123)])


@mock.patch("orchestrator.OrchestratorCli.describe_service", return_value=r)
def test_orch_ls(_describe_service):
    cmd = {
        'prefix': 'orch ls',
    }
    m = OrchestratorCli('orchestrator', 0, 0)
    r = m._handle_command(None, cmd)
    out = 'NAME  PORTS  RUNNING  REFRESHED  AGE  PLACEMENT  \n' \
          'osd              123  -          -               '
    assert r == HandleCommandResult(retval=0, stdout=out, stderr='')

    cmd = {
        'prefix': 'orch ls',
        'format': 'yaml',
    }
    m = OrchestratorCli('orchestrator', 0, 0)
    r = m._handle_command(None, cmd)
    out = textwrap.dedent("""
        service_type: osd
        service_name: osd
        spec:
          filter_logic: AND
          objectstore: bluestore
        status:
          running: 123
          size: 0
        """).lstrip()
    assert r == HandleCommandResult(retval=0, stdout=out, stderr='')


dlist = OrchResult([DaemonDescription(daemon_type="osd", daemon_id="1"), DaemonDescription(
    daemon_type="osd", daemon_id="10"), DaemonDescription(daemon_type="osd", daemon_id="2")])


@mock.patch("orchestrator.OrchestratorCli.list_daemons", return_value=dlist)
def test_orch_ps(_describe_service):

    # Ensure natural sorting on daemon names (osd.1, osd.2, osd.10)
    cmd = {
        'prefix': 'orch ps'
    }
    m = OrchestratorCli('orchestrator', 0, 0)
    r = m._handle_command(None, cmd)
    expected_out = 'NAME    HOST       PORTS  STATUS   REFRESHED  AGE  MEM USE  MEM LIM  VERSION    IMAGE ID   \n'\
                   'osd.1   <unknown>         unknown          -    -        -        -  <unknown>  <unknown>  \n'\
                   'osd.2   <unknown>         unknown          -    -        -        -  <unknown>  <unknown>  \n'\
                   'osd.10  <unknown>         unknown          -    -        -        -  <unknown>  <unknown>  '
    expected_out = [c for c in expected_out if c.isalpha()]
    actual_out = [c for c in r.stdout if c.isalpha()]
    assert r.retval == 0
    assert expected_out == actual_out
    assert r.stderr == ''


hlist = OrchResult([HostSpec("ceph-node-1"), HostSpec("ceph-node-2"), HostSpec("ceph-node-10")])


@mock.patch("orchestrator.OrchestratorCli.get_hosts", return_value=hlist)
def test_orch_host_ls(_describe_service):

    # Ensure natural sorting on hostnames (ceph-node-1, ceph-node-2, ceph-node-10)
    cmd = {
        'prefix': 'orch host ls'
    }
    m = OrchestratorCli('orchestrator', 0, 0)
    r = m._handle_command(None, cmd)
    expected_out = 'HOST          ADDR          LABELS  STATUS  \n'\
                   'ceph-node-1   ceph-node-1                   \n'\
                   'ceph-node-2   ceph-node-2                   \n'\
                   'ceph-node-10  ceph-node-10                  \n'\
                   '3 hosts in cluster'
    expected_out = [c for c in expected_out if c.isalpha()]
    actual_out = [c for c in r.stdout if c.isalpha()]
    assert r.retval == 0
    assert expected_out == actual_out
    assert r.stderr == ''


def test_orch_device_ls():
    devices = Devices([Device("/dev/vdb", available=True)])
    ilist = OrchResult([InventoryHost("ceph-node-1", devices=devices), InventoryHost("ceph-node-2",
                       devices=devices), InventoryHost("ceph-node-10", devices=devices)])

    with mock.patch("orchestrator.OrchestratorCli.get_inventory", return_value=ilist):
        # Ensure natural sorting on hostnames (ceph-node-1, ceph-node-2, ceph-node-10)
        cmd = {
            'prefix': 'orch device ls'
        }
        m = OrchestratorCli('orchestrator', 0, 0)
        r = m._handle_command(None, cmd)
        expected_out = 'HOST          PATH      TYPE     DEVICE ID   SIZE  AVAILABLE  REFRESHED  REJECT REASONS  \n'\
                       'ceph-node-1   /dev/vdb  unknown  None          0   Yes        0s ago                     \n'\
                       'ceph-node-2   /dev/vdb  unknown  None          0   Yes        0s ago                     \n'\
                       'ceph-node-10  /dev/vdb  unknown  None          0   Yes        0s ago                     '
        expected_out = [c for c in expected_out if c.isalpha()]
        actual_out = [c for c in r.stdout if c.isalpha()]
        assert r.retval == 0
        assert expected_out == actual_out
        assert r.stderr == ''


def test_preview_table_osd_smoke():
    data = [
        {
            'service_type': 'osd',
            'data':
            {
                'foo host':
                [
                    {
                        'osdspec': 'foo',
                        'error': '',
                        'data':
                        [
                            {
                                "block_db": "/dev/nvme0n1",
                                "block_db_size": "66.67 GB",
                                "data": "/dev/sdb",
                                "data_size": "300.00 GB",
                                "encryption": "None"
                            },
                            {
                                "block_db": "/dev/nvme0n1",
                                "block_db_size": "66.67 GB",
                                "data": "/dev/sdc",
                                "data_size": "300.00 GB",
                                "encryption": "None"
                            },
                            {
                                "block_db": "/dev/nvme0n1",
                                "block_db_size": "66.67 GB",
                                "data": "/dev/sdd",
                                "data_size": "300.00 GB",
                                "encryption": "None"
                            }
                        ]
                    }
                ]
            }
        }
    ]
    preview_table_osd(data)
