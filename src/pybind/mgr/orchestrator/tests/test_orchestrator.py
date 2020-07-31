from __future__ import absolute_import

import datetime
import json

import pytest
import yaml

from ceph.deployment.service_spec import ServiceSpec
from ceph.deployment import inventory

from test_orchestrator import TestOrchestrator as _TestOrchestrator
from tests import mock

from orchestrator import raise_if_exception, Completion, ProgressReference
from orchestrator import InventoryHost, DaemonDescription, ServiceDescription
from orchestrator import OrchestratorValidationError
from orchestrator.module import to_format


def _test_resource(data, resource_class, extra=None):
    # ensure we can deserialize and serialize
    rsc = resource_class.from_json(data)
    rsc.to_json()

    if extra:
        # if there is an unexpected data provided
        data.update(extra)
        with pytest.raises(OrchestratorValidationError):
            resource_class.from_json(data)


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
        'daemon_id': 'a'
    }
    _test_resource(json_data, DaemonDescription, {'abc': False})


def test_raise():
    c = Completion()
    c._exception = ZeroDivisionError()
    with pytest.raises(ZeroDivisionError):
        raise_if_exception(c)


def test_promise():
    p = Completion(value=3)
    p.finalize()
    assert p.result == 3


def test_promise_then():
    p = Completion(value=3).then(lambda three: three + 1)
    p.finalize()
    assert p.result == 4


def test_promise_mondatic_then():
    p = Completion(value=3)
    p.then(lambda three: Completion(value=three + 1))
    p.finalize()
    assert p.result == 4


def some_complex_completion():
    c = Completion(value=3).then(
        lambda three: Completion(value=three + 1).then(
            lambda four: four + 1))
    return c

def test_promise_mondatic_then_combined():
    p = some_complex_completion()
    p.finalize()
    assert p.result == 5


def test_promise_flat():
    p = Completion()
    p.then(lambda r1: Completion(value=r1 + ' there').then(
        lambda r11: r11 + '!'))
    p.finalize('hello')
    assert p.result == 'hello there!'


def test_side_effect():
    foo = {'x': 1}

    def run(x):
        foo['x'] = x

    foo['x'] = 1
    Completion(value=3).then(run).finalize()
    assert foo['x'] == 3


def test_progress():
    c = some_complex_completion()
    mgr = mock.MagicMock()
    mgr.process = lambda cs: [c.finalize(None) for c in cs]

    progress_val = 0.75
    c._last_promise().then(
        on_complete=ProgressReference(message='hello world',
                                      mgr=mgr,
                                      completion=lambda: Completion(
                                          on_complete=lambda _: progress_val))
    )
    mgr.remote.assert_called_with('progress', 'update', c.progress_reference.progress_id, 'hello world', 0.0, [('origin', 'orchestrator')])

    c.finalize()
    mgr.remote.assert_called_with('progress', 'complete', c.progress_reference.progress_id)

    c.progress_reference.update()
    mgr.remote.assert_called_with('progress', 'update', c.progress_reference.progress_id, 'hello world', progress_val, [('origin', 'orchestrator')])
    assert not c.progress_reference.effective

    progress_val = 1
    c.progress_reference.update()
    assert c.progress_reference.effective
    mgr.remote.assert_called_with('progress', 'complete', c.progress_reference.progress_id)


def test_with_progress():
    mgr = mock.MagicMock()
    mgr.process = lambda cs: [c.finalize(None) for c in cs]

    def execute(y):
        return str(y)

    def run(x):
        def two(_):
            return execute(x * 2)

        return Completion.with_progress(
            message='message',
            on_complete=two,
            mgr=mgr

        )
    c = Completion(on_complete=lambda x: x * 10).then(run)._first_promise
    c.finalize(2)
    assert c.result == '40'
    c.progress_reference.update()
    assert c.progress_reference.effective


def test_exception():

    def run(x):
        raise KeyError(x)

    c = Completion(value=3).then(run)
    c.finalize()
    with pytest.raises(KeyError):
        raise_if_exception(c)


def test_fail():
    c = Completion().then(lambda _: 3)
    c._first_promise.fail(KeyError())
    assert isinstance(c.exception, KeyError)

    with pytest.raises(ValueError,
                  match='Invalid State: called fail, but Completion is already finished: {}'.format(
                      str(ZeroDivisionError()))):
        c._first_promise.fail(ZeroDivisionError())


def test_pretty_print():
    mgr = mock.MagicMock()
    mgr.process = lambda cs: [c.finalize(None) for c in cs]

    def add_one(x):
        return x+1

    c = Completion(value=1, on_complete=add_one).then(
        str
    ).add_progress('message', mgr)

    assert c.pretty_print() == """<Completion>[
       add_one(1),
       str(...),
       ProgressReference(...),
]"""
    c.finalize()
    assert c.pretty_print() == """<Completion>[
(done) add_one(1),
(done) str(2),
(done) ProgressReference('2'),
]"""

    p = some_complex_completion()
    assert p.pretty_print() == """<Completion>[
       <lambda>(3),
       lambda x: x(...),
]"""
    p.finalize()
    assert p.pretty_print() == """<Completion>[
(done) <lambda>(3),
(done) <lambda>(4),
(done) lambda x: x(5),
(done) lambda x: x(5),
]"""

    assert p.result == 5

def test_apply():
    to = _TestOrchestrator('', 0, 0)
    completion = to.apply([
        ServiceSpec(service_type='nfs'),
        ServiceSpec(service_type='nfs'),
        ServiceSpec(service_type='nfs'),
    ])
    completion.finalize(42)
    assert  completion.result == [None, None, None]


def test_yaml():
    y = """daemon_type: crash
daemon_id: ubuntu
hostname: ubuntu
status: 1
status_desc: starting
events:
- 2020-06-10T10:08:22.933241 daemon:crash.ubuntu [INFO] "Deployed crash.ubuntu on
  host 'ubuntu'"
---
service_type: crash
service_name: crash
placement:
  host_pattern: '*'
status:
  container_image_id: 74803e884bea289d2d2d3ebdf6d37cd560499e955595695b1390a89800f4e37a
  container_image_name: docker.io/ceph/daemon-base:latest-master-devel
  created: '2020-06-10T10:37:31.051288'
  last_refresh: '2020-06-10T10:57:40.715637'
  running: 1
  size: 1
events:
- 2020-06-10T10:37:31.139159 service:crash [INFO] "service was created"
"""
    types = (DaemonDescription, ServiceDescription)

    for y, cls in zip(y.split('---\n'), types):
        data = yaml.safe_load(y)
        object = cls.from_json(data)

        assert to_format(object, 'yaml', False, cls) == y
        assert to_format([object], 'yaml', True, cls) == y

        j = json.loads(to_format(object, 'json', False, cls))
        assert to_format(cls.from_json(j), 'yaml', False, cls) == y


def test_event_multiline():
    from .._interface import OrchestratorEvent
    e = OrchestratorEvent(datetime.datetime.utcnow(), 'service', 'subject', 'ERROR', 'message')
    assert OrchestratorEvent.from_json(e.to_json()) == e

    e = OrchestratorEvent(datetime.datetime.utcnow(), 'service', 'subject', 'ERROR', 'multiline\nmessage')
    assert OrchestratorEvent.from_json(e.to_json()) == e
