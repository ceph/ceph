from __future__ import absolute_import
import json
from unittest.mock import MagicMock

import pytest

from ceph.deployment import inventory
from orchestrator import raise_if_exception, RGWSpec, Completion, ProgressReference
from orchestrator import InventoryNode, ServiceDescription
from orchestrator import OrchestratorValidationError
from orchestrator import parse_host_specs


@pytest.mark.parametrize("test_input,expected, require_network",
                         [("myhost", ('myhost', '', ''), False),
                          ("myhost=sname", ('myhost', '', 'sname'), False),
                          ("myhost:10.1.1.10", ('myhost', '10.1.1.10', ''), True),
                          ("myhost:10.1.1.10=sname", ('myhost', '10.1.1.10', 'sname'), True),
                          ("myhost:10.1.1.0/32", ('myhost', '10.1.1.0/32', ''), True),
                          ("myhost:10.1.1.0/32=sname", ('myhost', '10.1.1.0/32', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789]", ('myhost', '[v1:10.1.1.10:6789]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789]=sname", ('myhost', '[v1:10.1.1.10:6789]', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]=sname", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', 'sname'), True),
                          ])
def test_parse_host_specs(test_input, expected, require_network):
    ret = parse_host_specs(test_input, require_network=require_network)
    assert ret == expected

@pytest.mark.parametrize("test_input",
                         # wrong subnet
                         [("myhost:1.1.1.1/24"),
                          # wrong ip format
                          ("myhost:1"),
                          # empty string
                          ("myhost=1"),
                          ])
def test_parse_host_specs_raises(test_input):
    with pytest.raises(ValueError):
        ret = parse_host_specs(test_input)


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
    _test_resource(json_data, InventoryNode, {'abc': False})
    for devices in json_data['devices']:
        _test_resource(devices, inventory.Device)

    json_data = [{}, {'name': 'host0'}, {'devices': []}]
    for data in json_data:
        with pytest.raises(OrchestratorValidationError):
            InventoryNode.from_json(data)


def test_service_description():
    json_data = {
        'nodename': 'test',
        'service_type': 'mon',
        'service_instance': 'a'
    }
    _test_resource(json_data, ServiceDescription, {'abc': False})


def test_raise():
    c = Completion()
    c._exception = ZeroDivisionError()
    with pytest.raises(ZeroDivisionError):
        raise_if_exception(c)


def test_rgwspec():
    """
    {
        "rgw_zone": "zonename",
        "rgw_frontend_port": 8080,
        "rgw_zonegroup": "group",
        "rgw_zone_user": "user",
        "rgw_realm": "realm",
        "count": 3
    }
    """
    example = json.loads(test_rgwspec.__doc__.strip())
    spec = RGWSpec.from_json(example)
    assert spec.validate_add() is None


def test_promise():
    p = Completion(value=3)
    p.finalize()
    assert p.result == 3


def test_promise_then():
    p = Completion(value=3).then(lambda three: three + 1)
    p._first_promise.finalize()
    assert p.result == 4


def test_promise_mondatic_then():
    p = Completion(value=3)
    p.then(lambda three: Completion(value=three + 1))
    p._first_promise.finalize()
    assert p.result == 4


def some_complex_completion():
    c = Completion(value=3).then(
        lambda three: Completion(value=three + 1).then(
            lambda four: four + 1))
    return c._first_promise

def test_promise_mondatic_then_combined():
    p = some_complex_completion()
    p._first_promise.finalize()
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
    Completion(value=3).then(run)._first_promise.finalize()
    assert foo['x'] == 3


def test_progress():
    c = some_complex_completion()
    mgr = MagicMock()
    mgr.process = lambda cs: [c.finalize(None) for c in cs]

    progress_val = 0.75
    c._last_promise().then(
        on_complete=ProgressReference(message='hello world',
                                      mgr=mgr,
                                      completion=lambda: Completion(
                                          on_complete=lambda _: progress_val))
    )
    mgr.remote.assert_called_with('progress', 'update', c.progress_reference.progress_id, 'hello world', 0.0, ['orchestrator'])

    c.finalize()
    mgr.remote.assert_called_with('progress', 'update', c.progress_reference.progress_id, 'hello world', 0.5, ['orchestrator'])

    c.progress_reference.update()
    mgr.remote.assert_called_with('progress', 'update', c.progress_reference.progress_id, 'hello world', progress_val, ['orchestrator'])
    assert not c.progress_reference.effective

    progress_val = 1
    c.progress_reference.update()
    assert c.progress_reference.effective
    mgr.remote.assert_called_with('progress', 'complete', c.progress_reference.progress_id)


def test_with_progress():
    mgr = MagicMock()
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

    c = Completion(value=3).then(run)._first_promise
    c.finalize()

    assert isinstance(c.exception, KeyError)


def test_fail():
    c = Completion().then(lambda _: 3)
    c._first_promise.fail(KeyError())
    assert isinstance(c.exception, KeyError)
