from __future__ import absolute_import
import json

import pytest

from orchestrator import ReadCompletion, raise_if_exception, RGWSpec
from orchestrator import InventoryNode, InventoryDevice, ServiceDescription
from orchestrator import OrchestratorValidationError


def _test_resource(data, resource_class, extra):
    # create the instance with normal way
    rsc = resource_class(**data)
    if hasattr(rsc, 'pretty_print'):
        assert rsc.pretty_print()

    # ensure we can deserialize and serialize
    rsc = resource_class.from_json(data)
    rsc.to_json()

    # if there is an unexpected data provided
    data.update(extra)
    with pytest.raises(OrchestratorValidationError):
        resource_class.from_json(data)


def test_inventory():
    json_data = {
        'name': 'host0',
        'devices': [
            {
                'type': 'hdd',
                'id': '/dev/sda',
                'size': 1024,
                'rotates': True
            }
        ]
    }
    _test_resource(json_data, InventoryNode, {'abc': False})
    for devices in json_data['devices']:
        _test_resource(devices, InventoryDevice, {'abc': False})

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
    c = ReadCompletion()
    c.exception = ZeroDivisionError()
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
