from __future__ import absolute_import

import json

import pytest


from orchestrator import InventoryDevice, ReadCompletion, raise_if_exception, RGWSpec

def test_inventory_device():
    i_d = InventoryDevice()
    s = i_d.pretty_print()
    assert len(s)


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
