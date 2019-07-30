from __future__ import absolute_import
import pytest


from orchestrator import InventoryDevice, ReadCompletion, raise_if_exception

def test_inventory_device():
    i_d = InventoryDevice()
    s = i_d.pretty_print()
    assert len(s)


def test_raise():
    c = ReadCompletion()
    c.exception = ZeroDivisionError()
    with pytest.raises(ZeroDivisionError):
        raise_if_exception(c)
